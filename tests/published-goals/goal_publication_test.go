//go:build published_goals

package publishedgoals_test

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/remote"
	"github.com/colony-2/jobdb/pkg/jobdb/runtimetest"
	"github.com/colony-2/pgjobdb/pkg/pgjobdb"
	"github.com/colony-2/pgjobdb/pkg/pgjobdb/installer"
	pgruntime "github.com/colony-2/pgjobdb/pkg/pgjobdb/runtime"
	embeddedpostgres "github.com/fergusstrange/embedded-postgres"
	"github.com/stretchr/testify/require"
)

// Required PostgreSQL startup and SQL errors are failures, never skips.
func TestPublishedGoals(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := uint32(listener.Addr().(*net.TCPAddr).Port)
	require.NoError(t, listener.Close())
	directory := t.TempDir()
	postgres := embeddedpostgres.NewDatabase(embeddedpostgres.DefaultConfig().Port(port).RuntimePath(filepath.Join(directory, "runtime")).DataPath(filepath.Join(directory, "data")))
	require.NoError(t, postgres.Start())
	defer func() { require.NoError(t, postgres.Stop()) }()
	ctx := runtimetest.PrimaryGoalContext(context.Background())
	admin, err := sql.Open("postgres", fmt.Sprintf("host=localhost port=%d user=postgres password=postgres dbname=postgres sslmode=disable", port))
	require.NoError(t, err)
	defer admin.Close()
	var version string
	require.NoError(t, admin.QueryRowContext(ctx, `SELECT version()`).Scan(&version))
	t.Log(version)
	fresh := func(name string) *sql.DB {
		_, err := admin.ExecContext(ctx, "CREATE DATABASE "+name)
		require.NoError(t, err)
		db, err := sql.Open("postgres", fmt.Sprintf("host=localhost port=%d user=postgres password=postgres dbname=%s sslmode=disable", port, name))
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, db.Close()) })
		return db
	}
	transport := func(t *testing.T, rt *pgruntime.Runtime, name string) jobdb.ConsumerRuntime {
		if name == "direct" {
			return rt
		}
		var handler http.Handler = remote.NewConsumerServer(rt)
		if name == "signed" {
			handler, err = remote.NewConsumerServerWithOptions(rt, remote.ServerOptions{LeaseTokenSigningKey: []byte("published-acceptance-key-at-least-32-bytes"), LeaseTokenAudience: "published-goals"})
			require.NoError(t, err)
		}
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			handler.ServeHTTP(w, r.WithContext(runtimetest.PrimaryGoalContext(r.Context())))
		}))
		t.Cleanup(server.Close)
		client, err := remote.New(server.URL, server.Client())
		require.NoError(t, err)
		return client
	}
	for _, name := range []string{"direct", "default", "signed"} {
		t.Run(name, func(t *testing.T) {
			db := fresh("published_" + name)
			rt, err := pgruntime.NewSQLDB(ctx, db, pgruntime.Config{BlobStoreURI: "blobfs://" + t.TempDir()})
			require.NoError(t, err)
			defer rt.Close(ctx)
			client := transport(t, rt, name)
			t.Run("admission", func(t *testing.T) { runtimetest.RunGoalAdmissionConformance(t, client) })
			t.Run("protected", func(t *testing.T) { runtimetest.RunTerminalFailureConformance(t, client) })
			require.NoError(t, (installer.Installer{DB: db}).Verify(ctx))
		})
	}
	t.Run("migrated", func(t *testing.T) {
		db := fresh("published_migrated")
		historicalFormat11, err := os.ReadFile(os.Getenv("GOALS_HISTORY_FIXTURE"))
		require.NoError(t, err)
		_, err = db.ExecContext(ctx, string(historicalFormat11))
		require.NoError(t, err)
		_, err = pgjobdb.CreateCell(ctx, db, jobdb.CreateCellRequest{TenantID: "tenant", Name: "work", Repo: "example.test/work"})
		require.NoError(t, err)
		_, err = db.ExecContext(ctx, `SELECT pgjobdb.submit_native_job('tenant','historical-source','submitter','work','{}','{}',NULL,NULL,NULL,ARRAY[]::TEXT[],NULL,NULL,NULL,NULL,NULL,FALSE,'[]',jsonb_build_object('cellId',cell_id,'cell',name,'cellRepo',repo)) FROM pgjobdb.cells WHERE tenant_id='tenant' AND name='work'`)
		require.NoError(t, err)
		var goal string
		require.NoError(t, db.QueryRowContext(ctx, `SELECT goal_id FROM pgjobdb.job_facts WHERE job_id='historical-source'`).Scan(&goal))
		_, err = db.ExecContext(ctx, `SELECT pgjobdb.fail_goal('tenant',$1,E' original\n保存 failure ')`, goal)
		require.NoError(t, err)
		var report bytes.Buffer
		cfg := pgruntime.Config{BlobStoreURI: "blobfs://" + t.TempDir(), ConversionReportWriter: &report}
		rt, err := pgruntime.NewSQLDB(ctx, db, cfg)
		require.NoError(t, err)
		require.Contains(t, report.String(), `"sourceFormat": 11`)
		require.NoError(t, rt.Close(ctx))
		rt, err = pgruntime.NewSQLDB(ctx, db, cfg)
		require.NoError(t, err)
		defer rt.Close(ctx)
		for _, name := range []string{"direct", "default", "signed"} {
			t.Run(name, func(t *testing.T) {
				client := transport(t, rt, name)
				outcome, err := client.GetGoal(ctx, jobdb.GoalKey{TenantId: "tenant", GoalID: goal})
				require.NoError(t, err)
				key := jobdb.JobKey{TenantId: "tenant", JobId: outcome.FailureJobID}
				require.NotEmpty(t, key.JobId)
				job, err := client.GetJob(ctx, key)
				require.NoError(t, err)
				require.Equal(t, jobdb.JobRecordMigratedGoalFailure, job.RecordKind)
				require.Equal(t, " original\n保存 failure ", job.TerminalFailure.Message)
				require.Empty(t, job.OtherGoalIDs)
				require.Nil(t, job.NextRoute)
				run, err := jobdb.GetJobRun(ctx, client, jobdb.GetJobRunRequest{JobKey: key, IncludeArtifacts: true, IncludeAttemptInputs: true, IncludeInputs: true, IncludeOutputs: true})
				require.NoError(t, err)
				require.Empty(t, run.Execution)
				require.Empty(t, run.Attempts)
				require.Zero(t, run.Start)
				data, err := job.Data.GetData()
				require.Nil(t, data)
				require.ErrorIs(t, err, jobdb.ErrJobFailed)
				require.ErrorIs(t, err, jobdb.ErrGoalFailed)
				chapters, err := client.ListChapters(ctx, jobdb.ListChaptersRequest{JobKey: key})
				require.NoError(t, err)
				require.Empty(t, chapters)
				_, err = client.SubmitRestartJob(ctx, jobdb.SubmitRestartJobRequest{Job: jobdb.SubmitRestartJob{PriorJobKey: key, Cell: "work", CellRepo: "example.test/work"}})
				require.ErrorIs(t, err, jobdb.ErrJobRecordOperationUnsupported)
			})
		}
		require.NoError(t, (installer.Installer{DB: db}).Verify(ctx))
	})
}

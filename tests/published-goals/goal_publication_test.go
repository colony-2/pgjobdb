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
	"github.com/colony-2/jobdb/pkg/jobdb/projectcatalog"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/remote"
	"github.com/colony-2/jobdb/pkg/jobdb/runtimetest"
	"github.com/colony-2/pgjobdb/pkg/pgjobdb/installer"
	"github.com/colony-2/pgjobdb/pkg/pgjobdb/projects"
	pgruntime "github.com/colony-2/pgjobdb/pkg/pgjobdb/runtime"
	embeddedpostgres "github.com/fergusstrange/embedded-postgres"
	"github.com/stretchr/testify/require"
	"io"
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
	ctx := jobdb.WithProjectAdministration(runtimetest.PrimaryGoalContext(context.Background()), func(context.Context) error { return nil })
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
		view := NewProjectBackend(rt, "")
		if name == "direct" {
			return view
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
		return NewProjectClient(client, rt, "")
	}
	for _, name := range []string{"direct", "default", "signed"} {
		t.Run(name, func(t *testing.T) {
			db := fresh("published_" + name)
			_, err := projects.Initialize(ctx, db, participants, io.Discard)
			require.NoError(t, err)
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
		_, err = db.ExecContext(ctx, `INSERT INTO pgjobdb.cells(tenant_id,cell_id,name,repo,state,description,metadata,revision,created_at_ns,updated_at_ns) VALUES('c2','historical-cell','work','example.test/work','ACTIVE','','{}','historical-revision',0,0); INSERT INTO pgjobdb.cell_names VALUES('c2','work','historical-cell')`)
		require.NoError(t, err)
		_, err = db.ExecContext(ctx, `SELECT pgjobdb.submit_native_job('c2','historical-source','submitter','work','{}','{}',NULL,NULL,NULL,ARRAY[]::TEXT[],NULL,NULL,NULL,NULL,NULL,FALSE,'[]',jsonb_build_object('cellId',cell_id,'cell',name,'cellRepo',repo)) FROM pgjobdb.cells WHERE tenant_id='c2' AND name='work'`)
		require.NoError(t, err)
		var goal string
		require.NoError(t, db.QueryRowContext(ctx, `SELECT goal_id FROM pgjobdb.job_facts WHERE job_id='historical-source'`).Scan(&goal))
		_, err = db.ExecContext(ctx, `SELECT pgjobdb.fail_goal('c2',$1,E' original\n保存 failure ')`, goal)
		require.NoError(t, err)
		var report bytes.Buffer
		_, err = (installer.Installer{DB: db}).ApplyWithReport(ctx, &report)
		require.NoError(t, err)
		var scheduleCount int
		require.NoError(t, db.QueryRowContext(ctx, `SELECT count(*) FROM pgjobdb.schedules`).Scan(&scheduleCount))
		require.Zero(t, scheduleCount)
		_, err = db.ExecContext(ctx, `-- Fixture-only retirement: no schedule rows or references may be lost.
ALTER TABLE pgjobdb.job_facts DROP CONSTRAINT job_facts_tenant_id_schedule_id_fkey;
DROP FUNCTION pgjobdb.upsert_schedule(text,text,text,text,jsonb,text,jsonb,text,jsonb,timestamptz,text,text,text,text,bigint),
 pgjobdb.mutate_schedule(text,text,text,timestamptz,text,bigint),
 pgjobdb.pause_schedule(text,text,bigint),
 pgjobdb.resume_schedule(text,text,timestamptz,text,bigint),
 pgjobdb.archive_schedule(text,text,bigint),
 pgjobdb.get_schedule(text,text),
 pgjobdb.list_schedules(text,text[],text[],timestamptz,text,integer,text[]);
DROP TABLE pgjobdb.schedules`)
		require.NoError(t, err)
		require.NoError(t, projects.PrepareShared(ctx, db))
		inventory, err := projects.Inspect(ctx, db, participants)
		require.NoError(t, err)
		_, err = projects.Install(ctx, db, inventory, []projectcatalog.Seed{{ID: "c2", Name: "c2"}}, io.Discard)
		require.NoError(t, err)
		cfg := pgruntime.Config{BlobStoreURI: "blobfs://" + t.TempDir()}
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
				outcome, err := client.GetGoal(ctx, jobdb.GoalKey{TenantId: "c2", GoalID: goal})
				require.NoError(t, err)
				key := jobdb.JobKey{TenantId: "c2", JobId: outcome.FailureJobID}
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

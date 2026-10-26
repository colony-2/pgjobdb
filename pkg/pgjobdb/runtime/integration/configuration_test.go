package integration_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/remote"
	"github.com/colony-2/jobdb/pkg/jobdb/runtimetest"
	wf "github.com/colony-2/jobdb/pkg/workflow"
	pgruntime "github.com/colony-2/pgjobdb/pkg/pgjobdb/runtime"
	"github.com/stretchr/testify/require"
)

func configurationTransport(t *testing.T, rt *pgruntime.Runtime, transport string) jobdb.WorkflowRuntime {
	t.Helper()
	if transport == "embedded" {
		return rt
	}
	server := httptest.NewServer(remote.NewServer(rt))
	t.Cleanup(server.Close)
	client, err := remote.New(server.URL, server.Client())
	require.NoError(t, err)
	return client
}

func openConfigurationRuntime(t *testing.T, dsn string, cfg pgruntime.Config) *pgruntime.Runtime {
	t.Helper()
	rt, err := pgruntime.OpenDSN(context.Background(), dsn, cfg)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, rt.Close(context.Background())) })
	return rt
}

func TestTenantConfiguration(t *testing.T) {
	newDSN := conformanceDSNFactory(t)
	for _, transport := range []string{"embedded", "http"} {
		t.Run(transport, func(t *testing.T) {
			t.Run("conformance", func(t *testing.T) {
				rt := openConfigurationRuntime(t, newDSN(t), pgruntime.Config{BlobStoreURI: "blobfs://" + t.TempDir()})
				runtimetest.RunConfigurationConformance(t, configurationTransport(t, rt, transport))
			})
			t.Run("concurrent_replicas_and_reopen", func(t *testing.T) {
				ctx := context.Background()
				dsn := newDSN(t)
				cfg := pgruntime.Config{BlobStoreURI: "blobfs://" + t.TempDir()}
				first := openConfigurationRuntime(t, dsn, cfg)
				second := openConfigurationRuntime(t, dsn, cfg)
				admins := []jobdb.ConfigurationAdmin{
					configurationTransport(t, first, transport).(jobdb.ConfigurationAdmin),
					configurationTransport(t, second, transport).(jobdb.ConfigurationAdmin),
				}
				// Separate connection pools must serialize even the first writes
				// to a previously absent tenant, without losing unrelated keys.
				start := make(chan struct{})
				results := make(chan error, 12)
				for i := range 12 {
					go func() {
						<-start
						_, err := admins[i%2].SetTenantConfigurationValue(ctx, jobdb.SetTenantConfigurationValueRequest{TenantID: "tenant", Name: fmt.Sprintf("key-%d", i), Value: json.RawMessage(`9007199254740993123456789`)})
						results <- err
					}()
				}
				close(start)
				for range 12 {
					require.NoError(t, <-results)
				}
				snapshot, err := admins[0].GetTenantConfiguration(ctx, "tenant")
				require.NoError(t, err)
				require.Len(t, snapshot.Values, 12)
				for _, value := range snapshot.Values {
					require.Equal(t, "9007199254740993123456789", string(value))
				}
				// Exactly one compare-and-swap may consume a given revision.
				start = make(chan struct{})
				for i := range 2 {
					go func() {
						<-start
						_, err := admins[i].SetTenantConfigurationValue(ctx, jobdb.SetTenantConfigurationValueRequest{TenantID: "tenant", Name: "winner", Value: json.RawMessage(fmt.Sprint(i)), ExpectedRevision: &snapshot.Revision})
						results <- err
					}()
				}
				close(start)
				wins, conflicts := 0, 0
				for range 2 {
					err := <-results
					switch {
					case err == nil:
						wins++
					case errors.Is(err, jobdb.ErrConfigurationRevisionMismatch):
						conflicts++
					default:
						t.Fatalf("concurrent update: %v", err)
					}
				}
				require.Equal(t, 1, wins)
				require.Equal(t, 1, conflicts)
				before, err := admins[0].GetTenantConfiguration(ctx, "tenant")
				require.NoError(t, err)
				_, err = admins[0].SetTenantConfigurationValue(ctx, jobdb.SetTenantConfigurationValueRequest{TenantID: "removed", Name: "last", Value: json.RawMessage(`null`)})
				require.NoError(t, err)
				removedRevision, err := admins[0].RemoveTenantConfigurationValue(ctx, jobdb.RemoveTenantConfigurationValueRequest{TenantID: "removed", Name: "last"})
				require.NoError(t, err)
				require.NoError(t, first.Close(ctx))
				require.NoError(t, second.Close(ctx))
				reopened := configurationTransport(t, openConfigurationRuntime(t, dsn, cfg), transport).(jobdb.ConfigurationAdmin)
				after, err := reopened.GetTenantConfiguration(ctx, "tenant")
				require.NoError(t, err)
				require.Equal(t, before, after)
				removed, err := reopened.GetTenantConfiguration(ctx, "removed")
				require.NoError(t, err)
				require.Empty(t, removed.Values)
				require.Equal(t, removedRevision, removed.Revision)
			})
			t.Run("limits_are_atomic", func(t *testing.T) {
				ctx := context.Background()
				backend := openConfigurationRuntime(t, newDSN(t), pgruntime.Config{BlobStoreURI: "blobfs://" + t.TempDir(), ConfigurationLimits: jobdb.ConfigurationLimits{MaxEntries: 1, MaxValueBytes: 4}})
				admin := configurationTransport(t, backend, transport).(jobdb.ConfigurationAdmin)
				revision, err := admin.SetTenantConfigurationValue(ctx, jobdb.SetTenantConfigurationValueRequest{TenantID: "tenant", Name: "one", Value: json.RawMessage(`true`)})
				require.NoError(t, err)
				for _, req := range []jobdb.SetTenantConfigurationValueRequest{
					{TenantID: "tenant", Name: "one", Value: json.RawMessage(`false`)},
					{TenantID: "tenant", Name: "two", Value: json.RawMessage(`null`)},
				} {
					_, err := admin.SetTenantConfigurationValue(ctx, req)
					require.ErrorIs(t, err, jobdb.ErrConfigurationTooLarge)
				}
				snapshot, err := admin.GetTenantConfiguration(ctx, "tenant")
				require.NoError(t, err)
				require.Equal(t, revision, snapshot.Revision)
				require.Equal(t, map[string]json.RawMessage{"one": json.RawMessage(`true`)}, snapshot.Values)
			})
			t.Run("workflow_attempt_snapshot", func(t *testing.T) {
				ctx := context.Background()
				rt := configurationTransport(t, openConfigurationRuntime(t, newDSN(t), pgruntime.Config{BlobStoreURI: "blobfs://" + t.TempDir()}), transport)
				admin := rt.(jobdb.ConfigurationAdmin)
				_, err := admin.SetTenantConfigurationValue(ctx, jobdb.SetTenantConfigurationValueRequest{TenantID: "tenant", Name: "setting", Value: json.RawMessage(`"initial"`)})
				require.NoError(t, err)
				job := &configurationReadingJob{admin: admin}
				handle, err := rt.SubmitJob(ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", JobType: job.Name(), Data: jobdb.NewTaskDataOrPanic("input")}})
				require.NoError(t, err)
				run, err := wf.GetJobForRun(ctx, rt, wf.GetJobForRunRequest{JobKey: handle.JobKey, JobWorker: job, WorkerID: "worker", LeaseDuration: time.Minute})
				require.NoError(t, err)
				outcome, err := run.Run(nil)
				require.NoError(t, err)
				require.Equal(t, wf.JobRunCompleted, outcome.Status, "%v", outcome.JobError)
				expired, err := job.captured.GetConfiguration(ctx)
				require.Error(t, err)
				require.Empty(t, expired.Values)
				chapters, err := rt.ListChapters(ctx, jobdb.ListChaptersRequest{JobKey: handle.JobKey})
				require.NoError(t, err)
				raw, err := json.Marshal(chapters)
				require.NoError(t, err)
				require.NotContains(t, string(raw), "configuration-updated-value")
			})
		})
	}
	t.Run("additive_upgrade_and_connection_ownership", func(t *testing.T) {
		ctx := context.Background()
		db, err := sql.Open("postgres", newDSN(t))
		require.NoError(t, err)
		t.Cleanup(func() { _ = db.Close() })
		cfg := pgruntime.Config{BlobStoreURI: "blobfs://" + t.TempDir()}
		before, err := pgruntime.NewSQLDB(ctx, db, cfg)
		require.NoError(t, err)
		t.Cleanup(func() { _ = before.Close(ctx) })
		job, err := before.SubmitJob(ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", JobType: "existing", Data: jobdb.NewTaskDataOrPanic("existing-input")}})
		require.NoError(t, err)
		require.NoError(t, before.Close(ctx))
		require.NoError(t, db.PingContext(ctx), "NewSQLDB must not own the pool")
		// Model an existing format-6 database from before configuration support.
		_, err = db.ExecContext(ctx, "DROP TABLE jobdb_configuration")
		require.NoError(t, err)
		after, err := pgruntime.NewSQLDB(ctx, db, cfg)
		require.NoError(t, err)
		t.Cleanup(func() { _ = after.Close(ctx) })
		_, err = after.SetTenantConfigurationValue(ctx, jobdb.SetTenantConfigurationValueRequest{TenantID: "tenant", Name: "new", Value: json.RawMessage(`1`)})
		require.NoError(t, err)
		_, err = after.GetJob(ctx, job.JobKey)
		require.NoError(t, err)
		chapter, err := after.GetChapter(ctx, jobdb.ChapterRef{JobKey: job.JobKey, Ordinal: 0})
		require.NoError(t, err)
		start, ok := chapter.Body.(jobdb.JobStartChapter)
		require.True(t, ok)
		require.JSONEq(t, `"existing-input"`, string(start.Input.Data))
		var version int
		require.NoError(t, db.QueryRowContext(ctx, "SELECT format_version FROM pgjobdb.installation WHERE name='pgjobdb'").Scan(&version))
		require.Equal(t, 7, version)
	})
}

type configurationReadingJob struct {
	admin    jobdb.ConfigurationAdmin
	captured wf.ExecutionContext
}

func (*configurationReadingJob) Name() string { return "configuration-job" }
func (w *configurationReadingJob) Run(ctx wf.ExecutionContext, input jobdb.JobData) (jobdb.JobData, error) {
	w.captured = ctx
	first, err := ctx.GetConfiguration(context.Background())
	if err != nil {
		return nil, err
	}
	_, err = w.admin.SetTenantConfigurationValue(context.Background(), jobdb.SetTenantConfigurationValueRequest{TenantID: "tenant", Name: "setting", Value: json.RawMessage(`"configuration-updated-value"`)})
	if err != nil {
		return nil, err
	}
	second, err := ctx.GetConfiguration(context.Background())
	if err != nil {
		return nil, err
	}
	// v0.0.32 pins the first successful workflow read to this attempt.
	// Administrative and direct lease reads remain fresh.
	if first.Revision != second.Revision || string(first.Values["setting"]) != string(second.Values["setting"]) {
		return nil, fmt.Errorf("configuration snapshot changed within one attempt")
	}
	fresh, err := w.admin.GetTenantConfiguration(context.Background(), "tenant")
	if err != nil {
		return nil, err
	}
	if fresh.Revision == second.Revision || string(fresh.Values["setting"]) != `"configuration-updated-value"` {
		return nil, fmt.Errorf("administrative read did not observe the update")
	}
	return input, nil
}

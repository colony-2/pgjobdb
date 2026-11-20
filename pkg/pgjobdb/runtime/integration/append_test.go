package integration_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	runtimecore "github.com/colony-2/jobdb/pkg/jobdb/runtime/core"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/remote"
	"github.com/colony-2/jobdb/pkg/jobdb/runtimetest"
	"github.com/colony-2/jobdb/pkg/workflow"
	pgruntime "github.com/colony-2/pgjobdb/pkg/pgjobdb/runtime"
	"github.com/colony-2/pgjobdb/pkg/pgjobdb/runtime/internal/runtimeadapter"
	"github.com/stretchr/testify/require"
)

func TestStartOrAppendConformance(t *testing.T) {
	for _, http := range []bool{false, true} {
		name := "direct"
		if http {
			name = "remote"
		}
		t.Run(name, func(t *testing.T) {
			runDatabaseTest(t, func(ctx context.Context, db *sql.DB) {
				rt, err := pgruntime.NewSQLDB(ctx, db, pgruntime.Config{BlobStoreURI: "blobfs://" + t.TempDir()})
				require.NoError(t, err)
				defer rt.Close(ctx)
				var client jobdb.ConsumerRuntime = rt
				if http {
					server := httptest.NewServer(celltest.Handler(remote.NewConsumerServer(rt)))
					defer server.Close()
					client, err = remote.New(server.URL, server.Client())
					require.NoError(t, err)
				}
				runtimetest.RunStartOrAppendConformance(t, runtimetest.Harness{Name: name, New: func(testing.TB) runtimetest.Fixture { return runtimetest.Fixture{Runtime: client} }})
			})
		})
	}
}

func TestAppendClaimsPreserveSchedulerState(t *testing.T) {
	runDatabaseTest(t, func(ctx context.Context, db *sql.DB) {
		rt, err := pgruntime.NewSQLDB(ctx, db, pgruntime.Config{BlobStoreURI: "blobfs://" + t.TempDir()})
		require.NoError(t, err)
		defer rt.Close(ctx)
		_, err = rt.CreateCell(ctx, jobdb.CreateCellRequest{TenantID: "tenant", Name: "test", Repo: "example.test/test"})
		require.NoError(t, err)
		at := time.Now().Add(time.Hour)
		h, err := rt.StartOrAppendJob(ctx, jobdb.StartOrAppendJobRequest{Job: jobdb.StartOrAppendJob{AppendKey: "claims", Job: jobdb.SubmitJob{TenantId: "tenant", Cell: "test", JobType: "batch", Data: jobdb.NewTaskDataOrPanic(1), AvailableAt: &at}, Append: json.RawMessage(`1`)}})
		require.NoError(t, err)
		scheduler := runtimeadapter.Scheduler{DB: db}
		var before string
		require.NoError(t, db.QueryRowContext(ctx, `SELECT jsonb_build_array(route_job_type,work_kind,task_type,wait_for,available_at,payload,alternate_job_type,alternate_at)::text FROM pgjobdb.jobs WHERE tenant_id=$1 AND job_id=$2`, h.JobKey.TenantId, h.JobKey.JobId).Scan(&before))
		var previous runtimecore.LeaseIdentity
		for i := 0; i < 8; i++ {
			claim, err := scheduler.ClaimAppend(ctx, runtimecore.ClaimAppendRequest{JobKey: h.JobKey, WorkerID: runtimecore.AppendWorkerPrefix + "test", Duration: time.Second, Now: time.Now()})
			require.NoError(t, err)
			if i > 0 {
				require.NoError(t, scheduler.ReleaseAppend(ctx, previous))
				var id string
				require.NoError(t, db.QueryRowContext(ctx, `SELECT lease_id FROM pgjobdb.jobs WHERE tenant_id=$1 AND job_id=$2`, h.JobKey.TenantId, h.JobKey.JobId).Scan(&id))
				require.Equal(t, claim.LeaseID, id)
			}
			_, err = db.ExecContext(ctx, `UPDATE pgjobdb.jobs SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE tenant_id=$1 AND job_id=$2`, h.JobKey.TenantId, h.JobKey.JobId)
			require.NoError(t, err)
			previous = claim
		}
		require.NoError(t, scheduler.ReleaseAppend(ctx, previous))
		var after string
		var count int
		require.NoError(t, db.QueryRowContext(ctx, `SELECT jsonb_build_array(route_job_type,work_kind,task_type,wait_for,available_at,payload,alternate_job_type,alternate_at)::text,consecutive_expirations FROM pgjobdb.jobs WHERE tenant_id=$1 AND job_id=$2`, h.JobKey.TenantId, h.JobKey.JobId).Scan(&after, &count))
		require.Equal(t, before, after)
		require.Zero(t, count)
	})
}

func TestAppendListCursorBindsFilterKind(t *testing.T) {
	runDatabaseTest(t, func(ctx context.Context, db *sql.DB) {
		rt, err := pgruntime.NewSQLDB(ctx, db, pgruntime.Config{BlobStoreURI: "blobfs://" + t.TempDir()})
		require.NoError(t, err)
		defer rt.Close(ctx)
		_, err = rt.CreateCell(ctx, jobdb.CreateCellRequest{TenantID: "tenant", Name: "test", Repo: "example.test/test"})
		require.NoError(t, err)
		q := jobdb.StartOrAppendJobRequest{Job: jobdb.StartOrAppendJob{AppendKey: "same-filter-value", MaxInputs: 1, Job: jobdb.SubmitJob{TenantId: "tenant", Cell: "test", JobType: "batch", Data: jobdb.NewTaskDataOrPanic(nil)}, Append: json.RawMessage(`null`)}}
		for i := 0; i < 2; i++ {
			_, err = rt.StartOrAppendJob(ctx, q)
			require.NoError(t, err)
		}
		query := jobdb.ListJobsRequest{TenantIds: []string{"tenant"}, AppendKeys: []string{q.Job.AppendKey}, PageSize: 1}
		page, err := rt.ListJobs(ctx, query)
		require.NoError(t, err)
		require.NotEmpty(t, page.NextPageToken)
		query.PageToken = page.NextPageToken
		query.GoalIDs, query.AppendKeys = query.AppendKeys, nil
		_, err = rt.ListJobs(ctx, query)
		require.ErrorContains(t, err, "invalid job page token")
	})
}

func TestAppendInputAndReplay(t *testing.T) {
	runDatabaseTest(t, func(ctx context.Context, db *sql.DB) {
		rt, err := pgruntime.NewSQLDB(ctx, db, pgruntime.Config{BlobStoreURI: "blobfs://" + t.TempDir()})
		require.NoError(t, err)
		defer rt.Close(ctx)
		source, err := celltest.Submit(rt, ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", Cell: "test", CellRepo: "example.test/test", JobType: "source", Data: jobdb.NewTaskDataOrPanic(nil, jobdb.NewArtifactFromBytes("f.txt", []byte("content")))}})
		require.NoError(t, err)
		for _, overHTTP := range []bool{false, true} {
			name := "direct"
			if overHTTP {
				name = "remote"
			}
			t.Run(name, func(t *testing.T) {
				var client jobdb.ConsumerRuntime = rt
				if overHTTP {
					server := httptest.NewServer(celltest.Handler(remote.NewConsumerServer(rt)))
					defer server.Close()
					client, err = remote.New(server.URL, server.Client())
					require.NoError(t, err)
				}
				q := jobdb.StartOrAppendJobRequest{Job: jobdb.StartOrAppendJob{AppendKey: name, Job: jobdb.SubmitJob{TenantId: "tenant", Cell: "test", JobType: "batch", Data: jobdb.NewTaskDataOrPanic("start")}, Append: json.RawMessage(`null`), AppendArtifacts: []jobdb.ArtifactKey{{JobId: source.JobKey.JobId, TaskOrdinal: 0, Name: "f.txt", SizeBytes: 7}}}}
				a, err := client.StartOrAppendJob(ctx, q)
				require.NoError(t, err)
				_, err = client.StartOrAppendJob(ctx, q)
				require.NoError(t, err)
				restart, err := client.SubmitRestartJob(ctx, jobdb.SubmitRestartJobRequest{Job: jobdb.SubmitRestartJob{Cell: "test", PriorJobKey: a.JobKey, LastStepToKeep: 1}})
				require.NoError(t, err)
				info, err := client.GetJob(ctx, restart.JobKey)
				require.NoError(t, err)
				require.Empty(t, info.AppendKey)
				worker := workflow.ExecutionWorker{JobType: "batch", Execute: func(c workflow.ExecutionContext, in jobdb.JobData) (jobdb.JobData, error) {
					require.JSONEq(t, `{"start":"start","appends":[null,null]}`, string(in.GetDataOrPanic()))
					arts, err := in.GetArtifacts()
					require.NoError(t, err)
					require.Len(t, arts, 2)
					require.Equal(t, "appends/0/f.txt", arts[0].Name())
					require.Equal(t, "appends/1/f.txt", arts[1].Name())
					raw, err := arts[1].Bytes(c.Context())
					require.NoError(t, err)
					require.Equal(t, "content", string(raw))
					return in, nil
				}}
				for _, key := range []jobdb.JobKey{a.JobKey, restart.JobKey} {
					runnable, err := workflow.GetJobForRun(ctx, client, workflow.GetJobForRunRequest{JobKey: key, JobWorker: worker, WorkerID: "worker", LeaseDuration: time.Minute})
					require.NoError(t, err)
					out, err := runnable.Run(nil)
					require.NoError(t, err)
					require.Equal(t, workflow.JobRunCompleted, out.Status)
					engine, err := workflow.NewEngineBuilder().WithRuntime(client).WithWorkerTenantId("tenant").PlusWorkers(worker).BuildEngine()
					require.NoError(t, err)
					_, err = engine.ReplayJobRun(ctx, workflow.ReplayRunRequest{JobKey: key})
					require.NoError(t, err)
				}
			})
		}
	})
}

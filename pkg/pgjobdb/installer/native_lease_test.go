package installer_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"testing"
	"time"

	"github.com/colony-2/pgjobdb/pkg/pgjobdb"
)

func TestNativeLeaseSelection(t *testing.T) {
	runDatabaseTest(t, func(ctx context.Context, db *sql.DB) {
		for _, item := range []struct {
			id       pgjobdb.JobID
			metadata string
		}{
			{"job-api", `{"source":"api"}`},
			{"job-batch", `{"source":"batch"}`},
			{"job-task", `{"source":"api"}`},
		} {
			if _, err := pgjobdb.SubmitJob(ctx, db, pgjobdb.SubmitJobRequest{
				TenantID: "tenant", JobID: item.id, WorkerID: "submitter",
				JobType: "collect", AppMetadata: json.RawMessage(item.metadata),
			}); err != nil {
				t.Fatalf("submit %s: %v", item.id, err)
			}
		}
		if _, err := db.ExecContext(ctx, `UPDATE pgjobdb.jobs SET
			work_kind = 'TASK', task_type = 'download',
			resume_job_type = 'collect', task_input_ordinal = 1,
			task_output_ordinal = 2, task_input_hash = 'sha256:input'
			WHERE tenant_id = 'tenant' AND job_id = 'job-task'`); err != nil {
			t.Fatalf("set task route: %v", err)
		}

		jobSelector := pgjobdb.WorkSelector{JobTypes: []pgjobdb.JobType{"collect"}}
		batch, err := pgjobdb.GetWork(ctx, db, "worker-batch", jobSelector,
			pgjobdb.GetWorkOptions{
				TenantIDs:           []pgjobdb.TenantID{"tenant"},
				AppMetadataContains: json.RawMessage(`{"source":"batch"}`),
				LeaseDuration:       5 * time.Second,
			})
		if err != nil || batch == nil || batch.JobID != "job-batch" ||
			batch.WorkKind != pgjobdb.WorkKindJob {
			t.Fatalf("batch lease = %+v, %v", batch, err)
		}
		if unavailable, err := pgjobdb.GetJobLease(ctx, db, "tenant", "job-batch",
			"worker-2", jobSelector, pgjobdb.GetJobLeaseOptions{}); err != nil || unavailable != nil {
			t.Fatalf("active job should be unavailable: %+v, %v", unavailable, err)
		}

		taskSelector := pgjobdb.WorkSelector{Tasks: []pgjobdb.TaskSelector{
			{JobType: "collect", TaskType: "download"},
		}}
		task, err := pgjobdb.GetWork(ctx, db, "worker-task", taskSelector,
			pgjobdb.GetWorkOptions{})
		if err != nil || task == nil || task.JobID != "job-task" ||
			task.WorkKind != pgjobdb.WorkKindTask || task.Task == nil ||
			task.Task.InputOrdinal != 1 || task.Task.OutputOrdinal != 2 ||
			task.Task.InputHash != "sha256:input" {
			t.Fatalf("task lease = %+v, %v", task, err)
		}
		if task.RouteJobType != "collect" || task.Task.TaskType != "download" {
			t.Fatalf("task route = %+v", task)
		}
		if wrong, err := pgjobdb.GetJobLease(ctx, db, "tenant", "job-api",
			"worker-task", taskSelector, pgjobdb.GetJobLeaseOptions{}); err != nil || wrong != nil {
			t.Fatalf("wrong task selector claimed job: %+v, %v", wrong, err)
		}

		api, err := pgjobdb.GetJobLease(ctx, db, "tenant", "job-api",
			"worker-api", jobSelector, pgjobdb.GetJobLeaseOptions{})
		if err != nil || api == nil || api.JobID != "job-api" {
			t.Fatalf("api lease = %+v, %v", api, err)
		}
		future := time.Now().UTC().Add(time.Hour)
		for _, req := range []pgjobdb.SubmitJobRequest{
			{TenantID: "tenant", JobID: "job-future", WorkerID: "submitter",
				JobType: "collect", AvailableAt: &future},
			{TenantID: "tenant", JobID: "job-waiting", WorkerID: "submitter",
				JobType: "collect", WaitFor: []pgjobdb.JobID{"job-api"}},
			{TenantID: "tenant", JobID: "job-cancelled", WorkerID: "submitter",
				JobType: "collect"},
		} {
			if _, err := pgjobdb.SubmitJob(ctx, db, req); err != nil {
				t.Fatalf("submit blocked job %s: %v", req.JobID, err)
			}
		}
		if _, err := db.ExecContext(ctx, `UPDATE pgjobdb.jobs
			SET cancel_requested = TRUE
			WHERE tenant_id = 'tenant' AND job_id = 'job-cancelled'`); err != nil {
			t.Fatalf("cancel job: %v", err)
		}
		if none, err := pgjobdb.GetWork(ctx, db, "worker-extra", jobSelector,
			pgjobdb.GetWorkOptions{}); err != nil || none != nil {
			t.Fatalf("expected no more job routes: %+v, %v", none, err)
		}
	})
}

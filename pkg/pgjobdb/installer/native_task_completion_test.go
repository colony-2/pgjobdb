package installer_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"testing"

	"github.com/colony-2/pgjobdb/pkg/pgjobdb"
)

func TestNativeTaskCompletionComparesCoordinates(t *testing.T) {
	runDatabaseTest(t, func(ctx context.Context, db *sql.DB) {
		if _, err := pgjobdb.SubmitJob(ctx, db, pgjobdb.SubmitJobRequest{
			TenantID: "tenant", JobID: "job", WorkerID: "submitter", JobType: "collect",
		}); err != nil {
			t.Fatalf("submit: %v", err)
		}
		task := pgjobdb.TaskWork{
			TaskType: "download", ResumeJobType: "collect",
			InputOrdinal: 2, OutputOrdinal: 3, InputHash: "sha256:input",
		}
		if err := pgjobdb.RescheduleUnheldJob(ctx, db, "tenant", "job", "worker",
			pgjobdb.RescheduleRequest{
				RouteJobType: "collect", WorkKind: pgjobdb.WorkKindTask, Task: &task,
			}); err != nil {
			t.Fatalf("route to task: %v", err)
		}
		req := pgjobdb.CompleteTaskWorkRequest{
			TenantID: "tenant", JobID: "job", WorkerID: "external",
			JobType: "collect", Task: task,
			ClientPayloadUpdate: &pgjobdb.ClientPayloadUpdate{Mode: "reset", ExpectedRevision: ptrRevision(0), Value: json.RawMessage(`{"external":true}`)},
		}
		wrong := req
		wrong.Task.InputHash = "sha256:wrong"
		if err := pgjobdb.CompleteTaskWork(ctx, db, wrong); err == nil {
			t.Fatal("expected input hash mismatch to fail")
		}
		var kind string
		if err := db.QueryRowContext(ctx, `SELECT work_kind FROM pgjobdb.jobs
			WHERE tenant_id = 'tenant' AND job_id = 'job'`).Scan(&kind); err != nil || kind != "TASK" {
			t.Fatalf("failed completion changed route: %q, %v", kind, err)
		}
		taskLease, err := pgjobdb.GetJobLease(ctx, db, "tenant", "job", "task-worker",
			pgjobdb.WorkSelector{Tasks: []pgjobdb.TaskSelector{
				{JobType: "collect", TaskType: "download"},
			}}, pgjobdb.GetJobLeaseOptions{})
		if err != nil || taskLease == nil {
			t.Fatalf("lease task: %+v, %v", taskLease, err)
		}
		if err := pgjobdb.CompleteTaskWork(ctx, db, req); err == nil {
			t.Fatal("expected active lease to prevent external completion")
		}
		if err := taskLease.Reschedule(ctx, db, pgjobdb.RescheduleRequest{
			RouteJobType: "collect", WorkKind: pgjobdb.WorkKindTask, Task: &task,
		}); err != nil {
			t.Fatalf("return task to queue: %v", err)
		}
		if err := pgjobdb.CompleteTaskWork(ctx, db, req); err != nil {
			t.Fatalf("complete waiting task: %v", err)
		}
		if err := pgjobdb.CompleteTaskWork(ctx, db, req); err == nil {
			t.Fatal("expected repeated task completion to fail")
		}
		var taskType sql.NullString
		var payload string
		if err := db.QueryRowContext(ctx, `SELECT work_kind, task_type,
			(SELECT client_payload::text FROM pgjobdb.job_client_state c WHERE c.tenant_id=jobs.tenant_id AND c.job_id=jobs.job_id) FROM pgjobdb.jobs
			WHERE tenant_id = 'tenant' AND job_id = 'job'`).Scan(
			&kind, &taskType, &payload,
		); err != nil {
			t.Fatalf("read resumed job: %v", err)
		}
		if kind != "JOB" || taskType.Valid || payload != `{"external":true}` {
			t.Fatalf("resumed job = %q %+v %q", kind, taskType, payload)
		}
		selector := pgjobdb.WorkSelector{JobTypes: []pgjobdb.JobType{"collect"}}
		lease, err := pgjobdb.GetJobLease(ctx, db, "tenant", "job", "job-worker",
			selector, pgjobdb.GetJobLeaseOptions{})
		if err != nil || lease == nil || lease.WorkKind != pgjobdb.WorkKindJob {
			t.Fatalf("resume lease = %+v, %v", lease, err)
		}
	})
}

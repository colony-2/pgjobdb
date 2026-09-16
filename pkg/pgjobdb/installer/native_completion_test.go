package installer_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"testing"

	"github.com/colony-2/pgjobdb/pkg/pgjobdb"
)

func TestNativeCompletionPreservesFinalState(t *testing.T) {
	runDatabaseTest(t, func(ctx context.Context, db *sql.DB) {
		for _, req := range []pgjobdb.SubmitJobRequest{
			{TenantID: "tenant", JobID: "root", WorkerID: "submitter",
				JobType: "collect", LeasePayload: json.RawMessage(`{"attempt":1}`)},
			{TenantID: "tenant", JobID: "child", WorkerID: "submitter",
				JobType: "collect", WaitFor: []pgjobdb.JobID{"root"}},
		} {
			if _, err := pgjobdb.SubmitJob(ctx, db, req); err != nil {
				t.Fatalf("submit %s: %v", req.JobID, err)
			}
		}
		selector := pgjobdb.WorkSelector{JobTypes: []pgjobdb.JobType{"collect"}}
		lease, err := pgjobdb.GetJobLease(ctx, db, "tenant", "root",
			"worker", selector, pgjobdb.GetJobLeaseOptions{})
		if err != nil || lease == nil {
			t.Fatalf("lease root = %+v, %v", lease, err)
		}
		wrong := lease.Identity()
		wrong.LeaseID = "wrong"
		if err := pgjobdb.CompleteJob(ctx, db, wrong,
			pgjobdb.Completion{Status: pgjobdb.CompletionSuccess}); err == nil {
			t.Fatal("expected mismatched lease to fail")
		}
		if err := lease.Complete(ctx, db,
			pgjobdb.Completion{Status: pgjobdb.CompletionSuccess}); err != nil {
			t.Fatalf("complete root: %v", err)
		}
		if err := lease.Complete(ctx, db,
			pgjobdb.Completion{Status: pgjobdb.CompletionSuccess}); err == nil {
			t.Fatal("expected double completion to fail")
		}
		var route, kind, status, payload string
		var cancelled bool
		if err := db.QueryRowContext(ctx, `SELECT final_route_job_type,
			final_work_kind, completion_status, final_lease_payload::text,
			final_cancel_requested FROM pgjobdb.jobs_archive
			WHERE tenant_id = 'tenant' AND job_id = 'root'`).Scan(
			&route, &kind, &status, &payload, &cancelled,
		); err != nil {
			t.Fatalf("read root archive: %v", err)
		}
		if route != "collect" || kind != "JOB" || status != "success" ||
			payload != `{"attempt": 1}` || cancelled {
			t.Fatalf("root archive = %q %q %q %q %v",
				route, kind, status, payload, cancelled)
		}
		var waitCount, factCount int
		if err := db.QueryRowContext(ctx, `SELECT cardinality(wait_for)
			FROM pgjobdb.jobs WHERE tenant_id = 'tenant' AND job_id = 'child'`).Scan(
			&waitCount,
		); err != nil || waitCount != 0 {
			t.Fatalf("child wait set = %d, %v", waitCount, err)
		}
		if err := db.QueryRowContext(ctx, `SELECT count(*) FROM pgjobdb.job_facts
			WHERE tenant_id = 'tenant' AND job_id = 'root'`).Scan(&factCount); err != nil || factCount != 1 {
			t.Fatalf("root facts count = %d, %v", factCount, err)
		}

		if _, err := db.ExecContext(ctx, `UPDATE pgjobdb.jobs SET
			work_kind = 'TASK', task_type = 'download',
			resume_job_type = 'collect', task_input_ordinal = 3,
			task_output_ordinal = 4, task_input_hash = 'sha256:task',
			lease_payload = '{"work":"item"}'
			WHERE tenant_id = 'tenant' AND job_id = 'child'`); err != nil {
			t.Fatalf("route child to task: %v", err)
		}
		taskLease, err := pgjobdb.GetJobLease(ctx, db, "tenant", "child",
			"task-worker", pgjobdb.WorkSelector{Tasks: []pgjobdb.TaskSelector{
				{JobType: "collect", TaskType: "download"},
			}}, pgjobdb.GetJobLeaseOptions{})
		if err != nil || taskLease == nil {
			t.Fatalf("lease child task = %+v, %v", taskLease, err)
		}
		if _, err := db.ExecContext(ctx, `UPDATE pgjobdb.jobs
			SET cancel_requested = TRUE
			WHERE tenant_id = 'tenant' AND job_id = 'child'`); err != nil {
			t.Fatalf("request cancellation: %v", err)
		}
		retryable := false
		if err := taskLease.Complete(ctx, db, pgjobdb.Completion{
			Status: pgjobdb.CompletionFailedApp, Detail: "task failed",
			ErrorKind: "DownloadError", Retryable: &retryable,
		}); err != nil {
			t.Fatalf("complete task route: %v", err)
		}
		var taskType, inputHash, detail, errorKind string
		var input, output int64
		var retryFlag bool
		if err := db.QueryRowContext(ctx, `SELECT final_task_type,
			final_task_input_ordinal, final_task_output_ordinal,
			final_task_input_hash, final_cancel_requested,
			completion_detail, completion_error_kind, completion_retryable
			FROM pgjobdb.jobs_archive
			WHERE tenant_id = 'tenant' AND job_id = 'child'`).Scan(
			&taskType, &input, &output, &inputHash, &cancelled,
			&detail, &errorKind, &retryFlag,
		); err != nil {
			t.Fatalf("read child archive: %v", err)
		}
		if taskType != "download" || input != 3 || output != 4 ||
			inputHash != "sha256:task" || !cancelled ||
			detail != "task failed" || errorKind != "DownloadError" || retryFlag {
			t.Fatalf("child archive = %q %d %d %q %v %q %q %v",
				taskType, input, output, inputHash, cancelled, detail, errorKind, retryFlag)
		}

		if _, err := pgjobdb.SubmitJob(ctx, db, pgjobdb.SubmitJobRequest{
			TenantID: "tenant", JobID: "unheld", WorkerID: "submitter", JobType: "collect",
		}); err != nil {
			t.Fatalf("submit unheld: %v", err)
		}
		if err := pgjobdb.CompleteUnheldJob(ctx, db, "tenant", "unheld", "worker",
			pgjobdb.Completion{Status: pgjobdb.CompletionCancelled}); err != nil {
			t.Fatalf("complete unheld: %v", err)
		}
	})
}

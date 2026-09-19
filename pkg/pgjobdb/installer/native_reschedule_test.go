package installer_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/colony-2/pgjobdb/pkg/pgjobdb"
)

func TestNativeRescheduleTypedState(t *testing.T) {
	runDatabaseTest(t, func(ctx context.Context, db *sql.DB) {
		for _, id := range []pgjobdb.JobID{"job", "blocker"} {
			if _, err := pgjobdb.SubmitJob(ctx, db, pgjobdb.SubmitJobRequest{
				TenantID: "tenant", JobID: id, WorkerID: "submitter", JobType: "collect",
			}); err != nil {
				t.Fatalf("submit %s: %v", id, err)
			}
		}
		selector := pgjobdb.WorkSelector{JobTypes: []pgjobdb.JobType{"collect"}}
		lease, err := pgjobdb.GetJobLease(ctx, db, "tenant", "job", "worker",
			selector, pgjobdb.GetJobLeaseOptions{})
		if err != nil || lease == nil {
			t.Fatalf("lease job = %+v, %v", lease, err)
		}
		wrong := lease.Identity()
		wrong.WorkerID = "other"
		taskRoute := pgjobdb.RescheduleRequest{
			RouteJobType: "collect", WorkKind: pgjobdb.WorkKindTask,
			Task: &pgjobdb.TaskWork{
				TaskType: "download", ResumeJobType: "collect",
				InputOrdinal: 4, OutputOrdinal: 5, InputHash: "sha256:input",
			},
			ClientPayloadUpdate: &pgjobdb.ClientPayloadUpdate{Mode: "reset", ExpectedRevision: ptrRevision(0), Value: json.RawMessage(`{"app":"data"}`)},
			Alternate:           &pgjobdb.AlternateRoute{JobType: "fallback", After: time.Hour},
		}
		if err := pgjobdb.RescheduleJob(ctx, db, wrong, taskRoute); err == nil {
			t.Fatal("expected wrong worker reschedule to fail")
		}
		if err := lease.Reschedule(ctx, db, taskRoute); err != nil {
			t.Fatalf("reschedule to task: %v", err)
		}
		if _, err := pgjobdb.ValidateLease(ctx, db, lease.Identity()); !errors.Is(err, pgjobdb.ErrLeaseLost) {
			t.Fatalf("old lease remained valid: %v", err)
		}
		var workKind, taskType, alternate, payload string
		var input, output int64
		var alternateAfter int
		if err := db.QueryRowContext(ctx, `SELECT work_kind, task_type,
			task_input_ordinal, task_output_ordinal, alternate_job_type,
			alternate_after_seconds, (SELECT client_payload::text FROM pgjobdb.job_client_state c WHERE c.tenant_id=jobs.tenant_id AND c.job_id=jobs.job_id) FROM pgjobdb.jobs
			WHERE tenant_id = 'tenant' AND job_id = 'job'`).Scan(
			&workKind, &taskType, &input, &output, &alternate,
			&alternateAfter, &payload,
		); err != nil {
			t.Fatalf("read task route: %v", err)
		}
		if workKind != "TASK" || taskType != "download" || input != 4 ||
			output != 5 || alternate != "fallback" || alternateAfter != 3600 ||
			payload != `{"app":"data"}` {
			t.Fatalf("task route = %q %q %d %d %q %d %q",
				workKind, taskType, input, output, alternate, alternateAfter, payload)
		}
		taskLease, err := pgjobdb.GetJobLease(ctx, db, "tenant", "job",
			"task-worker", pgjobdb.WorkSelector{Tasks: []pgjobdb.TaskSelector{
				{JobType: "collect", TaskType: "download"},
			}}, pgjobdb.GetJobLeaseOptions{})
		if err != nil || taskLease == nil || taskLease.Task == nil {
			t.Fatalf("lease task = %+v, %v", taskLease, err)
		}
		future := time.Now().UTC().Add(time.Hour)
		jobRoute := pgjobdb.RescheduleRequest{
			RouteJobType: "collect", WorkKind: pgjobdb.WorkKindJob,
			WaitFor: []pgjobdb.JobID{"blocker"}, AvailableAt: &future,
			Alternate: nil,
		}
		if err := taskLease.Reschedule(ctx, db, jobRoute); err != nil {
			t.Fatalf("reschedule task to blocked job: %v", err)
		}
		var clearedAlt sql.NullString
		if err := db.QueryRowContext(ctx, `SELECT alternate_job_type
			FROM pgjobdb.jobs WHERE tenant_id = 'tenant' AND job_id = 'job'`).Scan(
			&clearedAlt,
		); err != nil || clearedAlt.Valid {
			t.Fatalf("alternate route not cleared: %+v, %v", clearedAlt, err)
		}
		if blocked, err := pgjobdb.GetJobLease(ctx, db, "tenant", "job",
			"worker", selector, pgjobdb.GetJobLeaseOptions{}); err != nil || blocked != nil {
			t.Fatalf("blocked job leased: %+v, %v", blocked, err)
		}
		if err := pgjobdb.CompleteUnheldJob(ctx, db, "tenant", "blocker",
			"worker", pgjobdb.Completion{Status: pgjobdb.CompletionSuccess}); err != nil {
			t.Fatalf("complete blocker: %v", err)
		}
		if blocked, err := pgjobdb.GetJobLease(ctx, db, "tenant", "job",
			"worker", selector, pgjobdb.GetJobLeaseOptions{}); err != nil || blocked != nil {
			t.Fatalf("time blocked job leased: %+v, %v", blocked, err)
		}
		if _, err := db.ExecContext(ctx, `UPDATE pgjobdb.jobs
			SET available_at = clock_timestamp() - interval '1 second'
			WHERE tenant_id = 'tenant' AND job_id = 'job'`); err != nil {
			t.Fatalf("release time blocker: %v", err)
		}
		ready, err := pgjobdb.GetJobLease(ctx, db, "tenant", "job",
			"worker", selector, pgjobdb.GetJobLeaseOptions{})
		if err != nil || ready == nil || ready.WorkKind != pgjobdb.WorkKindJob || ready.Task != nil {
			t.Fatalf("ready job lease = %+v, %v", ready, err)
		}
		if _, err := pgjobdb.SubmitJob(ctx, db, pgjobdb.SubmitJobRequest{
			TenantID: "tenant", JobID: "unheld", WorkerID: "submitter", JobType: "collect",
		}); err != nil {
			t.Fatalf("submit unheld job: %v", err)
		}
		taskRoute.ClientPayloadUpdate = nil
		if err := pgjobdb.RescheduleUnheldJob(ctx, db, "tenant", "unheld",
			"worker", taskRoute); err != nil {
			t.Fatalf("reschedule unheld job: %v", err)
		}
		if err := pgjobdb.RescheduleUnheldJob(ctx, db, "tenant", "unheld",
			"worker", pgjobdb.RescheduleRequest{
				RouteJobType: "collect", WorkKind: pgjobdb.WorkKindJob,
				WaitFor: []pgjobdb.JobID{"unheld"},
			}); err == nil {
			t.Fatal("expected self dependency to fail")
		}
	})
}

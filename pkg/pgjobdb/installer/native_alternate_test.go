package installer_test

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/colony-2/pgjobdb/pkg/pgjobdb"
)

func TestNativeAlternateRoutes(t *testing.T) {
	runDatabaseTest(t, func(ctx context.Context, db *sql.DB) {
		for _, id := range []pgjobdb.JobID{"job", "delayed", "task", "task-to-job"} {
			if _, err := pgjobdb.SubmitJob(ctx, db, pgjobdb.SubmitJobRequest{
				TenantID: "tenant", JobID: id, WorkerID: "submitter", JobType: "collect",
			}); err != nil {
				t.Fatalf("submit %s: %v", id, err)
			}
		}
		jobRoute := pgjobdb.RescheduleRequest{
			RouteJobType: "collect", WorkKind: pgjobdb.WorkKindJob,
			Alternate: &pgjobdb.AlternateRoute{JobType: "fallback"},
		}
		if err := pgjobdb.RescheduleUnheldJob(ctx, db, "tenant", "job", "worker", jobRoute); err != nil {
			t.Fatalf("set job alternate: %v", err)
		}
		original := pgjobdb.WorkSelector{JobTypes: []pgjobdb.JobType{"collect"}}
		fallback := pgjobdb.WorkSelector{JobTypes: []pgjobdb.JobType{"fallback"}}
		if lease, err := pgjobdb.GetJobLease(ctx, db, "tenant", "job", "worker",
			original, pgjobdb.GetJobLeaseOptions{}); err != nil || lease != nil {
			t.Fatalf("original route still selected: %+v, %v", lease, err)
		}
		lease, err := pgjobdb.GetJobLease(ctx, db, "tenant", "job", "worker",
			fallback, pgjobdb.GetJobLeaseOptions{})
		if err != nil || lease == nil || lease.RouteJobType != "fallback" ||
			lease.WorkKind != pgjobdb.WorkKindJob {
			t.Fatalf("fallback job lease = %+v, %v", lease, err)
		}

		jobRoute.Alternate.After = time.Hour
		if err := pgjobdb.RescheduleUnheldJob(ctx, db, "tenant", "delayed", "worker", jobRoute); err != nil {
			t.Fatalf("set delayed alternate: %v", err)
		}
		if notDue, err := pgjobdb.GetJobLease(ctx, db, "tenant", "delayed", "worker",
			fallback, pgjobdb.GetJobLeaseOptions{}); err != nil || notDue != nil {
			t.Fatalf("early fallback lease = %+v, %v", notDue, err)
		}
		if due, err := pgjobdb.GetJobLease(ctx, db, "tenant", "delayed", "worker",
			original, pgjobdb.GetJobLeaseOptions{}); err != nil || due == nil {
			t.Fatalf("original route before delay = %+v, %v", due, err)
		}

		task := &pgjobdb.TaskWork{
			TaskType: "download", ResumeJobType: "collect",
			InputOrdinal: 2, OutputOrdinal: 3, InputHash: "sha256:input",
		}
		taskRoute := pgjobdb.RescheduleRequest{
			RouteJobType: "collect", WorkKind: pgjobdb.WorkKindTask, Task: task,
			Alternate: &pgjobdb.AlternateRoute{
				JobType: "fallback", TaskType: "alternate-download",
			},
		}
		if err := pgjobdb.RescheduleUnheldJob(ctx, db, "tenant", "task", "worker", taskRoute); err != nil {
			t.Fatalf("set task alternate: %v", err)
		}
		altTask, err := pgjobdb.GetJobLease(ctx, db, "tenant", "task", "worker",
			pgjobdb.WorkSelector{Tasks: []pgjobdb.TaskSelector{
				{JobType: "fallback", TaskType: "alternate-download"},
			}}, pgjobdb.GetJobLeaseOptions{})
		if err != nil || altTask == nil || altTask.WorkKind != pgjobdb.WorkKindTask ||
			altTask.Task == nil || altTask.Task.TaskType != "alternate-download" ||
			altTask.Task.InputOrdinal != 2 || altTask.RouteJobType != "fallback" {
			t.Fatalf("alternate task lease = %+v, %v", altTask, err)
		}

		taskRoute.Alternate = &pgjobdb.AlternateRoute{JobType: "fallback"}
		if err := pgjobdb.RescheduleUnheldJob(ctx, db, "tenant", "task-to-job",
			"worker", taskRoute); err != nil {
			t.Fatalf("set task-to-job alternate: %v", err)
		}
		altJob, err := pgjobdb.GetJobLease(ctx, db, "tenant", "task-to-job",
			"worker", fallback, pgjobdb.GetJobLeaseOptions{})
		if err != nil || altJob == nil || altJob.WorkKind != pgjobdb.WorkKindJob ||
			altJob.Task == nil || altJob.Task.TaskType != "download" ||
			altJob.Task.OutputOrdinal != 3 {
			t.Fatalf("alternate job lease lost task context = %+v, %v", altJob, err)
		}
	})
}

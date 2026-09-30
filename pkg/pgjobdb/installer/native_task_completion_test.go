package installer_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/colony-2/pgjobdb/pkg/pgjobdb"
	"github.com/colony-2/pgjobdb/pkg/pgjobdb/installer"
)

func waitingClaim(t *testing.T, ctx context.Context, db *sql.DB, id string) pgjobdb.ClaimTaskRequest {
	t.Helper()
	_, err := pgjobdb.SubmitJob(ctx, db, pgjobdb.SubmitJobRequest{
		TenantID: "tenant", JobID: pgjobdb.JobID(id), WorkerID: "submitter", JobType: "collect",
		ClientPayloadUpdate: &pgjobdb.ClientPayloadUpdate{Mode: "reset", Value: json.RawMessage(`{"keep":true}`)},
	})
	if err != nil {
		t.Fatal(err)
	}
	task := pgjobdb.TaskWork{TaskType: "download", ResumeJobType: "resume", InputOrdinal: 2, OutputOrdinal: 3, InputHash: "sha256:input"}
	if err := pgjobdb.RescheduleUnheldJob(ctx, db, "tenant", pgjobdb.JobID(id), "worker", pgjobdb.RescheduleRequest{
		RouteJobType: "collect", WorkKind: pgjobdb.WorkKindTask, Task: &task,
		Alternate: &pgjobdb.AlternateRoute{JobType: "wrong", TaskType: "wrong", After: time.Hour},
	}); err != nil {
		t.Fatal(err)
	}
	return pgjobdb.ClaimTaskRequest{TenantID: "tenant", JobID: pgjobdb.JobID(id), WorkerID: "external", JobType: "collect", Task: task, LeaseDuration: 30 * time.Second}
}

func TestNativeTaskClaimGuards(t *testing.T) {
	runDatabaseTest(t, func(ctx context.Context, db *sql.DB) {
		req := waitingClaim(t, ctx, db, "guards")
		for name, mutate := range map[string]func(*pgjobdb.ClaimTaskRequest){
			"tenant":         func(r *pgjobdb.ClaimTaskRequest) { r.TenantID = "other" },
			"job":            func(r *pgjobdb.ClaimTaskRequest) { r.JobID = "missing" },
			"route":          func(r *pgjobdb.ClaimTaskRequest) { r.JobType = "other" },
			"task":           func(r *pgjobdb.ClaimTaskRequest) { r.Task.TaskType = "other" },
			"resume":         func(r *pgjobdb.ClaimTaskRequest) { r.Task.ResumeJobType = "other" },
			"input ordinal":  func(r *pgjobdb.ClaimTaskRequest) { r.Task.InputOrdinal++ },
			"output ordinal": func(r *pgjobdb.ClaimTaskRequest) { r.Task.OutputOrdinal++ },
			"input hash":     func(r *pgjobdb.ClaimTaskRequest) { r.Task.InputHash = "other" },
		} {
			t.Run(name, func(t *testing.T) {
				wrong := req
				mutate(&wrong)
				if claim, err := pgjobdb.ClaimTask(ctx, db, wrong); !errors.Is(err, pgjobdb.ErrTaskConflict) || claim.Identity.LeaseID != "" {
					t.Fatalf("claim=%+v err=%v", claim, err)
				}
			})
		}
		for _, tc := range []struct{ name, update string }{
			{"future", "available_at=clock_timestamp()+interval '1 hour'"},
			{"expired", "expires_at=clock_timestamp()-interval '1 second'"},
			{"dependency", "wait_for=ARRAY['guards']"},
			{"cancelled", "cancel_requested=true"},
			{"live lease", "lease_id='held',lease_worker_id='owner',lease_expires_at=clock_timestamp()+interval '1 hour'"},
		} {
			t.Run(tc.name, func(t *testing.T) {
				r := waitingClaim(t, ctx, db, tc.name)
				if _, err := db.ExecContext(ctx, "UPDATE pgjobdb.jobs SET "+tc.update+" WHERE tenant_id='tenant' AND job_id=$1", string(r.JobID)); err != nil {
					t.Fatal(err)
				}
				if _, err := pgjobdb.ClaimTask(ctx, db, r); !errors.Is(err, pgjobdb.ErrTaskConflict) {
					t.Fatalf("claim: %v", err)
				}
				row, err := pgjobdb.GetJob(ctx, db, r.TenantID, r.JobID)
				if err != nil || row.WorkKind != pgjobdb.WorkKindTask || string(row.ClientPayload) != `{"keep":true}` || row.ClientPayloadRevision != 1 {
					t.Fatalf("failed claim changed state: %+v %v", row, err)
				}
			})
		}
		if err := pgjobdb.CancelJob(ctx, db, pgjobdb.CancelJobRequest{TenantID: req.TenantID, JobID: req.JobID, WorkerID: "canceller"}); err != nil {
			t.Fatal(err)
		}
		if _, err := pgjobdb.ClaimTask(ctx, db, req); !errors.Is(err, pgjobdb.ErrTaskConflict) {
			t.Fatalf("archived/cancelled task: %v", err)
		}
	})
}

func TestNativeTaskClaimCommitsRecoveryRoute(t *testing.T) {
	runDatabaseTest(t, func(ctx context.Context, db *sql.DB) {
		req := waitingClaim(t, ctx, db, "claim")
		before, err := pgjobdb.GetJob(ctx, db, req.TenantID, req.JobID)
		if err != nil {
			t.Fatal(err)
		}
		claim, err := pgjobdb.ClaimTask(ctx, db, req)
		if err != nil {
			t.Fatal(err)
		}
		if claim.Identity.LeaseID == "" || claim.Identity.WorkerID != req.WorkerID || time.Until(claim.ExpiresAt) <= 0 || time.Until(claim.ExpiresAt) > 31*time.Second {
			t.Fatalf("claim: %+v", claim)
		}
		row, err := pgjobdb.GetJob(ctx, db, req.TenantID, req.JobID)
		if err != nil || row.WorkKind != pgjobdb.WorkKindJob || row.RouteJobType != "resume" || row.TaskType != "" || row.ResumeJobType != "" || row.TaskInputOrdinal != nil || row.TaskOutputOrdinal != nil || row.TaskInputHash != "" || row.AlternateJobType != "" || row.AlternateTaskType != "" || row.AlternateAfterSeconds != nil {
			t.Fatalf("recovery route: %+v %v", row, err)
		}
		if row.LeaseWorkerID != "external" || row.ClientPayloadRevision != before.ClientPayloadRevision || string(row.ClientPayload) != string(before.ClientPayload) || !row.AvailableAt.Equal(before.AvailableAt) {
			t.Fatalf("claim changed preserved state: %+v", row)
		}
		if _, err := pgjobdb.ClaimTask(ctx, db, req); !errors.Is(err, pgjobdb.ErrTaskConflict) {
			t.Fatalf("repeat claim: %v", err)
		}
		if _, err := pgjobdb.ValidateLease(ctx, db, claim.Identity); err != nil {
			t.Fatalf("claim not committed: %v", err)
		}
		if _, err := db.ExecContext(ctx, "UPDATE pgjobdb.jobs SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE job_id='claim'"); err != nil {
			t.Fatal(err)
		}
		taskLease, err := pgjobdb.GetJobLease(ctx, db, "tenant", "claim", "task-worker", pgjobdb.WorkSelector{Tasks: []pgjobdb.TaskSelector{{JobType: "collect", TaskType: "download"}, {JobType: "wrong", TaskType: "wrong"}}}, pgjobdb.GetJobLeaseOptions{})
		if err != nil || taskLease != nil {
			t.Fatalf("task got recovery work: %+v %v", taskLease, err)
		}
		jobLease, err := pgjobdb.GetJobLease(ctx, db, "tenant", "claim", "recovery", pgjobdb.WorkSelector{JobTypes: []pgjobdb.JobType{"resume"}}, pgjobdb.GetJobLeaseOptions{})
		if err != nil || jobLease == nil || jobLease.LeaseID == claim.Identity.LeaseID {
			t.Fatalf("recovery lease: %+v %v", jobLease, err)
		}
		if _, err := pgjobdb.ValidateLease(ctx, db, claim.Identity); !errors.Is(err, pgjobdb.ErrLeaseLost) {
			t.Fatalf("old lease: %v", err)
		}
	})
}

func TestNativeTaskClaimConcurrent(t *testing.T) {
	runDatabaseTest(t, func(ctx context.Context, db *sql.DB) {
		req := waitingClaim(t, ctx, db, "race")
		start := make(chan struct{})
		results := make(chan error, 8)
		for i := 0; i < cap(results); i++ {
			go func(i int) {
				<-start
				r := req
				r.WorkerID = pgjobdb.WorkerID(fmt.Sprintf("worker-%d", i))
				_, err := pgjobdb.ClaimTask(ctx, db, r)
				results <- err
			}(i)
		}
		close(start)
		winners := 0
		for i := 0; i < cap(results); i++ {
			err := <-results
			if err == nil {
				winners++
			} else if !errors.Is(err, pgjobdb.ErrTaskConflict) {
				t.Errorf("unexpected claim error: %v", err)
			}
		}
		if winners != 1 {
			t.Fatalf("winners=%d", winners)
		}
	})
}

func TestInstallerRequiresTaskClaim(t *testing.T) {
	runDatabaseTest(t, func(ctx context.Context, db *sql.DB) {
		if _, err := db.ExecContext(ctx, `DROP FUNCTION pgjobdb.claim_native_task(TEXT,TEXT,TEXT,TEXT,TEXT,TEXT,BIGINT,BIGINT,TEXT,INTEGER)`); err != nil {
			t.Fatal(err)
		}
		if err := (installer.Installer{DB: db}).Verify(ctx); err == nil {
			t.Fatal("missing task claim accepted")
		}
	})
}

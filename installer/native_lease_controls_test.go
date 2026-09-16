package installer_test

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/colony-2/pgjobdb"
)

func TestNativeLeaseControls(t *testing.T) {
	runDatabaseTest(t, func(ctx context.Context, db *sql.DB) {
		if _, err := pgjobdb.SubmitJob(ctx, db, pgjobdb.SubmitJobRequest{
			TenantID: "tenant", JobID: "job", WorkerID: "submitter", JobType: "collect",
		}); err != nil {
			t.Fatalf("submit: %v", err)
		}
		selector := pgjobdb.WorkSelector{JobTypes: []pgjobdb.JobType{"collect"}}
		lease, err := pgjobdb.GetWork(ctx, db, "worker", selector,
			pgjobdb.GetWorkOptions{LeaseDuration: 5 * time.Second})
		if err != nil || lease == nil {
			t.Fatalf("get work = %+v, %v", lease, err)
		}
		if _, err := pgjobdb.ValidateLease(ctx, db, lease.Identity()); err != nil {
			t.Fatalf("validate live lease: %v", err)
		}
		wrongWorker := lease.Identity()
		wrongWorker.WorkerID = "other-worker"
		if _, err := pgjobdb.ValidateLease(ctx, db, wrongWorker); !errors.Is(err, pgjobdb.ErrLeaseLost) {
			t.Fatalf("wrong worker validation = %v", err)
		}
		if err := pgjobdb.CompleteJob(ctx, db, wrongWorker,
			pgjobdb.Completion{Status: pgjobdb.CompletionSuccess}); err == nil {
			t.Fatal("expected wrong worker completion to fail")
		}
		before := lease.ExpiresAt
		if err := lease.KeepAlive(ctx, db, 20*time.Second); err != nil {
			t.Fatalf("keep alive: %v", err)
		}
		if !lease.ExpiresAt.After(before) {
			t.Fatalf("renewed expiry %s did not pass %s", lease.ExpiresAt, before)
		}
		if _, err := pgjobdb.KeepAliveLease(ctx, db, wrongWorker, time.Second); err == nil {
			t.Fatal("expected wrong worker renewal to fail")
		}
		if err := pgjobdb.CancelJob(ctx, db, pgjobdb.CancelJobRequest{
			TenantID: "tenant", JobID: "job", WorkerID: "operator", Reason: "stop",
		}); err != nil {
			t.Fatalf("cancel job: %v", err)
		}
		if _, err := pgjobdb.KeepAliveLease(ctx, db, lease.Identity(), time.Second); err == nil {
			t.Fatal("expected cancelled job renewal to fail")
		}
		if err := lease.Complete(ctx, db,
			pgjobdb.Completion{Status: pgjobdb.CompletionCancelled}); err != nil {
			t.Fatalf("complete cancelled job: %v", err)
		}
		if _, err := pgjobdb.ValidateLease(ctx, db, lease.Identity()); !errors.Is(err, pgjobdb.ErrLeaseLost) {
			t.Fatalf("completed lease validation = %v", err)
		}

		if _, err := pgjobdb.SubmitJob(ctx, db, pgjobdb.SubmitJobRequest{
			TenantID: "tenant", JobID: "expired", WorkerID: "submitter", JobType: "collect",
		}); err != nil {
			t.Fatalf("submit expired test job: %v", err)
		}
		expiring, err := pgjobdb.GetWork(ctx, db, "worker", selector,
			pgjobdb.GetWorkOptions{})
		if err != nil || expiring == nil {
			t.Fatalf("lease expiring job = %+v, %v", expiring, err)
		}
		if _, err := db.ExecContext(ctx, `UPDATE pgjobdb.jobs
			SET lease_expires_at = clock_timestamp() - interval '1 second'
			WHERE tenant_id = 'tenant' AND job_id = 'expired'`); err != nil {
			t.Fatalf("expire lease: %v", err)
		}
		if _, err := pgjobdb.ValidateLease(ctx, db, expiring.Identity()); !errors.Is(err, pgjobdb.ErrLeaseLost) {
			t.Fatalf("expired lease validation = %v", err)
		}
		if err := expiring.Complete(ctx, db,
			pgjobdb.Completion{Status: pgjobdb.CompletionSuccess}); err == nil {
			t.Fatal("expected expired lease completion to fail")
		}
	})
}

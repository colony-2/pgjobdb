package integration_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/pgjobdb/pkg/pgjobdb"
	"github.com/stretchr/testify/require"
)

func TestClearCrashConcernAtomicRecovery(t *testing.T) {
	runDatabaseTest(t, func(ctx context.Context, db *sql.DB) {
		_, err := submitCellJob(ctx, db, pgjobdb.SubmitJobRequest{TenantID: "tenant", JobID: "flapping", JobType: "job", WorkerID: "submitter", Cell: "test", CellRepo: "example.test/test"})
		require.NoError(t, err)
		lease, err := pgjobdb.GetJobLease(ctx, db, "tenant", "flapping", "worker", pgjobdb.WorkSelector{JobTypes: []pgjobdb.JobType{"job"}}, pgjobdb.GetJobLeaseOptions{})
		require.NoError(t, err)
		require.NotNil(t, lease)
		_, err = db.ExecContext(ctx, `UPDATE pgjobdb.jobs SET consecutive_expirations=5, lease_expiration_count=9, lease_expires_at=clock_timestamp()-interval '1 second' WHERE tenant_id='tenant' AND job_id='flapping'`)
		require.NoError(t, err)
		req := jobdb.ClearCrashConcernRequest{JobKey: jobdb.JobKey{TenantId: "tenant", JobId: "flapping"}, WorkerID: "operator", Reason: "worker fixed"}
		var before, after json.RawMessage
		require.NoError(t, db.QueryRowContext(ctx, `SELECT to_jsonb(j) FROM pgjobdb.jobs j WHERE tenant_id='tenant' AND job_id='flapping'`).Scan(&before))
		tx, err := db.BeginTx(ctx, nil)
		require.NoError(t, err)
		defer tx.Rollback()
		require.NoError(t, pgjobdb.ClearCrashConcern(ctx, tx, req))
		require.NoError(t, tx.Rollback())
		require.NoError(t, db.QueryRowContext(ctx, `SELECT to_jsonb(j) FROM pgjobdb.jobs j WHERE tenant_id='tenant' AND job_id='flapping'`).Scan(&after))
		require.JSONEq(t, string(before), string(after), "borrowed transaction rollback must restore the counter")
		require.NoError(t, pgjobdb.ClearCrashConcern(ctx, db, req))
		require.NoError(t, pgjobdb.ClearCrashConcern(ctx, db, req))
		require.NoError(t, db.QueryRowContext(ctx, `SELECT to_jsonb(j) FROM pgjobdb.jobs j WHERE tenant_id='tenant' AND job_id='flapping'`).Scan(&after))
		var expected map[string]any
		require.NoError(t, json.Unmarshal(before, &expected))
		expected["consecutive_expirations"] = 0
		encoded, err := json.Marshal(expected)
		require.NoError(t, err)
		require.JSONEq(t, string(encoded), string(after), "clear changes only the consecutive count")
		next, err := pgjobdb.GetJobLease(ctx, db, "tenant", "flapping", "next", pgjobdb.WorkSelector{JobTypes: []pgjobdb.JobType{"job"}}, pgjobdb.GetJobLeaseOptions{})
		require.NoError(t, err)
		require.NotNil(t, next)
		require.NotEqual(t, lease.LeaseID, next.LeaseID)
		require.ErrorIs(t, pgjobdb.ClearCrashConcern(ctx, db, req), jobdb.ErrConflict)
		req.JobKey.TenantId = "wrong"
		require.ErrorIs(t, pgjobdb.ClearCrashConcern(ctx, db, req), jobdb.ErrJobNotFound)
		req.JobKey.TenantId = "tenant"

		// An acquisition that wins the row lock must not subsequently have its
		// counter cleared using a stale pre-lock view of the job.
		tx, err = db.BeginTx(ctx, nil)
		require.NoError(t, err)
		defer tx.Rollback()
		_, err = tx.ExecContext(ctx, `UPDATE pgjobdb.jobs SET consecutive_expirations=5, lease_expires_at=clock_timestamp()+interval '1 minute' WHERE tenant_id='tenant' AND job_id='flapping'`)
		require.NoError(t, err)
		done := make(chan error, 1)
		go func() { done <- pgjobdb.ClearCrashConcern(ctx, db, req) }()
		select {
		case err := <-done:
			t.Fatalf("clear did not wait for the row lock: %v", err)
		case <-time.After(50 * time.Millisecond):
		}
		require.NoError(t, tx.Commit())
		require.ErrorIs(t, <-done, jobdb.ErrConflict)
		var count int
		require.NoError(t, db.QueryRowContext(ctx, `SELECT consecutive_expirations FROM pgjobdb.jobs WHERE tenant_id='tenant' AND job_id='flapping'`).Scan(&count))
		require.Equal(t, 5, count)
	})
}

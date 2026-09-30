package installer_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"testing"
	"time"

	"github.com/colony-2/pgjobdb/pkg/pgjobdb"
	"github.com/stretchr/testify/require"
)

func TestNativeLeaseExecutionSnapshot(t *testing.T) {
	runDatabaseTest(t, func(ctx context.Context, db *sql.DB) {
		for _, kind := range []string{"job", "task", "job-alternate", "task-to-job", "task-alternate"} {
			t.Run(kind, func(t *testing.T) {
				id := pgjobdb.JobID(kind)
				_, err := pgjobdb.SubmitJob(ctx, db, pgjobdb.SubmitJobRequest{
					TenantID: "tenant", JobID: id, WorkerID: "submitter", JobType: "primary",
					Runtime: pgjobdb.RuntimeMetadata{SchemaHash: "schema"}, RunPolicy: pgjobdb.RunPolicy{Retry: pgjobdb.RetryPolicy{MaximumAttempts: 3}},
					ClientPayloadUpdate: &pgjobdb.ClientPayloadUpdate{Mode: "reset", Value: json.RawMessage(`{"cursor":1}`)},
				})
				require.NoError(t, err)
				route := pgjobdb.RescheduleRequest{RouteJobType: "primary", WorkKind: pgjobdb.WorkKindJob}
				if kind == "task" || kind == "task-to-job" || kind == "task-alternate" {
					route.WorkKind = pgjobdb.WorkKindTask
					route.Task = &pgjobdb.TaskWork{TaskType: "task", ResumeJobType: "resume", InputOrdinal: 2, OutputOrdinal: 3, InputHash: "input-hash"}
				}
				if kind == "job-alternate" || kind == "task-to-job" || kind == "task-alternate" {
					route.Alternate = &pgjobdb.AlternateRoute{JobType: "fallback", After: time.Second}
					if kind == "task-alternate" {
						route.Alternate.TaskType = "fallback-task"
					}
				}
				require.NoError(t, pgjobdb.RescheduleUnheldJob(ctx, db, "tenant", id, "submitter", route))
				selector := pgjobdb.WorkSelector{JobTypes: []pgjobdb.JobType{"primary"}}
				if route.Task != nil {
					selector = pgjobdb.WorkSelector{Tasks: []pgjobdb.TaskSelector{{JobType: "primary", TaskType: "task"}}}
				}
				if route.Alternate != nil {
					selector = pgjobdb.WorkSelector{JobTypes: []pgjobdb.JobType{"fallback"}}
					if kind == "task-alternate" {
						selector = pgjobdb.WorkSelector{Tasks: []pgjobdb.TaskSelector{{JobType: "fallback", TaskType: "fallback-task"}}}
					}
					early, err := pgjobdb.GetJobLease(ctx, db, "tenant", id, "owner", selector, pgjobdb.GetJobLeaseOptions{})
					require.NoError(t, err)
					require.Nil(t, early)
					// Advance eligibility across the boundary without a wall-clock sleep.
					_, err = db.ExecContext(ctx, `UPDATE pgjobdb.jobs SET created_at=clock_timestamp()-interval '2 seconds', available_at=clock_timestamp()-interval '2 seconds' WHERE job_id=$1`, id)
					require.NoError(t, err)
				}
				acquired, err := pgjobdb.GetJobLease(ctx, db, "tenant", id, "owner", selector, pgjobdb.GetJobLeaseOptions{LeaseDuration: 10 * time.Second})
				require.NoError(t, err)
				require.NotNil(t, acquired)
				require.Equal(t, "schema", acquired.SchemaHash)
				if route.Alternate != nil {
					require.Equal(t, pgjobdb.JobType("fallback"), acquired.RouteJobType)
				}
				if kind == "task-alternate" {
					require.Equal(t, pgjobdb.TaskType("fallback-task"), acquired.Task.TaskType)
				}
				for i := 0; i < 3; i++ {
					valid, err := pgjobdb.ValidateLease(ctx, db, acquired.Identity())
					require.NoError(t, err)
					same := *valid
					same.ExpiresAt = acquired.ExpiresAt
					require.Equal(t, *acquired, same)
					before := time.Now()
					renewed, err := pgjobdb.KeepAliveLease(ctx, db, acquired.Identity(), 1500*time.Millisecond)
					require.NoError(t, err)
					require.WithinDuration(t, before.Add(2*time.Second), renewed.ExpiresAt, 500*time.Millisecond)
					same = *renewed
					same.ExpiresAt = acquired.ExpiresAt
					require.Equal(t, *acquired, same)
					var expiry time.Time
					require.NoError(t, db.QueryRowContext(ctx, `SELECT lease_expires_at FROM pgjobdb.jobs WHERE job_id=$1`, id).Scan(&expiry))
					require.True(t, expiry.Equal(renewed.ExpiresAt))
				}
				// An expired alternate lease must not consume or promote the alternate.
				// Before its delay elapses again, the original route is eligible.
				if route.Alternate != nil {
					_, err = db.ExecContext(ctx, `UPDATE pgjobdb.jobs SET lease_expires_at=clock_timestamp()-interval '1 millisecond', alternate_after_seconds=60 WHERE job_id=$1`, id)
					require.NoError(t, err)
					primary := pgjobdb.WorkSelector{JobTypes: []pgjobdb.JobType{"primary"}}
					if route.Task != nil {
						primary = pgjobdb.WorkSelector{Tasks: []pgjobdb.TaskSelector{{JobType: "primary", TaskType: "task"}}}
					}
					next, err := pgjobdb.GetJobLease(ctx, db, "tenant", id, "new-owner", primary, pgjobdb.GetJobLeaseOptions{})
					require.NoError(t, err)
					require.NotNil(t, next)
					require.NotEqual(t, acquired.LeaseID, next.LeaseID)
					require.Equal(t, pgjobdb.JobType("primary"), next.RouteJobType)
					_, err = pgjobdb.ValidateLease(ctx, db, acquired.Identity())
					require.ErrorIs(t, err, pgjobdb.ErrLeaseLost)
				}
			})
		}
	})
}

func TestNativeRenewalCannotReviveLeaseAfterLockWait(t *testing.T) {
	runDatabaseTest(t, func(ctx context.Context, db *sql.DB) {
		_, err := pgjobdb.SubmitJob(ctx, db, pgjobdb.SubmitJobRequest{TenantID: "tenant", JobID: "locked", WorkerID: "submitter", JobType: "job"})
		require.NoError(t, err)
		lease, err := pgjobdb.GetWork(ctx, db, "owner", pgjobdb.WorkSelector{JobTypes: []pgjobdb.JobType{"job"}}, pgjobdb.GetWorkOptions{LeaseDuration: time.Second})
		require.NoError(t, err)
		require.NotNil(t, lease)
		tx, err := db.BeginTx(ctx, nil)
		require.NoError(t, err)
		defer tx.Rollback()
		_, err = tx.ExecContext(ctx, `SELECT 1 FROM pgjobdb.jobs WHERE job_id='locked' FOR UPDATE`)
		require.NoError(t, err)
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		defer tx.Rollback() // Release the blocker before waiting for connection closure.
		var pid int
		require.NoError(t, conn.QueryRowContext(ctx, `SELECT pg_backend_pid()`).Scan(&pid))
		result := make(chan error, 1)
		go func() { _, err := pgjobdb.KeepAliveLease(ctx, conn, lease.Identity(), time.Minute); result <- err }()
		require.Eventually(t, func() bool {
			var blocked bool
			err := db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid=$1 AND wait_event_type='Lock')`, pid).Scan(&blocked)
			return err == nil && blocked
		}, time.Second, 5*time.Millisecond)
		time.Sleep(time.Until(lease.ExpiresAt) + 20*time.Millisecond)
		require.NoError(t, tx.Commit())
		require.Error(t, <-result)
		var expiry time.Time
		require.NoError(t, db.QueryRowContext(ctx, `SELECT lease_expires_at FROM pgjobdb.jobs WHERE job_id='locked'`).Scan(&expiry))
		require.True(t, expiry.Equal(lease.ExpiresAt), "renewal revived an expired lease")
	})
}

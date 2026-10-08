package installer_test

import (
	"context"
	"database/sql"
	"sync"
	"testing"
	"time"

	"github.com/colony-2/pgjobdb/pkg/pgjobdb"
	"github.com/stretchr/testify/require"
)

func TestCrashConcernExcludesNativeAcquisition(t *testing.T) {
	runDatabaseTest(t, func(ctx context.Context, db *sql.DB) {
		var threshold int
		require.NoError(t, db.QueryRowContext(ctx, `SELECT pgjobdb.crash_concern_threshold()`).Scan(&threshold))
		require.Equal(t, 5, threshold)
		// A non-default threshold proves acquisition follows the status view.
		_, err := db.ExecContext(ctx, `SELECT pgjobdb.set_crash_concern_threshold(2)`)
		require.NoError(t, err)
		for _, task := range []bool{false, true} {
			t.Run(map[bool]string{false: "job", true: "task-with-alternate"}[task], func(t *testing.T) {
				id := pgjobdb.JobID("flapping-job")
				selector := pgjobdb.WorkSelector{JobTypes: []pgjobdb.JobType{"collect"}}
				var claim pgjobdb.ClaimTaskRequest
				if task {
					id = "flapping-task"
					claim = waitingClaim(t, ctx, db, string(id))
					selector = pgjobdb.WorkSelector{Tasks: []pgjobdb.TaskSelector{{JobType: "collect", TaskType: "download"}, {JobType: "wrong", TaskType: "wrong"}}}
				} else {
					_, err := pgjobdb.SubmitJob(ctx, db, pgjobdb.SubmitJobRequest{TenantID: "tenant", JobID: id, JobType: "collect", WorkerID: "submitter"})
					require.NoError(t, err)
				}
				get := func() *pgjobdb.JobLease {
					lease, err := pgjobdb.GetJobLease(ctx, db, "tenant", id, "worker", selector, pgjobdb.GetJobLeaseOptions{})
					require.NoError(t, err)
					return lease
				}
				poll := func() *pgjobdb.JobLease {
					lease, err := pgjobdb.GetWork(ctx, db, "worker", selector, pgjobdb.GetWorkOptions{TenantIDs: []pgjobdb.TenantID{"tenant"}})
					require.NoError(t, err)
					return lease
				}
				expire := func() {
					_, err := db.ExecContext(ctx, `UPDATE pgjobdb.jobs SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE tenant_id='tenant' AND job_id=$1`, id)
					require.NoError(t, err)
				}
				count := func() int {
					var n int
					require.NoError(t, db.QueryRowContext(ctx, `SELECT consecutive_expirations FROM pgjobdb.jobs WHERE tenant_id='tenant' AND job_id=$1`, id).Scan(&n))
					return n
				}
				for n := 0; n <= 2; n++ {
					var lease *pgjobdb.JobLease
					if n%2 == 0 {
						lease = get()
					} else {
						lease = poll()
					}
					require.NotNil(t, lease)
					require.Equal(t, id, lease.JobID)
					require.Equal(t, n, count())
					_, err := pgjobdb.KeepAliveLease(ctx, db, lease.Identity(), time.Minute)
					require.NoError(t, err)
					require.Equal(t, n, count(), "renewal must not erase consecutive crashes")
					row, err := pgjobdb.GetJob(ctx, db, "tenant", id)
					require.NoError(t, err)
					require.Equal(t, pgjobdb.JobStatusActive, row.Status)
					expire()
				}
				if task {
					_, err := db.ExecContext(ctx, `UPDATE pgjobdb.jobs SET alternate_at=clock_timestamp()-interval '1 second' WHERE tenant_id='tenant' AND job_id=$1`, id)
					require.NoError(t, err)
					_, err = pgjobdb.ClaimTask(ctx, db, claim)
					require.ErrorIs(t, err, pgjobdb.ErrTaskConflict)
				}
				before, err := pgjobdb.GetJob(ctx, db, "tenant", id)
				require.NoError(t, err)
				require.Equal(t, pgjobdb.JobStatusCrashConcern, before.Status)
				for i := 0; i < 2; i++ {
					require.Nil(t, get())
					require.Nil(t, poll())
				}
				after, err := pgjobdb.GetJob(ctx, db, "tenant", id)
				require.NoError(t, err)
				require.Equal(t, before, after, "rejected acquisition must not promote alternates or change the lease")
				_, err = db.ExecContext(ctx, `SELECT pgjobdb.clear_crash_concern('tenant',$1,'operator','investigated')`, id)
				require.NoError(t, err)
				lease := get()
				require.NotNil(t, lease, "explicit clearing restores acquisition")
				require.NoError(t, lease.Reschedule(ctx, db, pgjobdb.RescheduleRequest{RouteJobType: "reset", WorkKind: pgjobdb.WorkKindJob}))
				require.Zero(t, count(), "successful rescheduling resets the streak")
				selector = pgjobdb.WorkSelector{JobTypes: []pgjobdb.JobType{"reset"}}
				require.NotNil(t, get())
				expire()
				row, err := pgjobdb.GetJob(ctx, db, "tenant", id)
				require.NoError(t, err)
				require.Equal(t, pgjobdb.JobStatusReady, row.Status)
			})
		}
	})
}

func TestNativeReadyAcquisitionPreservesRowLocking(t *testing.T) {
	runDatabaseTest(t, func(ctx context.Context, db *sql.DB) {
		_, err := pgjobdb.SubmitJob(ctx, db, pgjobdb.SubmitJobRequest{TenantID: "tenant", JobID: "job", JobType: "collect", WorkerID: "submitter"})
		require.NoError(t, err)
		selector := pgjobdb.WorkSelector{JobTypes: []pgjobdb.JobType{"collect"}}
		tx, err := db.BeginTx(ctx, nil)
		require.NoError(t, err)
		defer tx.Rollback()
		_, err = tx.ExecContext(ctx, `SELECT 1 FROM pgjobdb.jobs WHERE tenant_id='tenant' AND job_id='job' FOR UPDATE`)
		require.NoError(t, err)
		short, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()
		lease, err := pgjobdb.GetWork(short, db, "worker", selector, pgjobdb.GetWorkOptions{})
		require.NoError(t, err, "status-view filtering must preserve SKIP LOCKED")
		require.Nil(t, lease)
		require.NoError(t, tx.Rollback())

		start := make(chan struct{})
		var wg sync.WaitGroup
		leases := make([]*pgjobdb.JobLease, 2)
		errs := make([]error, 2)
		for i := range leases {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				<-start
				leases[i], errs[i] = pgjobdb.GetJobLease(ctx, db, "tenant", "job", "worker", selector, pgjobdb.GetJobLeaseOptions{})
			}(i)
		}
		close(start)
		wg.Wait()
		acquired := 0
		for i, lease := range leases {
			require.NoError(t, errs[i])
			if lease != nil {
				acquired++
			}
		}
		require.Equal(t, 1, acquired, "concurrent claimers must not lease the same ready job")
	})
}

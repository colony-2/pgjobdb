package installer_test

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/colony-2/pgjobdb/pkg/pgjobdb"
	"github.com/colony-2/pgjobdb/pkg/pgjobdb/installer"
	"github.com/stretchr/testify/require"
)

func TestNativeJobDeletionRollbackAndTime(t *testing.T) {
	runDatabaseTest(t, func(ctx context.Context, db *sql.DB) {
		for _, id := range []pgjobdb.JobID{"a", "b"} {
			_, err := pgjobdb.SubmitJob(ctx, db, pgjobdb.SubmitJobRequest{TenantID: "tenant", JobID: id, WorkerID: "submit", JobType: "job"})
			require.NoError(t, err)
		}
		lease, err := pgjobdb.GetJobLease(ctx, db, "tenant", "a", "worker", pgjobdb.WorkSelector{JobTypes: []pgjobdb.JobType{"job"}}, pgjobdb.GetJobLeaseOptions{LeaseDuration: time.Hour})
		require.NoError(t, err)
		require.NotNil(t, lease)
		_, err = db.ExecContext(ctx, `CREATE FUNCTION pgjobdb.reject_second_hide() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.job_id='b' AND NEW.hidden_at IS NOT NULL THEN RAISE EXCEPTION 'injected deletion failure'; END IF; RETURN NEW; END $$;
CREATE TRIGGER reject_second_hide BEFORE UPDATE ON pgjobdb.job_facts FOR EACH ROW EXECUTE FUNCTION pgjobdb.reject_second_hide()`)
		require.NoError(t, err)
		now := time.Date(2026, 10, 19, 12, 34, 56, 0, time.UTC)
		req := pgjobdb.DeleteJobsRequest{TenantID: "tenant", All: true, Now: now}
		result, err := pgjobdb.DeleteJobs(ctx, db, req)
		require.ErrorContains(t, err, "injected deletion failure")
		require.Equal(t, pgjobdb.DeleteJobsResult{}, result)
		_, err = pgjobdb.ValidateLease(ctx, db, lease.Identity())
		require.NoError(t, err, "partial deletion must roll back revocation")
		page, err := pgjobdb.ListJobs(ctx, db, pgjobdb.ListJobsOptions{TenantIDs: []pgjobdb.TenantID{"tenant"}})
		require.NoError(t, err)
		require.Len(t, page.Jobs, 2)
		var count int
		require.NoError(t, db.QueryRowContext(ctx, `SELECT count(*) FROM pgjobdb.jobs_archive`).Scan(&count))
		require.Zero(t, count)
		require.NoError(t, db.QueryRowContext(ctx, `SELECT count(*) FROM pgjobdb.jobs_trace WHERE event_type='job_deleted'`).Scan(&count))
		require.Zero(t, count)
		_, err = db.ExecContext(ctx, `DROP TRIGGER reject_second_hide ON pgjobdb.job_facts`)
		require.NoError(t, err)
		result, err = pgjobdb.DeleteJobs(ctx, db, req)
		require.NoError(t, err)
		require.Equal(t, pgjobdb.DeleteJobsResult{MatchedCount: 2, DeletedCount: 2}, result)
		require.NoError(t, db.QueryRowContext(ctx, `SELECT count(*) FROM pgjobdb.job_facts f JOIN pgjobdb.jobs_archive a USING(tenant_id,job_id) JOIN pgjobdb.jobs_trace t USING(tenant_id,job_id) WHERE f.hidden_at=$1 AND a.archived_at=$1 AND a.cancel_requested_at=$1 AND t.event_type='job_deleted' AND t.event_at=$1 AND a.lease_id IS NULL AND a.completion_status='cancelled' AND a.completion_detail='job deleted'`, now).Scan(&count))
		require.Equal(t, 2, count)
		_, err = pgjobdb.ValidateLease(ctx, db, lease.Identity())
		require.ErrorIs(t, err, pgjobdb.ErrLeaseLost)
		require.NoError(t, (installer.Installer{DB: db}).Apply(ctx))
		result, err = pgjobdb.DeleteJobs(ctx, db, req)
		require.NoError(t, err)
		require.Zero(t, result.MatchedCount)
	})
}

func TestNativeJobDeletionRaces(t *testing.T) {
	runDatabaseTest(t, func(ctx context.Context, db *sql.DB) {
		submit := func(id pgjobdb.JobID) *pgjobdb.JobLease {
			_, err := pgjobdb.SubmitJob(ctx, db, pgjobdb.SubmitJobRequest{TenantID: "tenant", JobID: id, WorkerID: "submit", JobType: "job"})
			require.NoError(t, err)
			l, err := pgjobdb.GetJobLease(ctx, db, "tenant", id, "worker", pgjobdb.WorkSelector{JobTypes: []pgjobdb.JobType{"job"}}, pgjobdb.GetJobLeaseOptions{LeaseDuration: time.Hour})
			require.NoError(t, err)
			require.NotNil(t, l)
			return l
		}
		t.Run("completion_first", func(t *testing.T) {
			l := submit("completed")
			tx, err := db.BeginTx(ctx, nil)
			require.NoError(t, err)
			defer tx.Rollback()
			require.NoError(t, pgjobdb.CompleteJob(ctx, tx, l.Identity(), pgjobdb.Completion{Status: pgjobdb.CompletionSuccess, Detail: "original outcome"}))
			done := make(chan error, 1)
			go func() {
				_, err := pgjobdb.DeleteJobs(ctx, db, pgjobdb.DeleteJobsRequest{TenantID: "tenant", JobIDs: []pgjobdb.JobID{"completed"}})
				done <- err
			}()
			waitForDeletionLock(t, db)
			require.NoError(t, tx.Commit())
			require.NoError(t, <-done)
			row, err := pgjobdb.GetJob(ctx, db, "tenant", "completed")
			require.NoError(t, err)
			require.Equal(t, pgjobdb.CompletionSuccess, *row.CompletionStatus)
			require.Equal(t, "original outcome", *row.CompletionDetail)
		})
		t.Run("deletion_first", func(t *testing.T) {
			l := submit("cancelled")
			tx, err := db.BeginTx(ctx, nil)
			require.NoError(t, err)
			defer tx.Rollback()
			_, err = pgjobdb.DeleteJobs(ctx, tx, pgjobdb.DeleteJobsRequest{TenantID: "tenant", JobIDs: []pgjobdb.JobID{"cancelled"}})
			require.NoError(t, err)
			done := make(chan error, 1)
			go func() {
				done <- pgjobdb.CompleteJob(ctx, db, l.Identity(), pgjobdb.Completion{Status: pgjobdb.CompletionSuccess})
			}()
			require.Eventually(t, func() bool {
				var n int
				err := db.QueryRowContext(ctx, `SELECT count(*) FROM pg_stat_activity WHERE query LIKE '%complete_native_job(%' AND pid<>pg_backend_pid() AND cardinality(pg_blocking_pids(pid))>0`).Scan(&n)
				return err == nil && n > 0
			}, 5*time.Second, 10*time.Millisecond)
			require.NoError(t, tx.Commit())
			require.Error(t, <-done)
			row, err := pgjobdb.GetJob(ctx, db, "tenant", "cancelled")
			require.NoError(t, err)
			require.Equal(t, pgjobdb.CompletionCancelled, *row.CompletionStatus)
		})
		t.Run("duplicate_deletion", func(t *testing.T) {
			submit("duplicate")
			tx, err := db.BeginTx(ctx, nil)
			require.NoError(t, err)
			defer tx.Rollback()
			req := pgjobdb.DeleteJobsRequest{TenantID: "tenant", JobIDs: []pgjobdb.JobID{"duplicate"}}
			first, err := pgjobdb.DeleteJobs(ctx, tx, req)
			require.NoError(t, err)
			require.EqualValues(t, 1, first.DeletedCount)
			type result struct {
				count int64
				err   error
			}
			done := make(chan result, 1)
			go func() { r, err := pgjobdb.DeleteJobs(ctx, db, req); done <- result{r.DeletedCount, err} }()
			waitForDeletionLock(t, db)
			require.NoError(t, tx.Commit())
			second := <-done
			require.NoError(t, second.err)
			require.Zero(t, second.count)
		})
	})
}
func waitForDeletionLock(t *testing.T, db *sql.DB) {
	t.Helper()
	require.Eventually(t, func() bool {
		var n int
		err := db.QueryRow(`SELECT count(*) FROM pg_stat_activity WHERE query LIKE '%delete_native_jobs(%' AND pid<>pg_backend_pid() AND cardinality(pg_blocking_pids(pid))>0`).Scan(&n)
		return err == nil && n > 0
	}, 5*time.Second, 10*time.Millisecond)
}

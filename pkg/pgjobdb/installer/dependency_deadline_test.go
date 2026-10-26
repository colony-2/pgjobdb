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

func TestNativeDependencyDeadline(t *testing.T) {
	runDatabaseTest(t, func(ctx context.Context, db *sql.DB) {
		for _, id := range []pgjobdb.JobID{"parent", "child1", "child2", "unfinished"} {
			_, err := pgjobdb.SubmitJob(ctx, db, pgjobdb.SubmitJobRequest{TenantID: "tenant", JobID: id, WorkerID: "submit", JobType: pgjobdb.JobType(id)})
			require.NoError(t, err)
		}
		selector := pgjobdb.WorkSelector{JobTypes: []pgjobdb.JobType{"parent"}}
		acquire := func() *pgjobdb.JobLease {
			l, err := pgjobdb.GetWork(ctx, db, "worker", selector, pgjobdb.GetWorkOptions{TenantIDs: []pgjobdb.TenantID{"tenant"}})
			require.NoError(t, err)
			return l
		}
		lease := acquire()
		require.NotNil(t, lease)
		deadline := time.Now().Add(time.Hour)
		req := pgjobdb.RescheduleRequest{RouteJobType: "parent", WorkKind: pgjobdb.WorkKindJob, WaitFor: []pgjobdb.JobID{"child1", "child2"}, AvailableAt: &deadline, WaitUntilIsDeadline: true}
		require.NoError(t, lease.Reschedule(ctx, db, req))
		require.Nil(t, acquire())
		require.NoError(t, pgjobdb.CompleteUnheldJob(ctx, db, "tenant", "child1", "worker", pgjobdb.Completion{Status: pgjobdb.CompletionSuccess}))
		require.Nil(t, acquire(), "one unfinished child must still block")
		require.NoError(t, pgjobdb.CompleteUnheldJob(ctx, db, "tenant", "child2", "worker", pgjobdb.Completion{Status: pgjobdb.CompletionFailedApp}))
		lease = acquire()
		require.NotNil(t, lease, "all terminal children release the parent before its deadline")

		task := &pgjobdb.TaskWork{TaskType: "answer", ResumeJobType: "parent", InputOrdinal: 0, OutputOrdinal: 1, InputHash: "input"}
		req.WorkKind = pgjobdb.WorkKindTask
		req.Task = task
		req.WaitFor = []pgjobdb.JobID{"unfinished"}
		require.NoError(t, lease.Reschedule(ctx, db, req))
		claim := pgjobdb.ClaimTaskRequest{TenantID: "tenant", JobID: "parent", WorkerID: "answerer", JobType: "parent", Task: *task, LeaseDuration: time.Minute}
		_, err := pgjobdb.ClaimTask(ctx, db, claim)
		require.ErrorIs(t, err, pgjobdb.ErrTaskConflict)
		// Reinstallation must preserve the wait mode and its fixed deadline.
		require.NoError(t, (installer.Installer{DB: db}).Apply(ctx))
		var saved time.Time
		var isDeadline bool
		require.NoError(t, db.QueryRowContext(ctx, `SELECT available_at,wait_until_is_deadline FROM pgjobdb.jobs WHERE tenant_id='tenant' AND job_id='parent'`).Scan(&saved, &isDeadline))
		require.True(t, isDeadline)
		require.WithinDuration(t, deadline, saved, time.Microsecond)
		_, err = db.ExecContext(ctx, `UPDATE pgjobdb.jobs SET available_at=clock_timestamp()-interval '1 second' WHERE tenant_id='tenant' AND job_id='parent'`)
		require.NoError(t, err)
		claimed, err := pgjobdb.ClaimTask(ctx, db, claim)
		require.NoError(t, err)
		// A later ordinary delay must clear the old dependency-deadline mode.
		require.NoError(t, pgjobdb.RescheduleJob(ctx, db, claimed.Identity, pgjobdb.RescheduleRequest{RouteJobType: "parent", WorkKind: pgjobdb.WorkKindJob, AvailableAt: &deadline}))
		require.Nil(t, acquire())
		require.NoError(t, db.QueryRowContext(ctx, `SELECT wait_until_is_deadline FROM pgjobdb.jobs WHERE tenant_id='tenant' AND job_id='parent'`).Scan(&isDeadline))
		require.False(t, isDeadline)
	})
}

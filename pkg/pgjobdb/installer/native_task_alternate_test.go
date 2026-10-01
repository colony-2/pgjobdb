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

func alternateWaitingTask(t *testing.T, ctx context.Context, db *sql.DB, id string, after time.Duration) pgjobdb.ClaimTaskRequest {
	t.Helper()
	tenant := pgjobdb.TenantID(id)
	_, err := pgjobdb.UpsertSchedule(ctx, db, pgjobdb.UpsertScheduleRequest{TenantID: tenant, ScheduleID: "daily", State: pgjobdb.ScheduleStateActive, SpecHash: "hash", Trigger: json.RawMessage(`{}`), TargetJobType: "collect", TargetSnapshot: json.RawMessage(`{}`), OverlapPolicy: "ALLOW", FailurePolicy: json.RawMessage(`{}`)})
	require.NoError(t, err)
	_, err = pgjobdb.SubmitJob(ctx, db, pgjobdb.SubmitJobRequest{TenantID: tenant, JobID: "job", WorkerID: "submitter", JobType: "collect", Runtime: pgjobdb.RuntimeMetadata{Schedule: &pgjobdb.ScheduleOccurrence{ScheduleID: "daily", Generation: 1, SpecHash: "hash", ScheduledAt: time.Now().UTC(), RunID: id}}})
	require.NoError(t, err)
	task := pgjobdb.TaskWork{TaskType: "external", ResumeJobType: "collect", InputOrdinal: 1, OutputOrdinal: 2, InputHash: "hash"}
	require.NoError(t, pgjobdb.RescheduleUnheldJob(ctx, db, tenant, "job", "submitter", pgjobdb.RescheduleRequest{RouteJobType: "collect", WorkKind: pgjobdb.WorkKindTask, Task: &task, Alternate: &pgjobdb.AlternateRoute{JobType: "collect", TaskType: "fallback", After: after}}))
	return pgjobdb.ClaimTaskRequest{TenantID: tenant, JobID: "job", WorkerID: "human", JobType: "collect", Task: task, LeaseDuration: time.Second}
}

func assertAlternatePendingListings(t *testing.T, ctx context.Context, db *sql.DB, req pgjobdb.ClaimTaskRequest) pgjobdb.JobDetail {
	t.Helper()
	page, err := pgjobdb.ListJobs(ctx, db, pgjobdb.ListJobsOptions{TenantIDs: []pgjobdb.TenantID{req.TenantID}, TaskSelectors: []pgjobdb.TaskSelector{{JobType: "collect", TaskType: "external"}}})
	require.NoError(t, err)
	require.Len(t, page.Jobs, 1)
	runs, err := pgjobdb.ListScheduleRuns(ctx, db, pgjobdb.ListScheduleRunsOptions{TenantID: req.TenantID, ScheduleID: "daily"})
	require.NoError(t, err)
	require.Len(t, runs.Runs, 1)
	for _, row := range []pgjobdb.JobDetail{page.Jobs[0], runs.Runs[0]} {
		require.Equal(t, pgjobdb.TaskType("external"), row.TaskType)
		require.Equal(t, pgjobdb.TaskType("fallback"), row.AlternateTaskType)
		require.NotNil(t, row.AlternateAt)
		require.Equal(t, req.Task.InputHash, row.TaskInputHash)
	}
	return page.Jobs[0]
}

func TestNativeTaskAlternateLifecycle(t *testing.T) {
	runDatabaseTest(t, func(ctx context.Context, db *sql.DB) {
		for _, method := range []string{"poll", "direct"} {
			t.Run(method, func(t *testing.T) {
				req := alternateWaitingTask(t, ctx, db, method, 1250*time.Millisecond)
				acquire := func(task pgjobdb.TaskType) (*pgjobdb.JobLease, error) {
					selector := pgjobdb.WorkSelector{Tasks: []pgjobdb.TaskSelector{{JobType: "collect", TaskType: task}}}
					if method == "poll" {
						return pgjobdb.GetWork(ctx, db, "worker", selector, pgjobdb.GetWorkOptions{TenantIDs: []pgjobdb.TenantID{req.TenantID}, LeaseDuration: 2 * time.Second})
					}
					return pgjobdb.GetJobLease(ctx, db, req.TenantID, req.JobID, "worker", selector, pgjobdb.GetJobLeaseOptions{LeaseDuration: 2 * time.Second})
				}
				before := assertAlternatePendingListings(t, ctx, db, req)
				require.Equal(t, 1.25, *before.AlternateAfterSeconds)
				early, err := acquire("fallback")
				require.NoError(t, err)
				require.Nil(t, early)
				primary, err := acquire("external")
				require.NoError(t, err)
				require.NotNil(t, primary)
				renewed, err := pgjobdb.KeepAliveLease(ctx, db, primary.Identity(), 2*time.Second)
				require.NoError(t, err)
				held := assertAlternatePendingListings(t, ctx, db, req)
				require.True(t, before.AlternateAt.Equal(*held.AlternateAt), "primary acquisition/renewal moved alternate eligibility")
				_, err = pgjobdb.ClaimTask(ctx, db, req)
				require.ErrorIs(t, err, pgjobdb.ErrTaskConflict)
				// Allow the real primary lease to expire. The alternate is already due;
				// there must be no new delay measured from that expiration.
				time.Sleep(time.Until(renewed.ExpiresAt) + 10*time.Millisecond)
				noMatch, err := acquire("external")
				require.NoError(t, err)
				require.Nil(t, noMatch)
				assertAlternatePendingListings(t, ctx, db, req)
				fallback, err := acquire("fallback")
				require.NoError(t, err)
				require.NotNil(t, fallback)
				require.NotEqual(t, primary.LeaseID, fallback.LeaseID)
				require.Equal(t, pgjobdb.TaskType("fallback"), fallback.Task.TaskType)
				require.Equal(t, req.Task.InputOrdinal, fallback.Task.InputOrdinal)
				require.Equal(t, req.Task.OutputOrdinal, fallback.Task.OutputOrdinal)
				require.Equal(t, req.Task.InputHash, fallback.Task.InputHash)
				row, err := pgjobdb.GetJob(ctx, db, req.TenantID, req.JobID)
				require.NoError(t, err)
				require.Equal(t, pgjobdb.TaskType("fallback"), row.TaskType)
				require.Empty(t, row.AlternateJobType)
				require.Nil(t, row.AlternateAt)
				require.Nil(t, row.AlternateAfterSeconds)
				validated, err := pgjobdb.ValidateLease(ctx, db, fallback.Identity())
				require.NoError(t, err)
				require.Equal(t, fallback, validated)
				again, err := pgjobdb.KeepAliveLease(ctx, db, fallback.Identity(), time.Second)
				require.NoError(t, err)
				require.Equal(t, fallback.Task, again.Task)
				_, err = pgjobdb.ClaimTask(ctx, db, req)
				require.ErrorIs(t, err, pgjobdb.ErrTaskConflict)
				_, err = db.ExecContext(ctx, `UPDATE pgjobdb.jobs SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE tenant_id=$1`, req.TenantID)
				require.NoError(t, err)
				_, err = pgjobdb.ClaimTask(ctx, db, req)
				require.ErrorIs(t, err, pgjobdb.ErrTaskConflict, "promotion must survive alternate-worker failure")
				recovery, err := acquire("fallback")
				require.NoError(t, err)
				require.NotNil(t, recovery)
				require.Equal(t, fallback.Task, recovery.Task)
			})
		}
		for _, phase := range []string{"before-due", "after-due"} {
			t.Run("completion/"+phase, func(t *testing.T) {
				req := alternateWaitingTask(t, ctx, db, phase, time.Hour)
				if phase == "after-due" {
					_, err := db.ExecContext(ctx, `UPDATE pgjobdb.jobs SET alternate_at=clock_timestamp()-interval '1 second' WHERE tenant_id=$1`, req.TenantID)
					require.NoError(t, err)
				}
				assertAlternatePendingListings(t, ctx, db, req)
				claim, err := pgjobdb.ClaimTask(ctx, db, req)
				require.NoError(t, err)
				lease, err := pgjobdb.ValidateLease(ctx, db, claim.Identity)
				require.NoError(t, err)
				require.Equal(t, pgjobdb.WorkKindJob, lease.WorkKind)
				require.Nil(t, lease.Task)
			})
		}
	})
}

func TestNativeTaskAlternateOwnershipRace(t *testing.T) {
	runDatabaseTest(t, func(ctx context.Context, db *sql.DB) {
		for _, method := range []string{"poll", "direct"} {
			t.Run(method, func(t *testing.T) {
				for i := 0; i < 8; i++ {
					req := alternateWaitingTask(t, ctx, db, t.Name()+"/"+string(rune('a'+i)), 0)
					start := make(chan struct{})
					completion := make(chan error, 1)
					go func() { <-start; _, err := pgjobdb.ClaimTask(ctx, db, req); completion <- err }()
					close(start)
					selector := pgjobdb.WorkSelector{Tasks: []pgjobdb.TaskSelector{{JobType: "collect", TaskType: "fallback"}}}
					var lease *pgjobdb.JobLease
					var err error
					if method == "poll" {
						lease, err = pgjobdb.GetWork(ctx, db, "fallback", selector, pgjobdb.GetWorkOptions{TenantIDs: []pgjobdb.TenantID{req.TenantID}})
					} else {
						lease, err = pgjobdb.GetJobLease(ctx, db, req.TenantID, req.JobID, "fallback", selector, pgjobdb.GetJobLeaseOptions{})
					}
					require.NoError(t, err)
					completionErr := <-completion
					if lease == nil {
						require.NoError(t, completionErr)
					} else {
						require.ErrorIs(t, completionErr, pgjobdb.ErrTaskConflict)
					}
				}
			})
		}
	})
}

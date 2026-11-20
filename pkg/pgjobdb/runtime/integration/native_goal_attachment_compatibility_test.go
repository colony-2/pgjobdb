package integration_test

import (
	"context"
	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

// Native compatibility deliberately retains real attachment, outside consumer paths.
func nativeGoalRuntimes(t *testing.T, run func(*testing.T, jobdb.WorkflowRuntime)) {
	run(t, conformanceRuntimeFactory(t)(t))
}
func TestNativeAttachmentCompatibilityGoalsLifecycle(t *testing.T) {
	nativeGoalRuntimes(t, func(t *testing.T, r jobdb.WorkflowRuntime) {
		ctx := celltest.Context(context.Background())
		a := goalSubmit(t, r, "a")
		info, err := r.GetGoal(ctx, goalKey(a))
		require.NoError(t, err)
		require.Equal(t, jobdb.GoalOpen, info.State)
		require.EqualValues(t, 1, info.Revision)
		consumer := goalSubmit(t, r, "consumer", jobdb.Dependency{Target: jobdb.DependencyTarget{GoalID: a.GoalID}, Condition: jobdb.JobPrereqSuccess})
		require.Nil(t, goalLease(t, r, consumer))
		goalComplete(t, r, a, false)
		after, err := r.GetGoal(ctx, goalKey(a))
		require.NoError(t, err)
		require.Equal(t, info, after)
		require.Nil(t, goalLease(t, r, consumer))
		restart := jobdb.SubmitRestartJobRequest{RequestID: "replace-a", Job: jobdb.SubmitRestartJob{Cell: "test", PriorJobKey: a.JobKey, ShareGoal: true, LastStepToKeep: 0}}
		b, err := r.SubmitRestartJob(ctx, restart)
		require.NoError(t, err)
		require.Equal(t, a.GoalID, b.GoalID)
		other := goalSubmit(t, r, "other")
		attached, err := r.AddGoalCandidate(ctx, jobdb.AddGoalCandidateRequest{GoalKey: goalKey(a), JobID: other.JobKey.JobId})
		require.NoError(t, err)
		repeat, err := r.AddGoalCandidate(ctx, jobdb.AddGoalCandidateRequest{GoalKey: goalKey(a), JobID: other.JobKey.JobId})
		require.NoError(t, err)
		require.Equal(t, attached, repeat)
		goalComplete(t, r, b, true)
		retry, err := r.SubmitRestartJob(ctx, restart)
		require.NoError(t, err)
		require.Equal(t, b, retry)
		wait, err := r.GetDependencyWait(ctx, jobdb.DependencyWaitKey{JobKey: consumer.JobKey, WaitID: "submission"})
		require.NoError(t, err)
		require.Equal(t, jobdb.DependencySatisfied, wait.State)
		require.Equal(t, b.JobKey.JobId, wait.Dependencies[0].SatisfiedByJobID)
		require.NotNil(t, goalLease(t, r, consumer))
		goalComplete(t, r, other, true)
		winner, err := r.GetGoal(ctx, goalKey(a))
		require.NoError(t, err)
		require.Equal(t, b.JobKey.JobId, winner.SuccessfulJobID)
		require.EqualValues(t, 2, winner.Revision)
		own, err := r.GetGoal(ctx, goalKey(other))
		require.NoError(t, err)
		require.Equal(t, other.JobKey.JobId, own.SuccessfulJobID)
		job, err := r.GetJob(ctx, other.JobKey)
		require.NoError(t, err)
		require.Equal(t, []string{a.GoalID}, job.OtherGoalIDs)
		list, err := r.ListJobs(ctx, jobdb.ListJobsRequest{TenantIds: []string{"tenant"}, GoalIDs: []string{a.GoalID, other.GoalID}, PageSize: 100})
		require.NoError(t, err)
		require.Len(t, list.Jobs, 3)
		_, err = r.FailGoal(ctx, jobdb.FailGoalRequest{GoalKey: goalKey(a), Message: "late"})
		require.ErrorIs(t, err, jobdb.ErrGoalConflict)
		fresh := goalSubmit(t, r, "fresh")
		_, err = r.AddGoalCandidate(ctx, jobdb.AddGoalCandidateRequest{GoalKey: goalKey(a), JobID: fresh.JobKey.JobId})
		require.ErrorIs(t, err, jobdb.ErrGoalConflict)
	})
}
func TestNativeAttachmentCompatibilityGoalAttachSuccessCycleAndDurableWait(t *testing.T) {
	nativeGoalRuntimes(t, func(t *testing.T, r jobdb.WorkflowRuntime) {
		ctx := celltest.Context(context.Background())
		a := goalSubmit(t, r, "a")
		b := goalSubmit(t, r, "b")
		lease := goalLease(t, r, b)
		req := jobdb.RescheduleExecutionRequest{NextRoute: jobdb.Route{JobType: "work"}, DependencyWait: &jobdb.DependencyWait{WaitID: "join-1", Dependencies: []jobdb.Dependency{{Target: jobdb.DependencyTarget{GoalID: a.GoalID}, Condition: jobdb.JobPrereqSuccess}}}}
		require.NoError(t, lease.Reschedule(ctx, req))
		_, err := r.AddGoalCandidate(ctx, jobdb.AddGoalCandidateRequest{GoalKey: goalKey(a), JobID: b.JobKey.JobId})
		require.ErrorIs(t, err, jobdb.ErrDependencyCycle)
		bi, err := r.GetJob(ctx, b.JobKey)
		require.NoError(t, err)
		require.Empty(t, bi.OtherGoalIDs)
		winner := goalSubmit(t, r, "winner")
		goalComplete(t, r, winner, true)
		_, err = r.AddGoalCandidate(ctx, jobdb.AddGoalCandidateRequest{GoalKey: goalKey(a), JobID: winner.JobKey.JobId})
		require.NoError(t, err)
		wait, err := r.GetDependencyWait(ctx, jobdb.DependencyWaitKey{JobKey: b.JobKey, WaitID: "join-1"})
		require.NoError(t, err)
		require.Equal(t, winner.JobKey.JobId, wait.Dependencies[0].SatisfiedByJobID)
		lease = goalLease(t, r, b)
		require.NotNil(t, lease)
		require.NoError(t, lease.Reschedule(ctx, req)) // Same descriptor retains binding.
		lease = goalLease(t, r, b)
		require.NotNil(t, lease)
		req.DependencyWait.Dependencies[0].Target.GoalID = b.GoalID
		require.ErrorIs(t, lease.Reschedule(ctx, req), jobdb.ErrDependencyWaitConflict)
	})
}
func TestNativeAttachmentCompatibilityPostgresGoalSharedRestartIdentity(t *testing.T) {
	nativeGoalRuntimes(t, func(t *testing.T, r jobdb.WorkflowRuntime) {
		ctx := celltest.Context(context.Background())
		source := goalSubmit(t, r, "source")
		extra := goalSubmit(t, r, "extra")
		_, err := r.AddGoalCandidate(ctx, jobdb.AddGoalCandidateRequest{GoalKey: goalKey(extra), JobID: source.JobKey.JobId})
		require.NoError(t, err)
		lease := goalLease(t, r, source)
		require.NotNil(t, lease)
		renewed, err := lease.(jobdb.RenewableExecutionLease).Renew(ctx)
		require.NoError(t, err)
		require.Equal(t, source.GoalID, renewed.Job().GoalID)
		token := ""
		if signed, ok := renewed.(interface{ LeaseToken() string }); ok {
			token = signed.LeaseToken()
		}
		// A restart needs a recorded boundary. Keep the scheduler lease live
		// while recording its outcome, as in the history-before-completion window.
		require.NoError(t, r.PutChapter(ctx, jobdb.PutChapterRequest{LeaseID: renewed.LeaseID(), LeaseToken: token, Ref: jobdb.ChapterRef{JobKey: source.JobKey, Ordinal: 1}, Chapter: jobdb.Chapter{Ordinal: 1, TaskType: "work", CreatedAt: time.Now().UTC(), Body: jobdb.JobAttemptOutcomeChapter{Outcome: jobdb.AppErrorOutcome{Error: jobdb.AppErrorPayload{Message: "retry elsewhere"}}}}}))
		req := jobdb.SubmitRestartJobRequest{RequestID: "shared-operation", Job: jobdb.SubmitRestartJob{Cell: "test", PriorJobKey: source.JobKey, ShareGoal: true}}
		shared, err := r.SubmitRestartJob(ctx, req)
		require.NoError(t, err)
		require.Equal(t, source.GoalID, shared.GoalID)
		info, err := r.GetJob(ctx, shared.JobKey)
		require.NoError(t, err)
		require.Empty(t, info.OtherGoalIDs)
		independent, err := r.SubmitRestartJob(ctx, jobdb.SubmitRestartJobRequest{Job: jobdb.SubmitRestartJob{Cell: "test", PriorJobKey: source.JobKey}})
		require.NoError(t, err)
		require.NotEqual(t, source.GoalID, independent.GoalID)
		req.Job.ShareGoal = false
		_, err = r.SubmitRestartJob(ctx, req)
		require.ErrorIs(t, err, jobdb.ErrExistingJobMismatch)
		require.ErrorIs(t, err, jobdb.ErrConflict)
		req.Job.ShareGoal = true
		goalComplete(t, r, shared, true)
		repeat, err := r.SubmitRestartJob(ctx, req)
		require.NoError(t, err)
		require.Equal(t, shared, repeat)
		_, err = r.AddGoalCandidate(ctx, jobdb.AddGoalCandidateRequest{GoalKey: goalKey(shared), JobID: shared.JobKey.JobId})
		require.NoError(t, err, "primary membership is idempotent after closure")
	})
}

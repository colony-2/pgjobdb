package integration_test

import (
	"context"
	"github.com/colony-2/jobdb/pkg/jobdb"
	pgruntime "github.com/colony-2/pgjobdb/pkg/pgjobdb/runtime"
	"sync/atomic"
)

// Native instrumentation for negative route tests, never a consumer facade.
type nativeMutationProbe struct {
	*pgruntime.Runtime
	attachments atomic.Int64
	mutations   atomic.Int64
}

func (r *nativeMutationProbe) AddGoalCandidate(ctx context.Context, q jobdb.AddGoalCandidateRequest) (jobdb.GoalCandidateInfo, error) {
	r.attachments.Add(1)
	return r.Runtime.AddGoalCandidate(ctx, q)
}
func (r *nativeMutationProbe) SubmitJob(ctx context.Context, q jobdb.SubmitJobRequest) (jobdb.JobHandle, error) {
	r.mutations.Add(1)
	return r.Runtime.SubmitJob(ctx, q)
}
func (r *nativeMutationProbe) StartOrAppendJob(ctx context.Context, q jobdb.StartOrAppendJobRequest) (jobdb.JobHandle, error) {
	r.mutations.Add(1)
	return r.Runtime.StartOrAppendJob(ctx, q)
}
func (r *nativeMutationProbe) SubmitRestartJob(ctx context.Context, q jobdb.SubmitRestartJobRequest) (jobdb.JobHandle, error) {
	r.mutations.Add(1)
	return r.Runtime.SubmitRestartJob(ctx, q)
}
func (r *nativeMutationProbe) FailGoal(ctx context.Context, q jobdb.FailGoalRequest) (jobdb.GoalInfo, error) {
	r.mutations.Add(1)
	return r.Runtime.FailGoal(ctx, q)
}
func (r *nativeMutationProbe) CancelJob(ctx context.Context, q jobdb.CancelJobRequest) error {
	r.mutations.Add(1)
	return r.Runtime.CancelJob(ctx, q)
}

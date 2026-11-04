package runtimeadapter

import (
	"context"

	"github.com/colony-2/jobdb/pkg/jobdb"
	runtimecore "github.com/colony-2/jobdb/pkg/jobdb/runtime/core"
	"github.com/colony-2/pgjobdb/pkg/pgjobdb"
)

func (s Scheduler) PrepareJob(ctx context.Context, req runtimecore.JobPreparation) (runtimecore.PreparedJob, error) {
	p, err := pgjobdb.PrepareJob(ctx, s.DB, pgjobdb.JobPreparation{Cell: req.Cell, CellRepo: req.CellRepo, ParentJobID: req.ParentJobID, Target: req.Target, Key: req.Key, RequestID: req.RequestID, PriorJobID: req.PriorJobID, ShareGoal: req.ShareGoal, Descriptor: req.Descriptor, Dependencies: req.Dependencies})
	return runtimecore.PreparedJob{Target: p.Target, Key: p.Key, GoalID: p.GoalID, Published: p.Published}, err
}
func (s Scheduler) GetGoal(ctx context.Context, key jobdb.GoalKey) (jobdb.GoalInfo, error) {
	return pgjobdb.GetGoal(ctx, s.DB, key)
}
func (s Scheduler) AddGoalCandidate(ctx context.Context, req jobdb.AddGoalCandidateRequest) (jobdb.GoalCandidateInfo, error) {
	return pgjobdb.AddGoalCandidate(ctx, s.DB, req)
}
func (s Scheduler) FailGoal(ctx context.Context, req jobdb.FailGoalRequest) (jobdb.GoalInfo, error) {
	return pgjobdb.FailGoal(ctx, s.DB, req)
}
func (s Scheduler) GetDependencyWait(ctx context.Context, key jobdb.DependencyWaitKey) (jobdb.DependencyWaitInfo, error) {
	return pgjobdb.GetDependencyWait(ctx, s.DB, key)
}

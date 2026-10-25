package runtimeadapter

import (
	"context"
	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/pgjobdb/pkg/pgjobdb"
)

func (s Scheduler) ListGoals(ctx context.Context, req jobdb.ListGoalsRequest) (jobdb.ListGoalsResponse, error) {
	return pgjobdb.ListGoals(ctx, s.DB, req)
}
func (s Scheduler) ListDependencyWaits(ctx context.Context, req jobdb.ListDependencyWaitsRequest) (jobdb.ListDependencyWaitsResponse, error) {
	return pgjobdb.ListDependencyWaits(ctx, s.DB, req)
}

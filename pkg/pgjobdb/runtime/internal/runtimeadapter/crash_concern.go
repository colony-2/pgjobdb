package runtimeadapter

import (
	"context"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/pgjobdb/pkg/pgjobdb"
)

func (s Scheduler) ClearCrashConcern(ctx context.Context, req jobdb.ClearCrashConcernRequest) error {
	return pgjobdb.ClearCrashConcern(ctx, s.DB, req)
}

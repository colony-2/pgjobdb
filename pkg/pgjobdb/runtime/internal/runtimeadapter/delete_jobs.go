package runtimeadapter

import (
	"context"

	"github.com/colony-2/jobdb/pkg/jobdb"
	runtimecore "github.com/colony-2/jobdb/pkg/jobdb/runtime/core"
	"github.com/colony-2/pgjobdb/pkg/pgjobdb"
)

var _ runtimecore.JobDeletionScheduler = Scheduler{}

func (s Scheduler) DeleteJobs(ctx context.Context, req runtimecore.DeleteJobsRequest) (jobdb.DeleteJobsResponse, error) {
	predicates, err := metadataPredicatesToPgjobdb(req.MetadataEquals)
	if err != nil {
		return jobdb.DeleteJobsResponse{}, err
	}
	native := pgjobdb.DeleteJobsRequest{
		TenantID: pgjobdb.TenantID(req.TenantID), CreatedBefore: req.CreatedBefore,
		MetadataPredicates: predicates, All: req.All, DryRun: req.DryRun, Now: req.Now,
	}
	for _, id := range req.JobIDs {
		native.JobIDs = append(native.JobIDs, pgjobdb.JobID(id))
	}
	result, err := pgjobdb.DeleteJobs(ctx, s.DB, native)
	if err != nil {
		return jobdb.DeleteJobsResponse{}, err
	}
	return jobdb.DeleteJobsResponse{MatchedCount: result.MatchedCount, DeletedCount: result.DeletedCount}, nil
}

package runtimeadapter

import (
	"context"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/pgjobdb/pkg/pgjobdb"
)

func (s Scheduler) CreateCell(ctx context.Context, req jobdb.CreateCellRequest) (jobdb.Cell, error) {
	return pgjobdb.CreateCell(ctx, s.DB, req)
}

func (s Scheduler) CreateCellWithLeaseByID(ctx context.Context, key jobdb.JobKey, leaseID, workerID string, req jobdb.CreateCellRequest) (jobdb.Cell, error) {
	return pgjobdb.CreateCellWithLeaseByID(ctx, s.DB, key, leaseID, workerID, req)
}
func (s Scheduler) GetCell(ctx context.Context, req jobdb.CellKey) (jobdb.Cell, error) {
	return pgjobdb.GetCell(ctx, s.DB, req)
}
func (s Scheduler) ListCells(ctx context.Context, req jobdb.ListCellsRequest) (jobdb.ListCellsResponse, error) {
	return pgjobdb.ListCells(ctx, s.DB, req)
}
func (s Scheduler) UpdateCell(ctx context.Context, req jobdb.UpdateCellRequest) (jobdb.Cell, error) {
	return pgjobdb.UpdateCell(ctx, s.DB, req)
}
func (s Scheduler) RetireCell(ctx context.Context, req jobdb.RetireCellRequest) (jobdb.Cell, error) {
	return pgjobdb.RetireCell(ctx, s.DB, req)
}
func (s Scheduler) DiscardJobPreparation(ctx context.Context, key jobdb.JobKey) error {
	return pgjobdb.DiscardJobPreparation(ctx, s.DB, key)
}
func (s Scheduler) ResolveCellWithLeaseByID(ctx context.Context, key jobdb.JobKey, leaseID, workerID, name string) (jobdb.ResolvedCell, error) {
	return pgjobdb.ResolveCellWithLeaseByID(ctx, s.DB, key, leaseID, workerID, name)
}
func (s Scheduler) LookupCellWithLeaseByID(ctx context.Context, key jobdb.JobKey, leaseID, workerID, name string) (jobdb.ResolvedCell, error) {
	return pgjobdb.LookupCellWithLeaseByID(ctx, s.DB, key, leaseID, workerID, name)
}
func (s Scheduler) ResolveScheduleCell(ctx context.Context, tenant, name, repo string) (jobdb.ResolvedCell, error) {
	return pgjobdb.ResolveScheduleCell(ctx, s.DB, tenant, name, repo)
}

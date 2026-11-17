package pgjobdb

import (
	"context"
	"database/sql"
	"errors"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/cellstate"
)

// ResolveAppendCell checks submission authority without taking the tenant goal
// lock or allocating a preparation. Claims/publication revalidate lease proof.
func ResolveAppendCell(ctx context.Context, db DB, j jobdb.SubmitJob) (jobdb.ResolvedCell, error) {
	s, err := loadCells(ctx, db, j.TenantId)
	if err != nil {
		return jobdb.ResolvedCell{}, err
	}
	var parent *jobdb.Cell
	if p, ok := cellstate.Lease(ctx); ok {
		if p.JobKey.TenantId != j.TenantId {
			return jobdb.ResolvedCell{}, jobdb.ErrCellForbidden
		}
		var id string
		err = db.QueryRowContext(ctx, `SELECT f.cell_id FROM pgjobdb.jobs j JOIN pgjobdb.job_facts f USING(tenant_id,job_id) WHERE j.tenant_id=$1 AND j.job_id=$2 AND j.lease_id=$3 AND j.lease_worker_id=$4 AND j.lease_expires_at>clock_timestamp() AND NOT j.cancel_requested AND f.hidden_at IS NULL`, j.TenantId, p.JobKey.JobId, p.LeaseID, p.WorkerID).Scan(&id)
		if errors.Is(err, sql.ErrNoRows) {
			return jobdb.ResolvedCell{}, jobdb.ErrExecutionLeaseLost
		}
		if err != nil {
			return jobdb.ResolvedCell{}, err
		}
		cell, ok := s.Cells[id]
		if !ok || cell.State != jobdb.CellActive {
			return jobdb.ResolvedCell{}, jobdb.ErrExecutionLeaseLost
		}
		parent = &cell
	}
	return s.Resolve(ctx, j.TenantId, j.Cell, j.CellRepo, parent, nil, false)
}

package pgjobdb

import (
	"context"
	"database/sql"
	"errors"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/cellstate"
)

func PrepareJob(ctx context.Context, db DB, req JobPreparation) (out PreparedJob, err error) {
	if err = validateSelectedPreparation(ctx, req); err != nil {
		return
	}
	err = cellTx(ctx, db, req.Key.TenantId, func(tx *sql.Tx, s *cellstate.State) error {
		if receipt, found, e := selectedReceipt(ctx, tx, s, req); e != nil || found {
			out = receipt
			return e
		}
		// Request-ID retries first recover the original tenant-wide job identity.
		if req.RequestID != "" && req.Key.JobId == "" {
			e := tx.QueryRowContext(ctx, `SELECT job_id FROM pgjobdb.job_preparations WHERE tenant_id=$1 AND request_id=$2`, req.Key.TenantId, req.RequestID).Scan(&req.Key.JobId)
			if e != nil && !errors.Is(e, sql.ErrNoRows) {
				return e
			}
		}
		target, e := resolvePreparation(ctx, tx, s, req)
		if e != nil {
			return e
		}
		if req.Target.CellID != "" && req.Target.CellID != target.CellID || req.Target.CellRepo != "" && req.Target.CellRepo != target.CellRepo {
			return jobdb.NewExistingJobMismatchError("cell ownership differs")
		}
		req.Target = target
		if req.SelectedGoalID != "" {
			if e = jobdb.AuthorizePrimaryGoalSelection(ctx, jobdb.GoalKey{TenantId: req.Key.TenantId, GoalID: req.SelectedGoalID}, target); e != nil {
				return e
			}
		}
		out, e = prepareJob(ctx, tx, req)
		if e != nil {
			return e
		}
		if out.Target.CellID != target.CellID {
			return jobdb.NewExistingJobMismatchError("cell identity differs")
		}
		if out.Target.CellRepo != target.CellRepo {
			return jobdb.ErrCellRepoMismatch
		}
		out.Target.Cell = target.Cell
		return nil
	})
	return
}
func DiscardJobPreparation(ctx context.Context, db DB, key jobdb.JobKey) error {
	return cellTx(ctx, db, key.TenantId, func(tx *sql.Tx, _ *cellstate.State) error {
		_, err := tx.ExecContext(ctx, `DELETE FROM pgjobdb.job_preparations WHERE tenant_id=$1 AND job_id=$2 AND NOT published AND selected_goal_id IS NULL`, key.TenantId, key.JobId)
		return err
	})
}
func SubmitJob(ctx context.Context, db DB, req SubmitJobRequest) (out *SubmitJobResult, err error) {
	err = cellTx(ctx, db, string(req.TenantID), func(tx *sql.Tx, s *cellstate.State) error {
		prep := JobPreparation{Key: jobdb.JobKey{TenantId: string(req.TenantID), JobId: string(req.JobID)}, Cell: req.Cell, CellRepo: req.CellRepo, ParentJobID: string(req.Runtime.ParentJobID)}
		var prior sql.NullString
		e := tx.QueryRowContext(ctx, `SELECT requested_cell,requested_repo,cell_id,cell_name,cell_repo,parent_job_id,prior_job_id FROM pgjobdb.job_preparations WHERE tenant_id=$1 AND job_id=$2`, req.TenantID, req.JobID).Scan(&prep.Cell, &prep.CellRepo, &prep.Target.CellID, &prep.Target.Cell, &prep.Target.CellRepo, &prep.ParentJobID, &prior)
		if e != nil && !errors.Is(e, sql.ErrNoRows) {
			return e
		}
		prep.PriorJobID = prior.String
		var selected sql.NullString
		e = tx.QueryRowContext(ctx, `SELECT selected_goal_id FROM pgjobdb.job_preparations WHERE tenant_id=$1 AND job_id=$2`, req.TenantID, req.JobID).Scan(&selected)
		if e != nil && !errors.Is(e, sql.ErrNoRows) {
			return e
		}
		if selected.String != req.SelectedGoalID {
			return jobdb.NewExistingJobMismatchError("selected goal differs from preparation")
		}
		prep.SelectedGoalID = req.SelectedGoalID
		if e = validateSelectedPreparation(ctx, prep); e != nil {
			return e
		}
		if err := jobdb.ValidateCellName(req.Cell); err != nil {
			return err
		}
		requested := prep
		requested.Cell = req.Cell
		requested.CellRepo = req.CellRepo
		requestTarget, e := resolvePreparation(ctx, tx, s, requested)
		if e != nil {
			return e
		}
		target, e := resolvePreparation(ctx, tx, s, prep)
		if e != nil {
			return e
		}
		if requestTarget.Cell != target.Cell || requestTarget.CellRepo != target.CellRepo {
			return jobdb.NewExistingJobMismatchError("cell identity or repo differs")
		}
		if prep.Target.CellRepo != "" && prep.Target.CellRepo != target.CellRepo {
			return jobdb.ErrCellRepoMismatch
		}
		if req.Target.CellID != "" && req.Target.CellID != target.CellID {
			return jobdb.NewExistingJobMismatchError("cell identity differs")
		}
		if req.Target.CellRepo != "" && req.Target.CellRepo != target.CellRepo {
			return jobdb.ErrCellRepoMismatch
		}
		if prep.Target.CellID != "" && prep.Target.CellID != target.CellID {
			return jobdb.NewExistingJobMismatchError("cell identity differs")
		}
		if req.SelectedGoalID != "" {
			if e = jobdb.AuthorizePrimaryGoalSelection(ctx, jobdb.GoalKey{TenantId: string(req.TenantID), GoalID: req.SelectedGoalID}, target); e != nil {
				return e
			}
		}
		req.Target = target
		out, e = submitJob(ctx, tx, req)
		return e
	})
	return
}

package pgjobdb

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"reflect"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/cellstate"
)

// Receipt lookup precedes live job/cell eligibility checks. It does not revive
// hidden work or change the immutable ownership captured at acceptance.
func selectedReceipt(ctx context.Context, tx *sql.Tx, s *cellstate.State, req JobPreparation) (PreparedJob, bool, error) {
	var descriptor, requestedCell string
	var raw, dependencyRaw []byte
	var parent string
	err := tx.QueryRowContext(ctx, `SELECT descriptor,requested_cell,receipt,parent_job_id,dependencies FROM pgjobdb.job_preparations WHERE tenant_id=$1 AND job_id=$2 AND selected_goal_id IS NOT NULL AND published`, req.Key.TenantId, req.Key.JobId).Scan(&descriptor, &requestedCell, &raw, &parent, &dependencyRaw)
	if errors.Is(err, sql.ErrNoRows) {
		return PreparedJob{}, false, nil
	}
	if err != nil {
		return PreparedJob{}, false, err
	}
	var h jobdb.JobHandle
	if len(raw) == 0 || json.Unmarshal(raw, &h) != nil || h.JobKey != req.Key || h.GoalID == "" || h.CellID == "" || h.Cell == "" || h.CellRepo == "" {
		return PreparedJob{}, true, jobdb.ErrSubmissionReceiptUnavailable
	}
	target := jobdb.ResolvedCell{Cell: h.Cell, CellID: h.CellID, CellRepo: h.CellRepo}
	if err = jobdb.AuthorizePrimaryGoalSelection(ctx, jobdb.GoalKey{TenantId: req.Key.TenantId, GoalID: h.GoalID}, target); err != nil {
		return PreparedJob{}, true, err
	}
	var deps []jobdb.Dependency
	if err = json.Unmarshal(dependencyRaw, &deps); err != nil {
		return PreparedJob{}, true, jobdb.ErrSubmissionReceiptUnavailable
	}
	requestedDeps := req.Dependencies
	if len(requestedDeps) == 0 {
		requestedDeps = []jobdb.Dependency{}
	}
	if req.ParentJobID != parent || !reflect.DeepEqual(deps, requestedDeps) {
		return PreparedJob{}, true, jobdb.NewExistingJobMismatchError("submission dependencies or parent differ")
	}
	if req.Descriptor != descriptor || req.SelectedGoalID != h.GoalID || req.PriorJobID != "" || req.ShareGoal || req.RequestID != "" {
		return PreparedJob{}, true, jobdb.NewExistingJobMismatchError("different selected submission")
	}
	if req.Cell != requestedCell {
		c, ok := s.ByName(req.Cell)
		if !ok || c.ID != h.CellID {
			return PreparedJob{}, true, jobdb.NewExistingJobMismatchError("cell differs")
		}
	}
	if req.CellRepo != "" {
		repo, err := jobdb.NormalizeCellRepo(req.CellRepo)
		if err != nil {
			return PreparedJob{}, true, err
		}
		if repo != h.CellRepo {
			return PreparedJob{}, true, jobdb.ErrCellRepoMismatch
		}
	}
	if req.Target.CellID != "" && req.Target.CellID != h.CellID || req.Target.CellRepo != "" && req.Target.CellRepo != h.CellRepo {
		return PreparedJob{}, true, jobdb.NewExistingJobMismatchError("cell ownership differs")
	}
	return PreparedJob{Target: target, Key: h.JobKey, GoalID: h.GoalID, Published: true, Receipt: &h}, true, nil
}

func validateSelectedPreparation(ctx context.Context, req JobPreparation) error {
	if err := jobdb.ValidatePrimaryGoalSelection(jobdb.SubmitJob{TenantId: req.Key.TenantId, JobID: req.Key.JobId, GoalID: req.SelectedGoalID}); err != nil {
		return err
	}
	if req.SelectedGoalID == "" {
		return nil
	}
	if _, ok := cellstate.Lease(ctx); ok {
		return jobdb.ErrPrimaryGoalSelectionUnsupported
	}
	if req.ShareGoal || req.PriorJobID != "" || req.RequestID != "" {
		return jobdb.NewExistingJobMismatchError("selection is a fresh submit operation")
	}
	return nil
}

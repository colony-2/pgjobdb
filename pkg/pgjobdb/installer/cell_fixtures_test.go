package installer_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/pgjobdb/pkg/pgjobdb"
)

func upsertCellSchedule(ctx context.Context, db *sql.DB, req pgjobdb.UpsertScheduleRequest) (*pgjobdb.Schedule, error) {
	c, err := pgjobdb.CreateCell(ctx, db, jobdb.CreateCellRequest{TenantID: string(req.TenantID), Name: "test", Repo: "example.test/test"})
	if errors.Is(err, jobdb.ErrCellConflict) {
		listed, e := pgjobdb.ListCells(ctx, db, jobdb.ListCellsRequest{TenantID: string(req.TenantID), Name: "test"})
		if e != nil {
			return nil, e
		}
		c = listed.Cells[0]
	} else if err != nil {
		return nil, err
	}
	var target map[string]any
	if err = json.Unmarshal(req.TargetSnapshot, &target); err != nil {
		return nil, err
	}
	target["cell"] = c.Name
	target["cellId"] = c.ID
	target["cellRepo"] = c.Repo
	req.TargetSnapshot, err = json.Marshal(target)
	if err != nil {
		return nil, err
	}
	return pgjobdb.UpsertSchedule(ctx, db, req)
}

// submitCellJob explicitly registers the target used by non-cell test fixtures.
func submitCellJob(ctx context.Context, db pgjobdb.DB, req pgjobdb.SubmitJobRequest) (*pgjobdb.SubmitJobResult, error) {
	if req.Cell != "" && req.CellRepo != "" {
		_, err := pgjobdb.CreateCell(ctx, db, jobdb.CreateCellRequest{TenantID: string(req.TenantID), Name: req.Cell, Repo: req.CellRepo})
		if err != nil && !errors.Is(err, jobdb.ErrCellConflict) {
			return nil, err
		}
	}
	return pgjobdb.SubmitJob(ctx, db, req)
}

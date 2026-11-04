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

package installer_test

import (
	"context"
	"errors"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/pgjobdb/pkg/pgjobdb"
)

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

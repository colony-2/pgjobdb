package pgjobdb

import (
	"context"
	"database/sql"
	"errors"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/pgjobdb/pkg/pgjobdb/internal/projectstorage"
)

// AdmitProjectWrite commits a permanent first-use seal before starting resource
// effects. Call this on the pool BEFORE beginning a borrowed transaction. A
// transaction/custom query handle can only check a previously committed seal;
// it never opens a hidden connection or rolls a seal back with resource data.
func AdmitProjectWrite(ctx context.Context, db DB, id string) error {
	if err := validateDB(ctx, db); err != nil {
		return err
	}
	if pool, ok := db.(*sql.DB); ok {
		return projectstorage.Control(pool, nil).AdmitWrite(ctx, id)
	}
	if err := projectstorage.Verify(ctx, db); err != nil {
		return jobdb.ErrProjectUnavailable
	}
	var ignored any
	err := db.QueryRowContext(ctx, `SELECT pgjobdb._project_write_check($1)`, id).Scan(&ignored)
	return projectError(err)
}

// ProjectError maps only dedicated SQL diagnostics, without exposing resource
// payloads or confusing project admission with lease loss.
func ProjectError(err error) error { return projectError(err) }
func projectError(err error) error {
	var state interface{ SQLState() string }
	if errors.As(err, &state) {
		switch state.SQLState() {
		case "PJC01":
			return jobdb.ErrProjectUnavailable
		case "PJC02":
			return jobdb.ErrProjectNotFound
		case "PJC03":
			return jobdb.ErrInvalidProject
		}
	}
	return err
}

package installer_test

import (
	"context"
	"database/sql"
	_ "embed"
	"testing"
	"time"

	"github.com/colony-2/pgjobdb/pkg/pgjobdb"
	"github.com/colony-2/pgjobdb/pkg/pgjobdb/installer"
	"github.com/stretchr/testify/require"
)

// Baseline format-12 SQL from a13f5e36df9da7efe5083b0b3bffeef22ec949b9.
//
//go:embed testdata/format12.sql
var format12BeforeAvailabilityRepair string

func TestSubmissionFormat12Upgrade(t *testing.T) {
	withBareDatabase(t, func(ctx context.Context, db *sql.DB) {
		_, err := db.ExecContext(ctx, format12BeforeAvailabilityRepair)
		require.NoError(t, err)
		before := time.Date(1850, 1, 1, 0, 0, 0, 123456000, time.UTC)
		_, err = submitCellJob(ctx, db, pgjobdb.SubmitJobRequest{TenantID: "tenant", JobID: "existing", WorkerID: "submit", JobType: "work", Cell: "test", CellRepo: "example.test/test", AvailableAt: &before})
		require.NoError(t, err)
		db.SetMaxOpenConns(1)
		_, err = db.ExecContext(ctx, `SET TimeZone='Europe/Paris'`)
		require.NoError(t, err)
		_, err = pgjobdb.GetJob(ctx, db, "tenant", "existing")
		// Baseline's historical seconds in the local offset cannot be decoded.
		// created_at is modern, so availability alone exposes the defect.
		require.ErrorContains(t, err, "decode native job")
		inst := installer.Installer{DB: db}
		require.NoError(t, inst.Apply(ctx))
		require.NoError(t, inst.Verify(ctx))
		require.NoError(t, inst.Apply(ctx))
		var version int
		require.NoError(t, db.QueryRowContext(ctx, `SELECT format_version FROM pgjobdb.installation`).Scan(&version))
		require.Equal(t, 13, version)
		var port string
		require.NoError(t, db.QueryRowContext(ctx, `SHOW port`).Scan(&port))
		reopened, err := sql.Open("postgres", "host=localhost port="+port+" user=postgres password=postgres dbname=postgres sslmode=disable")
		require.NoError(t, err)
		defer reopened.Close()
		row, err := pgjobdb.GetJob(ctx, reopened, "tenant", "existing")
		require.NoError(t, err)
		require.Equal(t, before, row.AvailableAt)
		lease, err := pgjobdb.GetJobLease(ctx, reopened, "tenant", "existing", "worker", pgjobdb.WorkSelector{JobTypes: []pgjobdb.JobType{"work"}}, pgjobdb.GetJobLeaseOptions{})
		require.NoError(t, err)
		require.NotNil(t, lease)
		var zone string
		require.NoError(t, db.QueryRowContext(ctx, `SHOW TimeZone`).Scan(&zone))
		require.Equal(t, "Europe/Paris", zone)
	})
}

package installer_test

import (
	"context"
	"database/sql"
	_ "embed"
	"testing"
	"time"

	"github.com/colony-2/pgjobdb/internal/projectfixture"
	"github.com/colony-2/pgjobdb/pkg/pgjobdb"
	"github.com/colony-2/pgjobdb/pkg/pgjobdb/installer"
	"github.com/colony-2/pgjobdb/pkg/pgjobdb/projects"
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
		historicalJob(t, ctx, db, 12, "existing")
		_, err = db.ExecContext(ctx, `UPDATE pgjobdb.jobs SET available_at=$1 WHERE tenant_id='c2' AND job_id='existing'`, before)
		require.NoError(t, err)
		db.SetMaxOpenConns(1)
		_, err = db.ExecContext(ctx, `SET TimeZone='Europe/Paris'`)
		require.NoError(t, err)
		_, err = pgjobdb.GetJob(ctx, db, "c2", "existing")
		// Baseline's historical seconds in the local offset cannot be decoded.
		// created_at is modern, so availability alone exposes the defect.
		require.ErrorContains(t, err, "decode native job")
		inst := installer.Installer{DB: db}
		require.NoError(t, inst.Migrate(ctx))
		require.NoError(t, inst.Verify(ctx))
		require.NoError(t, inst.Migrate(ctx))
		var version int
		require.NoError(t, db.QueryRowContext(ctx, `SELECT format_version FROM pgjobdb.installation`).Scan(&version))
		require.Equal(t, 16, version)
		var port string
		require.NoError(t, db.QueryRowContext(ctx, `SHOW port`).Scan(&port))
		reopened, err := sql.Open("postgres", "host=localhost port="+port+" user=postgres password=postgres dbname=postgres sslmode=disable")
		require.NoError(t, err)
		defer reopened.Close()
		row, err := pgjobdb.GetJob(ctx, reopened, "c2", "existing")
		require.NoError(t, err)
		require.Equal(t, before, row.AvailableAt)
		require.NoError(t, projects.PrepareShared(projectfixture.Context(ctx), reopened))
		require.NoError(t, projectfixture.Adopt(ctx, reopened))
		lease, err := pgjobdb.GetJobLease(ctx, reopened, "c2", "existing", "worker", pgjobdb.WorkSelector{JobTypes: []pgjobdb.JobType{"collect"}}, pgjobdb.GetJobLeaseOptions{})
		require.NoError(t, err)
		require.NotNil(t, lease)
		var zone string
		require.NoError(t, db.QueryRowContext(ctx, `SHOW TimeZone`).Scan(&zone))
		require.Equal(t, "Europe/Paris", zone)
	})
}

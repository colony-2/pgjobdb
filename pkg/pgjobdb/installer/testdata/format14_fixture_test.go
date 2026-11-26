package installer_test

import (
	"context"
	"database/sql"
	"io"
	"os"
	"testing"

	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	"github.com/colony-2/jobdb/pkg/jobdb/projectcatalog"
	"github.com/colony-2/pgjobdb/internal/projectfixture"
	"github.com/colony-2/pgjobdb/pkg/pgjobdb/installer"
	"github.com/colony-2/pgjobdb/pkg/pgjobdb/projects"
	"github.com/stretchr/testify/require"
)

func TestCreateFormat14Fixture(t *testing.T) {
	ctx := projectfixture.Context(celltest.Context(context.Background()))
	db, err := sql.Open("postgres", os.Getenv("FIXTURE_DSN"))
	require.NoError(t, err)
	defer db.Close()
	install := installer.Installer{DB: db}
	layout := os.Getenv("FIXTURE_LAYOUT")
	switch layout {
	case "plain", "direct_project":
		require.NoError(t, install.Apply(ctx))
	case "migrated_project":
		_, err = db.ExecContext(ctx, format13)
		require.NoError(t, err)
		format13Job(t, ctx, db, "original")
		require.NoError(t, projects.PrepareShared(ctx, db))
		installFormat13ProjectFixture(t, ctx, db)
		require.NoError(t, install.Migrate(ctx))
	default:
		t.Fatalf("unknown layout %q", layout)
	}
	for _, id := range []string{"live-empty", "live-funded", "retained-empty", "retained-funded"} {
		format13Job(t, ctx, db, id)
	}
	for _, id := range []string{"retained-empty", "retained-funded"} {
		_, err = db.ExecContext(ctx, `SELECT pgjobdb.complete_native_unheld_job('c2',$1,'operator','success')`, id)
		require.NoError(t, err)
	}
	_, err = db.ExecContext(ctx, `SELECT pgjobdb.set_crash_concern_threshold(7);
UPDATE pgjobdb.jobs SET consecutive_expirations=1, crash_budget_remaining=CASE job_id WHEN 'live-empty' THEN 0 ELSE 2 END;
UPDATE pgjobdb.jobs_archive SET consecutive_expirations=2, crash_budget_remaining=CASE job_id WHEN 'retained-empty' THEN 0 ELSE 3 END`)
	require.NoError(t, err)
	if layout == "direct_project" {
		require.NoError(t, projects.PrepareShared(ctx, db))
		inv, inspectErr := projects.Inspect(ctx, db, projectfixture.Participants)
		require.NoError(t, inspectErr)
		_, err = projects.Install(ctx, db, inv, []projectcatalog.Seed{{ID: "c2", Name: "c2"}}, io.Discard)
		require.NoError(t, err)
	}
	require.NoError(t, install.Verify(ctx))
}

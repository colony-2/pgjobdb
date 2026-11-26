package installer_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"testing"

	"github.com/colony-2/pgjobdb/pkg/pgjobdb/installer"
	"github.com/colony-2/pgjobdb/pkg/pgjobdb/internal/projectstorage"
	"github.com/colony-2/pgjobdb/pkg/pgjobdb/projects"
	pgruntime "github.com/colony-2/pgjobdb/pkg/pgjobdb/runtime"
	"github.com/stretchr/testify/require"
)

var archiveLayouts = []string{"plain", "direct_project", "migrated_project"}

func archiveFixture(t *testing.T, ctx context.Context, dsn, layout string) {
	t.Helper()
	path := os.Getenv("PGJOBDB_TEST_FORMAT14_FIXTURE")
	if path == "" {
		t.Skip("build the pinned format-14 fixture binary: see installer/testdata/README.md")
	}
	cmd := exec.CommandContext(ctx, path, "-test.run=^TestCreateFormat14Fixture$", "-test.count=1")
	cmd.Env = append(os.Environ(), "FIXTURE_DSN="+dsn, "FIXTURE_LAYOUT="+layout)
	output, err := cmd.CombinedOutput()
	require.NoError(t, err, "%s", output)
}

type archiveSource struct {
	allowance int64
	counts    map[string]archiveExecutionCounts
	rows      string
}

type archiveExecutionCounts struct {
	Active, Retained, Exhausted, Protected int
}

func readArchiveSource(t *testing.T, ctx context.Context, db *sql.DB) archiveSource {
	t.Helper()
	source := archiveSource{counts: map[string]archiveExecutionCounts{}}
	require.NoError(t, db.QueryRowContext(ctx, `SELECT crash_allowance FROM pgjobdb.resource_configuration WHERE singleton`).Scan(&source.allowance))
	var storedLive, storedRetained, historicalLive, historicalRetained int
	for _, store := range []struct{ relation, query string }{
		{"live", `SELECT tenant_id,consecutive_expirations,crash_budget_remaining,'EXECUTION' FROM pgjobdb.jobs`},
		{"retained", `SELECT tenant_id,consecutive_expirations,crash_budget_remaining,record_kind FROM pgjobdb.jobs_archive JOIN pgjobdb.job_facts USING(tenant_id,job_id)`},
	} {
		rows, err := db.QueryContext(ctx, store.query)
		require.NoError(t, err)
		for rows.Next() {
			var tenant, kind string
			var expired, remaining int64
			require.NoError(t, rows.Scan(&tenant, &expired, &remaining, &kind))
			counts := source.counts[tenant]
			if kind != "EXECUTION" {
				counts.Protected++
			} else {
				if store.relation == "live" {
					counts.Active++
					if expired >= source.allowance {
						historicalLive++
					}
					if remaining == 0 {
						storedLive++
					}
				} else {
					counts.Retained++
					if expired >= source.allowance {
						historicalRetained++
					}
					if remaining == 0 {
						storedRetained++
					}
				}
				if remaining == 0 {
					counts.Exhausted++
				}
			}
			source.counts[tenant] = counts
		}
		require.NoError(t, rows.Err())
		require.NoError(t, rows.Close())
	}
	require.Equal(t, 1, storedLive)
	require.Equal(t, 1, storedRetained)
	require.Zero(t, historicalLive)
	require.Zero(t, historicalRetained)
	require.NoError(t, db.QueryRowContext(ctx, `SELECT jsonb_build_array(
 (SELECT jsonb_agg(to_jsonb(c)-'archived_at_ns' ORDER BY cell_id) FROM pgjobdb.cells c),
 (SELECT jsonb_agg(to_jsonb(j) ORDER BY job_id) FROM pgjobdb.jobs j),
 (SELECT jsonb_agg(to_jsonb(a) ORDER BY job_id) FROM pgjobdb.jobs_archive a),
 (SELECT jsonb_agg(to_jsonb(f) ORDER BY job_id) FROM pgjobdb.job_facts f))::text`).Scan(&source.rows))
	return source
}

func checkArchiveReport(t *testing.T, source archiveSource, report installer.ConversionReport) {
	t.Helper()
	require.Equal(t, source.allowance, report.OriginalCrashDefault)
	require.Equal(t, source.allowance, report.TargetCrashDefault)
	require.Equal(t, "crash_budget_remaining", report.CreditFormula)
	observed := map[string]archiveExecutionCounts{}
	for _, raw := range report.CrashExecutions {
		var entry struct {
			Tenant   string `json:"tenantId"`
			Active   int    `json:"activeExecutions"`
			Retained int    `json:"archivedExecutions"`
			Empty    int    `json:"exhaustedExecutions"`
			Other    int    `json:"protectedRecords"`
		}
		require.NoError(t, json.Unmarshal(raw, &entry))
		require.NotContains(t, observed, entry.Tenant)
		observed[entry.Tenant] = archiveExecutionCounts{entry.Active, entry.Retained, entry.Empty, entry.Other}
	}
	require.Equal(t, source.counts, observed)
}

type archiveProjectSnapshot struct {
	receipt, catalog, admission []byte
	guards                      string
	certification               sql.NullString
}

func archiveProject(t *testing.T, ctx context.Context, db *sql.DB, layout string) archiveProjectSnapshot {
	t.Helper()
	var snapshot archiveProjectSnapshot
	if layout == "plain" {
		var absent bool
		require.NoError(t, db.QueryRowContext(ctx, `SELECT to_regclass('pgjobdb.project_installation') IS NULL AND to_regclass('public.jobdb_project_catalog') IS NULL`).Scan(&absent))
		require.True(t, absent)
		return snapshot
	}
	require.NoError(t, projectstorage.Verify(ctx, db))
	require.NoError(t, db.QueryRowContext(ctx, `SELECT i.payload,c.payload,c.admission,to_jsonb(i)->>'scheduler_structure'
 FROM pgjobdb.project_installation i CROSS JOIN public.jobdb_project_catalog c`).Scan(&snapshot.receipt, &snapshot.catalog, &snapshot.admission, &snapshot.certification))
	require.NoError(t, db.QueryRowContext(ctx, `SELECT jsonb_agg(jsonb_build_array(t.oid,pg_get_triggerdef(t.oid),t.tgenabled) ORDER BY t.oid)::text
 FROM pg_trigger t WHERE t.tgname IN ('project_admission_row','project_admission_truncate')`).Scan(&snapshot.guards))
	return snapshot
}

func assertArchiveLeaseEligibility(t *testing.T, ctx context.Context, db *sql.DB, layout string) {
	t.Helper()
	tx, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer tx.Rollback()
	for _, job := range []string{"live-empty", "live-funded"} {
		var granted bool
		require.NoError(t, tx.QueryRowContext(ctx, `SELECT pgjobdb.claim_append('c2',$1,'test-lease','jobdb-append-test',30) IS NOT NULL`, job).Scan(&granted))
		require.Equal(t, job == "live-funded", granted, job)
	}
	if layout != "plain" {
		_, err = tx.ExecContext(ctx, `SELECT pgjobdb.release_append('missing','no-op','lease')`)
		require.ErrorContains(t, err, "project not found")
	}
}

func TestPostgresCellArchiveMigration(t *testing.T) {
	for _, layout := range archiveLayouts {
		t.Run(layout, func(t *testing.T) {
			withBareDatabaseDSN(t, func(ctx context.Context, db *sql.DB, dsn string) {
				archiveFixture(t, ctx, dsn, layout)
				install := installer.Installer{DB: db}
				beforeSchema := catalogSnapshot(t, ctx, db)
				before := readArchiveSource(t, ctx, db)
				project := archiveProject(t, ctx, db, layout)
				require.Equal(t, layout == "migrated_project", project.certification.Valid)
				assertArchiveLeaseEligibility(t, ctx, db, layout)
				_, err := pgruntime.NewSQLDB(ctx, db, pgruntime.Config{BlobStoreURI: "blobfs://" + t.TempDir()})
				require.ErrorContains(t, err, "offline migration to format 15")
				preflight, err := install.Preflight(ctx)
				require.NoError(t, err)
				require.True(t, preflight.Advisory)
				require.Equal(t, []int{14, 15}, preflight.PhasePath)
				checkArchiveReport(t, before, preflight)
				require.Equal(t, beforeSchema, catalogSnapshot(t, ctx, db))
				require.Equal(t, before, readArchiveSource(t, ctx, db))
				require.Equal(t, project, archiveProject(t, ctx, db, layout))

				var staged bytes.Buffer
				confirmation, err := install.MigrateWithReport(ctx, &staged)
				require.NoError(t, err)
				require.True(t, confirmation.Committed)
				require.Equal(t, 14, confirmation.SourceFormat)
				require.Equal(t, 15, confirmation.TargetFormat)
				require.Equal(t, fmt.Sprintf("%x", sha256.Sum256(staged.Bytes())), confirmation.ReportSHA256)
				var committed installer.ConversionReport
				require.NoError(t, json.Unmarshal(staged.Bytes(), &committed))
				checkArchiveReport(t, before, committed)
				preflight.Advisory = false
				encoded, err := json.Marshal(preflight)
				require.NoError(t, err)
				require.JSONEq(t, string(encoded), staged.String())
				require.NoError(t, install.Verify(ctx))
				var marked int
				require.NoError(t, db.QueryRowContext(ctx, `SELECT count(*) FROM pgjobdb.cells WHERE archived_at_ns IS NOT NULL`).Scan(&marked))
				require.Zero(t, marked)
				require.Equal(t, before, readArchiveSource(t, ctx, db))
				assertArchiveLeaseEligibility(t, ctx, db, layout)
				after := archiveProject(t, ctx, db, layout)
				if layout != "plain" {
					require.NoError(t, projects.Verify(ctx, db))
					require.True(t, after.certification.Valid)
					require.NotEqual(t, project.certification, after.certification)
					project.certification = after.certification
				}
				require.Equal(t, project, after)
				noop, err := install.Preflight(ctx)
				require.NoError(t, err)
				require.True(t, noop.NoOp)
				checkArchiveReport(t, before, noop)
			})
		})
	}
}

func TestPostgresCellArchiveMigrationRollback(t *testing.T) {
	for _, layout := range archiveLayouts {
		t.Run(layout, func(t *testing.T) {
			withBareDatabaseDSN(t, func(ctx context.Context, db *sql.DB, dsn string) {
				archiveFixture(t, ctx, dsn, layout)
				beforeSchema := catalogSnapshot(t, ctx, db)
				before := readArchiveSource(t, ctx, db)
				project := archiveProject(t, ctx, db, layout)
				confirmation, err := (installer.Installer{DB: db}).MigrateWithReport(ctx, failingReportWriter{})
				require.ErrorContains(t, err, "injected report export failure")
				require.False(t, confirmation.Committed)
				reopened, err := sql.Open("postgres", dsn)
				require.NoError(t, err)
				defer reopened.Close()
				require.Equal(t, beforeSchema, catalogSnapshot(t, ctx, reopened))
				require.Equal(t, before, readArchiveSource(t, ctx, reopened))
				require.Equal(t, project, archiveProject(t, ctx, reopened, layout))
				require.NoError(t, (installer.Installer{DB: reopened}).Migrate(ctx))
			})
		})
	}
}

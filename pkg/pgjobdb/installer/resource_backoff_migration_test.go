package installer_test

import (
	"bytes"
	"context"
	"database/sql"
	_ "embed"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/colony-2/jobdb/pkg/jobdb/projectcatalog"
	"github.com/colony-2/pgjobdb/internal/projectfixture"
	"github.com/colony-2/pgjobdb/pkg/pgjobdb"
	"github.com/colony-2/pgjobdb/pkg/pgjobdb/installer"
	"github.com/colony-2/pgjobdb/pkg/pgjobdb/internal/projectstorage"
	"github.com/colony-2/pgjobdb/pkg/pgjobdb/projects"
	"github.com/stretchr/testify/require"
)

// Genuine format 13, assembled from 15ae3bf's original embedding order.
//
//go:embed testdata/format13.sql
var format13 string

func format13Job(t *testing.T, ctx context.Context, db *sql.DB, jobID string) {
	t.Helper()
	_, err := db.ExecContext(ctx, `INSERT INTO pgjobdb.cells(tenant_id,cell_id,name,repo,state,description,metadata,revision,created_at_ns,updated_at_ns) VALUES('c2','historical-cell','test','example.test/test','ACTIVE','','{}','historical-revision',0,0) ON CONFLICT DO NOTHING; INSERT INTO pgjobdb.cell_names VALUES('c2','test','historical-cell') ON CONFLICT DO NOTHING`)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, `SELECT pgjobdb.submit_native_job('c2',$1,'submitter','collect','{}','{}',NULL,NULL,ARRAY[]::TEXT[],NULL,NULL,NULL,NULL,NULL,FALSE,'[]',jsonb_build_object('cellId',cell_id,'cell',name,'cellRepo',repo)) FROM pgjobdb.cells WHERE tenant_id='c2' AND name='test'`, jobID)
	require.NoError(t, err)
}

func TestPostgresResourceBackoffMigrationFormat13(t *testing.T) {
	for _, threshold := range []int64{2, 9} {
		t.Run(fmt.Sprint(threshold), func(t *testing.T) {
			withBareDatabase(t, func(ctx context.Context, db *sql.DB) {
				_, err := db.ExecContext(ctx, format13)
				require.NoError(t, err)
				_, err = db.ExecContext(ctx, `SELECT pgjobdb.set_crash_concern_threshold($1)`, threshold)
				require.NoError(t, err)
				counts := []int64{0, 1, threshold - 1, threshold, threshold + 1}
				for i, count := range counts {
					id := fmt.Sprintf("job-%d", i)
					format13Job(t, ctx, db, id)
					_, err = db.ExecContext(ctx, `UPDATE pgjobdb.jobs SET consecutive_expirations=$1,lease_expiration_count=20,lease_id='old-lease',lease_worker_id='old-worker',lease_expires_at='2020-01-01' WHERE job_id=$2`, count, id)
					require.NoError(t, err)
				}
				_, err = db.ExecContext(ctx, `SELECT pgjobdb.complete_native_unheld_job('c2','job-0','operator','success')`)
				require.NoError(t, err)
				install := installer.Installer{DB: db}
				before := catalogSnapshot(t, ctx, db)
				require.ErrorContains(t, install.Apply(ctx), "explicit offline migration")
				require.JSONEq(t, before, catalogSnapshot(t, ctx, db))
				// Both public SQL and startup reject without changing the original store.
				_, err = db.ExecContext(ctx, pgjobdb.SQL)
				require.Error(t, err)
				_, err = db.ExecContext(ctx, `ROLLBACK`)
				require.NoError(t, err)
				report, err := install.CheckMigration(ctx)
				require.NoError(t, err)
				require.Equal(t, int64(threshold), report.OriginalCrashDefault)
				require.False(t, report.LegacyInputsAvailable)
				var staged bytes.Buffer
				confirmation, err := install.MigrateWithReport(ctx, &staged)
				require.NoError(t, err)
				require.True(t, confirmation.Committed)
				require.Equal(t, 13, confirmation.SourceFormat)
				require.Equal(t, 14, confirmation.TargetFormat)
				require.NoError(t, install.Verify(ctx))
				var markerGuard string
				require.NoError(t, db.QueryRowContext(ctx, `SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conrelid='pgjobdb.installation'::regclass AND conname='installation_format_version_check'`).Scan(&markerGuard))
				require.Equal(t, "CHECK ((format_version = 14))", markerGuard)
				require.NoError(t, json.Unmarshal(staged.Bytes(), &report))
				require.Equal(t, 2, report.ReportVersion)
				require.Equal(t, []int{13, 14}, report.PhasePath)
				require.Empty(t, report.MigrationFailures)
				for i, count := range counts {
					var credit, lifetime int64
					require.NoError(t, db.QueryRowContext(ctx, `SELECT crash_budget_remaining,lease_expiration_count FROM (SELECT job_id,crash_budget_remaining,lease_expiration_count FROM pgjobdb.jobs UNION ALL SELECT job_id,crash_budget_remaining,lease_expiration_count FROM pgjobdb.jobs_archive) s WHERE job_id=$1`, fmt.Sprintf("job-%d", i)).Scan(&credit, &lifetime))
					require.Equal(t, max(threshold-count, 0), credit)
					require.EqualValues(t, 20, lifetime)
				}
				var actual int64
				require.NoError(t, db.QueryRowContext(ctx, `SELECT pgjobdb.crash_concern_threshold()`).Scan(&actual))
				require.Equal(t, threshold, actual)
				_, err = db.ExecContext(ctx, `UPDATE pgjobdb.jobs SET crash_budget_remaining=100 WHERE job_id='job-1'`)
				require.NoError(t, err)
				require.NoError(t, install.Apply(ctx))
				require.NoError(t, install.Migrate(ctx))
				require.NoError(t, db.QueryRowContext(ctx, `SELECT crash_budget_remaining FROM pgjobdb.jobs WHERE job_id='job-1'`).Scan(&actual))
				require.EqualValues(t, 100, actual)
				require.NoError(t, db.QueryRowContext(ctx, `SELECT pgjobdb.crash_concern_threshold()`).Scan(&actual))
				require.Equal(t, threshold, actual)
				noop, err := install.CheckMigration(ctx)
				require.NoError(t, err)
				require.True(t, noop.NoOp)
			})
		})
	}
}

func TestPostgresResourceBackoffMigrationRollback(t *testing.T) {
	withBareDatabase(t, func(ctx context.Context, db *sql.DB) {
		_, err := db.ExecContext(ctx, format13)
		require.NoError(t, err)
		_, err = db.ExecContext(ctx, `SELECT pgjobdb.set_crash_concern_threshold(9)`)
		require.NoError(t, err)
		install := installer.Installer{DB: db}
		before := catalogSnapshot(t, ctx, db)
		for _, writer := range []interface{ Write([]byte) (int, error) }{failingReportWriter{}, shortReportWriter{}} {
			_, err := install.MigrateWithReport(ctx, writer)
			require.Error(t, err)
			require.JSONEq(t, before, catalogSnapshot(t, ctx, db))
			var value int
			require.NoError(t, db.QueryRowContext(ctx, `SELECT format_version FROM pgjobdb.installation`).Scan(&value))
			require.Equal(t, 13, value)
			require.NoError(t, db.QueryRowContext(ctx, `SELECT pgjobdb.crash_concern_threshold()`).Scan(&value))
			require.Equal(t, 9, value)
		}
	})
}

func TestPostgresResourceBackoffMigrationPreservesProjects(t *testing.T) {
	withBareDatabase(t, func(ctx context.Context, db *sql.DB) {
		ctx = projectfixture.Context(ctx)
		_, err := db.ExecContext(ctx, format13)
		require.NoError(t, err)
		format13Job(t, ctx, db, "existing")
		_, err = db.ExecContext(ctx, `SELECT pgjobdb.set_crash_concern_threshold(9); UPDATE pgjobdb.jobs SET consecutive_expirations=2 WHERE job_id='existing'`)
		require.NoError(t, err)
		require.NoError(t, projects.PrepareShared(ctx, db))
		receipt := installFormat13ProjectFixture(t, ctx, db)
		readBytes := func(query string) []byte {
			var data []byte
			require.NoError(t, db.QueryRowContext(ctx, query).Scan(&data))
			return data
		}
		const receiptSQL = `SELECT payload FROM pgjobdb.project_installation WHERE singleton=1`
		const catalogSQL = `SELECT payload FROM public.jobdb_project_catalog WHERE singleton=1`
		const admissionSQL = `SELECT admission FROM public.jobdb_project_catalog WHERE singleton=1`
		rawReceipt, catalog, admission := readBytes(receiptSQL), readBytes(catalogSQL), readBytes(admissionSQL)
		install := installer.Installer{DB: db}
		_, err = db.ExecContext(ctx, `ALTER TABLE pgjobdb.jobs DISABLE TRIGGER project_admission_row`)
		require.NoError(t, err)
		_, err = install.CheckMigration(ctx)
		require.ErrorContains(t, err, "project storage definitions or guard bindings changed")
		require.ErrorContains(t, install.Migrate(ctx), "project storage definitions or guard bindings changed")
		_, err = db.ExecContext(ctx, `ALTER TABLE pgjobdb.jobs ENABLE TRIGGER project_admission_row`)
		require.NoError(t, err)
		blocker, err := db.BeginTx(ctx, nil)
		require.NoError(t, err)
		defer blocker.Rollback()
		_, err = blocker.ExecContext(ctx, `LOCK TABLE public.jobdb_chapter_stories IN ROW EXCLUSIVE MODE`)
		require.NoError(t, err)
		require.ErrorContains(t, install.Migrate(ctx), "exclude project participant writers")
		require.NoError(t, blocker.Rollback())
		_, err = install.MigrateWithReport(ctx, failingReportWriter{})
		require.ErrorContains(t, err, "export staged conversion report")
		require.NoError(t, projectstorage.Verify(ctx, db))
		require.Equal(t, rawReceipt, readBytes(receiptSQL))
		var format int
		require.NoError(t, db.QueryRowContext(ctx, `SELECT format_version FROM pgjobdb.installation`).Scan(&format))
		require.Equal(t, 13, format)

		require.NoError(t, install.Migrate(ctx))
		require.NoError(t, projects.Verify(ctx, db))
		require.Equal(t, rawReceipt, readBytes(receiptSQL))
		require.Equal(t, catalog, readBytes(catalogSQL))
		require.Equal(t, admission, readBytes(admissionSQL))
		var inv projects.Inventory
		require.NoError(t, json.Unmarshal(receipt.Inventory, &inv))
		var retry bytes.Buffer
		replay, err := projects.Install(ctx, db, inv, receipt.Seeds, &retry)
		require.NoError(t, err)
		require.Equal(t, receipt, replay)
		var credit int64
		require.NoError(t, db.QueryRowContext(ctx, `SELECT crash_budget_remaining FROM pgjobdb.jobs WHERE job_id='existing'`).Scan(&credit))
		require.EqualValues(t, 7, credit)
		_, err = db.ExecContext(ctx, `SELECT pgjobdb.release_append('missing','no-op','lease')`)
		require.ErrorContains(t, err, "project not found")
		for _, table := range []string{"resource_receipts", "resource_sequences"} {
			_, err = db.ExecContext(ctx, `TRUNCATE TABLE pgjobdb.`+table)
			require.ErrorContains(t, err, "scoped resource truncation is forbidden")
		}
		_, err = db.ExecContext(ctx, pgjobdb.SQL)
		require.ErrorContains(t, err, "preserve project guards")
		_, err = db.ExecContext(ctx, `ROLLBACK`)
		require.NoError(t, err)
		require.NoError(t, install.Apply(ctx))
		require.NoError(t, projects.Verify(ctx, db))
	})
}

func installFormat13ProjectFixture(t *testing.T, ctx context.Context, db *sql.DB) projectstorage.Receipt {
	t.Helper()
	tx, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer tx.Rollback()
	_, err = tx.ExecContext(ctx, `CREATE TABLE public.jobdb_project_catalog(singleton INTEGER PRIMARY KEY CHECK(singleton=1),payload BYTEA,admission BYTEA);
 CREATE TABLE pgjobdb.project_installation(singleton INTEGER PRIMARY KEY CHECK(singleton=1),payload BYTEA NOT NULL);
 INSERT INTO public.jobdb_project_catalog(singleton) VALUES(1)`)
	require.NoError(t, err)
	tables := []string{}
	for _, table := range projectstorage.TableNames() {
		if table != "pgjobdb.resource_receipts" && table != "pgjobdb.resource_sequences" {
			tables = append(tables, table)
		}
	}
	inventory, err := json.Marshal(projects.Inventory{Protocol: projectstorage.Protocol,
		TenantIDs: []string{"c2"}, ParticipantsSHA256: projectfixture.Participants,
		ResourcesSHA256: projectstorage.Digest([]byte("format-13-resources")), StructureSHA256: projectstorage.Digest([]byte("format-13-structure")),
	})
	require.NoError(t, err)
	seeds := []projectcatalog.Seed{{ID: "c2", Name: "c2"}}
	store := projectstorage.TxStore{Tx: tx}
	control := projectcatalog.Service{Store: store}
	require.NoError(t, control.Install(ctx, projectcatalog.Installation{Manifest: projectstorage.Digest(inventory), Seeds: seeds}))
	require.NoError(t, projectstorage.InstallGuards(ctx, tx, tables))
	state, err := store.Read(ctx)
	require.NoError(t, err)
	structure, err := projectstorage.Structure(ctx, tx)
	require.NoError(t, err)
	receipt := projectstorage.Receipt{Protocol: projectstorage.Protocol, Inventory: inventory, Seeds: seeds,
		CatalogID: state.ID, CatalogManifest: state.Manifest, GuardSource: projectstorage.GuardDigest(), Structure: structure,
	}
	payload, err := json.Marshal(receipt)
	require.NoError(t, err)
	_, err = tx.ExecContext(ctx, `INSERT INTO pgjobdb.project_installation VALUES(1,$1)`, payload)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
	require.NoError(t, projectstorage.Verify(ctx, db))
	return receipt
}

func TestPostgresResourceBackoffMigrationDefaultSerialization(t *testing.T) {
	withBareDatabase(t, func(ctx context.Context, db *sql.DB) {
		_, err := db.ExecContext(ctx, format13)
		require.NoError(t, err)
		format13Job(t, ctx, db, "existing")
		_, err = db.ExecContext(ctx, `UPDATE pgjobdb.jobs SET consecutive_expirations=1 WHERE job_id='existing'; SELECT pgjobdb.set_crash_concern_threshold(2)`)
		require.NoError(t, err)
		install := installer.Installer{DB: db}
		require.NoError(t, install.Migrate(ctx))
		_, err = db.ExecContext(ctx, `UPDATE pgjobdb.jobs SET crash_budget_remaining=100 WHERE job_id='existing'`)
		require.NoError(t, err)
		setter, err := db.BeginTx(ctx, nil)
		require.NoError(t, err)
		defer setter.Rollback()
		_, err = setter.ExecContext(ctx, `SELECT pgjobdb.set_crash_concern_threshold(7)`)
		require.NoError(t, err)
		require.ErrorContains(t, install.Migrate(ctx), "exclude conversion writers")
		var threshold, credit int64
		require.NoError(t, db.QueryRowContext(ctx, `SELECT pgjobdb.crash_concern_threshold(),crash_budget_remaining FROM pgjobdb.jobs WHERE job_id='existing'`).Scan(&threshold, &credit))
		require.EqualValues(t, 2, threshold)
		require.EqualValues(t, 100, credit)
		require.NoError(t, setter.Commit())
		db.SetMaxIdleConns(0)
		require.NoError(t, install.Migrate(ctx))
		require.NoError(t, install.Apply(ctx))
		require.NoError(t, db.QueryRowContext(ctx, `SELECT pgjobdb.crash_concern_threshold(),crash_budget_remaining FROM pgjobdb.jobs WHERE job_id='existing'`).Scan(&threshold, &credit))
		require.EqualValues(t, 7, threshold)
		require.EqualValues(t, 100, credit)
	})
}

func TestPostgresResourceBackoffMigrationRejectsInvalidCrashData(t *testing.T) {
	for _, test := range []struct{ name, change, diagnostic string }{
		{"missing_threshold", `ALTER FUNCTION pgjobdb.crash_concern_threshold() RENAME TO retired_crash_threshold`, "function pgjobdb.crash_concern_threshold() does not exist"},
		{"null_threshold", `CREATE OR REPLACE FUNCTION pgjobdb.crash_concern_threshold() RETURNS integer LANGUAGE plpgsql AS $$ BEGIN RETURN NULL; END $$`, "converting NULL"},
		{"zero_threshold", `CREATE OR REPLACE FUNCTION pgjobdb.crash_concern_threshold() RETURNS integer LANGUAGE plpgsql AS $$ BEGIN RETURN 0; END $$`, "invalid original crash default"},
		{"negative_threshold", `CREATE OR REPLACE FUNCTION pgjobdb.crash_concern_threshold() RETURNS integer LANGUAGE plpgsql AS $$ BEGIN RETURN -1; END $$`, "invalid original crash default"},
		{"negative_active_count", `UPDATE pgjobdb.jobs SET consecutive_expirations=-1 WHERE job_id='active'`, "negative crash count"},
		{"negative_archived_count", `UPDATE pgjobdb.jobs_archive SET consecutive_expirations=-1 WHERE job_id='archived'`, "negative crash count"},
	} {
		t.Run(test.name, func(t *testing.T) {
			withBareDatabase(t, func(ctx context.Context, db *sql.DB) {
				_, err := db.ExecContext(ctx, format13)
				require.NoError(t, err)
				format13Job(t, ctx, db, "active")
				format13Job(t, ctx, db, "archived")
				_, err = db.ExecContext(ctx, `SELECT pgjobdb.complete_native_unheld_job('c2','archived','operator','success'); SELECT pgjobdb.set_crash_concern_threshold(9)`)
				require.NoError(t, err)
				_, err = db.ExecContext(ctx, test.change)
				require.NoError(t, err)
				beforeCatalog, beforeRows := catalogSnapshot(t, ctx, db), resourceMigrationRows(t, ctx, db)
				install := installer.Installer{DB: db}
				_, err = install.CheckMigration(ctx)
				require.ErrorContains(t, err, test.diagnostic)
				require.Equal(t, beforeCatalog, catalogSnapshot(t, ctx, db))
				require.Equal(t, beforeRows, resourceMigrationRows(t, ctx, db))
				var staged bytes.Buffer
				confirmation, err := install.MigrateWithReport(ctx, &staged)
				require.ErrorContains(t, err, test.diagnostic)
				require.False(t, confirmation.Committed)
				require.Empty(t, staged.Bytes())
				require.Equal(t, beforeCatalog, catalogSnapshot(t, ctx, db))
				require.Equal(t, beforeRows, resourceMigrationRows(t, ctx, db))
			})
		})
	}
}

func resourceMigrationRows(t *testing.T, ctx context.Context, db *sql.DB) map[string]string {
	t.Helper()
	rows, err := db.QueryContext(ctx, `SELECT quote_ident(tablename) FROM pg_tables WHERE schemaname='pgjobdb'`)
	require.NoError(t, err)
	defer rows.Close()
	result := map[string]string{}
	for rows.Next() {
		var name, data string
		require.NoError(t, rows.Scan(&name))
		require.NoError(t, db.QueryRowContext(ctx, `SELECT COALESCE(jsonb_agg(value ORDER BY value::text),'[]')::text FROM (SELECT to_jsonb(row) AS value FROM pgjobdb.`+name+` row) snapshot`).Scan(&data))
		result[name] = data
	}
	require.NoError(t, rows.Err())
	return result
}

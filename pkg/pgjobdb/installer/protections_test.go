package installer_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/lib/pq"
	"testing"
	"time"

	"github.com/colony-2/pgjobdb/pkg/pgjobdb/installer"
	"github.com/stretchr/testify/require"
)

// Commit each damaged definition so the installer's independent transaction
// sees it. Capture the damaged catalog and rows, then prove rejection leaves
// that exact state intact; restore the authentic definition between variants.
func rejectAlteredProtection(t *testing.T, ctx context.Context, db *sql.DB, damage, restore string, rows func() string, target bool) {
	t.Helper()
	_, err := db.ExecContext(ctx, damage)
	require.NoError(t, err)
	before := catalogSnapshot(t, ctx, db)
	data := rows()
	install := installer.Installer{DB: db}
	_, err = install.Preflight(ctx)
	require.ErrorContains(t, err, "protection")
	require.ErrorContains(t, install.Migrate(ctx), "protection")
	if target {
		require.ErrorContains(t, install.Verify(ctx), "protection")
	}
	require.JSONEq(t, before, catalogSnapshot(t, ctx, db))
	require.JSONEq(t, data, rows())
	_, err = db.ExecContext(ctx, restore)
	require.NoError(t, err)
	_, err = install.Preflight(ctx)
	require.NoError(t, err)
}

// Include all retained closure fields and every owned row, without the
// normalization used when comparing data across different historical formats.
func protectionRows(t *testing.T, ctx context.Context, db *sql.DB) string {
	t.Helper()
	result := map[string]json.RawMessage{}
	rows, err := db.QueryContext(ctx, `SELECT tablename FROM pg_tables WHERE schemaname='pgjobdb' ORDER BY tablename`)
	require.NoError(t, err)
	var tables []string
	for rows.Next() {
		var name string
		require.NoError(t, rows.Scan(&name))
		tables = append(tables, name)
	}
	require.NoError(t, rows.Err())
	require.NoError(t, rows.Close())
	for _, table := range tables {
		var raw []byte
		require.NoError(t, db.QueryRowContext(ctx, `SELECT COALESCE(jsonb_agg(to_jsonb(r) ORDER BY to_jsonb(r)::text),'[]'::jsonb) FROM pgjobdb.`+pq.QuoteIdentifier(table)+` r`).Scan(&raw))
		result[table] = raw
	}
	raw, err := json.Marshal(result)
	require.NoError(t, err)
	return string(raw)
}

func TestRetainedGoalProtectionDefinitions(t *testing.T) {
	runDatabaseTest(t, func(ctx context.Context, db *sql.DB) {
		// Repeated complete catalog/data snapshots have a bounded multi-case budget.
		ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Minute)
		defer cancel()
		historicalJob(t, ctx, db, 12, "protected-origin")
		// The native submit signature remains compatible for this ordinary input.
		rows := func() string { return protectionRows(t, ctx, db) }
		var shape, origin, closure, immutable, active, body string
		require.NoError(t, db.QueryRowContext(ctx, `SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conrelid='pgjobdb.job_facts'::regclass AND conname='retained_goal_shape'`).Scan(&shape))
		require.NoError(t, db.QueryRowContext(ctx, `SELECT pg_get_indexdef('pgjobdb.facts_goal_origin_idx'::regclass)`).Scan(&origin))
		require.NoError(t, db.QueryRowContext(ctx, `SELECT pg_get_indexdef('pgjobdb.facts_goal_closure_idx'::regclass)`).Scan(&closure))
		require.NoError(t, db.QueryRowContext(ctx, `SELECT pg_get_triggerdef(oid) FROM pg_trigger WHERE tgrelid='pgjobdb.job_facts'::regclass AND tgname='immutable_retained_goal'`).Scan(&immutable))
		require.NoError(t, db.QueryRowContext(ctx, `SELECT pg_get_triggerdef(oid) FROM pg_trigger WHERE tgrelid='pgjobdb.jobs'::regclass AND tgname='check_active_goal'`).Scan(&active))
		require.NoError(t, db.QueryRowContext(ctx, `SELECT pg_get_functiondef('pgjobdb._immutable_retained_goal()'::regprocedure)`).Scan(&body))
		for _, c := range []struct{ name, damage, restore string }{
			{"weakened_check", `ALTER TABLE pgjobdb.job_facts DROP CONSTRAINT retained_goal_shape; ALTER TABLE pgjobdb.job_facts ADD CONSTRAINT retained_goal_shape CHECK(true)`, `ALTER TABLE pgjobdb.job_facts DROP CONSTRAINT retained_goal_shape; ALTER TABLE pgjobdb.job_facts ADD CONSTRAINT retained_goal_shape ` + shape},
			{"unvalidated_check", `ALTER TABLE pgjobdb.job_facts DROP CONSTRAINT retained_goal_shape; ALTER TABLE pgjobdb.job_facts ADD CONSTRAINT retained_goal_shape ` + shape + ` NOT VALID`, `ALTER TABLE pgjobdb.job_facts VALIDATE CONSTRAINT retained_goal_shape`},
			{"wrong_index_keys", `DROP INDEX pgjobdb.facts_goal_origin_idx; CREATE UNIQUE INDEX facts_goal_origin_idx ON pgjobdb.job_facts(tenant_id,job_id) WHERE goal_origin`, `DROP INDEX pgjobdb.facts_goal_origin_idx; ` + origin},
			{"wrong_index_collation", `DROP INDEX pgjobdb.facts_goal_origin_idx; CREATE UNIQUE INDEX facts_goal_origin_idx ON pgjobdb.job_facts(tenant_id,goal_id) WHERE goal_origin`, `DROP INDEX pgjobdb.facts_goal_origin_idx; ` + origin},
			{"wrong_index_predicate", `DROP INDEX pgjobdb.facts_goal_origin_idx; CREATE UNIQUE INDEX facts_goal_origin_idx ON pgjobdb.job_facts(tenant_id,goal_id COLLATE "C") WHERE goal_origin AND hidden_at IS NULL`, `DROP INDEX pgjobdb.facts_goal_origin_idx; ` + origin},
			{"wrong_index_table", `DROP INDEX pgjobdb.facts_goal_origin_idx; CREATE UNIQUE INDEX facts_goal_origin_idx ON pgjobdb.jobs_archive(tenant_id,goal_id) WHERE goal_id IS NOT NULL`, `DROP INDEX pgjobdb.facts_goal_origin_idx; ` + origin},
			{"nonunique_closure", `DROP INDEX pgjobdb.facts_goal_closure_idx; CREATE INDEX facts_goal_closure_idx ON pgjobdb.job_facts(tenant_id,goal_id) WHERE goal_closed_at IS NOT NULL`, `DROP INDEX pgjobdb.facts_goal_closure_idx; ` + closure},
			{"wrong_trigger_function", `DROP TRIGGER immutable_retained_goal ON pgjobdb.job_facts; CREATE TRIGGER immutable_retained_goal BEFORE UPDATE ON pgjobdb.job_facts FOR EACH ROW EXECUTE FUNCTION pgjobdb._immutable_goal_id()`, `DROP TRIGGER immutable_retained_goal ON pgjobdb.job_facts; ` + immutable},
			{"conditional_trigger", `DROP TRIGGER immutable_retained_goal ON pgjobdb.job_facts; CREATE TRIGGER immutable_retained_goal BEFORE UPDATE ON pgjobdb.job_facts FOR EACH ROW WHEN (false) EXECUTE FUNCTION pgjobdb._immutable_retained_goal()`, `DROP TRIGGER immutable_retained_goal ON pgjobdb.job_facts; ` + immutable},
			{"wrong_trigger_events", `DROP TRIGGER check_active_goal ON pgjobdb.jobs; CREATE TRIGGER check_active_goal BEFORE INSERT ON pgjobdb.jobs FOR EACH ROW EXECUTE FUNCTION pgjobdb._check_active_goal()`, `DROP TRIGGER check_active_goal ON pgjobdb.jobs; ` + active},
			{"disabled_trigger", `ALTER TABLE pgjobdb.job_facts DISABLE TRIGGER immutable_retained_goal`, `ALTER TABLE pgjobdb.job_facts ENABLE TRIGGER immutable_retained_goal`},
			{"noop_function", `CREATE OR REPLACE FUNCTION pgjobdb._immutable_retained_goal() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN RETURN NEW; END$$`, body},
		} {
			t.Run(c.name, func(t *testing.T) { rejectAlteredProtection(t, ctx, db, c.damage, c.restore, rows, true) })
		}
		t.Run("project_search_path", func(t *testing.T) {
			install := installer.Installer{DB: db}
			// Verify uses a canonical path to deparse retained scheduler protections.
			// Migration also checks the resolution of the shared project tables.
			_, err := db.ExecContext(ctx, `ALTER ROLE postgres SET search_path TO pgjobdb,public`)
			require.NoError(t, err)
			db.SetMaxIdleConns(0) // Use the new role default on the next connection.
			require.NoError(t, install.Verify(ctx))
			require.ErrorContains(t, install.Migrate(ctx), "project storage requires an unshadowed public search path")

			_, err = db.ExecContext(ctx, `ALTER ROLE postgres SET search_path TO public`)
			require.NoError(t, err)
			require.NoError(t, install.Migrate(ctx))
		})
	})
}

func TestGoalConversionRejectsAlteredProtections(t *testing.T) {
	for _, fixture := range []struct {
		version int
		schema  string
	}{{9, format9}, {10, format10}, {11, format11}, {12, format12BeforeAvailabilityRepair}} {
		t.Run(fmt.Sprint(fixture.version), func(t *testing.T) {
			withBareDatabase(t, func(ctx context.Context, db *sql.DB) {
				ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Minute)
				defer cancel()
				_, err := db.ExecContext(ctx, fixture.schema)
				require.NoError(t, err)
				historicalBaseline(t, ctx, db, fixture.version)
				rows := func() string { return protectionRows(t, ctx, db) }
				var trigger, body, fk string
				require.NoError(t, db.QueryRowContext(ctx, `SELECT pg_get_triggerdef(oid) FROM pg_trigger WHERE tgrelid='pgjobdb.job_facts'::regclass AND tgname='immutable_goal_id'`).Scan(&trigger))
				require.NoError(t, db.QueryRowContext(ctx, `SELECT pg_get_functiondef('pgjobdb._immutable_goal_id()'::regprocedure)`).Scan(&body))
				require.NoError(t, db.QueryRowContext(ctx, `SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conrelid='pgjobdb.jobs'::regclass AND conname='active_goal_fk'`).Scan(&fk))
				for _, c := range []struct{ name, damage, restore string }{
					{"missing_membership_trigger", `DROP TRIGGER immutable_goal_id ON pgjobdb.job_facts`, trigger},
					{"misbound_membership_trigger", `DROP TRIGGER immutable_goal_id ON pgjobdb.job_facts; CREATE TRIGGER immutable_goal_id BEFORE UPDATE OF goal_id ON pgjobdb.job_facts FOR EACH ROW EXECUTE FUNCTION pgjobdb._check_active_goal()`, `DROP TRIGGER immutable_goal_id ON pgjobdb.job_facts; ` + trigger},
					{"noop_membership_function", `CREATE OR REPLACE FUNCTION pgjobdb._immutable_goal_id() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN RETURN NEW; END$$`, body},
					{"unvalidated_lifecycle_fk", `ALTER TABLE pgjobdb.jobs DROP CONSTRAINT active_goal_fk; ALTER TABLE pgjobdb.jobs ADD CONSTRAINT active_goal_fk ` + fk + ` NOT VALID`, `ALTER TABLE pgjobdb.jobs VALIDATE CONSTRAINT active_goal_fk`},
					{"missing_lifecycle_fk", `ALTER TABLE pgjobdb.jobs DROP CONSTRAINT active_goal_fk`, `ALTER TABLE pgjobdb.jobs ADD CONSTRAINT active_goal_fk ` + fk},
				} {
					t.Run(c.name, func(t *testing.T) { rejectAlteredProtection(t, ctx, db, c.damage, c.restore, rows, false) })
				}
				require.NoError(t, (installer.Installer{DB: db}).Migrate(ctx))
				require.NoError(t, (installer.Installer{DB: db}).Verify(ctx))
			})
		})
	}
}

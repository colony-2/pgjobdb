package installer

import (
	"context"
	"database/sql"
	"fmt"

	pgjobdbsql "github.com/colony-2/pgjobdb"
)

// Installer applies and verifies the pgjobdb schema.
type Installer struct {
	DB     *sql.DB
	Schema string
}

// Apply executes the embedded pgjobdb DDL.
func (i Installer) Apply(ctx context.Context) error {
	if i.DB == nil {
		return fmt.Errorf("pgjobdb: installer DB is nil")
	}
	if ctx == nil {
		return fmt.Errorf("pgjobdb: context is nil")
	}
	if err := i.validateDatabase(ctx); err != nil {
		return err
	}

	_, err := i.DB.ExecContext(ctx, pgjobdbsql.SQL)
	return err
}

// Verify checks for core pgjobdb tables and functions.
func (i Installer) Verify(ctx context.Context) error {
	if i.DB == nil {
		return fmt.Errorf("pgjobdb: installer DB is nil")
	}
	if ctx == nil {
		return fmt.Errorf("pgjobdb: context is nil")
	}
	if err := i.validateSchemaName(); err != nil {
		return err
	}
	schema := i.schemaName()

	for _, tbl := range []string{"installation", "jobs", "jobs_archive", "jobs_trace", "schedules", "job_facts"} {
		if err := i.assertTable(ctx, schema, tbl); err != nil {
			return err
		}
	}
	for table, columns := range map[string][]string{
		"jobs":         {"route_job_type", "work_kind", "lease_payload", "lease_payload_visible", "lease_worker_id"},
		"jobs_archive": {"final_route_job_type", "final_work_kind", "final_lease_payload", "final_lease_payload_visible"},
	} {
		for _, column := range columns {
			if err := i.assertColumn(ctx, schema, table, column); err != nil {
				return err
			}
		}
	}
	for _, fn := range []string{
		"submit_native_job", "get_native_work",
		"complete_native_job", "complete_native_unheld_job",
		"validate_native_lease", "renew_native_lease", "cancel_native_job",
		"reschedule_native_job",
		"complete_native_task_work",
		"get_native_job", "get_native_job_status",
		"list_native_jobs",
		"list_native_schedule_runs",
		"upsert_schedule", "pause_schedule",
		"resume_schedule", "archive_schedule", "get_schedule", "list_schedules",
	} {
		if err := i.assertFunction(ctx, schema, fn); err != nil {
			return err
		}
	}
	if err := i.assertInstallation(ctx, schema); err != nil {
		return err
	}
	return nil
}

func (i Installer) validateDatabase(ctx context.Context) error {
	if err := i.validateSchemaName(); err != nil {
		return err
	}
	var oldScheduler bool
	if err := i.DB.QueryRowContext(ctx, `SELECT EXISTS (
		SELECT 1 FROM information_schema.schemata WHERE schema_name = 'pgwf'
	)`).Scan(&oldScheduler); err != nil {
		return fmt.Errorf("pgjobdb: inspect legacy scheduler: %w", err)
	}
	if oldScheduler {
		return fmt.Errorf("pgjobdb: legacy pgwf schema found; use a new empty database")
	}

	schema := i.schemaName()
	var initialized bool
	if err := i.DB.QueryRowContext(ctx, `SELECT EXISTS (
		SELECT 1 FROM information_schema.schemata WHERE schema_name = $1
	)`, schema).Scan(&initialized); err != nil {
		return fmt.Errorf("pgjobdb: inspect schema: %w", err)
	}
	if initialized {
		return i.assertInstallation(ctx, schema)
	}

	var existingTable sql.NullString
	if err := i.DB.QueryRowContext(ctx, `SELECT n.nspname || '.' || c.relname
		FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE c.relkind IN ('r', 'p')
			AND n.nspname NOT IN ('pg_catalog', 'information_schema')
			AND n.nspname NOT LIKE 'pg_toast%'
		ORDER BY n.nspname, c.relname LIMIT 1`).Scan(&existingTable); err != nil && err != sql.ErrNoRows {
		return fmt.Errorf("pgjobdb: inspect existing tables: %w", err)
	}
	if existingTable.Valid {
		return fmt.Errorf("pgjobdb: existing table %s found; use a new empty database", existingTable.String)
	}
	return nil
}

func (i Installer) validateSchemaName() error {
	schema := i.schemaName()
	if len(schema) == 0 || len(schema) > 63 {
		return fmt.Errorf("pgjobdb: invalid schema name %q", schema)
	}
	for offset, char := range schema {
		if (char >= 'a' && char <= 'z') || char == '_' ||
			(offset > 0 && char >= '0' && char <= '9') {
			continue
		}
		return fmt.Errorf("pgjobdb: invalid schema name %q", schema)
	}
	if schema != "pgjobdb" {
		return fmt.Errorf("pgjobdb: unsupported schema %q; use pgjobdb on a new empty database", schema)
	}
	return nil
}

func (i Installer) assertInstallation(ctx context.Context, schema string) error {
	var marker sql.NullString
	if err := i.DB.QueryRowContext(ctx,
		`SELECT to_regclass($1)::text`, schema+".installation").Scan(&marker); err != nil {
		return fmt.Errorf("pgjobdb: inspect installation marker: %w", err)
	}
	if !marker.Valid {
		return fmt.Errorf("pgjobdb: schema %s has no installation marker", schema)
	}
	var version int
	query := "SELECT format_version FROM " + schema + ".installation WHERE name = $1"
	if err := i.DB.QueryRowContext(ctx, query, schema).Scan(&version); err != nil {
		return fmt.Errorf("pgjobdb: read installation marker: %w", err)
	}
	if version != 1 {
		return fmt.Errorf("pgjobdb: unsupported schema format %d", version)
	}
	return nil
}

func (i Installer) schemaName() string {
	if i.Schema == "" {
		return "pgjobdb"
	}
	return i.Schema
}

func (i Installer) assertTable(ctx context.Context, schema, table string) error {
	const stmt = `
SELECT EXISTS (
    SELECT 1
    FROM information_schema.tables
    WHERE table_schema = $1 AND table_name = $2
)`
	var exists bool
	if err := i.DB.QueryRowContext(ctx, stmt, schema, table).Scan(&exists); err != nil {
		return err
	}
	if !exists {
		return fmt.Errorf("pgjobdb: missing table %s.%s", schema, table)
	}
	return nil
}

func (i Installer) assertColumn(ctx context.Context, schema, table, column string) error {
	const stmt = `
SELECT EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = $1 AND table_name = $2 AND column_name = $3
)`
	var exists bool
	if err := i.DB.QueryRowContext(ctx, stmt, schema, table, column).Scan(&exists); err != nil {
		return err
	}
	if !exists {
		return fmt.Errorf("pgjobdb: missing column %s.%s.%s", schema, table, column)
	}
	return nil
}

func (i Installer) assertFunction(ctx context.Context, schema, fn string) error {
	const stmt = `
SELECT EXISTS (
    SELECT 1
    FROM information_schema.routines
    WHERE routine_schema = $1 AND routine_name = $2
)`
	var exists bool
	if err := i.DB.QueryRowContext(ctx, stmt, schema, fn).Scan(&exists); err != nil {
		return err
	}
	if !exists {
		return fmt.Errorf("pgjobdb: missing function %s.%s", schema, fn)
	}
	return nil
}

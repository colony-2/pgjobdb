package installer

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

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

	_, err := i.DB.ExecContext(ctx, i.renderedSQL())
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
		"jobs":         {"route_job_type", "work_kind", "lease_payload"},
		"jobs_archive": {"final_route_job_type", "final_work_kind", "final_lease_payload"},
	} {
		for _, column := range columns {
			if err := i.assertColumn(ctx, schema, table, column); err != nil {
				return err
			}
		}
	}
	for _, fn := range []string{
		"submit_job", "get_work", "get_job_lease", "extend_lease",
		"reschedule_job", "complete_job", "upsert_schedule", "pause_schedule",
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

	var chapterTable sql.NullString
	if err := i.DB.QueryRowContext(ctx,
		`SELECT to_regclass('jobdb_chapter_stories')::text`).Scan(&chapterTable); err != nil {
		return fmt.Errorf("pgjobdb: inspect chapter state: %w", err)
	}
	if chapterTable.Valid {
		var hasChapters bool
		if err := i.DB.QueryRowContext(ctx,
			`SELECT EXISTS (SELECT 1 FROM jobdb_chapter_stories)`).Scan(&hasChapters); err != nil {
			return fmt.Errorf("pgjobdb: inspect chapter rows: %w", err)
		}
		if hasChapters {
			return fmt.Errorf("pgjobdb: existing JobDB chapter state found; use a new empty database")
		}
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

func (i Installer) renderedSQL() string {
	schema := i.schemaName()
	if schema == "pgjobdb" {
		return pgjobdbsql.SQL
	}
	replacement := strings.ReplaceAll(pgjobdbsql.SQL, "pgjobdb", schema)
	return replacement
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

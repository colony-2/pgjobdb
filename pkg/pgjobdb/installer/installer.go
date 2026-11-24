package installer

import (
	"context"
	"database/sql"
	"fmt"
	"io"
)

// Installer applies and verifies the pgjobdb schema.
type Installer struct {
	DB     *sql.DB
	Schema string
	// ReportWriter receives the authoritative staged report before commit. A write failure rolls back conversion.
	ReportWriter io.Writer
}

// Apply prepares fresh storage or validates current storage. For older formats use Migrate.
func (i Installer) Apply(ctx context.Context) error {
	_, err := i.ApplyWithReport(ctx, i.ReportWriter)
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
	if err := i.assertInstallation(ctx, schema); err != nil {
		return err
	}

	for _, tbl := range []string{"lease_events", "cells", "cell_names", "cell_catalog", "installation", "jobs", "jobs_archive", "jobs_trace", "job_facts", "job_client_state", "job_preparations", "dependency_waits", "dependency_targets"} {
		if err := i.assertTable(ctx, schema, tbl); err != nil {
			return err
		}
	}
	for table, columns := range map[string][]string{
		"dependency_waits": {"deadline_exact"},
		"job_preparations": {"selected_goal_id", "receipt"},
		"job_facts":        {"append_key", "append_gen", "hidden_at", "goal_id", "cell_id", "cell_repo", "cell_name", "goal_origin", "goal_created_at", "goal_closed_at", "goal_success_job_id", "historical_goal_success", "record_kind", "terminal_failure"},
		"job_client_state": {"client_payload", "revision", "initial_payload_digest"},
		"jobs":             {"goal_id", "dependency_wait_id", "route_job_type", "work_kind", "lease_worker_id", "lease_route_job_type", "lease_work_kind", "lease_task_type", "alternate_at", "wait_until_is_deadline"},
		"jobs_archive":     {"final_route_job_type", "final_work_kind", "final_alternate_at"},
	} {
		for _, column := range columns {
			if err := i.assertColumn(ctx, schema, table, column); err != nil {
				return err
			}
		}
	}
	for _, fn := range []string{
		"prepare_job", "goal_info", "fail_goal", "dependency_wait_info",
		"submit_native_job", "get_native_work", "delete_native_jobs",
		"job_status_at", "effective_native_route",
		"complete_native_job", "complete_native_unheld_job",
		"validate_native_lease", "renew_native_lease", "cancel_native_job",
		"reschedule_native_job",
		"claim_native_task", "claim_append", "release_append",
		"get_native_job", "get_native_job_status",
		"list_native_jobs",
	} {
		if err := i.assertFunction(ctx, schema, fn); err != nil {
			return err
		}
	}
	for _, object := range []string{"goals", "goal_candidates"} {
		var exists bool
		if err := i.DB.QueryRowContext(ctx, `SELECT to_regclass($1) IS NOT NULL`, schema+"."+object).Scan(&exists); err != nil {
			return err
		}
		if exists {
			return fmt.Errorf("pgjobdb: removed table %s remains", object)
		}
	}
	var retired bool
	if err := i.DB.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname=$1 AND p.proname='add_goal_candidate')`, schema).Scan(&retired); err != nil {
		return err
	}
	if retired {
		return fmt.Errorf("pgjobdb: removed add_goal_candidate function remains")
	}
	tx, err := i.DB.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	version, err := inspectInstallation(ctx, tx)
	if err != nil {
		return err
	}
	if version >= 9 && version < FormatVersion {
		return fmt.Errorf("pgjobdb: format %d requires explicit offline migration to format %d", version, FormatVersion)
	}
	if version != FormatVersion {
		return fmt.Errorf("pgjobdb: unsupported schema format %d", version)
	}
	if err = validateInput(ctx, tx, FormatVersion); err != nil {
		return err
	}
	return validateTargetContract(ctx, tx)
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
	if version >= 9 && version < FormatVersion {
		return fmt.Errorf("pgjobdb: format %d requires explicit offline migration to format %d", version, FormatVersion)
	}
	if version != FormatVersion {
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

// VerifyTransaction validates the scheduler contract and retained references in
// the caller's snapshot. Project adoption calls it after excluding writers.
func VerifyTransaction(ctx context.Context, tx *sql.Tx) error {
	if ctx == nil || tx == nil {
		return fmt.Errorf("pgjobdb: context and transaction are required")
	}
	version, err := inspectInstallation(ctx, tx)
	if err != nil {
		return err
	}
	if version != FormatVersion {
		return fmt.Errorf("pgjobdb: unsupported schema format %d", version)
	}
	return validateInput(ctx, tx, version)
}

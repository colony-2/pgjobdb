package installer

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"strings"

	pgsql "github.com/colony-2/pgjobdb/pkg/pgjobdb"
	"github.com/colony-2/pgjobdb/pkg/pgjobdb/internal/projectstorage"
	"github.com/lib/pq"
)

var leaseActivityDefinitions = []string{
	"_archive_and_delete_job", "_complete_native_locked_job", "_extend_lease",
	"archive_cancelled_jobs", "claim_append", "claim_native_task", "delete_native_jobs",
	"get_native_work", "list_native_jobs", "release_append", "renew_native_lease",
}

func validateLeaseActivity(ctx context.Context, tx *sql.Tx, format int) error {
	var total, valid int
	if err := tx.QueryRowContext(ctx, `SELECT count(*), count(*) FILTER (WHERE a.atttypid='timestamptz'::regtype AND a.atttypmod=-1 AND NOT a.attnotnull AND NOT a.atthasdef AND a.attidentity='' AND a.attgenerated='')
 FROM pg_attribute a WHERE a.attrelid IN ('pgjobdb.jobs'::regclass,'pgjobdb.jobs_archive'::regclass)
   AND a.attname='last_lease_updated_at' AND NOT a.attisdropped`).Scan(&total, &valid); err != nil {
		return err
	}
	program := pgsql.UpgradeSQL
	if format < FormatVersion && total != 0 {
		return fmt.Errorf("pgjobdb: unexpected lease activity columns on format %d", format)
	}
	switch {
	case format < 13:
		return nil
	case format == 13:
		program = format13SQL
		var partial bool
		if err := tx.QueryRowContext(ctx, `SELECT to_regclass('pgjobdb.resource_configuration') IS NOT NULL
 OR to_regclass('pgjobdb.resource_receipts') IS NOT NULL OR to_regclass('pgjobdb.resource_sequences') IS NOT NULL
 OR EXISTS(SELECT 1 FROM pg_attribute WHERE attrelid IN ('pgjobdb.jobs'::regclass,'pgjobdb.jobs_archive'::regclass) AND attname IN ('crash_budget_remaining','resource_not_before_ns','resource_not_before','resource_deadline','resource_resume') AND NOT attisdropped)`).Scan(&partial); err != nil {
			return err
		}
		if total != 0 || partial {
			return fmt.Errorf("pgjobdb: partial format-14 definitions are not a supported format-13 source")
		}
	case format < FormatVersion:
		program = pgsql.HistoricalLeaseSQL
	case total != 2 || valid != 2:
		return fmt.Errorf("pgjobdb: unsupported or incomplete format 16: expected nullable last_lease_updated_at TIMESTAMPTZ with no default on jobs and jobs_archive")
	}
	if format >= crashBudgetFormat {
		if err := projectstorage.VerifySchedulerClock(ctx, tx); err != nil {
			return err
		}
	}
	var guarded bool
	if err := tx.QueryRowContext(ctx, `SELECT to_regclass('pgjobdb.project_installation') IS NOT NULL`).Scan(&guarded); err != nil {
		return err
	}
	if guarded {
		var err error
		program, err = projectstorage.GuardSchedulerSQL(program)
		if err != nil {
			return err
		}
	}
	expected := make(map[string][]string)
	for _, definition := range projectstorage.EntryDefinition.FindAllStringSubmatch(program, -1) {
		expected[definition[1]] = definition
	}
	rows, err := tx.QueryContext(ctx, `SELECT p.proname,p.prosrc,l.lanname,p.provolatile::text,
 p.prokind='f' AND NOT p.proisstrict AND NOT p.prosecdef AND NOT p.proleakproof AND p.proparallel='u',
 lower(coalesce(array_to_string(p.proconfig,','),''))
 FROM pg_proc p JOIN pg_language l ON l.oid=p.prolang WHERE p.pronamespace='pgjobdb'::regnamespace AND p.proname=ANY($1)`, pq.Array(leaseActivityDefinitions))
	if err != nil {
		return err
	}
	defer rows.Close()
	seen := make(map[string]bool)
	for rows.Next() {
		var name, body, language, volatility string
		var executionAttributesValid bool
		var settings string
		if err := rows.Scan(&name, &body, &language, &volatility, &executionAttributesValid, &settings); err != nil {
			return err
		}
		definition := expected[name]
		if definition == nil {
			return fmt.Errorf("pgjobdb: unexpected lease definition %s in format %d", name, format)
		}
		header := strings.ToUpper(strings.SplitN(definition[0], "AS $$", 2)[0])
		wantLanguage, wantVolatility := "plpgsql", "v"
		if strings.Contains(header, "LANGUAGE SQL") {
			wantLanguage = "sql"
		}
		if strings.Contains(header, "STABLE") {
			wantVolatility = "s"
		}
		wantSettings := ""
		if strings.Contains(header, "SET TIMEZONE = 'UTC'") {
			wantSettings = "timezone=utc"
		}
		if seen[name] || body != definition[2] || language != wantLanguage || volatility != wantVolatility || !executionAttributesValid || settings != wantSettings {
			return fmt.Errorf("pgjobdb: altered lease definition %s in format %d", name, format)
		}
		seen[name] = true
	}
	if err := rows.Err(); err != nil {
		return err
	}
	for _, name := range leaseActivityDefinitions {
		if !seen[name] {
			return fmt.Errorf("pgjobdb: missing lease definition %s in format %d", name, format)
		}
	}
	return nil
}

var leaseStatusView = regexp.MustCompile(`(?s)CREATE OR REPLACE VIEW pgjobdb\.jobs_with_status AS.*?;`)

func upgradeCrashBudget(ctx context.Context, tx *sql.Tx, guarded bool, allowance int64) error {
	program := pgsql.UpgradeSQL
	for _, statement := range []string{
		"ALTER TABLE pgjobdb.cells ADD COLUMN IF NOT EXISTS archived_at_ns BIGINT;",
		"ALTER TABLE pgjobdb.jobs ADD COLUMN IF NOT EXISTS last_lease_updated_at TIMESTAMPTZ;",
		"ALTER TABLE pgjobdb.jobs_archive ADD COLUMN IF NOT EXISTS last_lease_updated_at TIMESTAMPTZ;",
	} {
		program = strings.Replace(program, statement, "", 1)
	}
	historical := make(map[string]string)
	for _, definition := range projectstorage.EntryDefinition.FindAllStringSubmatch(pgsql.HistoricalLeaseSQL, -1) {
		historical[definition[1]] = definition[0]
	}
	program = projectstorage.EntryDefinition.ReplaceAllStringFunc(program, func(definition string) string {
		name := projectstorage.EntryDefinition.FindStringSubmatch(definition)[1]
		if old, ok := historical[name]; ok {
			return old
		}
		return definition
	})
	program = leaseStatusView.ReplaceAllStringFunc(program, func(string) string { return leaseStatusView.FindString(pgsql.HistoricalLeaseSQL) })
	if guarded {
		var err error
		program, err = projectstorage.GuardSchedulerSQL(program)
		if err != nil {
			return err
		}
	}
	if err := executeProgram(ctx, tx, program); err != nil {
		return fmt.Errorf("pgjobdb: install countdown definitions: %w", err)
	}
	if guarded {
		if err := projectstorage.InstallRowGuards(ctx, tx, []string{"pgjobdb.resource_receipts", "pgjobdb.resource_sequences"}); err != nil {
			return err
		}
	}
	for _, query := range []string{
		`UPDATE pgjobdb.resource_configuration SET crash_allowance=$1 WHERE singleton`,
		`UPDATE pgjobdb.jobs SET crash_budget_remaining=greatest($1-consecutive_expirations,0)`,
		`UPDATE pgjobdb.jobs_archive a SET crash_budget_remaining=CASE WHEN f.record_kind='EXECUTION' THEN greatest($1-a.consecutive_expirations,0) ELSE 0 END FROM pgjobdb.job_facts f WHERE f.tenant_id=a.tenant_id AND f.job_id=a.job_id`,
	} {
		if _, err := tx.ExecContext(ctx, query, allowance); err != nil {
			return err
		}
	}
	return validateInput(ctx, tx, crashBudgetFormat)
}

func upgradeLeaseActivity(ctx context.Context, tx *sql.Tx, guarded bool) error {
	program := pgsql.UpgradeSQL
	if guarded {
		var err error
		program, err = projectstorage.GuardSchedulerSQL(program)
		if err != nil {
			return err
		}
	}
	var statements strings.Builder
	statements.WriteString(`ALTER TABLE pgjobdb.jobs ADD COLUMN last_lease_updated_at TIMESTAMPTZ;
ALTER TABLE pgjobdb.jobs_archive ADD COLUMN last_lease_updated_at TIMESTAMPTZ;
`)
	statements.WriteString(leaseStatusView.FindString(program))
	statements.WriteString("\n")
	for _, definition := range projectstorage.EntryDefinition.FindAllStringSubmatch(program, -1) {
		for _, name := range leaseActivityDefinitions {
			if definition[1] == name {
				statements.WriteString(definition[0])
				statements.WriteString("\n")
			}
		}
	}
	return executeProgram(ctx, tx, statements.String())
}

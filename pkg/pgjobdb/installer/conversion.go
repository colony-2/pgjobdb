package installer

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"slices"
	"strings"

	pgsql "github.com/colony-2/pgjobdb/pkg/pgjobdb"
	"github.com/lib/pq"
)

const FormatVersion = 13

// ConversionReport is evidence captured from locked installation inputs. It is
// never runtime storage. A preflight is advisory; only ApplyWithReport captures
// the inputs that were actually converted. Committed IDs require confirmation.
type ConversionReport struct {
	ReportVersion         int               `json:"reportVersion"`
	LegacyInputsAvailable bool              `json:"legacyInputsAvailable"`
	SourceFormat          int               `json:"sourceFormat"`
	TargetFormat          int               `json:"targetFormat"`
	Advisory              bool              `json:"advisory"`
	Tenants               []json.RawMessage `json:"tenants"`
	RemovedLinks          []json.RawMessage `json:"removedLinks"`
	AffectedWaits         []json.RawMessage `json:"affectedPendingWaits"`
	AffectedPreparations  []json.RawMessage `json:"affectedPreparations"`
	CrossPrimaryProducers []json.RawMessage `json:"crossPrimaryProducers"`
	MetadataDerivations   []json.RawMessage `json:"metadataDerivations"`
	LiveOverlaps          []json.RawMessage `json:"liveOverlaps"`
	MigrationFailures     []json.RawMessage `json:"migrationFailures"`
}

// CommitConfirmation binds a successful commit to its exact exported report.
// A commit error is uncertain: retain the staged report and reconcile on a new
// connection before retrying. The installer never retries an uncertain commit.
type CommitConfirmation struct {
	Committed           bool     `json:"committed"`
	SourceFormat        int      `json:"sourceFormat"`
	TargetFormat        int      `json:"targetFormat"`
	ReportSHA256        string   `json:"reportSha256"`
	MigrationFailureIDs []string `json:"migrationFailureIds"`
}

func (i Installer) Preflight(ctx context.Context) (ConversionReport, error) {
	tx, err := i.begin(ctx, true)
	if err != nil {
		return ConversionReport{}, err
	}
	defer tx.Rollback()
	version, err := inspectInstallation(ctx, tx)
	if err != nil {
		return ConversionReport{}, err
	}
	if version > 0 {
		if err = validateInput(ctx, tx, version); err != nil {
			return ConversionReport{}, err
		}
	}
	report, err := captureReport(ctx, tx, version)
	report.Advisory = true
	return report, err
}

func (i Installer) begin(ctx context.Context, readOnly bool) (*sql.Tx, error) {
	if i.DB == nil {
		return nil, fmt.Errorf("pgjobdb: installer DB is nil")
	}
	if ctx == nil {
		return nil, fmt.Errorf("pgjobdb: context is nil")
	}
	if err := i.validateSchemaName(); err != nil {
		return nil, err
	}
	return i.DB.BeginTx(ctx, &sql.TxOptions{ReadOnly: readOnly})
}

func (i Installer) ApplyWithReport(ctx context.Context, destination io.Writer) (CommitConfirmation, error) {
	tx, err := i.begin(ctx, false)
	if err != nil {
		return CommitConfirmation{}, err
	}
	defer tx.Rollback()
	// Competing installers serialize even for a fresh database, without touching
	// runtime's tenant lock protocol. Acquire before inspecting the marker.
	if _, err = tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtextextended('pgjobdb.installation',0))`); err != nil {
		return CommitConfirmation{}, err
	}
	version, err := inspectInstallation(ctx, tx)
	if err != nil {
		return CommitConfirmation{}, err
	}
	if version > 0 {
		if err = validateInput(ctx, tx, version); err != nil {
			return CommitConfirmation{}, err
		}
		tenants, err := inputTenants(ctx, tx)
		if err != nil {
			return CommitConfirmation{}, err
		}
		for _, tenant := range tenants {
			if _, err = tx.ExecContext(ctx, `SELECT pgjobdb._goal_lock($1)`, tenant); err != nil {
				return CommitConfirmation{}, err
			}
		}
		rows, err := tx.QueryContext(ctx, `SELECT tablename FROM pg_tables WHERE schemaname='pgjobdb' ORDER BY tablename COLLATE "C"`)
		if err != nil {
			return CommitConfirmation{}, err
		}
		var tables []string
		for rows.Next() {
			var name string
			if err = rows.Scan(&name); err != nil {
				rows.Close()
				return CommitConfirmation{}, err
			}
			tables = append(tables, "pgjobdb."+pq.QuoteIdentifier(name))
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return CommitConfirmation{}, err
		}
		// NOWAIT prevents deadlock with a new-tenant writer holding a table/row lock
		// while awaiting a tenant lock. Failure is a clean transactional rejection.
		if _, err = tx.ExecContext(ctx, "LOCK TABLE "+strings.Join(tables, ",")+" IN ACCESS EXCLUSIVE MODE NOWAIT"); err != nil {
			return CommitConfirmation{}, fmt.Errorf("pgjobdb: exclude conversion writers: %w", err)
		}
		current, err := inspectInstallation(ctx, tx)
		if err != nil {
			return CommitConfirmation{}, err
		}
		covered, err := inputTenants(ctx, tx)
		if err != nil {
			return CommitConfirmation{}, err
		}
		if current != version || !slices.Equal(tenants, covered) {
			return CommitConfirmation{}, fmt.Errorf("pgjobdb: installation inputs changed before writer exclusion; retry from current inputs")
		}
		if err = validateInput(ctx, tx, version); err != nil {
			return CommitConfirmation{}, err
		}
	}
	report, err := captureReport(ctx, tx, version)
	if err != nil {
		return CommitConfirmation{}, err
	}
	if version > 0 && version < 13 {
		// Historical views include retired scheduler columns. Replace only these
		// owned projections; unexpected external dependents fail without CASCADE.
		if _, err = tx.ExecContext(ctx, `DROP VIEW pgjobdb.native_jobs`); err != nil {
			return CommitConfirmation{}, err
		}
	}
	var projectsInstalled bool
	if version == 13 {
		if err = tx.QueryRowContext(ctx, `SELECT to_regclass('pgjobdb.project_installation') IS NOT NULL`).Scan(&projectsInstalled); err != nil {
			return CommitConfirmation{}, err
		}
	}
	if !projectsInstalled {
		if _, err = tx.ExecContext(ctx, pgsql.TransactionSQL); err != nil {
			return CommitConfirmation{}, fmt.Errorf("pgjobdb: transactional conversion: %w", err)
		}
	}
	if err = validateInput(ctx, tx, 13); err != nil {
		return CommitConfirmation{}, fmt.Errorf("pgjobdb: validate converted contract: %w", err)
	}
	if version > 0 && version < 13 {
		report.MigrationFailures, err = reportRows(ctx, tx, `SELECT jsonb_build_object('tenantId',tenant_id,'goalId',goal_id,'jobId',job_id,'failure',terminal_failure) FROM pgjobdb.job_facts WHERE record_kind='MIGRATED_GOAL_FAILURE' ORDER BY tenant_id COLLATE "C",goal_id COLLATE "C"`)
		if err != nil {
			return CommitConfirmation{}, err
		}
	}
	encoded, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return CommitConfirmation{}, err
	}
	encoded = append(encoded, '\n')
	if destination != nil {
		n, e := destination.Write(encoded)
		if e != nil {
			return CommitConfirmation{}, fmt.Errorf("pgjobdb: export staged conversion report: %w", e)
		}
		if n != len(encoded) {
			return CommitConfirmation{}, io.ErrShortWrite
		}
	}
	digest := sha256.Sum256(encoded)
	confirmation := CommitConfirmation{SourceFormat: version, TargetFormat: 13, ReportSHA256: hex.EncodeToString(digest[:]), MigrationFailureIDs: []string{}}
	for _, raw := range report.MigrationFailures {
		var identity struct {
			JobID string `json:"jobId"`
		}
		if err = json.Unmarshal(raw, &identity); err != nil {
			return CommitConfirmation{}, err
		}
		confirmation.MigrationFailureIDs = append(confirmation.MigrationFailureIDs, identity.JobID)
	}
	if err = tx.Commit(); err != nil {
		return confirmation, fmt.Errorf("pgjobdb: conversion commit not confirmed; reconcile staged report before retry: %w", err)
	}
	confirmation.Committed = true
	return confirmation, nil
}

func inspectInstallation(ctx context.Context, tx *sql.Tx) (int, error) {
	var legacy, exists bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM pg_namespace WHERE nspname='pgwf'),EXISTS(SELECT 1 FROM pg_namespace WHERE nspname='pgjobdb')`).Scan(&legacy, &exists); err != nil {
		return 0, err
	}
	if legacy {
		return 0, fmt.Errorf("pgjobdb: legacy pgwf schema found; use a new empty database")
	}
	if !exists {
		var table sql.NullString
		if err := tx.QueryRowContext(ctx, `SELECT min(n.nspname||'.'||c.relname) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE c.relkind IN ('r','p') AND n.nspname NOT IN ('pg_catalog','information_schema') AND n.nspname NOT LIKE 'pg_toast%'`).Scan(&table); err != nil {
			return 0, err
		}
		if table.Valid {
			return 0, fmt.Errorf("pgjobdb: existing table %s found; use a new empty database", table.String)
		}
		return 0, nil
	}
	if err := tx.QueryRowContext(ctx, `SELECT to_regclass('pgjobdb.installation') IS NOT NULL`).Scan(&exists); err != nil {
		return 0, err
	}
	if !exists {
		return 0, fmt.Errorf("pgjobdb: schema pgjobdb has no installation marker")
	}
	var version, count int
	if err := tx.QueryRowContext(ctx, `SELECT count(*),COALESCE(min(format_version) FILTER(WHERE name='pgjobdb'),0) FROM pgjobdb.installation`).Scan(&count, &version); err != nil {
		return 0, err
	}
	if count != 1 {
		return 0, fmt.Errorf("pgjobdb: corrupt installation marker")
	}
	if version < 9 || version > 13 {
		return 0, fmt.Errorf("pgjobdb: unsupported schema format %d", version)
	}
	return version, nil
}

func inputTenants(ctx context.Context, tx *sql.Tx) ([]string, error) {
	rows, err := tx.QueryContext(ctx, `SELECT tenant_id FROM (SELECT tenant_id FROM pgjobdb.cells UNION SELECT tenant_id FROM pgjobdb.job_facts UNION SELECT tenant_id FROM pgjobdb.job_preparations) t ORDER BY tenant_id COLLATE "C"`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []string{}
	for rows.Next() {
		var tenant string
		if err = rows.Scan(&tenant); err != nil {
			return nil, err
		}
		out = append(out, tenant)
	}
	return out, rows.Err()
}

// Validate before DDL as well as after writer exclusion. Never repair incomplete
// or version-marker-only inputs by silently creating their missing stores.
func validateInput(ctx context.Context, tx *sql.Tx, version int) error {
	required := []string{"cells", "cell_catalog", "cell_names", "cell_requests", "jobs", "jobs_archive", "jobs_trace", "job_facts", "job_client_state", "job_preparations", "dependency_waits", "dependency_targets"}
	if version < 13 {
		required = append(required, "goals", "goal_candidates")
	} else {
		required = append(required, "lease_events")
	}
	for _, table := range required {
		var exists bool
		if err := tx.QueryRowContext(ctx, `SELECT to_regclass($1) IS NOT NULL`, "pgjobdb."+table).Scan(&exists); err != nil {
			return err
		}
		if !exists {
			return fmt.Errorf("pgjobdb: incomplete schema format %d: missing table %s", version, table)
		}
	}
	// Reject missing/altered guards before scanning data, and repeat this check
	// after writer exclusion through the same validation path.
	if err := validateProtections(ctx, tx, version); err != nil {
		return err
	}
	// PostgreSQL validates referenced columns on preparation, including empty inputs.
	checks := []string{
		`SELECT NOT EXISTS(SELECT 1 FROM pgjobdb.job_facts f LEFT JOIN pgjobdb.cells c USING(tenant_id,cell_id) WHERE c.cell_id IS NULL OR f.goal_id IS NULL OR f.goal_id='')`,
		`SELECT NOT EXISTS(SELECT 1 FROM pgjobdb.job_facts f LEFT JOIN pgjobdb.jobs j USING(tenant_id,job_id) LEFT JOIN pgjobdb.jobs_archive a USING(tenant_id,job_id) LEFT JOIN pgjobdb.job_client_state s USING(tenant_id,job_id) WHERE (j.job_id IS NULL)=(a.job_id IS NULL) OR COALESCE(j.goal_id,a.goal_id)<>f.goal_id OR s.job_id IS NULL)`,
		`SELECT NOT EXISTS(SELECT 1 FROM pgjobdb.job_preparations p LEFT JOIN pgjobdb.cells c USING(tenant_id,cell_id) WHERE c.cell_id IS NULL)`,
		`SELECT NOT EXISTS(SELECT 1 FROM pgjobdb.jobs j LEFT JOIN pgjobdb.job_facts f USING(tenant_id,job_id) WHERE f.job_id IS NULL)`,
		`SELECT NOT EXISTS(SELECT 1 FROM pgjobdb.jobs_archive a LEFT JOIN pgjobdb.job_facts f USING(tenant_id,job_id) WHERE f.job_id IS NULL)`,
		`SELECT NOT EXISTS(SELECT 1 FROM pgjobdb.dependency_waits w LEFT JOIN pgjobdb.job_facts f USING(tenant_id,job_id) WHERE f.job_id IS NULL)`,
	}
	if version < 13 {
		checks = append(checks,
			`SELECT NOT EXISTS(SELECT 1 FROM pgjobdb.goals g WHERE NOT EXISTS(SELECT 1 FROM pgjobdb.job_facts f WHERE f.tenant_id=g.tenant_id AND f.goal_id=g.goal_id AND f.cell_id=g.cell_id AND f.cell_repo=g.cell_repo))`,
			`SELECT NOT EXISTS(SELECT 1 FROM pgjobdb.job_facts f LEFT JOIN pgjobdb.goals g USING(tenant_id,goal_id) WHERE g.goal_id IS NULL OR f.cell_id<>g.cell_id OR f.cell_repo<>g.cell_repo)`,
			`SELECT NOT EXISTS(SELECT 1 FROM pgjobdb.goals g LEFT JOIN pgjobdb.jobs_archive a ON a.tenant_id=g.tenant_id AND a.job_id=g.successful_job_id WHERE g.state='SUCCEEDED' AND (a.job_id IS NULL OR a.completion_status<>'success' OR g.closed_at IS NULL))`,
			`SELECT NOT EXISTS(SELECT 1 FROM pgjobdb.goals g WHERE g.state='FAILED' AND (g.closed_at IS NULL OR g.failure IS NULL OR g.failure->>'code' IS DISTINCT FROM 'goal_failed' OR g.failure->>'goalId' IS DISTINCT FROM g.goal_id OR COALESCE(btrim(g.failure->>'message'),'')='' OR octet_length(g.failure->>'message')>16384 OR (g.failure->>'failedAt')::timestamptz IS DISTINCT FROM g.closed_at OR jsonb_typeof(g.failure->'terminalJobIds') IS DISTINCT FROM 'array' OR jsonb_typeof(g.failure->'nonTerminalJobIds') IS DISTINCT FROM 'array'))`,
			`SELECT NOT EXISTS(SELECT 1 FROM pgjobdb.goal_candidates l LEFT JOIN pgjobdb.goals g USING(tenant_id,goal_id) LEFT JOIN pgjobdb.job_facts f USING(tenant_id,job_id) WHERE g.goal_id IS NULL OR f.job_id IS NULL)`)
	} else {
		checks = append(checks, `SELECT NOT EXISTS(SELECT 1 FROM pgjobdb.job_facts f WHERE NOT EXISTS(SELECT 1 FROM pgjobdb.job_facts o WHERE o.tenant_id=f.tenant_id AND o.goal_id=f.goal_id AND o.goal_origin))`)
	}
	for _, query := range checks {
		var valid bool
		if err := tx.QueryRowContext(ctx, query).Scan(&valid); err != nil {
			return fmt.Errorf("pgjobdb: inspect format %d: %w", version, err)
		}
		if !valid {
			return fmt.Errorf("pgjobdb: corrupt retained references in format %d", version)
		}
	}
	// Known schema differences are additive. Missing format-specific columns or
	// definitions are corruption, not a migration opportunity.
	columns := map[string][]string{"jobs": {"lease_route_job_type", "wait_until_is_deadline"}, "job_facts": {"initial_dependencies", "hidden_at", "cell_name"}, "dependency_waits": {"deadline_exact"}}
	if version >= 11 {
		columns["job_preparations"] = []string{"selected_goal_id", "receipt"}
	}
	if version >= 12 {
		columns["job_facts"] = append(columns["job_facts"], "append_key", "append_gen")
	}
	if version >= 13 {
		columns["job_facts"] = append(columns["job_facts"], "goal_origin", "goal_created_at", "goal_closed_at", "goal_success_job_id", "historical_goal_success", "record_kind", "terminal_failure")
	}
	for table, names := range columns {
		for _, column := range names {
			var exists bool
			if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema='pgjobdb' AND table_name=$1 AND column_name=$2)`, table, column).Scan(&exists); err != nil {
				return err
			}
			if !exists {
				return fmt.Errorf("pgjobdb: incomplete format %d: missing %s.%s", version, table, column)
			}
		}
	}

	for _, name := range []string{"_goal_lock", "prepare_job", "goal_info", "fail_goal", "_goal_members", "_register_wait", "_evaluate_wait", "_settle_target", "_check_goal_cycle", "submit_native_job", "get_native_job", "list_native_jobs", "complete_native_job", "complete_native_unheld_job", "_archive_goal_identity", "_terminal_goals"} {
		var found bool
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='pgjobdb' AND p.proname=$1 AND p.prosrc<>'')`, name).Scan(&found); err != nil {
			return err
		}
		if !found {
			return fmt.Errorf("pgjobdb: incomplete format %d: missing definition %s", version, name)
		}
	}
	if version == 13 {
		return validateTargetContract(ctx, tx)
	}
	return nil
}

func validateTargetContract(ctx context.Context, tx *sql.Tx) error {
	var retired bool
	if err := tx.QueryRowContext(ctx, `SELECT to_regclass('pgjobdb.goals') IS NOT NULL OR to_regclass('pgjobdb.goal_candidates') IS NOT NULL OR EXISTS(SELECT 1 FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='pgjobdb' AND (p.proname='add_goal_candidate' OR (p.proname<>'_goal_lock' AND p.prosrc ~ 'pgjobdb[.](goals|goal_candidates)\M')) )`).Scan(&retired); err != nil {
		return err
	}
	if retired {
		return fmt.Errorf("pgjobdb: removed lifecycle object remains")
	}
	var references bool
	if err := tx.QueryRowContext(ctx, `SELECT NOT EXISTS(SELECT 1 FROM pgjobdb.job_facts f JOIN pgjobdb.job_facts o ON o.tenant_id=f.tenant_id AND o.goal_id=f.goal_id AND o.goal_origin LEFT JOIN pgjobdb.jobs_archive winner ON winner.tenant_id=f.tenant_id AND winner.job_id=f.goal_success_job_id WHERE f.cell_id<>o.cell_id OR f.cell_repo<>o.cell_repo OR (f.goal_success_job_id IS NOT NULL AND (winner.job_id IS NULL OR winner.completion_status<>'success' OR (f.historical_goal_success AND NOT f.goal_origin) OR (NOT f.historical_goal_success AND f.goal_success_job_id<>f.job_id))))`).Scan(&references); err != nil {
		return err
	}
	if !references {
		return fmt.Errorf("pgjobdb: inconsistent retained owner or closure witness")
	}
	var valid bool
	if err := tx.QueryRowContext(ctx, `SELECT NOT EXISTS(SELECT 1 FROM pgjobdb.job_facts f LEFT JOIN pgjobdb.jobs j USING(tenant_id,job_id) LEFT JOIN pgjobdb.jobs_archive a USING(tenant_id,job_id) WHERE f.record_kind<>'EXECUTION' AND (j.job_id IS NOT NULL OR a.job_id IS NULL OR a.completion_status IS DISTINCT FROM 'failed' OR a.archived_at IS DISTINCT FROM f.goal_closed_at OR a.created_at IS DISTINCT FROM f.created_at OR a.completion_detail IS DISTINCT FROM f.terminal_failure->>'message' OR a.lease_id IS NOT NULL OR a.final_route_job_type IS NOT NULL OR a.payload<>'{}'::jsonb OR a.metadata<>'{}'::jsonb))`).Scan(&valid); err != nil {
		return err
	}
	if !valid {
		return fmt.Errorf("pgjobdb: inconsistent protected archive mirror")
	}

	return nil
}

func reportRows(ctx context.Context, tx *sql.Tx, query string) ([]json.RawMessage, error) {
	rows, err := tx.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []json.RawMessage{}
	for rows.Next() {
		var raw []byte
		if err = rows.Scan(&raw); err != nil {
			return nil, err
		}
		out = append(out, append(json.RawMessage(nil), raw...))
	}
	return out, rows.Err()
}
func captureReport(ctx context.Context, tx *sql.Tx, version int) (ConversionReport, error) {
	r := ConversionReport{ReportVersion: 1, LegacyInputsAvailable: version > 0 && version < 13, SourceFormat: version, TargetFormat: 13, Tenants: []json.RawMessage{}, RemovedLinks: []json.RawMessage{}, AffectedWaits: []json.RawMessage{}, AffectedPreparations: []json.RawMessage{}, CrossPrimaryProducers: []json.RawMessage{}, MetadataDerivations: []json.RawMessage{}, LiveOverlaps: []json.RawMessage{}, MigrationFailures: []json.RawMessage{}}
	if version == 0 || version == 13 {
		return r, nil
	}
	entries := []struct {
		target *[]json.RawMessage
		query  string
	}{
		{&r.Tenants, `SELECT jsonb_build_object('tenantId',t.tenant_id,'goals',(SELECT count(*) FROM pgjobdb.goals g WHERE g.tenant_id=t.tenant_id),'jobs',(SELECT count(*) FROM pgjobdb.job_facts f WHERE f.tenant_id=t.tenant_id),'preparations',(SELECT count(*) FROM pgjobdb.job_preparations p WHERE p.tenant_id=t.tenant_id),'pendingWaits',(SELECT count(*) FROM pgjobdb.dependency_waits w WHERE w.tenant_id=t.tenant_id AND w.state='PENDING'),'removedLinks',(SELECT count(*) FROM pgjobdb.goal_candidates l WHERE l.tenant_id=t.tenant_id)) FROM (SELECT tenant_id FROM pgjobdb.cells UNION SELECT tenant_id FROM pgjobdb.job_facts UNION SELECT tenant_id FROM pgjobdb.job_preparations) t ORDER BY t.tenant_id COLLATE "C"`},
		{&r.RemovedLinks, `SELECT to_jsonb(l)||jsonb_build_object('primaryGoalId',f.goal_id,'conflicting',f.goal_id<>l.goal_id) FROM pgjobdb.goal_candidates l JOIN pgjobdb.job_facts f USING(tenant_id,job_id) ORDER BY l.tenant_id COLLATE "C",l.goal_id COLLATE "C",l.job_id COLLATE "C"`},
		{&r.AffectedWaits, `SELECT to_jsonb(w) FROM pgjobdb.dependency_waits w WHERE w.state='PENDING' AND EXISTS(SELECT 1 FROM pgjobdb.dependency_targets t JOIN pgjobdb.goal_candidates l ON l.tenant_id=t.tenant_id AND l.goal_id=t.target_id WHERE t.tenant_id=w.tenant_id AND t.job_id=w.job_id AND t.wait_id=w.wait_id AND t.target_kind='goal') ORDER BY w.tenant_id COLLATE "C",w.job_id COLLATE "C",w.wait_id COLLATE "C"`},
		{&r.AffectedPreparations, `SELECT to_jsonb(p) FROM pgjobdb.job_preparations p WHERE EXISTS(SELECT 1 FROM pgjobdb.goal_candidates l WHERE l.tenant_id=p.tenant_id AND (p.goal_id=l.goal_id OR EXISTS(SELECT 1 FROM jsonb_array_elements(p.dependencies) d WHERE d#>>'{target,goalId}'=l.goal_id))) ORDER BY p.tenant_id COLLATE "C",p.job_id COLLATE "C"`},
		{&r.CrossPrimaryProducers, `SELECT jsonb_build_object('tenantId',g.tenant_id,'goalId',g.goal_id,'successfulJobId',g.successful_job_id,'producerPrimaryGoalId',f.goal_id,'closedAt',g.closed_at) FROM pgjobdb.goals g JOIN pgjobdb.job_facts f ON f.tenant_id=g.tenant_id AND f.job_id=g.successful_job_id WHERE g.state='SUCCEEDED' AND f.goal_id<>g.goal_id ORDER BY g.tenant_id COLLATE "C",g.goal_id COLLATE "C"`},
		{&r.MetadataDerivations, `SELECT jsonb_build_object('tenantId',g.tenant_id,'goalId',g.goal_id,'originJobId',f.job_id,'goalCreatedAt',g.created_at,'rule','earliest matching primary creation, then bytewise job ID') FROM pgjobdb.goals g CROSS JOIN LATERAL(SELECT job_id FROM pgjobdb.job_facts f WHERE f.tenant_id=g.tenant_id AND f.goal_id=g.goal_id AND f.cell_id=g.cell_id AND f.cell_repo=g.cell_repo ORDER BY f.created_at,f.job_id COLLATE "C" LIMIT 1) f ORDER BY g.tenant_id COLLATE "C",g.goal_id COLLATE "C"`},
		{&r.LiveOverlaps, `SELECT jsonb_build_object('tenantId',g.tenant_id,'goalId',g.goal_id,'nonTerminalJobIds',jsonb_agg(f.job_id ORDER BY f.job_id COLLATE "C")) FROM pgjobdb.goals g JOIN pgjobdb.job_facts f USING(tenant_id,goal_id) LEFT JOIN pgjobdb.jobs_archive a USING(tenant_id,job_id) WHERE g.state='OPEN' AND a.job_id IS NULL GROUP BY g.tenant_id,g.goal_id HAVING count(*)>1 ORDER BY g.tenant_id COLLATE "C",g.goal_id COLLATE "C"`},
	}
	for _, entry := range entries {
		var err error
		*entry.target, err = reportRows(ctx, tx, entry.query)
		if err != nil {
			return r, fmt.Errorf("pgjobdb: capture conversion report: %w", err)
		}
	}
	return r, nil
}

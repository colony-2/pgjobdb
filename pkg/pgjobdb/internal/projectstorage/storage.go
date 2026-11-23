// Package projectstorage binds the public catalog port to an explicitly prepared
// PostgreSQL installation. Catalog rules remain in JobDB's projectcatalog.Service.
package projectstorage

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/projectcatalog"
)

const Protocol = 1

//go:embed guards.sql
var GuardSQL string

func Digest(b []byte) string { h := sha256.Sum256(b); return hex.EncodeToString(h[:]) }
func GuardDigest() string    { return Digest([]byte(GuardSQL)) }

// admissionProjection excludes opaque metadata, descriptions and receipts.
// PostgreSQL JSON field extraction decodes even unrelated string tokens, which
// may legally contain NUL in the public BYTEA catalog encoding. Keep only the
// identity/seal fields in a separate projection, bound to the exact payload.
// Both values share one row, transaction and xmin; this is not another catalog.
func admissionProjection(s *projectcatalog.State, payload []byte) ([]byte, error) {
	type record struct {
		ID      string `json:"id"`
		Deleted bool   `json:"deleted"`
		Sealed  bool   `json:"sealed"`
	}
	projection := struct {
		Version       int               `json:"version"`
		ID            string            `json:"id"`
		Manifest      string            `json:"manifest"`
		CursorKey     []byte            `json:"cursorKey"`
		ReceiptsReady bool              `json:"receiptsReady"`
		PayloadSHA256 string            `json:"payloadSHA256"`
		Projects      map[string]record `json:"projects"`
	}{s.Version, s.ID, s.Manifest, s.CursorKey, s.Receipts != nil, Digest(payload), make(map[string]record, len(s.Projects))}
	for id, r := range s.Projects {
		projection.Projects[id] = record{r.Project.ID, r.Deleted, r.Sealed}
	}
	return json.Marshal(projection)
}

// Receipt is the immutable completion record, not a second project registry.
type Receipt struct {
	Protocol        int                   `json:"protocol"`
	Inventory       json.RawMessage       `json:"inventory"`
	Seeds           []projectcatalog.Seed `json:"seeds"`
	CatalogID       string                `json:"catalogID"`
	CatalogManifest string                `json:"catalogManifest"`
	GuardSource     string                `json:"guardSource"`
	Structure       string                `json:"structure"`
}

type Queryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

// Tables is the complete supported participant set. Installation rejects unknown
// JobDB tables, foreign tables and partitions instead of asserting false coverage.
var Tables = map[string]string{
	"pgjobdb.cell_catalog": "tenant_id", "pgjobdb.cells": "tenant_id", "pgjobdb.cell_names": "tenant_id", "pgjobdb.cell_requests": "tenant_id",
	"pgjobdb.job_preparations": "tenant_id", "pgjobdb.dependency_waits": "tenant_id", "pgjobdb.dependency_targets": "tenant_id",
	"pgjobdb.jobs": "tenant_id", "pgjobdb.jobs_archive": "tenant_id", "pgjobdb.jobs_trace": "tenant_id", "pgjobdb.lease_events": "tenant_id", "pgjobdb.job_facts": "tenant_id", "pgjobdb.job_client_state": "tenant_id",
	"public.jobdb_configuration": "tenant_id", "public.jobdb_schemas": "tenant_id", "public.jobdb_chapter_stories": "anthology_id", "public.jobdb_chapter_chapters": "anthology_id", "public.jobdb_chapter_artifacts": "anthology_id",
}

func TableNames() []string {
	a := []string{}
	for t := range Tables {
		a = append(a, t)
	}
	sort.Strings(a)
	return a
}

// CheckResolution never changes a caller's search path. Shared public stores use
// unqualified SQL, so only the stable public relation layout is supported.
func CheckResolution(ctx context.Context, q Queryer) error {
	var good bool
	if err := q.QueryRowContext(ctx, `SELECT current_schemas(false)=ARRAY['public']::name[] AND NOT EXISTS(SELECT 1 FROM pg_class WHERE relnamespace=pg_my_temp_schema() AND relname LIKE 'jobdb_%')`).Scan(&good); err != nil {
		return err
	}
	if !good {
		return fmt.Errorf("project storage requires an unshadowed public search path")
	}
	for _, name := range []string{"jobdb_configuration", "jobdb_schemas", "jobdb_chapter_stories", "jobdb_chapter_chapters", "jobdb_chapter_artifacts", "jobdb_project_catalog"} {
		if err := q.QueryRowContext(ctx, `SELECT to_regclass($1) IS NOT DISTINCT FROM to_regclass($2)`, name, "public."+name).Scan(&good); err != nil {
			return err
		}
		if !good {
			return fmt.Errorf("project storage relation is shadowed: %s", name)
		}
	}
	return nil
}

// Structure hashes owned catalog definitions and bindings, never resource content.
// The three public mutable SQL settings are deliberately outside this immutable
// definition contract; extra application functions/triggers are not owned guards.
// It includes disabled triggers, changed function bodies and relation OIDs.
func Structure(ctx context.Context, q Queryer) (string, error) {
	const query = `SELECT v FROM (
 SELECT 'relation:'||n.nspname||'.'||c.relname||':'||c.oid::text||':'||c.relkind::text||':'||c.relispartition::text AS v FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE (n.nspname='pgjobdb' OR n.nspname='public' AND c.relname LIKE 'jobdb_%') AND c.relkind IN ('r','p','f','v','m')
 UNION ALL SELECT 'column:'||n.nspname||'.'||c.relname||':'||a.attnum||':'||a.attname||':'||format_type(a.atttypid,a.atttypmod)||':'||a.attnotnull||':'||coalesce(pg_get_expr(d.adbin,d.adrelid),'') FROM pg_attribute a JOIN pg_class c ON c.oid=a.attrelid JOIN pg_namespace n ON n.oid=c.relnamespace LEFT JOIN pg_attrdef d ON d.adrelid=a.attrelid AND d.adnum=a.attnum WHERE a.attnum>0 AND NOT a.attisdropped AND (n.nspname='pgjobdb' OR n.nspname='public' AND c.relname LIKE 'jobdb_%')
 UNION ALL SELECT 'constraint:'||conrelid::regclass::text||':'||conname||':'||pg_get_constraintdef(oid) FROM pg_constraint WHERE connamespace='pgjobdb'::regnamespace OR conrelid IN (SELECT oid FROM pg_class WHERE relnamespace='public'::regnamespace AND relname LIKE 'jobdb_%')
 UNION ALL SELECT 'index:'||schemaname||'.'||indexname||':'||indexdef FROM pg_indexes WHERE schemaname='pgjobdb' OR schemaname='public' AND tablename LIKE 'jobdb_%'
 UNION ALL SELECT 'function:'||p.oid::regprocedure::text||':'||pg_get_functiondef(p.oid) FROM pg_proc p WHERE p.pronamespace='pgjobdb'::regnamespace AND p.proname IN ('_archive_and_delete_job','_archive_goal_identity','_cancel_job','_cancel_waits','_canonical_client_json','_check_active_goal','_check_goal_cycle','_client_json_string','_client_update_value','_close_wait','_complete_locked_job','_complete_native_locked_job','_emit_trace_event','_evaluate_wait','_expire_goal_waits','_extend_lease','_goal_lock','_goal_members','_immutable_goal_id','_immutable_retained_goal','_lock_job_for_status','_merge_client_json','_notify_work','_preparation_mismatch','_project_no_truncate','_project_row_guard','_project_write_check','_record_lease_event','_register_wait','_resolve_goal','_save_wait','_settle_target','_terminal_goals','_update_client_payload','_update_waiters_for_completion','_update_waiters_for_completion_bulk','_validate_dependencies','_validate_selected_goal','archive_cancelled_jobs','cancel_native_job','claim_append','claim_native_task','clear_crash_concern','complete_native_job','complete_native_unheld_job','delete_native_jobs','dependency_wait_info','effective_native_route','fail_goal','get_native_job','get_native_job_status','get_native_work','goal_info','job_status_at','list_native_jobs','normalize_wait_for','prepare_job','release_append','renew_native_lease','reschedule_native_job','set_crash_concern_threshold','set_notify','set_trace','submit_native_job','validate_native_lease')
 UNION ALL SELECT 'view:'||schemaname||'.'||viewname||':'||definition FROM pg_views WHERE schemaname='pgjobdb'
 UNION ALL SELECT 'trigger:'||t.tgrelid::regclass::text||':'||t.tgenabled::text||':'||t.tgfoid::text||':'||pg_get_triggerdef(t.oid) FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE NOT t.tgisinternal AND t.tgname IN ('archive_goal_identity','check_active_goal','immutable_goal_id','immutable_retained_goal','project_admission_row','project_admission_truncate','terminal_goals') AND (n.nspname='pgjobdb' OR n.nspname='public' AND c.relname LIKE 'jobdb_%')
 ) objects ORDER BY v COLLATE "C"`
	rows, err := q.QueryContext(ctx, query)
	if err != nil {
		return "", err
	}
	defer rows.Close()
	h := sha256.New()
	for rows.Next() {
		var v string
		if err = rows.Scan(&v); err != nil {
			return "", err
		}
		fmt.Fprintf(h, "%d:%s", len(v), v)
	}
	if err = rows.Err(); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
func ReadReceipt(ctx context.Context, q Queryer) (Receipt, error) {
	var r Receipt
	var b []byte
	err := q.QueryRowContext(ctx, `SELECT payload FROM pgjobdb.project_installation WHERE singleton=1`).Scan(&b)
	if err != nil {
		return r, err
	}
	err = json.Unmarshal(b, &r)
	return r, err
}
func Verify(ctx context.Context, q Queryer) error {
	if err := CheckResolution(ctx, q); err != nil {
		return err
	}
	r, err := ReadReceipt(ctx, q)
	if err != nil {
		return fmt.Errorf("project installation is incomplete: %w", err)
	}
	if r.Protocol != Protocol || r.GuardSource != GuardDigest() {
		return fmt.Errorf("unsupported project installation or guard contract")
	}
	var b, admission []byte
	if err = q.QueryRowContext(ctx, `SELECT payload,admission FROM public.jobdb_project_catalog WHERE singleton=1`).Scan(&b, &admission); err != nil {
		return err
	}
	var s projectcatalog.State
	if err = json.Unmarshal(b, &s); err != nil {
		return err
	}
	if s.Version != 1 || s.ID != r.CatalogID || s.Manifest != r.CatalogManifest || len(s.CursorKey) != 32 || s.Projects == nil || s.Receipts == nil {
		return fmt.Errorf("project catalog does not match installation")
	}
	expected, err := admissionProjection(&s, b)
	if err != nil {
		return err
	}
	if !bytes.Equal(admission, expected) {
		return fmt.Errorf("project admission projection does not match catalog")
	}
	digest, err := Structure(ctx, q)
	if err != nil {
		return err
	}
	if digest != r.Structure {
		return fmt.Errorf("project storage definitions or guard bindings changed")
	}
	return nil
}

// Store uses qualified SQL and a transaction-bound public Store adapter. Checking
// readiness inside each transaction avoids a check on a different pooled session.
type Store struct{ DB *sql.DB }

func (s Store) Read(ctx context.Context) (*projectcatalog.State, error) {
	if s.DB == nil || ctx == nil {
		return nil, jobdb.ErrProjectUnavailable
	}
	tx, err := s.DB.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	if err = Verify(ctx, tx); err != nil {
		return nil, err
	}
	return (TxStore{Tx: tx}).Read(ctx)
}
func (s Store) Update(ctx context.Context, f func(**projectcatalog.State) error) error {
	if s.DB == nil || ctx == nil {
		return jobdb.ErrProjectUnavailable
	}
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err = Verify(ctx, tx); err != nil {
		return err
	}
	if err = (TxStore{Tx: tx}).Update(ctx, f); err != nil {
		return err
	}
	return tx.Commit()
}

// TxStore does not commit; the explicit installer owns its exclusion/commit.
type TxStore struct{ Tx *sql.Tx }

func (s TxStore) Read(ctx context.Context) (*projectcatalog.State, error) {
	var raw []byte
	if err := s.Tx.QueryRowContext(ctx, `SELECT payload FROM public.jobdb_project_catalog WHERE singleton=1`).Scan(&raw); err != nil {
		return nil, err
	}
	if len(raw) == 0 {
		return nil, nil
	}
	var state projectcatalog.State
	err := json.Unmarshal(raw, &state)
	return &state, err
}
func (s TxStore) Update(ctx context.Context, f func(**projectcatalog.State) error) error {
	if _, err := s.Tx.ExecContext(ctx, `SET LOCAL synchronous_commit=on`); err != nil {
		return err
	}
	var raw []byte
	if err := s.Tx.QueryRowContext(ctx, `SELECT payload FROM public.jobdb_project_catalog WHERE singleton=1 FOR UPDATE`).Scan(&raw); err != nil {
		return err
	}
	var state *projectcatalog.State
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &state); err != nil {
			return err
		}
	}
	if err := f(&state); err != nil {
		return err
	}
	if state == nil {
		return jobdb.ErrProjectUnavailable
	}
	b, err := json.Marshal(state)
	if err != nil {
		return err
	}
	admission, err := admissionProjection(state, b)
	if err != nil {
		return err
	}
	_, err = s.Tx.ExecContext(ctx, `UPDATE public.jobdb_project_catalog SET payload=$1,admission=$2 WHERE singleton=1`, b, admission)
	return err
}
func Control(db *sql.DB, canDelete func(context.Context, string) error) *projectcatalog.Service {
	return &projectcatalog.Service{Store: Store{DB: db}, CanDelete: canDelete}
}

package installer

import (
	"context"
	"database/sql"
	_ "embed"
	"encoding/json"
	"fmt"
	"slices"
)

// These read-only catalog expectations were captured from the authentic 9–12
// schemas and fresh format 13. Tests exercise every baseline against them.
// Checking names alone permits a CHECK(true), misbound trigger, no-op function,
// or unique index over the wrong keys to silently remove runtime guarantees.
// Extra application objects are allowed; every owned protection must match.
//
//go:embed protections.json
var protectionContracts []byte

// Use qualified deparsing, independent of the caller's search_path. PostgreSQL
// supplies canonical expressions including casts, collations and predicates.
const protectionCatalog = `
SELECT jsonb_object_agg(key, value) FROM (
 SELECT 'constraint/'||r.relname||'/'||c.conname AS key,
   jsonb_build_array(pg_get_constraintdef(c.oid),c.convalidated,c.condeferrable,c.condeferred,c.connoinherit) AS value
 FROM pg_constraint c JOIN pg_class r ON r.oid=c.conrelid
 JOIN pg_namespace n ON n.oid=r.relnamespace WHERE n.nspname='pgjobdb'
 AND c.conname IN ('retained_goal_shape','job_facts_tenant_id_goal_id_fkey','active_goal_fk','archive_goal_fk','candidate_job_fk')
 UNION ALL
 SELECT 'index/'||r.relname, jsonb_build_array(pg_get_indexdef(r.oid),i.indisunique,i.indisvalid,i.indisready,i.indislive)
 FROM pg_index i JOIN pg_class r ON r.oid=i.indexrelid JOIN pg_namespace n ON n.oid=r.relnamespace
 WHERE n.nspname='pgjobdb' AND r.relname IN ('facts_goal_idx','facts_goal_origin_idx','facts_goal_closure_idx')
 UNION ALL
 SELECT 'trigger/'||r.relname||'/'||t.tgname,
   jsonb_build_array(pg_get_triggerdef(t.oid),t.tgenabled,t.tgisinternal)
 FROM pg_trigger t JOIN pg_class r ON r.oid=t.tgrelid JOIN pg_namespace n ON n.oid=r.relnamespace
 WHERE n.nspname='pgjobdb' AND t.tgname IN ('immutable_goal_id','immutable_retained_goal','check_active_goal','archive_goal_identity','terminal_goals')
 UNION ALL
 SELECT 'function/'||p.proname, to_jsonb(pg_get_functiondef(p.oid))
 FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='pgjobdb'
 AND p.pronargs=0 AND p.proname IN ('_immutable_goal_id','_immutable_retained_goal','_check_active_goal','_archive_goal_identity','_terminal_goals')
) protections`

func validateProtections(ctx context.Context, tx *sql.Tx, version int) error {
	var contracts map[string]map[string]json.RawMessage
	if err := json.Unmarshal(protectionContracts, &contracts); err != nil {
		return fmt.Errorf("pgjobdb: invalid protection contract: %w", err)
	}
	key := "historical"
	if version == FormatVersion {
		key = "retained"
	}
	expected := contracts[key]
	if len(expected) == 0 {
		return fmt.Errorf("pgjobdb: missing protection contract for format %d", version)
	}
	// Restore the caller's path even on rejection. A failed catalog query aborts
	// this transaction, which the caller rolls back without installation writes.
	var searchPath string
	if err := tx.QueryRowContext(ctx, `SELECT current_setting('search_path')`).Scan(&searchPath); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `SET LOCAL search_path TO pg_catalog`); err != nil {
		return err
	}
	var raw []byte
	if err := tx.QueryRowContext(ctx, protectionCatalog).Scan(&raw); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `SELECT set_config('search_path',$1,true)`, searchPath); err != nil {
		return err
	}
	var actual map[string]json.RawMessage
	if err := json.Unmarshal(raw, &actual); err != nil {
		return err
	}
	objects := make([]string, 0, len(expected))
	for object := range expected {
		objects = append(objects, object)
	}
	slices.Sort(objects)
	for _, object := range objects {
		want := expected[object]
		// Decode before comparison so JSON formatting has no bearing on validity.
		var a, b any
		if err := json.Unmarshal(want, &a); err != nil {
			return err
		}
		got, ok := actual[object]
		if ok {
			if err := json.Unmarshal(got, &b); err != nil {
				return err
			}
		}
		left, _ := json.Marshal(a)
		right, _ := json.Marshal(b)
		if !ok || string(left) != string(right) {
			return fmt.Errorf("pgjobdb: incomplete or altered protection %s in format %d", object, version)
		}
	}
	return nil
}

package projectstorage

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"strings"
)

var entryGuards = map[string]string{
	"archive_cancelled_jobs": "unnest(p_tenant_ids)", "prepare_job": "t", "fail_goal": "t", "submit_native_job": "p_tenant_id", "get_native_work": "unnest(p_tenant_ids)", "complete_native_job": "p_tenant_id", "complete_native_unheld_job": "p_tenant_id", "renew_native_lease": "p_tenant_id", "cancel_native_job": "p_tenant_id", "reschedule_native_job": "p_tenant_id", "claim_native_task": "p_tenant_id", "delete_native_jobs": "p_tenant_id", "claim_append": "p_tenant", "clear_crash_concern": "p_tenant_id", "_expire_goal_waits": "unnest(tenants)", "_update_client_payload": "p_tenant",
}

const guardedReleaseBody = ` BEGIN PERFORM pgjobdb._project_write_check(p_tenant); UPDATE pgjobdb.jobs SET lease_id=NULL,lease_worker_id=NULL,lease_expires_at='-infinity',lease_route_job_type=NULL,lease_work_kind=NULL,lease_task_type=NULL WHERE tenant_id=p_tenant AND job_id=p_job AND lease_id=p_lease; END `

var EntryDefinition = regexp.MustCompile(`(?s)CREATE OR REPLACE FUNCTION pgjobdb\.([a-z_]+)\([^$]*?AS \$\$(.*?)\$\$;`)

func GuardSchedulerSQL(program string) (string, error) {
	var guardErr error
	result := EntryDefinition.ReplaceAllStringFunc(program, func(definition string) string {
		match := EntryDefinition.FindStringSubmatchIndex(definition)
		body, err := GuardedBody(definition[match[2]:match[3]], definition[match[4]:match[5]])
		if err != nil {
			guardErr = err
		}
		header := definition[:match[4]]
		if definition[match[2]:match[3]] == "release_append" {
			header = strings.Replace(header, "LANGUAGE sql", "LANGUAGE plpgsql", 1)
		}
		return header + body + definition[match[5]:]
	})
	if guardErr != nil {
		return "", guardErr
	}
	return result, nil
}

func GuardedBody(name, body string) (string, error) {
	if name == "release_append" {
		return guardedReleaseBody, nil
	}
	expr, ok := entryGuards[name]
	if !ok {
		return body, nil
	}
	return guardEntry(body, expr)
}

func guardEntry(def, expr string) (string, error) {
	index := strings.Index(def, "BEGIN")
	if index < 0 {
		return "", fmt.Errorf("unsupported scheduler function body")
	}
	check := `PERFORM pgjobdb._project_write_check(` + expr + `);`
	if strings.HasPrefix(expr, "unnest(") {
		array := strings.TrimSuffix(strings.TrimPrefix(expr, "unnest("), ")")
		check = `IF coalesce(cardinality(` + array + `),0)=0 THEN RAISE EXCEPTION USING ERRCODE='PJC01',MESSAGE='explicit project identities are required'; END IF; PERFORM pgjobdb._project_write_check(id) FROM ` + expr + ` AS id;`
	}
	check = "\n " + check
	if strings.HasPrefix(def[index+5:], check) {
		return def, nil
	}
	return def[:index+5] + check + def[index+5:], nil
}

func InstallGuards(ctx context.Context, tx *sql.Tx, tables []string) error {
	if _, err := tx.ExecContext(ctx, GuardSQL); err != nil {
		return err
	}
	if err := InstallRowGuards(ctx, tx, tables); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `CREATE OR REPLACE FUNCTION pgjobdb.release_append(p_tenant text,p_job text,p_lease text) RETURNS void LANGUAGE plpgsql AS $release$ BEGIN PERFORM pgjobdb._project_write_check(p_tenant); UPDATE pgjobdb.jobs SET lease_id=NULL,lease_worker_id=NULL,lease_expires_at='-infinity',lease_route_job_type=NULL,lease_work_kind=NULL,lease_task_type=NULL WHERE tenant_id=p_tenant AND job_id=p_job AND lease_id=p_lease; END $release$`); err != nil {
		return err
	}
	// Entry guards also reject empty/no-op direct calls. Row guards cover every
	// independent raw SQL writer and cascades. Only known plpgsql bodies are patched.

	for name, expr := range entryGuards {
		rows, err := tx.QueryContext(ctx, `SELECT pg_get_functiondef(p.oid) FROM pg_proc p JOIN pg_language l ON l.oid=p.prolang WHERE p.pronamespace='pgjobdb'::regnamespace AND p.proname=$1 AND l.lanname='plpgsql'`, name)
		if err != nil {
			return err
		}
		defs := []string{}
		for rows.Next() {
			var def string
			if err = rows.Scan(&def); err != nil {
				break
			}
			defs = append(defs, def)
		}
		readErr := rows.Err()
		rows.Close()
		if err != nil {
			return err
		}
		if readErr != nil {
			return readErr
		}
		if len(defs) == 0 {
			return fmt.Errorf("missing guarded entry %s", name)
		}
		for _, def := range defs {
			def, err = guardEntry(def, expr)
			if err != nil {
				return err
			}
			if _, err = tx.ExecContext(ctx, def); err != nil {
				return fmt.Errorf("guard %s: %w", name, err)
			}
		}
	}
	return nil
}

func InstallRowGuards(ctx context.Context, tx *sql.Tx, tables []string) error {
	for _, table := range tables {
		if _, err := tx.ExecContext(ctx, `CREATE OR REPLACE TRIGGER project_admission_row BEFORE INSERT OR UPDATE OR DELETE ON `+table+` FOR EACH ROW EXECUTE FUNCTION pgjobdb._project_row_guard('`+Tables[table]+`'); CREATE OR REPLACE TRIGGER project_admission_truncate BEFORE TRUNCATE ON `+table+` FOR EACH STATEMENT EXECUTE FUNCTION pgjobdb._project_no_truncate()`); err != nil {
			return err
		}
	}
	return nil
}

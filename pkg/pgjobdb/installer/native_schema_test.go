package installer_test

import (
	"context"
	"database/sql"
	"testing"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/pgjobdb/pkg/pgjobdb"

	pgjobdbinstaller "github.com/colony-2/pgjobdb/pkg/pgjobdb/installer"
)

func TestNativeSchemaRoundTrip(t *testing.T) {
	runDatabaseTest(t, func(ctx context.Context, db *sql.DB) {
		// A restart must accept a database already initialized by pgjobdb.
		if err := (pgjobdbinstaller.Installer{DB: db}).Apply(ctx); err != nil {
			t.Fatalf("reapply schema: %v", err)
		}

		cell, err := pgjobdb.CreateCell(ctx, db, jobdb.CreateCellRequest{TenantID: "tenant", Name: "test", Repo: "example.test/test"})
		if err != nil {
			t.Fatal(err)
		}
		const insertSchedule = `INSERT INTO pgjobdb.schedules
			(tenant_id, schedule_id, state, generation, spec_hash, trigger,
			target_job_type, target_snapshot, cell_id, cell_name, cell_repo, overlap_policy, failure_policy)
			VALUES ('tenant', 'daily', 'ACTIVE', 1, 'hash', '{}',
			'collect', '{}', $1, 'test', 'example.test/test', 'ALLOW', '{}')`
		if _, err := db.ExecContext(ctx, insertSchedule, cell.ID); err != nil {
			t.Fatalf("insert schedule: %v", err)
		}

		if _, err := db.ExecContext(ctx, `INSERT INTO pgjobdb.goals(tenant_id,goal_id,cell_id,cell_repo) VALUES ('tenant','goal-1',$1,'example.test/test')`, cell.ID); err != nil {
			t.Fatal(err)
		}
		const insertFact = `INSERT INTO pgjobdb.job_facts
			(cell_id,cell_name,cell_repo,tenant_id, job_id, goal_id, job_type, run_policy, app_metadata,
			schedule_id, schedule_generation, schedule_spec_hash,
			scheduled_at, schedule_run_id)
			VALUES ($3,'test','example.test/test','tenant', $1, 'goal-1', 'collect', '{}', '{"source":"test"}',
			'daily', 1, 'hash', now(), $2)`
		if _, err := db.ExecContext(ctx, insertFact, "job-1", "run-1", cell.ID); err != nil {
			t.Fatalf("insert job fact: %v", err)
		}
		if _, err := db.ExecContext(ctx, insertFact, "job-2", "run-1", cell.ID); err == nil {
			t.Fatal("expected duplicate schedule run to fail")
		}
		if _, err := db.ExecContext(ctx, `INSERT INTO pgjobdb.job_facts
			(tenant_id, job_id, job_type, schedule_id)
			VALUES ('tenant', 'incomplete', 'collect', 'daily')`); err == nil {
			t.Fatal("expected incomplete schedule provenance to fail")
		}

		if _, err := db.ExecContext(ctx, `INSERT INTO pgjobdb.jobs
			(tenant_id, job_id, route_job_type, work_kind, task_type)
			VALUES ('tenant', 'invalid', 'collect', 'TASK', 'task')`); err == nil {
			t.Fatal("expected incomplete task coordinates to fail")
		}
		if _, err := db.ExecContext(ctx, `INSERT INTO pgjobdb.jobs
			(tenant_id, job_id, goal_id, route_job_type, work_kind, task_type,
			resume_job_type, task_input_ordinal, task_output_ordinal,
			task_input_hash)
			VALUES ('tenant', 'job-1', 'goal-1', 'collect', 'TASK', 'task',
			'collect', 1, 2, 'sha256:input')`); err != nil {
			t.Fatalf("insert typed task route: %v", err)
		}

		if _, err := db.ExecContext(ctx, `INSERT INTO pgjobdb.jobs_archive
			(tenant_id, job_id, created_at, completion_status,
			final_route_job_type, final_work_kind, final_task_type,
			final_resume_job_type, final_task_input_ordinal,
			final_task_output_ordinal, final_task_input_hash,
			final_wait_for, final_available_at, final_cancel_requested)
			VALUES ('tenant', 'job-1', now(), 'failed_app',
			'collect', 'TASK', 'task', 'collect', 1, 2, 'sha256:input',
			ARRAY['prerequisite'], now(), TRUE)`); err != nil {
			t.Fatalf("insert archived task snapshot: %v", err)
		}
		var route, workKind, waitFor string
		var cancelled bool
		if err := db.QueryRowContext(ctx, `SELECT final_route_job_type,
			final_work_kind, final_wait_for[1],
			final_cancel_requested FROM pgjobdb.jobs_archive
			WHERE tenant_id = 'tenant' AND job_id = 'job-1'`).Scan(
			&route, &workKind, &waitFor, &cancelled,
		); err != nil {
			t.Fatalf("read archived task snapshot: %v", err)
		}
		if route != "collect" || workKind != "TASK" || waitFor != "prerequisite" ||
			!cancelled {
			t.Fatalf("archive snapshot mismatch: %q %q %q %v",
				route, workKind, waitFor, cancelled)
		}
	})
}

func TestGenericQueueProceduresAreAbsent(t *testing.T) {
	runDatabaseTest(t, func(ctx context.Context, db *sql.DB) {
		var count int
		err := db.QueryRowContext(ctx, `SELECT count(*)
			FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
			WHERE n.nspname = 'pgjobdb' AND p.proname IN (
				'submit_job', 'get_work', 'get_job_lease', 'reschedule_job',
				'complete_job', 'complete_unheld_job', 'reschedule_unheld_job',
				'cancel_job', 'extend_lease', 'complete_native_task_work')`).Scan(&count)
		if err != nil {
			t.Fatalf("inspect functions: %v", err)
		}
		if count != 0 {
			t.Fatalf("found %d generic queue procedures", count)
		}

		// The hard break exposes one current signature, with no older overloads.
		for name, args := range map[string]int{"submit_native_job": 18, "prepare_job": 13, "get_native_work": 10, "list_native_jobs": 20, "reschedule_native_job": 21} {
			var total, matched int
			err := db.QueryRowContext(ctx, `SELECT count(*),count(*) FILTER(WHERE p.pronargs=$2) FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='pgjobdb' AND p.proname=$1`, name, args).Scan(&total, &matched)
			if err != nil || total != 1 || matched != 1 {
				t.Fatalf("%s signatures: total=%d current=%d err=%v", name, total, matched, err)
			}
		}
	})
}

package installer_test

import (
	"context"
	"database/sql"
	"testing"
	"time"
)

func TestScheduleProcedures(t *testing.T) {
	runDatabaseTest(t, func(ctx context.Context, db *sql.DB) {
		var generation int64
		var state, target string
		const upsert = `SELECT generation, state, target_job_type
			FROM pgjobdb.upsert_schedule('tenant', 'daily', 'ACTIVE', 'hash-1',
			'{"kind":"interval"}', 'collect', '{"input":1}', 'ALLOW', '{}',
			now(), 'job-next', $1)`
		if err := db.QueryRowContext(ctx, upsert, nil).Scan(&generation, &state, &target); err != nil {
			t.Fatalf("create schedule: %v", err)
		}
		if generation != 1 || state != "ACTIVE" || target != "collect" {
			t.Fatalf("created schedule = %d %q %q", generation, state, target)
		}
		if err := db.QueryRowContext(ctx, upsert, int64(1)).Scan(
			&generation, &state, &target,
		); err != nil {
			t.Fatalf("update schedule: %v", err)
		}
		if generation != 2 {
			t.Fatalf("updated generation = %d, want 2", generation)
		}
		if err := db.QueryRowContext(ctx, upsert, int64(1)).Scan(
			&generation, &state, &target,
		); err == nil {
			t.Fatal("expected stale generation to fail")
		}

		if err := db.QueryRowContext(ctx, `SELECT generation, state
			FROM pgjobdb.pause_schedule('tenant', 'daily', 2)`).Scan(
			&generation, &state,
		); err != nil {
			t.Fatalf("pause schedule: %v", err)
		}
		if generation != 3 || state != "PAUSED" {
			t.Fatalf("paused schedule = %d %q", generation, state)
		}
		if err := db.QueryRowContext(ctx, `SELECT generation, state
			FROM pgjobdb.resume_schedule('tenant', 'daily', now(), 'job-new', 3)`).Scan(
			&generation, &state,
		); err != nil {
			t.Fatalf("resume schedule: %v", err)
		}
		if generation != 4 || state != "ACTIVE" {
			t.Fatalf("resumed schedule = %d %q", generation, state)
		}

		var snapshot string
		if err := db.QueryRowContext(ctx, `SELECT target_snapshot::text
			FROM pgjobdb.get_schedule('tenant', 'daily')`).Scan(&snapshot); err != nil {
			t.Fatalf("get schedule: %v", err)
		}
		if snapshot != `{"input": 1}` {
			t.Fatalf("target snapshot = %q", snapshot)
		}
		if _, err := db.ExecContext(ctx, `SELECT pgjobdb.upsert_schedule(
			'tenant', 'weekly', 'ACTIVE', 'hash-2', '{}', 'collect', '{}',
			'ALLOW', '{}', now(), 'job-weekly')`); err != nil {
			t.Fatalf("create weekly schedule: %v", err)
		}
		if _, err := db.ExecContext(ctx, `SELECT pgjobdb.upsert_schedule(
			'tenant', 'paused', 'PAUSED', 'hash-3', '{}', 'collect', '{}',
			'ALLOW', '{}', NULL, NULL)`); err != nil {
			t.Fatalf("create paused schedule: %v", err)
		}
		var count int
		if err := db.QueryRowContext(ctx, `SELECT count(*) FROM
			pgjobdb.list_schedules('tenant', ARRAY['ACTIVE'], ARRAY['collect'])`).Scan(
			&count,
		); err != nil {
			t.Fatalf("list schedules: %v", err)
		}
		if count != 2 {
			t.Fatalf("active schedule count = %d", count)
		}
		var firstID, secondID string
		var firstUpdated time.Time
		if err := db.QueryRowContext(ctx, `SELECT schedule_id, updated_at FROM
			pgjobdb.list_schedules('tenant', ARRAY['ACTIVE'], NULL, NULL, NULL, 1)`).Scan(
			&firstID, &firstUpdated,
		); err != nil {
			t.Fatalf("first active page: %v", err)
		}
		if err := db.QueryRowContext(ctx, `SELECT schedule_id FROM
			pgjobdb.list_schedules('tenant', ARRAY['ACTIVE'], NULL, $1, $2, 1)`,
			firstUpdated, firstID).Scan(&secondID); err != nil {
			t.Fatalf("second active page: %v", err)
		}
		if firstID != "weekly" || secondID != "daily" {
			t.Fatalf("active pages = %q, %q", firstID, secondID)
		}
		if err := db.QueryRowContext(ctx, `SELECT generation, state
			FROM pgjobdb.archive_schedule('tenant', 'daily', 4)`).Scan(
			&generation, &state,
		); err != nil {
			t.Fatalf("archive schedule: %v", err)
		}
		if generation != 5 || state != "ARCHIVED" {
			t.Fatalf("archived schedule = %d %q", generation, state)
		}
		if err := db.QueryRowContext(ctx, upsert, nil).Scan(
			&generation, &state, &target,
		); err == nil {
			t.Fatal("expected archived schedule update to fail")
		}
	})
}

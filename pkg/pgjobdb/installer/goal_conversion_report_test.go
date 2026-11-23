package installer_test

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/pgjobdb/pkg/pgjobdb"
	"github.com/colony-2/pgjobdb/pkg/pgjobdb/installer"
	"github.com/stretchr/testify/require"
)

type pausedReport struct {
	captured chan []byte
	release  chan struct{}
}

func (w pausedReport) Write(raw []byte) (int, error) {
	w.captured <- bytes.Clone(raw)
	<-w.release
	return len(raw), nil
}

func TestGoalConversionAuthoritativeReport(t *testing.T) {
	withBareDatabase(t, func(ctx context.Context, db *sql.DB) {
		_, err := db.ExecContext(ctx, format12BeforeAvailabilityRepair)
		require.NoError(t, err)
		_, _, _, goal := historicalBaseline(t, ctx, db, 12)
		install := installer.Installer{DB: db}
		before := stableHistoricalRows(t, ctx, db)
		catalog := catalogSnapshot(t, ctx, db)
		preflight, err := install.Preflight(ctx)
		require.NoError(t, err)
		require.Len(t, preflight.RemovedLinks, 2)
		require.JSONEq(t, before, stableHistoricalRows(t, ctx, db))
		require.JSONEq(t, catalog, catalogSnapshot(t, ctx, db))
		// Session B adds authentic inputs after A's advisory preflight.
		historicalJob(t, ctx, db, 12, "late-member")
		_, err = db.ExecContext(ctx, `INSERT INTO pgjobdb.goal_candidates VALUES('c2',$1,'late-member','2026-02-03 04:05:06.123456+00')`, goal)
		require.NoError(t, err)
		_, err = db.ExecContext(ctx, `SELECT pgjobdb._register_wait('c2','consumer',jsonb_build_object('waitId','late-wait','dependencies',jsonb_build_array(jsonb_build_object('target',jsonb_build_object('goalId',$1::text),'condition','success'))))`, goal)
		require.NoError(t, err)
		_, err = db.ExecContext(ctx, `SELECT pgjobdb.prepare_job('c2','late-preparation',NULL,NULL,FALSE,'late descriptor',jsonb_build_array(jsonb_build_object('target',jsonb_build_object('goalId',$1::text),'condition','success')),'test','example.test/test',cell_id,name,repo,'') FROM pgjobdb.cells WHERE tenant_id='c2' AND name='test'`, goal)
		require.NoError(t, err)
		expected, err := install.Preflight(ctx)
		require.NoError(t, err)
		require.Len(t, expected.RemovedLinks, 3)
		require.Len(t, expected.AffectedWaits, 2)
		require.Len(t, expected.AffectedPreparations, 2)
		var targets string
		require.NoError(t, db.QueryRowContext(ctx, `SELECT jsonb_agg(to_jsonb(t) ORDER BY job_id,wait_id)::text FROM pgjobdb.dependency_targets t`).Scan(&targets))
		sink := pausedReport{make(chan []byte, 1), make(chan struct{})}
		type outcome struct {
			confirmation installer.CommitConfirmation
			err          error
		}
		done := make(chan outcome, 1)
		go func() { c, e := install.ApplyWithReport(ctx, sink); done <- outcome{c, e} }()
		staged := <-sink.captured
		var report installer.ConversionReport
		require.NoError(t, json.Unmarshal(staged, &report))
		require.False(t, report.Advisory)
		compare := func(a, b []json.RawMessage) {
			t.Helper()
			require.Len(t, b, len(a))
			for i := range a {
				require.JSONEq(t, string(a[i]), string(b[i]))
			}
		}
		compare(expected.RemovedLinks, report.RemovedLinks)
		compare(expected.AffectedWaits, report.AffectedWaits)
		compare(expected.AffectedPreparations, report.AffectedPreparations)
		compare(expected.CrossPrimaryProducers, report.CrossPrimaryProducers)
		compare(expected.LiveOverlaps, report.LiveOverlaps)
		require.Len(t, report.MigrationFailures, 1)
		// Session C must be excluded after authoritative capture, through report
		// export and until commit. A real relation-lock wait is the barrier.
		writer, err := db.Conn(ctx)
		require.NoError(t, err)
		defer writer.Close()
		var pid int
		require.NoError(t, writer.QueryRowContext(ctx, `SELECT pg_backend_pid()`).Scan(&pid))
		writeDone := make(chan error, 1)
		go func() {
			_, e := writer.ExecContext(ctx, `UPDATE pgjobdb.job_facts SET app_metadata='{"afterConversion":true}' WHERE job_id='late-member'`)
			writeDone <- e
		}()
		require.Eventually(t, func() bool {
			var waiting bool
			e := db.QueryRowContext(ctx, `SELECT wait_event_type='Lock' FROM pg_stat_activity WHERE pid=$1`, pid).Scan(&waiting)
			return e == nil && waiting
		}, 5*time.Second, time.Millisecond)
		t.Logf("staged report contains all 3 removed links, 2 waits and 2 preparations; writer %d blocked on conversion table lock", pid)
		close(sink.release)
		result := <-done
		require.NoError(t, result.err)
		require.True(t, result.confirmation.Committed)
		require.NoError(t, <-writeDone)
		require.NoError(t, install.Verify(ctx))
		var after string
		require.NoError(t, db.QueryRowContext(ctx, `SELECT jsonb_agg(to_jsonb(t) ORDER BY job_id,wait_id)::text FROM pgjobdb.dependency_targets t`).Scan(&after))
		require.JSONEq(t, targets, after)
		var id string
		require.NoError(t, db.QueryRowContext(ctx, `SELECT job_id FROM pgjobdb.job_facts WHERE record_kind='MIGRATED_GOAL_FAILURE'`).Scan(&id))
		var marker map[string]any
		require.NoError(t, json.Unmarshal(report.MigrationFailures[0], &marker))
		require.Equal(t, id, marker["jobId"])
		afterReport, err := install.Preflight(ctx)
		require.NoError(t, err)
		require.Equal(t, 13, afterReport.SourceFormat)
		require.False(t, afterReport.LegacyInputsAvailable)
		require.Empty(t, afterReport.RemovedLinks, "removed legacy memberships cannot be reconstructed")
	})
}

func TestGoalConversionRejectsCorruption(t *testing.T) {
	for _, mutation := range []string{
		`DROP VIEW pgjobdb.native_jobs;DROP FUNCTION pgjobdb.add_goal_candidate(TEXT,TEXT,TEXT);DROP TABLE pgjobdb.goal_candidates`,
		`ALTER TABLE pgjobdb.job_preparations DROP COLUMN receipt`,
		`DROP FUNCTION pgjobdb.fail_goal(TEXT,TEXT,TEXT)`,
		`ALTER TABLE pgjobdb.job_facts DISABLE TRIGGER ALL;UPDATE pgjobdb.job_facts SET cell_id='missing';ALTER TABLE pgjobdb.job_facts ENABLE TRIGGER ALL`,
		`CREATE VIEW public.unexpected_goal_dependency AS SELECT * FROM pgjobdb.goals`,
	} {
		t.Run(mutation, func(t *testing.T) {
			withBareDatabase(t, func(ctx context.Context, db *sql.DB) {
				_, err := db.ExecContext(ctx, format12BeforeAvailabilityRepair)
				require.NoError(t, err)
				historicalBaseline(t, ctx, db, 12)
				_, err = db.ExecContext(ctx, mutation)
				require.NoError(t, err)
				before := catalogSnapshot(t, ctx, db)
				require.Error(t, (installer.Installer{DB: db}).Apply(ctx))
				require.JSONEq(t, before, catalogSnapshot(t, ctx, db))
				var version int
				require.NoError(t, db.QueryRowContext(ctx, `SELECT format_version FROM pgjobdb.installation`).Scan(&version))
				require.Equal(t, 12, version)
			})
		})
	}
	for _, version := range []int{9, 10, 11, 12, 13} {
		t.Run("incomplete-marker-"+string(rune('A'+version)), func(t *testing.T) {
			withBareDatabase(t, func(ctx context.Context, db *sql.DB) {
				_, err := db.ExecContext(ctx, `CREATE SCHEMA pgjobdb;CREATE TABLE pgjobdb.installation(name text,format_version int)`)
				require.NoError(t, err)
				_, err = db.ExecContext(ctx, `INSERT INTO pgjobdb.installation VALUES('pgjobdb',$1)`, version)
				require.NoError(t, err)
				before := catalogSnapshot(t, ctx, db)
				require.Error(t, (installer.Installer{DB: db}).Apply(ctx))
				require.JSONEq(t, before, catalogSnapshot(t, ctx, db))
			})
		})
	}
}

// Verify is read-only even when stale removed objects or damaged protection
// are present. No acceptance may be obtained by a format marker alone.
func TestRetainedGoalVerifyContract(t *testing.T) {
	runDatabaseTest(t, func(ctx context.Context, db *sql.DB) {
		for _, change := range []struct{ damage, repair string }{
			{`CREATE SCHEMA pgwf`, `DROP SCHEMA pgwf`},
			{`CREATE TABLE pgjobdb.goals(id text)`, `DROP TABLE pgjobdb.goals`},
			{`CREATE TABLE pgjobdb.goal_candidates(id text)`, `DROP TABLE pgjobdb.goal_candidates`},
			{`CREATE FUNCTION pgjobdb.add_goal_candidate(integer) RETURNS integer LANGUAGE sql AS 'SELECT 1'`, `DROP FUNCTION pgjobdb.add_goal_candidate(integer)`},
			{`ALTER TABLE pgjobdb.job_facts DROP CONSTRAINT retained_goal_shape`, ``},
		} {
			_, err := db.ExecContext(ctx, change.damage)
			require.NoError(t, err)
			before := catalogSnapshot(t, ctx, db)
			require.Error(t, (installer.Installer{DB: db}).Verify(ctx))
			require.JSONEq(t, before, catalogSnapshot(t, ctx, db))
			if change.repair != "" {
				_, err = db.ExecContext(ctx, change.repair)
				require.NoError(t, err)
			}
		}
	})
}

func TestRetainedGoalRejectsDamagedMirror(t *testing.T) {
	runDatabaseTest(t, func(ctx context.Context, db *sql.DB) {
		_, err := pgjobdb.CreateCell(ctx, db, jobdb.CreateCellRequest{TenantID: "c2", Name: "work", Repo: "example.test/work"})
		require.NoError(t, err)
		_, err = pgjobdb.SubmitJob(ctx, db, pgjobdb.SubmitJobRequest{TenantID: "c2", JobID: "source", WorkerID: "submitter", JobType: "work", Cell: "work", CellRepo: "example.test/work"})
		require.NoError(t, err)
		source, err := pgjobdb.GetJob(ctx, db, "c2", "source")
		require.NoError(t, err)
		failed, err := pgjobdb.FailGoal(ctx, db, jobdb.FailGoalRequest{GoalKey: jobdb.GoalKey{TenantId: "c2", GoalID: source.GoalID}, Message: "protected mirror"})
		require.NoError(t, err)
		_, err = db.ExecContext(ctx, `INSERT INTO pgjobdb.jobs(tenant_id,job_id,goal_id) VALUES('c2',$1,$2)`, failed.FailureJobID, source.GoalID)
		require.Error(t, err, "protected record cannot enter active storage")
		_, err = db.ExecContext(ctx, `UPDATE pgjobdb.jobs_archive SET completion_detail='changed' WHERE job_id=$1`, failed.FailureJobID)
		require.Error(t, err, "protected archive cannot diverge")
		_, err = db.ExecContext(ctx, `ALTER TABLE pgjobdb.jobs_archive DISABLE TRIGGER archive_goal_identity`)
		require.NoError(t, err)
		_, err = db.ExecContext(ctx, `UPDATE pgjobdb.jobs_archive SET completion_detail='corrupt' WHERE job_id=$1`, failed.FailureJobID)
		require.NoError(t, err)
		_, err = db.ExecContext(ctx, `ALTER TABLE pgjobdb.jobs_archive ENABLE TRIGGER archive_goal_identity`)
		require.NoError(t, err)
		before := catalogSnapshot(t, ctx, db)
		install := installer.Installer{DB: db}
		require.ErrorContains(t, install.Verify(ctx), "inconsistent protected archive mirror")
		require.ErrorContains(t, install.Apply(ctx), "inconsistent protected archive mirror")
		require.JSONEq(t, before, catalogSnapshot(t, ctx, db))
		var detail string
		require.NoError(t, db.QueryRowContext(ctx, `SELECT completion_detail FROM pgjobdb.jobs_archive WHERE job_id=$1`, failed.FailureJobID).Scan(&detail))
		require.Equal(t, "corrupt", detail)
	})
}

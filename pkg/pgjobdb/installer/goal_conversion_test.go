package installer_test

import (
	"bytes"
	"context"
	"database/sql"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	chapterpostgres "github.com/colony-2/jobdb/pkg/jobdb/chapterstore/postgres"
	runtimecore "github.com/colony-2/jobdb/pkg/jobdb/runtime/core"
	"io"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/pgjobdb/pkg/pgjobdb"
	"github.com/colony-2/pgjobdb/pkg/pgjobdb/installer"
	"github.com/stretchr/testify/require"
)

//go:embed testdata/format9.sql
var format9 string

//go:embed testdata/format11.sql
var format11 string

// These are real historical table/function definitions. Data is created through
// each format's native submit/completion functions, including selected receipts
// where that format supports them. No marker-only fixture is used.
func historicalJob(t *testing.T, ctx context.Context, db *sql.DB, version int, id string) string {
	t.Helper()
	_, err := pgjobdb.CreateCell(ctx, db, jobdb.CreateCellRequest{TenantID: "tenant", Name: "test", Repo: "example.test/test"})
	if !errors.Is(err, jobdb.ErrCellConflict) {
		require.NoError(t, err)
	}
	query := `SELECT pgjobdb.submit_native_job('tenant',$1,'submitter','collect','{}','{}',NULL,NULL,NULL,ARRAY[]::TEXT[],NULL,NULL,NULL,NULL,NULL,FALSE,'[]',jsonb_build_object('cellId',cell_id,'cell',name,'cellRepo',repo)) FROM pgjobdb.cells WHERE tenant_id='tenant' AND name='test'`
	if version == 12 {
		query = `SELECT pgjobdb.submit_native_job('tenant',$1,'submitter','collect','{}','{}',NULL,NULL,ARRAY[]::TEXT[],NULL,NULL,NULL,NULL,NULL,FALSE,'[]',jsonb_build_object('cellId',cell_id,'cell',name,'cellRepo',repo)) FROM pgjobdb.cells WHERE tenant_id='tenant' AND name='test'`
	}
	_, err = db.ExecContext(ctx, query, id)
	require.NoError(t, err)
	var goal string
	require.NoError(t, db.QueryRowContext(ctx, `SELECT goal_id FROM pgjobdb.job_facts WHERE tenant_id='tenant' AND job_id=$1`, id).Scan(&goal))
	return goal
}

func historicalBaseline(t *testing.T, ctx context.Context, db *sql.DB, version int) (string, string, string, string) {
	t.Helper()
	a := historicalJob(t, ctx, db, version, "origin")
	b := historicalJob(t, ctx, db, version, "cross-winner")
	failed := historicalJob(t, ctx, db, version, "failed-origin")
	overlap := historicalJob(t, ctx, db, version, "overlap-origin")
	historicalJob(t, ctx, db, version, "consumer")
	// Frozen historical secondary links resolve cross-primary success, while the
	// failed goal's original snapshot includes a subsequently discarded member.
	_, err := db.ExecContext(ctx, `SELECT pgjobdb.add_goal_candidate('tenant',$1,'cross-winner')`, a)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, `SELECT pgjobdb.add_goal_candidate('tenant',$1,'cross-winner')`, failed)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, `SELECT pgjobdb.fail_goal('tenant',$1,E' exact\n保存 failure ')`, failed)
	require.NoError(t, err)
	require.NoError(t, pgjobdb.CompleteUnheldJob(ctx, db, "tenant", "cross-winner", "worker", pgjobdb.Completion{Status: pgjobdb.CompletionSuccess}))
	// Preserve a genuine inherited overlap made by old shared Restart semantics.
	_, err = db.ExecContext(ctx, `SELECT pgjobdb.prepare_job('tenant','overlap-member',NULL,'overlap-origin',TRUE,'historical shared descriptor','[]','test','example.test/test',cell_id,name,repo,'') FROM pgjobdb.cells WHERE tenant_id='tenant' AND name='test'`)
	require.NoError(t, err)
	query := `SELECT pgjobdb.submit_native_job('tenant','overlap-member','submitter','collect','{}','{}',NULL,NULL,NULL,ARRAY[]::TEXT[],NULL,NULL,NULL,NULL,$1,TRUE,'[]',jsonb_build_object('cellId',cell_id,'cell',name,'cellRepo',repo)) FROM pgjobdb.cells WHERE tenant_id='tenant' AND name='test'`
	if version == 12 {
		query = `SELECT pgjobdb.submit_native_job('tenant','overlap-member','submitter','collect','{}','{}',NULL,NULL,ARRAY[]::TEXT[],NULL,NULL,NULL,NULL,$1,TRUE,'[]',jsonb_build_object('cellId',cell_id,'cell',name,'cellRepo',repo)) FROM pgjobdb.cells WHERE tenant_id='tenant' AND name='test'`
	}
	_, err = db.ExecContext(ctx, query, overlap)
	require.NoError(t, err)
	if version >= 11 {
		selectedGoal := historicalJob(t, ctx, db, version, "selected-origin")
		require.NoError(t, pgjobdb.CompleteUnheldJob(ctx, db, "tenant", "selected-origin", "worker", pgjobdb.Completion{Status: pgjobdb.CompletionFailedApp}))
		_, err = db.ExecContext(ctx, `SELECT pgjobdb.prepare_job('tenant','accepted-selected',NULL,NULL,FALSE,'selected original descriptor','[]','test','example.test/test',cell_id,name,repo,'',$1) FROM pgjobdb.cells WHERE tenant_id='tenant' AND name='test'`, selectedGoal)
		require.NoError(t, err)
		selectedQuery := `SELECT pgjobdb.submit_native_job('tenant','accepted-selected','submitter','collect','{}','{}',NULL,NULL,NULL,ARRAY[]::TEXT[],NULL,NULL,NULL,NULL,$1,FALSE,'[]',jsonb_build_object('cellId',cell_id,'cell',name,'cellRepo',repo),$1) FROM pgjobdb.cells WHERE tenant_id='tenant' AND name='test'`
		if version == 12 {
			selectedQuery = `SELECT pgjobdb.submit_native_job('tenant','accepted-selected','submitter','collect','{}','{}',NULL,NULL,ARRAY[]::TEXT[],NULL,NULL,NULL,NULL,$1,FALSE,'[]',jsonb_build_object('cellId',cell_id,'cell',name,'cellRepo',repo),$1) FROM pgjobdb.cells WHERE tenant_id='tenant' AND name='test'`
		}
		_, err = db.ExecContext(ctx, selectedQuery, selectedGoal)
		require.NoError(t, err)
		var receipt bool
		require.NoError(t, db.QueryRowContext(ctx, `SELECT published AND receipt IS NOT NULL FROM pgjobdb.job_preparations WHERE job_id='accepted-selected'`).Scan(&receipt))
		require.True(t, receipt)
	}
	// Actual durable wait/target functions preserve both frozen settlements and
	// pending targets. No evaluation or retargeting is performed by conversion.
	_, err = db.ExecContext(ctx, `SELECT pgjobdb._register_wait('tenant','consumer',jsonb_build_object('waitId','settled','dependencies',jsonb_build_array(jsonb_build_object('target',jsonb_build_object('goalId',$1::text),'condition','success'))))`, a)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, `SELECT pgjobdb._register_wait('tenant','consumer',jsonb_build_object('waitId','pending','dependencies',jsonb_build_array(jsonb_build_object('target',jsonb_build_object('goalId',$1::text),'condition','success'))))`, overlap)
	require.NoError(t, err)
	// Metadata/declarations/client payload stay byte-for-byte; public history and
	// artifact stores are unrelated to the conversion and must survive as well.
	_, err = db.ExecContext(ctx, `UPDATE pgjobdb.job_facts SET hidden_at='2026-01-02' WHERE job_id='origin';UPDATE pgjobdb.job_client_state SET client_payload='{"original":1.00}',revision=4 WHERE job_id='origin';CREATE TABLE public.conversion_unrelated_artifacts(id TEXT PRIMARY KEY,body BYTEA);INSERT INTO public.conversion_unrelated_artifacts VALUES('artifact',decode('010203','hex'))`)
	require.NoError(t, err)
	return a, b, failed, overlap
}

func stableHistoricalRows(t *testing.T, ctx context.Context, db *sql.DB) string {
	t.Helper()
	var out string
	require.NoError(t, db.QueryRowContext(ctx, `SELECT jsonb_build_object(
 'facts',(SELECT jsonb_agg(to_jsonb(f)-'goal_origin'-'goal_created_at'-'goal_closed_at'-'goal_success_job_id'-'historical_goal_success'-'record_kind'-'terminal_failure'-'append_key'-'append_gen' ORDER BY job_id) FROM pgjobdb.job_facts f WHERE job_type<>'jobdb.goal_failure'),
 'active',(SELECT jsonb_agg(to_jsonb(j) ORDER BY job_id) FROM pgjobdb.jobs j),
 'archive',(SELECT jsonb_agg(to_jsonb(a) ORDER BY job_id) FROM pgjobdb.jobs_archive a WHERE job_id IN (SELECT job_id FROM pgjobdb.job_facts WHERE job_type<>'jobdb.goal_failure')),
 'clients',(SELECT jsonb_agg(to_jsonb(s) ORDER BY job_id) FROM pgjobdb.job_client_state s WHERE job_id IN (SELECT job_id FROM pgjobdb.job_facts WHERE job_type<>'jobdb.goal_failure')),
 'preparations',(SELECT jsonb_agg(to_jsonb(p)-CASE WHEN to_jsonb(p)->'selected_goal_id' IS NULL OR to_jsonb(p)->'selected_goal_id'='null'::jsonb THEN 'selected_goal_id' ELSE '' END-CASE WHEN to_jsonb(p)->'receipt' IS NULL OR to_jsonb(p)->'receipt'='null'::jsonb THEN 'receipt' ELSE '' END ORDER BY job_id) FROM pgjobdb.job_preparations p),
 'waits',(SELECT jsonb_agg(to_jsonb(w) ORDER BY wait_id) FROM pgjobdb.dependency_waits w),
 'targets',(SELECT jsonb_agg(to_jsonb(d) ORDER BY wait_id) FROM pgjobdb.dependency_targets d),
 'artifacts',(SELECT jsonb_agg(to_jsonb(a)) FROM public.conversion_unrelated_artifacts a))::text`).Scan(&out))
	return out
}

func TestGoalConversionHistoricalFormats(t *testing.T) {
	for _, fixture := range []struct {
		version int
		ddl     string
	}{{9, format9}, {10, format10}, {11, format11}, {12, format12BeforeAvailabilityRepair}} {
		t.Run(fmt.Sprint(fixture.version), func(t *testing.T) {
			withBareDatabase(t, func(ctx context.Context, db *sql.DB) {
				_, err := db.ExecContext(ctx, fixture.ddl)
				require.NoError(t, err)
				a, b, failure, overlap := historicalBaseline(t, ctx, db, fixture.version)
				chapters, err := chapterpostgres.NewSQLDB(ctx, db, chapterpostgres.Config{BlobStoreURI: "blobfs://" + t.TempDir()})
				require.NoError(t, err)
				defer chapters.Close(ctx)
				logKey := runtimecore.ChapterLogKey{JobKey: jobdb.JobKey{TenantId: "tenant", JobId: "origin"}}
				payload := []byte("historical opaque declaration\x00\xff")
				artifactBytes := []byte("original artifact\x00保存")
				require.NoError(t, chapters.Create(ctx, logKey, runtimecore.EncodedChapter{Ordinal: 0, Payload: payload, ArtifactUploads: []jobdb.ArtifactUpload{{Name: "source.bin", Size: int64(len(artifactBytes)), Open: func() (io.ReadCloser, error) { return io.NopCloser(bytes.NewReader(artifactBytes)), nil }}}}))
				before := stableHistoricalRows(t, ctx, db)
				var oldFailure string
				var closed time.Time
				require.NoError(t, db.QueryRowContext(ctx, `SELECT failure::text,closed_at FROM pgjobdb.goals WHERE tenant_id='tenant' AND goal_id=$1`, failure).Scan(&oldFailure, &closed))
				var successTime time.Time
				require.NoError(t, db.QueryRowContext(ctx, `SELECT closed_at FROM pgjobdb.goals WHERE tenant_id='tenant' AND goal_id=$1`, a).Scan(&successTime))
				install := installer.Installer{DB: db}
				preflight, err := install.Preflight(ctx)
				require.NoError(t, err)
				require.True(t, preflight.Advisory)
				require.Len(t, preflight.RemovedLinks, 2)
				require.Len(t, preflight.CrossPrimaryProducers, 1)
				require.Len(t, preflight.LiveOverlaps, 1)
				require.Equal(t, b, func() string {
					var s string
					require.NoError(t, db.QueryRowContext(ctx, `SELECT goal_id FROM pgjobdb.job_facts WHERE job_id='cross-winner'`).Scan(&s))
					return s
				}())
				var report bytes.Buffer
				confirmation, err := install.ApplyWithReport(ctx, &report)
				require.NoError(t, err)
				require.True(t, confirmation.Committed)
				require.NotEmpty(t, confirmation.ReportSHA256)
				require.NoError(t, install.Verify(ctx))
				require.JSONEq(t, before, stableHistoricalRows(t, ctx, db))
				preserved, err := chapters.Get(ctx, logKey, 0)
				require.NoError(t, err)
				require.Equal(t, payload, preserved.Payload)
				artifact, err := chapters.OpenArtifact(ctx, runtimecore.ArtifactLookup{JobKey: logKey.JobKey, Ordinal: 0, Name: "source.bin"})
				require.NoError(t, err)
				reader, err := artifact.Open()
				require.NoError(t, err)
				gotBytes, err := io.ReadAll(reader)
				require.NoError(t, err)
				require.NoError(t, reader.Close())
				require.Equal(t, artifactBytes, gotBytes)
				var stale int
				require.NoError(t, db.QueryRowContext(ctx, `SELECT count(*) FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='pgjobdb' AND (p.proname='add_goal_candidate' OR (p.proname<>'_goal_lock' AND p.prosrc ~ 'pgjobdb[.](goals|goal_candidates)\M'))`).Scan(&stale))
				require.Zero(t, stale, "no obsolete lifecycle function readers or writers")
				var r installer.ConversionReport
				require.NoError(t, json.Unmarshal(report.Bytes(), &r))
				require.False(t, r.Advisory)
				capture, err := json.Marshal(struct {
					Report       installer.ConversionReport   `json:"report"`
					Confirmation installer.CommitConfirmation `json:"confirmation"`
				}{r, confirmation})
				require.NoError(t, err)
				t.Logf("CONVERSION_REPORT %s", capture)
				require.Len(t, r.MigrationFailures, 1)
				for idx := range preflight.RemovedLinks {
					require.JSONEq(t, string(preflight.RemovedLinks[idx]), string(r.RemovedLinks[idx]))
				}
				key := jobdb.GoalKey{TenantId: "tenant", GoalID: failure}
				g, err := pgjobdb.GetGoal(ctx, db, key)
				require.NoError(t, err)
				require.NotEmpty(t, g.FailureJobID)
				require.Equal(t, []string{g.FailureJobID}, confirmation.MigrationFailureIDs)
				var original jobdb.GoalFailure
				require.NoError(t, json.Unmarshal([]byte(oldFailure), &original))
				original.FailedAt = original.FailedAt.UTC()
				require.Equal(t, &original, g.Failure)
				var storedFailure string
				require.NoError(t, db.QueryRowContext(ctx, `SELECT terminal_failure::text FROM pgjobdb.job_facts WHERE job_id=$1`, g.FailureJobID).Scan(&storedFailure))
				require.JSONEq(t, oldFailure, storedFailure)
				require.True(t, g.ClosedAt.Equal(closed))
				require.Contains(t, g.Failure.NonTerminalJobIDs, "cross-winner")
				require.NotContains(t, g.Failure.NonTerminalJobIDs, g.FailureJobID)
				info, err := pgjobdb.GetJob(ctx, db, "tenant", pgjobdb.JobID(g.FailureJobID))
				require.NoError(t, err)
				require.Equal(t, jobdb.JobRecordMigratedGoalFailure, info.RecordKind)
				require.Empty(t, info.RouteJobType)
				success, err := pgjobdb.GetGoal(ctx, db, jobdb.GoalKey{TenantId: "tenant", GoalID: a})
				require.NoError(t, err)
				require.Equal(t, "cross-winner", success.SuccessfulJobID)
				require.True(t, success.ClosedAt.Equal(successTime))
				// No cancellation/reset: both inherited nonterminal members remain and
				// admission blocks until every one has drained through ordinary completion.
				selected := jobdb.WithPrimaryGoalSelectionAuthorizer(ctx, func(context.Context, jobdb.GoalKey, jobdb.ResolvedCell) error { return nil })
				p, err := pgjobdb.PrepareJob(selected, db, pgjobdb.JobPreparation{Key: jobdb.JobKey{TenantId: "tenant", JobId: "recovery"}, Cell: "test", CellRepo: "example.test/test", SelectedGoalID: overlap, Descriptor: "recovery"})
				require.NoError(t, err)
				req := pgjobdb.SubmitJobRequest{TenantID: "tenant", JobID: "recovery", WorkerID: "worker", JobType: "collect", Cell: "test", CellRepo: "example.test/test", GoalID: overlap, SelectedGoalID: overlap, Target: p.Target}
				_, err = pgjobdb.SubmitJob(selected, db, req)
				require.ErrorIs(t, err, jobdb.ErrGoalConflict)
				for _, id := range []pgjobdb.JobID{"overlap-origin", "overlap-member"} {
					require.NoError(t, pgjobdb.CompleteUnheldJob(ctx, db, "tenant", id, "worker", pgjobdb.Completion{Status: pgjobdb.CompletionFailedApp}))
					_, err = pgjobdb.SubmitJob(selected, db, req)
					if id == "overlap-origin" {
						require.ErrorIs(t, err, jobdb.ErrGoalConflict)
					} else {
						require.NoError(t, err)
					}
				}
				require.NoError(t, install.Apply(ctx))
				again, err := pgjobdb.GetGoal(ctx, db, key)
				require.NoError(t, err)
				require.Equal(t, g, again)
				t.Logf("format %d -> 13: preserved rows, exact cross-primary witness, failure %s, 2 dropped links, overlap drained", fixture.version, g.FailureJobID)
			})
		})
	}
}

type shortReportWriter struct{}

func (shortReportWriter) Write(raw []byte) (int, error) { return len(raw) / 2, nil }

type failingReportWriter struct{}

func (failingReportWriter) Write([]byte) (int, error) {
	return 0, errors.New("injected report export failure")
}

func catalogSnapshot(t *testing.T, ctx context.Context, db *sql.DB) string {
	t.Helper()
	var out string
	require.NoError(t, db.QueryRowContext(ctx, `SELECT jsonb_build_object(
 'tables',(SELECT jsonb_agg(tablename ORDER BY tablename) FROM pg_tables WHERE schemaname='pgjobdb'),
 'columns',(SELECT jsonb_agg(jsonb_build_array(table_name,column_name,data_type,column_default) ORDER BY table_name,ordinal_position) FROM information_schema.columns WHERE table_schema='pgjobdb'),
 'functions',(SELECT jsonb_agg(pg_get_functiondef(p.oid) ORDER BY p.oid) FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='pgjobdb'),
 'constraints',(SELECT jsonb_agg(pg_get_constraintdef(c.oid) ORDER BY c.oid) FROM pg_constraint c JOIN pg_namespace n ON n.oid=c.connamespace WHERE n.nspname='pgjobdb'),
 'indexes',(SELECT jsonb_agg(jsonb_build_array(pg_get_indexdef(i.indexrelid),i.indisvalid,i.indisready,i.indislive) ORDER BY i.indexrelid) FROM pg_index i JOIN pg_class r ON r.oid=i.indrelid JOIN pg_namespace n ON n.oid=r.relnamespace WHERE n.nspname='pgjobdb'),
 'triggers',(SELECT jsonb_agg(jsonb_build_array(pg_get_triggerdef(t.oid),t.tgenabled) ORDER BY t.oid) FROM pg_trigger t JOIN pg_class r ON r.oid=t.tgrelid JOIN pg_namespace n ON n.oid=r.relnamespace WHERE n.nspname='pgjobdb'),
 'marker',(SELECT to_jsonb(i) FROM pgjobdb.installation i))::text`).Scan(&out))
	return out
}

func TestGoalConversionRollback(t *testing.T) {
	withBareDatabase(t, func(ctx context.Context, db *sql.DB) {
		_, err := db.ExecContext(ctx, format12BeforeAvailabilityRepair)
		require.NoError(t, err)
		historicalBaseline(t, ctx, db, 12)
		install := installer.Installer{DB: db}
		before := stableHistoricalRows(t, ctx, db)
		catalog := catalogSnapshot(t, ctx, db)
		confirmation, err := install.ApplyWithReport(ctx, failingReportWriter{})
		require.ErrorContains(t, err, "injected report export failure")
		require.False(t, confirmation.Committed)
		require.JSONEq(t, before, stableHistoricalRows(t, ctx, db))
		require.JSONEq(t, catalog, catalogSnapshot(t, ctx, db))
		confirmation, err = install.ApplyWithReport(ctx, shortReportWriter{})
		require.ErrorIs(t, err, io.ErrShortWrite)
		require.False(t, confirmation.Committed)
		require.JSONEq(t, before, stableHistoricalRows(t, ctx, db))
		require.JSONEq(t, catalog, catalogSnapshot(t, ctx, db))
		// Fail after the failure facts and archive are written. The constraint checks
		// marker advancement at the very end, after function/table replacement.
		_, err = db.ExecContext(ctx, `ALTER TABLE pgjobdb.installation ADD CONSTRAINT injected_conversion_failure CHECK(format_version<>13)`)
		require.NoError(t, err)
		catalog = catalogSnapshot(t, ctx, db)
		require.ErrorContains(t, install.Apply(ctx), "injected_conversion_failure")
		require.JSONEq(t, before, stableHistoricalRows(t, ctx, db))
		require.JSONEq(t, catalog, catalogSnapshot(t, ctx, db))
		_, err = db.ExecContext(ctx, `ALTER TABLE pgjobdb.installation DROP CONSTRAINT injected_conversion_failure`)
		require.NoError(t, err)
		require.NoError(t, install.Apply(ctx))
		require.NoError(t, install.Apply(ctx))
		var count int
		require.NoError(t, db.QueryRowContext(ctx, `SELECT count(*) FROM pgjobdb.job_facts WHERE record_kind='MIGRATED_GOAL_FAILURE'`).Scan(&count))
		require.Equal(t, 1, count)
	})
}

func TestGoalConversionOriginTie(t *testing.T) {
	withBareDatabase(t, func(ctx context.Context, db *sql.DB) {
		_, err := db.ExecContext(ctx, format11)
		require.NoError(t, err)
		_, _, _, goal := historicalBaseline(t, ctx, db, 11)
		// Two matching primary members with the same timestamp use bytewise IDs.
		_, err = db.ExecContext(ctx, `UPDATE pgjobdb.job_facts SET created_at='2026-01-01' WHERE goal_id=$1`, goal)
		require.NoError(t, err)
		var expected string
		require.NoError(t, db.QueryRowContext(ctx, `SELECT job_id FROM pgjobdb.job_facts WHERE goal_id=$1 ORDER BY job_id COLLATE "C" LIMIT 1`, goal).Scan(&expected))
		install := installer.Installer{DB: db}
		report, err := install.Preflight(ctx)
		require.NoError(t, err)
		require.NoError(t, install.Apply(ctx))
		var actual string
		require.NoError(t, db.QueryRowContext(ctx, `SELECT job_id FROM pgjobdb.job_facts WHERE goal_id=$1 AND goal_origin`, goal).Scan(&actual))
		require.Equal(t, expected, actual)
		require.NotEmpty(t, report.MetadataDerivations)
	})
}

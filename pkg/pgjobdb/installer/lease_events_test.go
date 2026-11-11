package installer_test

import (
	"context"
	"database/sql"
	_ "embed"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/pgjobdb/pkg/pgjobdb"
	pginstaller "github.com/colony-2/pgjobdb/pkg/pgjobdb/installer"
	"github.com/stretchr/testify/require"
)

// Frozen immediate-prior schema from the managed checkout base; no event backfill.
//
//go:embed testdata/format9.sql
var format9 string

func eventJob(t *testing.T, ctx context.Context, db *sql.DB, id string) {
	t.Helper()
	_, err := submitCellJob(ctx, db, pgjobdb.SubmitJobRequest{Cell: "test", CellRepo: "example.test/test", TenantID: "tenant", JobID: pgjobdb.JobID(id), WorkerID: "submitter", JobType: "collect"})
	require.NoError(t, err)
}
func eventClaim(ctx context.Context, db pgjobdb.DB, id string) (*pgjobdb.JobLease, error) {
	return pgjobdb.GetJobLease(ctx, db, "tenant", pgjobdb.JobID(id), "worker", pgjobdb.WorkSelector{JobTypes: []pgjobdb.JobType{"collect"}}, pgjobdb.GetJobLeaseOptions{})
}
func eventList(t *testing.T, ctx context.Context, db pgjobdb.DB) []jobdb.LeaseEvent {
	t.Helper()
	v, e := pgjobdb.ListLeaseEvents(ctx, db, jobdb.ListLeaseEventsRequest{TenantID: "tenant", IncludeHidden: true})
	require.NoError(t, e)
	return v.Events
}
func TestLeaseEventsAtomicNative(t *testing.T) {
	runDatabaseTest(t, func(ctx context.Context, db *sql.DB) {
		eventJob(t, ctx, db, "job")
		// The tracing switch and lease mutations share this borrowed transaction.
		tx, err := db.BeginTx(ctx, nil)
		require.NoError(t, err)
		_, err = tx.ExecContext(ctx, `SELECT pgjobdb.set_trace(false)`)
		require.NoError(t, err)
		lease, err := eventClaim(ctx, tx, "job")
		require.NoError(t, err)
		require.NotNil(t, lease)
		updated, err := pgjobdb.KeepAliveLease(ctx, tx, lease.Identity(), time.Second)
		require.NoError(t, err)
		require.True(t, updated.ExpiresAt.Before(lease.ExpiresAt))
		events := eventList(t, ctx, tx)
		require.Len(t, events, 2)
		require.Equal(t, lease.ExpiresAt, *events[0].PreviousLeaseExpiresAt)
		require.Equal(t, updated.ExpiresAt, events[0].LeaseExpiresAt)
		require.Equal(t, jobdb.LeaseAcquisitionTargeted, events[1].AcquisitionSource)
		require.NoError(t, tx.Rollback())
		require.Empty(t, eventList(t, ctx, db))
		var n int
		require.NoError(t, db.QueryRowContext(ctx, `SELECT count(*) FROM pgjobdb.jobs WHERE lease_id IS NOT NULL`).Scan(&n))
		require.Zero(t, n)
		// A failed canonical write must roll the underlying lease mutation back.
		_, err = db.ExecContext(ctx, `ALTER TABLE pgjobdb.lease_events ADD CONSTRAINT injected_failure CHECK(false)`)
		require.NoError(t, err)
		_, err = eventClaim(ctx, db, "job")
		require.Error(t, err)
		require.NoError(t, db.QueryRowContext(ctx, `SELECT count(*) FROM pgjobdb.jobs WHERE lease_id IS NOT NULL`).Scan(&n))
		require.Zero(t, n)
		_, err = db.ExecContext(ctx, `ALTER TABLE pgjobdb.lease_events DROP CONSTRAINT injected_failure`)
		require.NoError(t, err)
		lease, err = eventClaim(ctx, db, "job")
		require.NoError(t, err)
		require.NotNil(t, lease)
		_, err = db.ExecContext(ctx, `ALTER TABLE pgjobdb.lease_events ADD CONSTRAINT injected_failure CHECK(event_type!='lease_renewed')`)
		require.NoError(t, err)
		_, err = pgjobdb.KeepAliveLease(ctx, db, lease.Identity(), time.Second)
		require.Error(t, err)
		live, err := pgjobdb.ValidateLease(ctx, db, lease.Identity())
		require.NoError(t, err)
		require.Equal(t, lease.ExpiresAt, live.ExpiresAt)
		require.Len(t, eventList(t, ctx, db), 1)
		_, err = db.ExecContext(ctx, `ALTER TABLE pgjobdb.lease_events DROP CONSTRAINT injected_failure`)
		require.NoError(t, err)
		tx, err = db.BeginTx(ctx, nil)
		require.NoError(t, err)
		_, err = tx.ExecContext(ctx, `SELECT pgjobdb.set_trace(false)`)
		require.NoError(t, err)
		_, err = pgjobdb.KeepAliveLease(ctx, tx, lease.Identity(), time.Minute)
		require.NoError(t, err)
		require.NoError(t, tx.Commit())
		require.Len(t, eventList(t, ctx, db), 2)

		// The committed tracing switch disables diagnostics, while both canonical
		// event types still persist (renewal above and this new acquisition).
		var tracing bool
		require.NoError(t, db.QueryRowContext(ctx, `SELECT pgjobdb.is_trace_enabled()`).Scan(&tracing))
		require.False(t, tracing)
		eventJob(t, ctx, db, "muted")
		muted, err := eventClaim(ctx, db, "muted")
		require.NoError(t, err)
		require.NotNil(t, muted)
		mutedPage, err := pgjobdb.ListLeaseEvents(ctx, db, jobdb.ListLeaseEventsRequest{TenantID: "tenant", JobIDs: []string{"muted"}})
		require.NoError(t, err)
		require.Len(t, mutedPage.Events, 1)
		require.Equal(t, jobdb.LeaseEventAcquired, mutedPage.Events[0].EventType)
		// Snapshot returned to the caller is detached from persistent history.
		events = eventList(t, ctx, db)
		events[0].WorkerID = "mutated"
		require.Equal(t, "worker", eventList(t, ctx, db)[0].WorkerID)
	})
}

func TestLeaseEventsUpgrade(t *testing.T) {
	withBareDatabase(t, func(ctx context.Context, db *sql.DB) {
		_, err := db.ExecContext(ctx, format9)
		require.NoError(t, err)
		historicalEventJob(t, ctx, db, "old")
		lease, err := eventClaim(ctx, db, "old")
		require.NoError(t, err)
		require.NotNil(t, lease)

		lease, err = pgjobdb.KeepAliveLease(ctx, db, lease.Identity(), time.Minute)
		require.NoError(t, err)
		historicalEventJob(t, ctx, db, "old-completed")
		require.NoError(t, pgjobdb.CompleteUnheldJob(ctx, db, "tenant", "old-completed", "worker", pgjobdb.Completion{Status: pgjobdb.CompletionSuccess}))
		// Compare retained data, including live lease identity/expiry, archived
		// history, primary goals and signing keys, rather than only row counts.
		snapshots := map[string]string{}
		for _, table := range []string{"jobs", "jobs_archive", "job_facts", "job_client_state", "cells", "cell_names", "cell_catalog", "goals", "goal_candidates", "jobs_trace"} {
			var snapshot string
			require.NoError(t, db.QueryRowContext(ctx, `SELECT COALESCE(jsonb_agg(to_jsonb(t) ORDER BY to_jsonb(t)::text),'[]'::jsonb)::text FROM pgjobdb.`+table+` t`).Scan(&snapshot))
			snapshots[table] = snapshot
		}
		var traceBefore int
		require.NoError(t, db.QueryRowContext(ctx, `SELECT count(*) FROM pgjobdb.jobs_trace`).Scan(&traceBefore))
		require.Greater(t, traceBefore, 0)
		installer := pginstaller.Installer{DB: db}
		require.Error(t, installer.Verify(ctx))
		require.NoError(t, installer.Apply(ctx))
		require.NoError(t, installer.Verify(ctx))
		for table, before := range snapshots {
			var after string
			require.NoError(t, db.QueryRowContext(ctx, `SELECT COALESCE(jsonb_agg(to_jsonb(t) ORDER BY to_jsonb(t)::text),'[]'::jsonb)::text FROM pgjobdb.`+table+` t`).Scan(&after))
			require.JSONEq(t, before, after, "upgrade must preserve %s", table)
		}
		require.Empty(t, eventList(t, ctx, db))
		require.NoError(t, installer.Apply(ctx))
		require.NoError(t, installer.Verify(ctx))
		var version, traceAfter int
		require.NoError(t, db.QueryRowContext(ctx, `SELECT format_version FROM pgjobdb.installation`).Scan(&version))
		require.Equal(t, 11, version)
		require.NoError(t, db.QueryRowContext(ctx, `SELECT count(*) FROM pgjobdb.jobs_trace`).Scan(&traceAfter))
		require.Equal(t, traceBefore, traceAfter)
		_, err = pgjobdb.KeepAliveLease(ctx, db, lease.Identity(), time.Minute)
		require.NoError(t, err)
		events := eventList(t, ctx, db)
		require.Len(t, events, 1)
		require.Equal(t, jobdb.LeaseEventRenewed, events[0].EventType)
		require.Equal(t, lease.ExpiresAt, *events[0].PreviousLeaseExpiresAt)
	})
}

type eventCountingDB struct {
	pgjobdb.DB
	reads int
	query string
	args  []any
}

func (c *eventCountingDB) QueryContext(ctx context.Context, q string, args ...any) (*sql.Rows, error) {
	c.reads++
	c.query = q
	c.args = args
	return c.DB.QueryContext(ctx, q, args...)
}
func (c *eventCountingDB) QueryRowContext(ctx context.Context, q string, args ...any) *sql.Row {
	c.reads++
	return c.DB.QueryRowContext(ctx, q, args...)
}

func TestLeaseEventsPrecisionBudgetPlans(t *testing.T) {
	runDatabaseTest(t, func(ctx context.Context, db *sql.DB) {
		eventJob(t, ctx, db, "job")
		lease, err := eventClaim(ctx, db, "job")
		require.NoError(t, err)
		require.NotNil(t, lease)
		// Representative renewal-heavy, mixed-tenant history uses typed fixtures to
		// keep this query test independent of scheduler throughput.
		_, err = db.ExecContext(ctx, `INSERT INTO pgjobdb.lease_events(event_type,occurred_at,tenant_id,job_id,cell_id,cell_name,cell_repo,goal_id,job_type,route_job_type,lease_id,worker_id,lease_expires_at,previous_lease_expires_at)
  SELECT 'lease_renewed','2026-01-01'::timestamptz + (n%1000)*interval '1 microsecond',CASE WHEN n%2=0 THEN 'tenant' ELSE 'other' END,'fixture-'||(n%500),cell_id,cell_name,cell_repo,goal_id,job_type,route_job_type,'fixture-lease-'||(n%500),'fixture-worker-'||(n%100),'2027-01-01','2027-01-01' FROM pgjobdb.lease_events CROSS JOIN generate_series(1,20000) n WHERE event_id=1;
  ANALYZE pgjobdb.lease_events`)
		require.NoError(t, err)
		for _, size := range []int{1, 100, 200} {
			for _, cardinality := range []int{1, 100} {
				t.Run(fmt.Sprintf("size%d-values%d", size, cardinality), func(t *testing.T) {
					values := []string{"worker"}
					for i := 1; i < cardinality; i++ {
						values = append(values, fmt.Sprintf("fixture-worker-%d", i))
					}
					counter := &eventCountingDB{DB: db}
					req := jobdb.ListLeaseEventsRequest{TenantID: "tenant", PageSize: size, WorkerIDs: values}
					page, err := pgjobdb.ListLeaseEvents(ctx, counter, req)
					require.NoError(t, err)
					require.Equal(t, 2, counter.reads)
					require.LessOrEqual(t, len(page.Events), size)
					t.Logf("query reads=%d page=%d filters=%d returned=%d", counter.reads, size, cardinality, len(page.Events))
					rows, err := db.QueryContext(ctx, "EXPLAIN (ANALYZE, BUFFERS) "+counter.query, counter.args...)
					require.NoError(t, err)
					for rows.Next() {
						var line string
						require.NoError(t, rows.Scan(&line))
						t.Log(line)
					}
					require.NoError(t, rows.Err())
					rows.Close()
				})
			}
		}
		// Both bounds ceil: a nanosecond after a stored microsecond excludes the
		// boundary at the lower bound, and includes it at the upper bound.
		instant := time.Date(2026, 2, 1, 0, 0, 0, 123456000, time.UTC)
		_, err = db.ExecContext(ctx, `UPDATE pgjobdb.lease_events SET occurred_at=$1 WHERE event_id=1`, instant)
		require.NoError(t, err)
		sub := instant.Add(time.Nanosecond)
		page, err := pgjobdb.ListLeaseEvents(ctx, db, jobdb.ListLeaseEventsRequest{TenantID: "tenant", JobIDs: []string{"job"}, OccurredAfter: &sub})
		require.NoError(t, err)
		require.Empty(t, page.Events)
		page, err = pgjobdb.ListLeaseEvents(ctx, db, jobdb.ListLeaseEventsRequest{TenantID: "tenant", JobIDs: []string{"job"}, OccurredBefore: &sub})
		require.NoError(t, err)
		require.Len(t, page.Events, 1)
		page, err = pgjobdb.ListLeaseEvents(ctx, db, jobdb.ListLeaseEventsRequest{TenantID: "tenant", JobIDs: []string{"job"}, OccurredAfter: &instant})
		require.NoError(t, err)
		require.Len(t, page.Events, 1)
		page, err = pgjobdb.ListLeaseEvents(ctx, db, jobdb.ListLeaseEventsRequest{TenantID: "tenant", JobIDs: []string{"job"}, OccurredBefore: &instant})
		require.NoError(t, err)
		require.Empty(t, page.Events)
		// Tied timestamps traverse by numeric ID once; reopening a connection keeps
		// the same persisted signing key.
		req := jobdb.ListLeaseEventsRequest{TenantID: "tenant", PageSize: 200}
		seen := map[string]bool{}
		count := 0
		for {
			page, err = pgjobdb.ListLeaseEvents(ctx, db, req)
			require.NoError(t, err)
			for _, e := range page.Events {
				require.False(t, seen[e.EventID])
				seen[e.EventID] = true
				count++
			}
			if page.NextPageToken == "" {
				break
			}
			req.PageToken = page.NextPageToken
		}
		require.Equal(t, 10001, count)
		_, err = pgjobdb.ListLeaseEvents(ctx, db, jobdb.ListLeaseEventsRequest{TenantID: "tenant", PageToken: "bad"})
		require.ErrorIs(t, err, jobdb.ErrInvalidLeaseEventPageToken)
	})
}

func TestLeaseEventsCompetingAndReplacement(t *testing.T) {
	runDatabaseTest(t, func(ctx context.Context, db *sql.DB) {
		eventJob(t, ctx, db, "job")
		var wg sync.WaitGroup
		var mu sync.Mutex
		var claims []*pgjobdb.JobLease
		for i := 0; i < 2; i++ {
			wg.Go(func() {
				l, e := eventClaim(ctx, db, "job")
				require.NoError(t, e)
				if l != nil {
					mu.Lock()
					claims = append(claims, l)
					mu.Unlock()
				}
			})
		}
		wg.Wait()
		require.Len(t, claims, 1)
		require.Len(t, eventList(t, ctx, db), 1)
		_, err := db.ExecContext(ctx, `UPDATE pgjobdb.jobs SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE job_id='job'`)
		require.NoError(t, err)
		replacement, err := eventClaim(ctx, db, "job")
		require.NoError(t, err)
		require.NotNil(t, replacement)
		events := eventList(t, ctx, db)
		require.Len(t, events, 2)
		require.Equal(t, claims[0].LeaseID, events[0].PreviousLeaseID)
		wrong := replacement.Identity()
		wrong.WorkerID = "wrong"
		_, err = pgjobdb.KeepAliveLease(ctx, db, wrong, time.Minute)
		require.Error(t, err)
		require.Len(t, eventList(t, ctx, db), 2)
	})
}

func TestLeaseEventsInternalClaimsAndRaces(t *testing.T) {
	runDatabaseTest(t, func(ctx context.Context, db *sql.DB) {
		req := waitingClaim(t, ctx, db, "internal")
		claim, err := pgjobdb.ClaimTask(ctx, db, req)
		require.NoError(t, err)
		events := eventList(t, ctx, db)
		require.Len(t, events, 1)
		require.Equal(t, jobdb.LeaseAcquisitionTaskCompletion, events[0].AcquisitionSource)
		require.Equal(t, jobdb.Route{JobType: "resume"}, events[0].Route)
		require.Equal(t, "collect", events[0].JobType)
		require.Equal(t, string(claim.Identity.WorkerID), events[0].WorkerID)
		// Later independent work fails; the already committed claim remains.
		_, err = db.ExecContext(ctx, `SELECT 1/0`)
		require.Error(t, err)
		require.Len(t, eventList(t, ctx, db), 1)
		_, err = pgjobdb.ClaimTask(ctx, db, req)
		require.Error(t, err)
		require.Len(t, eventList(t, ctx, db), 1)
		for _, kind := range []string{"cancel", "delete", "expire"} {
			t.Run(kind, func(t *testing.T) {
				eventJob(t, ctx, db, kind)
				lease, err := eventClaim(ctx, db, kind)
				require.NoError(t, err)
				require.NotNil(t, lease)
				start := make(chan struct{})
				renewed := make(chan error, 1)
				changed := make(chan error, 1)
				go func() {
					<-start
					_, err := pgjobdb.KeepAliveLease(ctx, db, lease.Identity(), time.Minute)
					renewed <- err
				}()
				go func() {
					<-start
					var err error
					switch kind {
					case "cancel":
						err = pgjobdb.CancelJob(ctx, db, pgjobdb.CancelJobRequest{TenantID: "tenant", JobID: pgjobdb.JobID(kind), WorkerID: "canceller"})
					case "delete":
						_, err = pgjobdb.DeleteJobs(ctx, db, pgjobdb.DeleteJobsRequest{TenantID: "tenant", JobIDs: []pgjobdb.JobID{pgjobdb.JobID(kind)}})
					case "expire":
						_, err = db.ExecContext(ctx, `UPDATE pgjobdb.jobs SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE tenant_id='tenant' AND job_id=$1`, kind)
					}
					changed <- err
				}()
				close(start)
				renewalErr := <-renewed
				require.NoError(t, <-changed)
				page, err := pgjobdb.ListLeaseEvents(ctx, db, jobdb.ListLeaseEventsRequest{TenantID: "tenant", JobIDs: []string{kind}, IncludeHidden: true})
				require.NoError(t, err)
				want := 1
				if renewalErr == nil {
					want++
				}
				require.Len(t, page.Events, want)
				// A completed cancellation/deletion/expiry rejects later renewal and adds none.
				_, err = pgjobdb.KeepAliveLease(ctx, db, lease.Identity(), time.Minute)
				require.Error(t, err)
				again, err := pgjobdb.ListLeaseEvents(ctx, db, jobdb.ListLeaseEventsRequest{TenantID: "tenant", JobIDs: []string{kind}, IncludeHidden: true})
				require.NoError(t, err)
				require.Equal(t, page, again)
			})
		}
	})
}

func TestLeaseEventsAttributionAndGoalIsolation(t *testing.T) {
	runDatabaseTest(t, func(ctx context.Context, db *sql.DB) {
		eventJob(t, ctx, db, "same")
		lease, err := eventClaim(ctx, db, "same")
		require.NoError(t, err)
		original := eventList(t, ctx, db)[0]
		cell, err := pgjobdb.GetCell(ctx, db, jobdb.CellKey{TenantID: "tenant", CellID: original.CellID})
		require.NoError(t, err)
		name, repo := "renamed", "example.test/changed"
		_, err = pgjobdb.UpdateCell(ctx, db, jobdb.UpdateCellRequest{CellKey: jobdb.CellKey{TenantID: "tenant", CellID: cell.ID}, IfMatch: fmt.Sprintf("%q", cell.Revision), Name: &name, Repo: &repo})
		require.NoError(t, err)
		_, err = pgjobdb.KeepAliveLease(ctx, db, lease.Identity(), time.Minute)
		require.NoError(t, err)
		events := eventList(t, ctx, db)
		require.Equal(t, original, events[1])
		require.Equal(t, name, events[0].Cell)
		require.Equal(t, repo, events[0].CellRepo)
		for _, name := range []string{"test", "renamed"} {
			page, err := pgjobdb.ListLeaseEvents(ctx, db, jobdb.ListLeaseEventsRequest{TenantID: "tenant", CellNames: []string{name}})
			require.NoError(t, err)
			require.Len(t, page.Events, 2)
		}
		// Same tenant-local job identity in a different tenant with an additional goal.
		_, err = submitCellJob(ctx, db, pgjobdb.SubmitJobRequest{Cell: "test", CellRepo: "example.test/test", TenantID: "other", JobID: "same", WorkerID: "submitter", JobType: "collect"})
		require.NoError(t, err)
		_, err = submitCellJob(ctx, db, pgjobdb.SubmitJobRequest{Cell: "test", CellRepo: "example.test/test", TenantID: "other", JobID: "goal-owner", WorkerID: "submitter", JobType: "collect"})
		require.NoError(t, err)
		var otherGoal string
		require.NoError(t, db.QueryRowContext(ctx, `SELECT goal_id FROM pgjobdb.job_facts WHERE tenant_id='other' AND job_id='goal-owner'`).Scan(&otherGoal))
		_, err = pgjobdb.AddGoalCandidate(ctx, db, jobdb.AddGoalCandidateRequest{GoalKey: jobdb.GoalKey{TenantId: "other", GoalID: otherGoal}, JobID: "same"})
		require.NoError(t, err)
		page, err := pgjobdb.ListLeaseEvents(ctx, db, jobdb.ListLeaseEventsRequest{TenantID: "tenant", GoalIDs: []string{otherGoal}})
		require.NoError(t, err)
		require.Empty(t, page.Events)
		// Duplicate memberships are deduplicated before the event join. New current
		// memberships find both historical snapshots without rewriting primary goal.
		_, err = submitCellJob(ctx, db, pgjobdb.SubmitJobRequest{Cell: "renamed", CellRepo: repo, TenantID: "tenant", JobID: "goal-owner", WorkerID: "submitter", JobType: "collect"})
		require.NoError(t, err)
		var extra string
		require.NoError(t, db.QueryRowContext(ctx, `SELECT goal_id FROM pgjobdb.job_facts WHERE tenant_id='tenant' AND job_id='goal-owner'`).Scan(&extra))
		_, err = pgjobdb.AddGoalCandidate(ctx, db, jobdb.AddGoalCandidateRequest{GoalKey: jobdb.GoalKey{TenantId: "tenant", GoalID: extra}, JobID: "same"})
		require.NoError(t, err)
		page, err = pgjobdb.ListLeaseEvents(ctx, db, jobdb.ListLeaseEventsRequest{TenantID: "tenant", GoalIDs: []string{extra, original.GoalID}})
		require.NoError(t, err)
		require.Len(t, page.Events, 2)
		for _, event := range page.Events {
			require.Equal(t, original.GoalID, event.GoalID)
		}
	})
}

func TestLeaseEventsScopeAndCursorSecurity(t *testing.T) {
	runDatabaseTest(t, func(ctx context.Context, db *sql.DB) {
		counter := &eventCountingDB{DB: db}
		page, err := pgjobdb.ListLeaseEvents(ctx, counter, jobdb.ListLeaseEventsRequest{TenantID: "uncatalogued"})
		require.NoError(t, err)
		require.NotNil(t, page.Events)
		require.Empty(t, page.Events)
		require.Equal(t, 1, counter.reads)
		var count int
		require.NoError(t, db.QueryRowContext(ctx, `SELECT count(*) FROM pgjobdb.cell_catalog WHERE tenant_id='uncatalogued'`).Scan(&count))
		require.Zero(t, count)
		_, err = pgjobdb.ListLeaseEvents(ctx, db, jobdb.ListLeaseEventsRequest{TenantID: "uncatalogued", PageToken: "token"})
		require.ErrorIs(t, err, jobdb.ErrInvalidLeaseEventPageToken)
		_, err = pgjobdb.ListLeaseEvents(ctx, db, jobdb.ListLeaseEventsRequest{TenantID: "uncatalogued", CellNames: []string{"absent"}})
		require.ErrorIs(t, err, jobdb.ErrCellNotFound)
		eventJob(t, ctx, db, "one")
		eventJob(t, ctx, db, "two")
		_, err = eventClaim(ctx, db, "one")
		require.NoError(t, err)
		_, err = eventClaim(ctx, db, "two")
		require.NoError(t, err)
		req := jobdb.ListLeaseEventsRequest{TenantID: "tenant", PageSize: 1}
		page, err = pgjobdb.ListLeaseEvents(ctx, db, req)
		require.NoError(t, err)
		require.NotEmpty(t, page.NextPageToken)
		req.PageToken = page.NextPageToken
		altered := req
		altered.PageSize = 100
		_, err = pgjobdb.ListLeaseEvents(ctx, db, altered)
		require.ErrorIs(t, err, jobdb.ErrInvalidLeaseEventPageToken)
		altered = req
		altered.PageToken += "x"
		_, err = pgjobdb.ListLeaseEvents(ctx, db, altered)
		require.ErrorIs(t, err, jobdb.ErrInvalidLeaseEventPageToken)
		altered = req
		altered.TenantID = "uncatalogued"
		_, err = pgjobdb.ListLeaseEvents(ctx, db, altered)
		require.ErrorIs(t, err, jobdb.ErrInvalidLeaseEventPageToken)
		authority := jobdb.CellAuthority{Principal: "reader", TenantID: "tenant", Permissions: map[string]bool{"jobs.list": true}, Cells: map[string]bool{}}
		denied := jobdb.WithCellAuthority(ctx, authority)
		_, err = pgjobdb.ListLeaseEvents(denied, db, req)
		require.ErrorIs(t, err, jobdb.ErrInvalidLeaseEventPageToken)
		altered = req
		altered.PageToken = ""
		empty, err := pgjobdb.ListLeaseEvents(denied, db, altered)
		require.NoError(t, err)
		require.Empty(t, empty.Events)
		altered.CellIDs = []string{page.Events[0].CellID}
		_, err = pgjobdb.ListLeaseEvents(denied, db, altered)
		require.ErrorIs(t, err, jobdb.ErrCellNotFound)
		_, err = pgjobdb.ListLeaseEvents(context.Background(), db, jobdb.ListLeaseEventsRequest{TenantID: "tenant"})
		require.ErrorIs(t, err, jobdb.ErrCellForbidden)
	})
}

func TestLeaseEventsVisibilityAndNoOps(t *testing.T) {
	runDatabaseTest(t, func(ctx context.Context, db *sql.DB) {
		eventJob(t, ctx, db, "job")
		lease, err := eventClaim(ctx, db, "job")
		require.NoError(t, err)
		require.NotNil(t, lease)
		history := eventList(t, ctx, db)
		require.Len(t, history, 1)
		// Read-only validation, a competing claim, denied acquisition, and a
		// cancelled caller cannot add history to the existing live lease.
		_, err = pgjobdb.ValidateLease(ctx, db, lease.Identity())
		require.NoError(t, err)
		other, err := eventClaim(ctx, db, "job")
		require.NoError(t, err)
		require.Nil(t, other)
		_, err = eventClaim(context.Background(), db, "job")
		require.Error(t, err)
		cancelled, cancel := context.WithCancel(ctx)
		cancel()
		_, err = eventClaim(cancelled, db, "job")
		require.Error(t, err)
		require.Equal(t, history, eventList(t, ctx, db))

		// Clearing crash concern after expiry changes eligibility, not history.
		_, err = db.ExecContext(ctx, `UPDATE pgjobdb.jobs SET lease_expires_at=clock_timestamp()-interval '1 second',consecutive_expirations=3 WHERE tenant_id='tenant' AND job_id='job'`)
		require.NoError(t, err)
		_, err = pgjobdb.KeepAliveLease(ctx, db, lease.Identity(), time.Minute)
		require.Error(t, err)
		clear := jobdb.ClearCrashConcernRequest{JobKey: jobdb.JobKey{TenantId: "tenant", JobId: "job"}, WorkerID: "operator", Reason: "fixture"}
		require.NoError(t, pgjobdb.ClearCrashConcern(ctx, db, clear))
		require.NoError(t, pgjobdb.ClearCrashConcern(ctx, db, clear))
		require.Equal(t, history, eventList(t, ctx, db))
		other, err = pgjobdb.GetJobLease(ctx, db, "tenant", "job", "worker", pgjobdb.WorkSelector{JobTypes: []pgjobdb.JobType{"wrong-route"}}, pgjobdb.GetJobLeaseOptions{})
		require.NoError(t, err)
		require.Nil(t, other)
		require.Equal(t, history, eventList(t, ctx, db))
		require.NoError(t, pgjobdb.CancelJob(ctx, db, pgjobdb.CancelJobRequest{TenantID: "tenant", JobID: "job", WorkerID: "operator"}))
		other, err = eventClaim(ctx, db, "job")
		require.NoError(t, err)
		require.Nil(t, other)
		require.Equal(t, history, eventList(t, ctx, db))

		// Deletion hides retained history by default, while explicit inclusion
		// keeps the immutable snapshots. Hidden jobs cannot be acquired.
		_, err = pgjobdb.DeleteJobs(ctx, db, pgjobdb.DeleteJobsRequest{TenantID: "tenant", JobIDs: []pgjobdb.JobID{"job"}})
		require.NoError(t, err)
		visible, err := pgjobdb.ListLeaseEvents(ctx, db, jobdb.ListLeaseEventsRequest{TenantID: "tenant"})
		require.NoError(t, err)
		require.Empty(t, visible.Events)
		require.Equal(t, history, eventList(t, ctx, db))
		other, err = eventClaim(ctx, db, "job")
		require.NoError(t, err)
		require.Nil(t, other)
		require.Equal(t, history, eventList(t, ctx, db))

		// Retiring the cell does not remove its retained identity from event scope.
		_, err = pgjobdb.FailGoal(ctx, db, jobdb.FailGoalRequest{GoalKey: jobdb.GoalKey{TenantId: "tenant", GoalID: history[0].GoalID}, Message: "fixture finished"})
		require.NoError(t, err)
		cell, err := pgjobdb.GetCell(ctx, db, jobdb.CellKey{TenantID: "tenant", CellID: history[0].CellID})
		require.NoError(t, err)
		_, err = pgjobdb.RetireCell(ctx, db, jobdb.RetireCellRequest{CellKey: jobdb.CellKey{TenantID: "tenant", CellID: cell.ID}, IfMatch: fmt.Sprintf("%q", cell.Revision)})
		require.NoError(t, err)
		for _, req := range []jobdb.ListLeaseEventsRequest{
			{TenantID: "tenant", IncludeHidden: true, CellIDs: []string{cell.ID}},
			{TenantID: "tenant", IncludeHidden: true, CellNames: []string{cell.Name}},
		} {
			page, err := pgjobdb.ListLeaseEvents(ctx, db, req)
			require.NoError(t, err)
			require.Equal(t, history, page.Events)
		}
	})
}

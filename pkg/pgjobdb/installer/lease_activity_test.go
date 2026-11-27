package installer_test

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/colony-2/pgjobdb/pkg/pgjobdb"
	"github.com/colony-2/pgjobdb/pkg/pgjobdb/installer"
	"github.com/stretchr/testify/require"
)

func TestLastLeaseUpdatedAtMutations(t *testing.T) {
	runDatabaseTest(t, func(ctx context.Context, db *sql.DB) {
		_, err := db.ExecContext(ctx, `ALTER FUNCTION pg_catalog.clock_timestamp() RENAME TO lease_fixture_real_clock`)
		require.NoError(t, err)
		now := time.Now().UTC().Truncate(time.Microsecond)
		tick := func(duration time.Duration) time.Time {
			now = now.Add(duration)
			_, err := db.ExecContext(ctx, `CREATE OR REPLACE FUNCTION pg_catalog.clock_timestamp() RETURNS TIMESTAMPTZ LANGUAGE sql VOLATILE AS $$ SELECT '`+now.Format(time.RFC3339Nano)+`'::timestamptz; $$`)
			require.NoError(t, err)
			return now
		}
		const job = "activity"
		assertList := func(reader pgjobdb.DB, id string, want, expires *time.Time, archived bool) {
			t.Helper()
			page, err := pgjobdb.ListJobSummaries(ctx, reader, pgjobdb.ListJobsOptions{TenantIDs: []pgjobdb.TenantID{"c2"}, IncludeHidden: true})
			require.NoError(t, err)
			for _, row := range page.Jobs {
				if string(row.JobID) == id {
					require.Equal(t, want, row.LastLeaseUpdatedAt)
					require.Equal(t, expires, row.LeaseExpiresAt)
					require.Equal(t, archived, row.ArchivedAt != nil)
					return
				}
			}
			t.Fatalf("job %s missing from compact list", id)
		}
		tick(time.Second)
		eventJob(t, ctx, db, job)
		assertList(db, job, nil, nil, false)
		polled := tick(time.Second + 123*time.Microsecond)
		lease, err := pgjobdb.GetWork(ctx, db, "worker", pgjobdb.WorkSelector{JobTypes: []pgjobdb.JobType{"collect"}}, pgjobdb.GetWorkOptions{TenantIDs: []pgjobdb.TenantID{"c2"}, LeaseDuration: time.Minute})
		require.NoError(t, err)
		require.NotNil(t, lease)
		require.Equal(t, polled.Add(time.Minute), lease.ExpiresAt)
		assertList(db, job, &polled, &lease.ExpiresAt, false)

		renewedAt := tick(time.Second)
		updated, err := pgjobdb.KeepAliveLease(ctx, db, lease.Identity(), 5*time.Second)
		require.NoError(t, err)
		require.Equal(t, renewedAt.Add(5*time.Second), updated.ExpiresAt)
		require.True(t, updated.ExpiresAt.Before(lease.ExpiresAt))
		assertList(db, job, &renewedAt, &updated.ExpiresAt, false)

		attempt := tick(time.Second)
		wrong := lease.Identity()
		wrong.WorkerID = "wrong"
		_, err = pgjobdb.KeepAliveLease(ctx, db, wrong, time.Minute)
		require.Error(t, err)
		assertList(db, job, &renewedAt, &updated.ExpiresAt, false)
		tx, err := db.BeginTx(ctx, nil)
		require.NoError(t, err)
		defer tx.Rollback()
		rolled, err := pgjobdb.KeepAliveLease(ctx, tx, lease.Identity(), time.Minute)
		require.NoError(t, err)
		assertList(tx, job, &attempt, &rolled.ExpiresAt, false)
		require.NoError(t, tx.Rollback())
		assertList(db, job, &renewedAt, &updated.ExpiresAt, false)

		claimed := tick(7 * time.Second)
		_, err = pgjobdb.KeepAliveLease(ctx, db, lease.Identity(), time.Minute)
		require.Error(t, err)
		assertList(db, job, &renewedAt, &updated.ExpiresAt, false)
		targeted, err := eventClaim(ctx, db, job)
		require.NoError(t, err)
		require.NotNil(t, targeted)
		require.NotEqual(t, lease.LeaseID, targeted.LeaseID)
		assertList(db, job, &claimed, &targeted.ExpiresAt, false)
		tick(time.Second)
		require.NoError(t, pgjobdb.RescheduleJob(ctx, db, targeted.Identity(), pgjobdb.RescheduleRequest{RouteJobType: "collect", WorkKind: pgjobdb.WorkKindJob}))
		assertList(db, job, &claimed, nil, false)

		appended := tick(time.Second)
		var appendExpiry time.Time
		err = db.QueryRowContext(ctx, `SELECT pgjobdb.claim_append('c2',$1,'append-lease','jobdb-append-activity',60)`, job).Scan(&appendExpiry)
		require.NoError(t, err)
		require.True(t, appended.Add(time.Minute).Equal(appendExpiry))
		appendExpiry = appendExpiry.UTC()
		_, err = db.ExecContext(ctx, `SELECT 1/0`)
		require.Error(t, err)
		assertList(db, job, &appended, &appendExpiry, false)
		tick(time.Second)
		_, err = db.ExecContext(ctx, `SELECT pgjobdb.release_append('c2',$1,'append-lease')`, job)
		require.NoError(t, err)
		assertList(db, job, &appended, nil, false)
		tick(time.Second)
		require.NoError(t, pgjobdb.CompleteUnheldJob(ctx, db, "c2", job, "operator", pgjobdb.Completion{Status: pgjobdb.CompletionSuccess}))
		assertList(db, job, &appended, nil, true)

		eventJob(t, ctx, db, "unclaimed")
		require.NoError(t, pgjobdb.CompleteUnheldJob(ctx, db, "c2", "unclaimed", "operator", pgjobdb.Completion{Status: pgjobdb.CompletionSuccess}))
		assertList(db, "unclaimed", nil, nil, true)

		request := waitingClaim(t, ctx, db, "task")
		handoff := tick(time.Second)
		claim, err := pgjobdb.ClaimTask(ctx, db, request)
		require.NoError(t, err)
		require.NotNil(t, claim)
		var expiry time.Time
		require.NoError(t, db.QueryRowContext(ctx, `SELECT lease_expires_at FROM pgjobdb.jobs WHERE job_id='task'`).Scan(&expiry))
		expiry = expiry.UTC()
		assertList(db, "task", &handoff, &expiry, false)
	})
}

func TestLastLeaseUpdatedAtRejectsUnsupported(t *testing.T) {
	withBareDatabase(t, func(ctx context.Context, db *sql.DB) {
		_, err := db.ExecContext(ctx, format13)
		require.NoError(t, err)
		installer := installer.Installer{DB: db}
		for _, test := range []struct{ change, undo, diagnostic string }{
			{`ALTER TABLE pgjobdb.jobs ADD COLUMN last_lease_updated_at TIMESTAMPTZ`, `ALTER TABLE pgjobdb.jobs DROP COLUMN last_lease_updated_at`, "unexpected lease activity columns"},
			{`ALTER FUNCTION pgjobdb.claim_append(text,text,text,text,double precision) RENAME TO broken_claim`, `ALTER FUNCTION pgjobdb.broken_claim(text,text,text,text,double precision) RENAME TO claim_append`, "missing lease definition"},
		} {
			_, err = db.ExecContext(ctx, test.change)
			require.NoError(t, err)
			_, err = installer.Preflight(ctx)
			require.ErrorContains(t, err, test.diagnostic)
			_, err = installer.ApplyWithReport(ctx, nil)
			require.ErrorContains(t, err, "explicit offline migration")
			_, err = installer.MigrateWithReport(ctx, nil)
			require.ErrorContains(t, err, test.diagnostic)
			var version int
			require.NoError(t, db.QueryRowContext(ctx, `SELECT format_version FROM pgjobdb.installation`).Scan(&version))
			require.Equal(t, 13, version)
			_, err = db.ExecContext(ctx, test.undo)
			require.NoError(t, err)
		}
	})
}

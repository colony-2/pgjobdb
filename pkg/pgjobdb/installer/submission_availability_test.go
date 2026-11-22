package installer_test

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/pgjobdb/pkg/pgjobdb"
	"github.com/colony-2/pgjobdb/pkg/pgjobdb/installer"
	"github.com/stretchr/testify/require"
)

func TestSubmissionAvailabilityNative(t *testing.T) {
	runDatabaseTest(t, func(ctx context.Context, db *sql.DB) {
		for i, text := range []string{"0001-01-01T00:00:00Z", "9999-12-31T23:59:59.999999Z", "1969-12-31T23:59:59.999999001Z", "1970-01-01T00:00:00.000000001Z", "2300-01-01T00:00:00.123456789Z", "2026-12-31T23:59:59.999999999Z"} {
			t.Run(text, func(t *testing.T) {
				original, err := time.Parse(time.RFC3339Nano, text)
				require.NoError(t, err)
				req := pgjobdb.SubmitJobRequest{TenantID: "tenant", JobID: pgjobdb.JobID(fmt.Sprintf("precision-%d", i)), WorkerID: "submit", JobType: "work", Cell: "test", CellRepo: "example.test/test", AvailableAt: &original}
				_, err = submitCellJob(ctx, db, req)
				require.NoError(t, err)
				effective := original.Truncate(time.Microsecond)
				if effective.Before(original) {
					effective = effective.Add(time.Microsecond)
				}
				row, err := pgjobdb.GetJob(ctx, db, "tenant", req.JobID)
				require.NoError(t, err)
				require.True(t, row.AvailableAt.Equal(effective), "%s != %s", row.AvailableAt, effective)
				require.False(t, row.AvailableAt.Before(original))
				var before, at string
				require.NoError(t, db.QueryRowContext(ctx, `SELECT pgjobdb.job_status_at(j,$2),pgjobdb.job_status_at(j,$3) FROM pgjobdb.jobs j WHERE job_id=$1`, req.JobID, effective.Add(-time.Microsecond), effective).Scan(&before, &at))
				require.Equal(t, "AWAITING_FUTURE", before)
				require.Equal(t, "READY", at)
				bad := time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC)
				req.AvailableAt = &bad
				_, err = submitCellJob(ctx, db, req)
				require.ErrorContains(t, err, "submission availability")
				unchanged, err := pgjobdb.GetJob(ctx, db, "tenant", req.JobID)
				require.NoError(t, err)
				require.Equal(t, row.AvailableAt, unchanged.AvailableAt)
			})
		}
		for i, bad := range []time.Time{time.Time{}.Add(-time.Nanosecond), time.Date(9999, 12, 31, 23, 59, 59, 999999001, time.UTC), time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC)} {
			req := pgjobdb.SubmitJobRequest{TenantID: "tenant", JobID: pgjobdb.JobID(fmt.Sprintf("invalid-%d", i)), WorkerID: "submit", JobType: "work", Cell: "test", CellRepo: "example.test/test", AvailableAt: &bad}
			_, err := submitCellJob(ctx, db, req)
			require.ErrorContains(t, err, "submission availability")
			assertNoAvailabilityPublication(t, ctx, db, string(req.JobID))
			good := time.Now().UTC().Add(-time.Hour)
			req.AvailableAt = &good
			_, err = submitCellJob(ctx, db, req)
			require.NoError(t, err)
		}
		req := pgjobdb.SubmitJobRequest{TenantID: "tenant", JobID: "omitted", WorkerID: "submit", JobType: "work", Cell: "test", CellRepo: "example.test/test"}
		var before, after time.Time
		require.NoError(t, db.QueryRowContext(ctx, `SELECT clock_timestamp()`).Scan(&before))
		_, err := submitCellJob(ctx, db, req)
		require.NoError(t, err)
		require.NoError(t, db.QueryRowContext(ctx, `SELECT clock_timestamp()`).Scan(&after))
		row, err := pgjobdb.GetJob(ctx, db, "tenant", "omitted")
		require.NoError(t, err)
		require.False(t, row.AvailableAt.Before(before))
		require.False(t, row.AvailableAt.After(after))
	})
}

func assertNoAvailabilityPublication(t *testing.T, ctx context.Context, db *sql.DB, id string) {
	t.Helper()
	for _, table := range []string{"jobs", "job_facts", "dependency_waits"} {
		var count int
		require.NoError(t, db.QueryRowContext(ctx, `SELECT count(*) FROM pgjobdb.`+table+` WHERE tenant_id='tenant' AND job_id=$1`, id).Scan(&count))
		require.Zero(t, count, table)
	}
	var count int
	require.NoError(t, db.QueryRowContext(ctx, `SELECT count(*) FROM pgjobdb.job_preparations WHERE tenant_id='tenant' AND job_id=$1 AND (published OR receipt IS NOT NULL)`, id).Scan(&count))
	require.Zero(t, count)
}

func TestSubmissionPreparationAvailability(t *testing.T) {
	runDatabaseTest(t, func(ctx context.Context, db *sql.DB) {
		_, err := pgjobdb.CreateCell(ctx, db, jobdb.CreateCellRequest{TenantID: "tenant", Name: "test", Repo: "example.test/test"})
		require.NoError(t, err)
		req := pgjobdb.JobPreparation{Key: jobdb.JobKey{TenantId: "tenant", JobId: "prepare"}, Cell: "test", CellRepo: "example.test/test", Descriptor: `{"Kind":"submit","Job":{"AvailableAt":"9999-12-31T23:59:59.999999001Z"}}`}
		_, err = pgjobdb.PrepareJob(ctx, db, req)
		require.ErrorContains(t, err, "submission availability")
		assertNoAvailabilityPublication(t, ctx, db, "prepare")
		var count int
		require.NoError(t, db.QueryRowContext(ctx, `SELECT count(*) FROM pgjobdb.job_preparations`).Scan(&count))
		require.Zero(t, count)
		generated := req
		generated.Key.JobId = ""
		generated.RequestID = "availability-retry"
		_, err = pgjobdb.PrepareJob(ctx, db, generated)
		require.ErrorContains(t, err, "submission availability")
		generated.Descriptor = `{"Kind":"submit","Job":{"AvailableAt":"2300-01-01T00:00:00.123456789Z"}}`
		reserved, err := pgjobdb.PrepareJob(ctx, db, generated)
		require.NoError(t, err)
		require.NotEmpty(t, reserved.Key.JobId)
		repeated, err := pgjobdb.PrepareJob(ctx, db, generated)
		require.NoError(t, err)
		require.Equal(t, reserved, repeated)
		corrected := time.Date(2300, 1, 1, 0, 0, 0, 123456789, time.UTC)
		_, err = pgjobdb.SubmitJob(ctx, db, pgjobdb.SubmitJobRequest{TenantID: "tenant", JobID: pgjobdb.JobID(reserved.Key.JobId), WorkerID: "submit", JobType: "work", Cell: "test", CellRepo: "example.test/test", GoalID: reserved.GoalID, Target: reserved.Target, AvailableAt: &corrected})
		require.NoError(t, err)

		req.Descriptor = "native opaque descriptor"
		prepared, err := pgjobdb.PrepareJob(ctx, db, req)
		require.NoError(t, err)
		at := time.Date(2300, 1, 1, 0, 0, 0, 123, time.UTC)
		_, err = pgjobdb.SubmitJob(ctx, db, pgjobdb.SubmitJobRequest{TenantID: "tenant", JobID: "prepare", WorkerID: "submit", JobType: "work", Cell: "test", CellRepo: "example.test/test", Target: prepared.Target, GoalID: prepared.GoalID, AvailableAt: &at})
		require.NoError(t, err)
		req.Descriptor = `{"Kind":"submit","Job":{"AvailableAt":"bad"}}`
		_, err = pgjobdb.PrepareJob(ctx, db, req)
		require.ErrorContains(t, err, "submission availability")
	})
}

func TestSubmissionTimestampReadsUTC(t *testing.T) {
	runDatabaseTest(t, func(ctx context.Context, db *sql.DB) {
		// A one-connection caller pool makes the selected zone deterministic.
		db.SetMaxOpenConns(1)
		for i, text := range []string{"0001-01-01T00:00:00Z", "9999-12-31T23:59:59.999999Z", "1850-01-01T00:00:00.123456Z"} {
			at, err := time.Parse(time.RFC3339Nano, text)
			require.NoError(t, err)
			id := pgjobdb.JobID(fmt.Sprintf("read-%d", i))
			_, err = submitCellJob(ctx, db, pgjobdb.SubmitJobRequest{TenantID: "tenant", JobID: id, WorkerID: "submit", JobType: "work", Cell: "test", CellRepo: "example.test/test", AvailableAt: &at, ExpiresAt: &at})
			require.NoError(t, err)
			_, err = db.ExecContext(ctx, `UPDATE pgjobdb.job_facts SET created_at=$2 WHERE job_id=$1`, id, at)
			require.NoError(t, err)
			if i == 2 {
				require.NoError(t, pgjobdb.CompleteUnheldJob(ctx, db, "tenant", id, "worker", pgjobdb.Completion{Status: pgjobdb.CompletionSuccess}))
				_, err = db.ExecContext(ctx, `UPDATE pgjobdb.jobs_archive SET archived_at=$2,final_lease_expires_at=$2,final_available_at=$2 WHERE job_id=$1`, id, at)
				require.NoError(t, err)
			} else {
				_, err = db.ExecContext(ctx, `UPDATE pgjobdb.jobs SET lease_expires_at=$2 WHERE job_id=$1`, id, at)
				require.NoError(t, err)
			}
			for _, zone := range []string{"UTC", "Etc/GMT-14", "Etc/GMT+12", "Europe/Paris"} {
				t.Run(fmt.Sprintf("%d/%s", i, zone), func(t *testing.T) {
					_, err = db.ExecContext(ctx, `SELECT set_config('TimeZone',$1,false)`, zone)
					require.NoError(t, err)
					detail, err := pgjobdb.GetJob(ctx, db, "tenant", id)
					require.NoError(t, err)
					status, err := pgjobdb.GetJobStatus(ctx, db, "tenant", id)
					require.NoError(t, err)
					opts := pgjobdb.ListJobsOptions{TenantIDs: []pgjobdb.TenantID{"tenant"}, JobKeys: []pgjobdb.JobKey{{TenantID: "tenant", JobID: id}}}
					full, err := pgjobdb.ListJobs(ctx, db, opts)
					require.NoError(t, err)
					require.Len(t, full.Jobs, 1)
					compact, err := pgjobdb.ListJobSummaries(ctx, db, opts)
					require.NoError(t, err)
					require.Len(t, compact.Jobs, 1)
					for _, got := range []time.Time{detail.AvailableAt, detail.CreatedAt, *detail.ExpiresAt, *detail.LeaseExpiresAt, status.CreatedAt, full.Jobs[0].AvailableAt, compact.Jobs[0].AvailableAt, compact.Jobs[0].CreatedAt, *compact.Jobs[0].ExpiresAt, *compact.Jobs[0].LeaseExpiresAt} {
						require.True(t, at.Equal(got), "%s != %s", got, at)
						require.Same(t, time.UTC, got.Location())
					}
					if i == 2 {
						require.Equal(t, at, *detail.ArchivedAt)
						require.Equal(t, at, *status.ArchivedAt)
						require.Equal(t, at, *compact.Jobs[0].ArchivedAt)
					}
					var configured string
					require.NoError(t, db.QueryRowContext(ctx, `SHOW TimeZone`).Scan(&configured))
					require.Equal(t, zone, configured)
					_, err = db.ExecContext(ctx, `SELECT * FROM pgjobdb.list_native_jobs(NULL,NULL,NULL,NULL,'[]','[]',NULL,false,'[]',NULL,NULL,NULL,NULL,NULL,0,'[]',clock_timestamp(),false,NULL,NULL)`)
					require.Error(t, err)
					require.NoError(t, db.QueryRowContext(ctx, `SHOW TimeZone`).Scan(&configured))
					require.Equal(t, zone, configured)
				})
			}
			if i != 2 {
				// Move only the fixture's admission fields before archiving a
				// future endpoint, then restore the exact final read values.
				_, err = db.ExecContext(ctx, `UPDATE pgjobdb.jobs SET available_at=clock_timestamp()-interval '1 hour',lease_expires_at='-infinity' WHERE job_id=$1`, id)
				require.NoError(t, err)
				require.NoError(t, pgjobdb.CompleteUnheldJob(ctx, db, "tenant", id, "worker", pgjobdb.Completion{Status: pgjobdb.CompletionSuccess}))
				_, err = db.ExecContext(ctx, `UPDATE pgjobdb.jobs_archive SET archived_at=$2,final_lease_expires_at=$2,final_available_at=$2 WHERE job_id=$1`, id, at)
				require.NoError(t, err)
				for _, zone := range []string{"UTC", "Etc/GMT-14", "Etc/GMT+12", "Europe/Paris"} {
					_, err = db.ExecContext(ctx, `SELECT set_config('TimeZone',$1,false)`, zone)
					require.NoError(t, err)
					row, err := pgjobdb.GetJob(ctx, db, "tenant", id)
					require.NoError(t, err)
					require.Equal(t, at, *row.ArchivedAt)
					require.Equal(t, at, row.AvailableAt)
					status, err := pgjobdb.GetJobStatus(ctx, db, "tenant", id)
					require.NoError(t, err)
					require.Equal(t, at, *status.ArchivedAt)
					opts := pgjobdb.ListJobsOptions{TenantIDs: []pgjobdb.TenantID{"tenant"}, JobKeys: []pgjobdb.JobKey{{TenantID: "tenant", JobID: id}}}
					full, err := pgjobdb.ListJobs(ctx, db, opts)
					require.NoError(t, err)
					require.Len(t, full.Jobs, 1)
					require.Equal(t, at, *full.Jobs[0].ArchivedAt)
					compact, err := pgjobdb.ListJobSummaries(ctx, db, opts)
					require.NoError(t, err)
					require.Len(t, compact.Jobs, 1)
					require.Equal(t, at, *compact.Jobs[0].ArchivedAt)
					require.Equal(t, at, compact.Jobs[0].AvailableAt)
				}
			}

		}
		require.NoError(t, (installer.Installer{DB: db}).Apply(ctx))
		require.NoError(t, (installer.Installer{DB: db}).Apply(ctx))
		require.NoError(t, (installer.Installer{DB: db}).Verify(ctx))
	})
}

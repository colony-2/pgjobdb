package integration_test

import (
	"context"
	"database/sql"
	"testing"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/pgjobdb/pkg/pgjobdb"
	"github.com/stretchr/testify/require"
)

func TestCellLookupStorageAuthority(t *testing.T) {
	runDatabaseTest(t, func(admin context.Context, db *sql.DB) {
		const tenant = "lookup"
		_, err := pgjobdb.CreateCell(admin, db, jobdb.CreateCellRequest{TenantID: tenant, Name: "A", Repo: "example.test/a"})
		require.NoError(t, err)
		b, err := pgjobdb.CreateCell(admin, db, jobdb.CreateCellRequest{TenantID: tenant, Name: "B", Repo: "example.test/b"})
		require.NoError(t, err)
		_, err = pgjobdb.SubmitJob(admin, db, pgjobdb.SubmitJobRequest{TenantID: tenant, JobID: "parent", WorkerID: "submitter", JobType: "recipe", Cell: "A"})
		require.NoError(t, err)
		selector := pgjobdb.WorkSelector{JobTypes: []pgjobdb.JobType{"recipe"}}
		lease, err := pgjobdb.GetJobLease(admin, db, tenant, "parent", "worker", selector, pgjobdb.GetJobLeaseOptions{})
		require.NoError(t, err)
		require.NotNil(t, lease)
		// Fail even on an attempted no-op write, including cursor/replay upserts.
		_, err = db.Exec(`CREATE FUNCTION pgjobdb.reject_lookup_write() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'lookup wrote registry'; END $$`)
		require.NoError(t, err)
		for _, table := range []string{"cells", "cell_names", "cell_catalog", "cell_requests"} {
			_, err = db.Exec(`CREATE TRIGGER reject_lookup_write BEFORE INSERT OR UPDATE OR DELETE ON pgjobdb.` + table + ` FOR EACH STATEMENT EXECUTE FUNCTION pgjobdb.reject_lookup_write()`)
			require.NoError(t, err)
		}
		ctx := context.Background()
		key := jobdb.JobKey{TenantId: tenant, JobId: "parent"}
		got, err := pgjobdb.LookupCellWithLeaseByID(ctx, db, key, lease.LeaseID, "worker", "B")
		require.NoError(t, err)
		require.Equal(t, b.Resolved(), got)
		for _, tc := range []struct {
			key           jobdb.JobKey
			lease, worker string
		}{
			{jobdb.JobKey{TenantId: "other", JobId: key.JobId}, lease.LeaseID, "worker"},
			{jobdb.JobKey{TenantId: tenant, JobId: "other"}, lease.LeaseID, "worker"},
			{key, "forged", "worker"},
			{key, lease.LeaseID, "forged"},
		} {
			_, err = pgjobdb.LookupCellWithLeaseByID(ctx, db, tc.key, tc.lease, tc.worker, "B")
			require.ErrorIs(t, err, jobdb.ErrExecutionLeaseLost)
		}
		tx, err := db.BeginTx(ctx, nil)
		require.NoError(t, err)
		defer tx.Rollback()
		_, err = pgjobdb.LookupCellWithLeaseByID(ctx, tx, key, lease.LeaseID, "worker", "a/b")
		require.ErrorIs(t, err, jobdb.ErrInvalidCell)
		got, err = pgjobdb.LookupCellWithLeaseByID(ctx, tx, key, lease.LeaseID, "worker", "B")
		require.NoError(t, err, "a failed lookup must not poison a borrowed transaction")
		require.Equal(t, b.Resolved(), got)
		require.NoError(t, tx.Commit())
		_, err = db.Exec(`UPDATE pgjobdb.jobs SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE tenant_id=$1 AND job_id=$2`, tenant, key.JobId)
		require.NoError(t, err)
		_, err = pgjobdb.LookupCellWithLeaseByID(ctx, db, key, lease.LeaseID, "worker", "B")
		require.ErrorIs(t, err, jobdb.ErrExecutionLeaseLost)
		next, err := pgjobdb.GetJobLease(admin, db, tenant, "parent", "next", selector, pgjobdb.GetJobLeaseOptions{})
		require.NoError(t, err)
		require.NotNil(t, next)
		require.NotEqual(t, lease.LeaseID, next.LeaseID)
		_, err = pgjobdb.LookupCellWithLeaseByID(ctx, db, key, lease.LeaseID, "worker", "B")
		require.ErrorIs(t, err, jobdb.ErrExecutionLeaseLost)
		got, err = pgjobdb.LookupCellWithLeaseByID(ctx, db, key, next.LeaseID, "next", "B")
		require.NoError(t, err)
		require.Equal(t, b.Resolved(), got)
	})
}

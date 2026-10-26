package installer_test

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/colony-2/pgjobdb/pkg/pgjobdb"
	"github.com/stretchr/testify/require"
)

func TestNativeAvailableRoutesAtTime(t *testing.T) {
	runDatabaseTest(t, func(ctx context.Context, db *sql.DB) {
		expiry := time.Now().Add(3 * time.Hour)
		_, err := pgjobdb.SubmitJob(ctx, db, pgjobdb.SubmitJobRequest{TenantID: "tenant", JobID: "task", WorkerID: "submit", JobType: "recipe", ExpiresAt: &expiry})
		require.NoError(t, err)
		lease, err := pgjobdb.GetJobLease(ctx, db, "tenant", "task", "worker", pgjobdb.WorkSelector{JobTypes: []pgjobdb.JobType{"recipe"}}, pgjobdb.GetJobLeaseOptions{})
		require.NoError(t, err)
		require.NotNil(t, lease)
		task := pgjobdb.TaskWork{TaskType: "input", ResumeJobType: "recipe", InputOrdinal: 0, OutputOrdinal: 1, InputHash: "input"}
		require.NoError(t, lease.Reschedule(ctx, db, pgjobdb.RescheduleRequest{RouteJobType: "recipe", WorkKind: pgjobdb.WorkKindTask, Task: &task, Alternate: &pgjobdb.AlternateRoute{JobType: "resume", After: time.Hour}}))
		before, err := pgjobdb.GetJob(ctx, db, "tenant", "task")
		require.NoError(t, err)
		require.NotNil(t, before.AlternateAt)
		list := func(at time.Time, route pgjobdb.Route, expected int) {
			page, err := pgjobdb.ListJobs(ctx, db, pgjobdb.ListJobsOptions{TenantIDs: []pgjobdb.TenantID{"tenant"}, AvailableRoutes: []pgjobdb.Route{route}, Now: at, Statuses: []pgjobdb.JobStatus{pgjobdb.JobStatusReady}})
			require.NoError(t, err)
			require.Len(t, page.Jobs, expected)
			if expected > 0 {
				require.Equal(t, pgjobdb.TaskType("input"), page.Jobs[0].TaskType)
				require.Equal(t, pgjobdb.JobType("recipe"), page.Jobs[0].RouteJobType)
			}
		}
		primary := pgjobdb.Route{JobType: "recipe", TaskType: "input"}
		alternate := pgjobdb.Route{JobType: "resume"}
		list(before.AlternateAt.Add(-time.Microsecond), primary, 1)
		list(before.AlternateAt.Add(-time.Microsecond), alternate, 0)
		list(*before.AlternateAt, primary, 0)
		list(*before.AlternateAt, alternate, 1)
		list(expiry.Add(time.Second), alternate, 0)
		after, err := pgjobdb.GetJob(ctx, db, "tenant", "task")
		require.NoError(t, err)
		require.Equal(t, before, after, "listing must not promote the alternate or acquire a lease")
	})
}

func TestNativeAvailableRouteCursor(t *testing.T) {
	runDatabaseTest(t, func(ctx context.Context, db *sql.DB) {
		for _, id := range []pgjobdb.JobID{"one", "two"} {
			_, err := pgjobdb.SubmitJob(ctx, db, pgjobdb.SubmitJobRequest{TenantID: "tenant", JobID: id, WorkerID: "submit", JobType: pgjobdb.JobType(id)})
			require.NoError(t, err)
		}
		opts := pgjobdb.ListJobsOptions{TenantIDs: []pgjobdb.TenantID{"tenant"}, AvailableRoutes: []pgjobdb.Route{{JobType: "one"}, {JobType: "two"}}, PageSize: 1}
		first, err := pgjobdb.ListJobs(ctx, db, opts)
		require.NoError(t, err)
		require.Len(t, first.Jobs, 1)
		require.NotEmpty(t, first.NextPageToken)
		opts.PageToken = first.NextPageToken
		opts.Now = time.Now().Add(time.Hour)
		opts.AvailableRoutes = []pgjobdb.Route{{JobType: "two"}, {JobType: "one"}, {JobType: "two"}}
		second, err := pgjobdb.ListJobs(ctx, db, opts)
		require.NoError(t, err)
		require.Len(t, second.Jobs, 1)
		require.NotEqual(t, first.Jobs[0].JobID, second.Jobs[0].JobID)
		require.Empty(t, second.NextPageToken)
		opts.AvailableRoutes = []pgjobdb.Route{{JobType: "one"}}
		_, err = pgjobdb.ListJobs(ctx, db, opts)
		require.ErrorContains(t, err, "page token")
	})
}

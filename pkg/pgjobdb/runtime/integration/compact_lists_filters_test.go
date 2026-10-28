package integration_test

import (
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

func TestCompactListFiltersAndReadiness(t *testing.T) {
	runDatabaseTest(t, func(ctx context.Context, db *sql.DB) {
		producer := nativeGoalJob(t, ctx, db, "producer")
		for _, id := range []string{"ready", "child", "active", "legacy", "cancelled", "hidden", "archived", "task", "pending", "future", "expired", "crashed"} {
			nativeGoalJob(t, ctx, db, id)
		}
		nativeGoalLease(t, ctx, db, "active")
		require.NoError(t, pgjobdb.RescheduleUnheldJob(ctx, db, "tenant", "legacy", "test", pgjobdb.RescheduleRequest{RouteJobType: "work", WorkKind: pgjobdb.WorkKindJob, WaitFor: []pgjobdb.JobID{"producer"}}))
		require.NoError(t, pgjobdb.RescheduleUnheldJob(ctx, db, "tenant", "pending", "test", pgjobdb.RescheduleRequest{RouteJobType: "work", WorkKind: pgjobdb.WorkKindJob, DependencyWait: nativeGoalWait("pending", producer.GoalID)}))
		require.NoError(t, pgjobdb.RescheduleUnheldJob(ctx, db, "tenant", "task", "test", pgjobdb.RescheduleRequest{RouteJobType: "routed", WorkKind: pgjobdb.WorkKindTask, Task: &pgjobdb.TaskWork{TaskType: "input", ResumeJobType: "work", InputOrdinal: 1, OutputOrdinal: 2, InputHash: "hash"}}))
		require.NoError(t, pgjobdb.CompleteUnheldJob(ctx, db, "tenant", "archived", "test", pgjobdb.Completion{Status: pgjobdb.CompletionFailedApp, Detail: "diagnostic text"}))
		require.NoError(t, pgjobdb.CancelJob(ctx, db, pgjobdb.CancelJobRequest{TenantID: "tenant", JobID: "cancelled", WorkerID: "test"}))
		require.NoError(t, pgjobdb.CancelJob(ctx, db, pgjobdb.CancelJobRequest{TenantID: "tenant", JobID: "hidden", WorkerID: "test"}))
		_, err := pgjobdb.DeleteJobs(ctx, db, pgjobdb.DeleteJobsRequest{TenantID: "tenant", JobIDs: []pgjobdb.JobID{"hidden"}})
		require.NoError(t, err)
		// Fixed timestamps make strict creation bounds and cursor ties reproducible.
		_, err = db.Exec(`UPDATE pgjobdb.job_facts SET created_at='2026-01-01',app_metadata='{"team":"blue","nested":{"n":3}}';
   UPDATE pgjobdb.job_facts SET parent_job_id='producer' WHERE job_id='child';
   UPDATE pgjobdb.jobs SET available_at=clock_timestamp()+interval '1 hour' WHERE job_id='future';
   UPDATE pgjobdb.jobs SET expires_at=clock_timestamp()-interval '1 hour' WHERE job_id='expired';
   UPDATE pgjobdb.jobs SET consecutive_expirations=10000 WHERE job_id='crashed'`)
		require.NoError(t, err)
		created := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
		before, after := created.Add(-time.Second), created.Add(time.Second)
		cases := map[string]pgjobdb.ListJobsOptions{
			"all": {}, "hidden": {IncludeHidden: true},
			"active-store":  {Stores: []pgjobdb.JobStore{pgjobdb.JobStoreActive}},
			"archive-store": {Stores: []pgjobdb.JobStore{pgjobdb.JobStoreArchived}},
			"parents":       {ParentJobIDs: []pgjobdb.JobID{"producer"}}, "roots": {RootOnly: true},
			"types":            {JobTypes: []pgjobdb.JobType{"work"}},
			"task":             {TaskSelectors: []pgjobdb.TaskSelector{{JobType: "routed", TaskType: "input"}}},
			"task-or-type":     {JobTypes: []pgjobdb.JobType{"absent"}, TaskSelectors: []pgjobdb.TaskSelector{{JobType: "routed", TaskType: "input"}}},
			"keys":             {JobKeys: []pgjobdb.JobKey{{TenantID: "tenant", JobID: "child"}, {TenantID: "other", JobID: "producer"}}},
			"metadata":         {MetadataPredicates: []pgjobdb.MetadataPredicate{{Path: []string{"nested", "n"}, Values: []json.RawMessage{json.RawMessage(`3`)}}}},
			"metadata-missing": {MetadataPredicates: []pgjobdb.MetadataPredicate{{Path: []string{"missing"}, Values: []json.RawMessage{json.RawMessage(`null`)}}}},
			"dates":            {CreatedAfter: &before, CreatedBefore: &after},
			"strict-after":     {CreatedAfter: &created}, "strict-before": {CreatedBefore: &created},
			"goal":   {GoalIDs: []string{producer.GoalID}},
			"routes": {AvailableRoutes: []pgjobdb.Route{{JobType: "work"}, {JobType: "routed", TaskType: "input"}}},
		}
		for _, status := range []pgjobdb.JobStatus{pgjobdb.JobStatusReady, pgjobdb.JobStatusActive, pgjobdb.JobStatusPendingJobs, pgjobdb.JobStatusCancelled, pgjobdb.JobStatusCompleted, pgjobdb.JobStatusAwaitingFuture, pgjobdb.JobStatusExpired, pgjobdb.JobStatusCrashConcern} {
			cases[string(status)] = pgjobdb.ListJobsOptions{Statuses: []pgjobdb.JobStatus{status}}
		}
		for name, opts := range cases {
			t.Run(name, func(t *testing.T) {
				opts.TenantIDs = []pgjobdb.TenantID{"tenant"}
				opts.PageSize = 2
				for {
					expected, err := pgjobdb.ListJobs(ctx, db, opts)
					require.NoError(t, err)
					actual, err := pgjobdb.ListJobSummaries(ctx, db, opts)
					require.NoError(t, err)
					require.Equal(t, expected.NextPageToken, actual.NextPageToken)
					require.Len(t, actual.Jobs, len(expected.Jobs))
					for i, j := range actual.Jobs {
						e := expected.Jobs[i]
						require.Equal(t, e.JobID, j.JobID)
						require.Equal(t, e.Status, j.Status)
						require.Equal(t, e.CompletionStatus, j.CompletionStatus)
						require.Equal(t, e.RouteJobType, j.RouteJobType)
						require.Equal(t, e.TaskType, j.TaskType)
						require.Equal(t, e.ParentJobID, j.ParentJobID)
						require.JSONEq(t, string(e.AppMetadata), string(j.AppMetadata))
						if j.JobID == "legacy" {
							require.Nil(t, j.BlockingWaitSummary)
							require.Equal(t, pgjobdb.JobStatusPendingJobs, j.Status)
						}
						if j.CancelRequested || j.Store == pgjobdb.JobStoreArchived {
							require.Nil(t, j.BlockingWaitSummary)
						}
					}
					if actual.NextPageToken == "" {
						break
					}
					opts.PageToken = actual.NextPageToken
				}
			})
		}
		// Eligibility agrees with actual leasing, including legacy prerequisites,
		// an active lease, cancellation, and a stored task route.
		ready, err := pgjobdb.ListJobSummaries(ctx, db, pgjobdb.ListJobsOptions{TenantIDs: []pgjobdb.TenantID{"tenant"}, AvailableRoutes: cases["routes"].AvailableRoutes})
		require.NoError(t, err)
		eligible := map[pgjobdb.JobID]bool{}
		for _, j := range ready.Jobs {
			eligible[j.JobID] = true
		}
		all, err := pgjobdb.ListJobSummaries(ctx, db, pgjobdb.ListJobsOptions{TenantIDs: []pgjobdb.TenantID{"tenant"}})
		require.NoError(t, err)
		for _, j := range all.Jobs {
			l, err := pgjobdb.GetJobLease(ctx, db, "tenant", j.JobID, "worker", pgjobdb.WorkSelector{JobTypes: []pgjobdb.JobType{"work"}, Tasks: []pgjobdb.TaskSelector{{JobType: "routed", TaskType: "input"}}}, pgjobdb.GetJobLeaseOptions{LeaseDuration: time.Minute})
			require.NoError(t, err)
			require.Equal(t, eligible[j.JobID], l != nil, "job %s", j.JobID)
		}
	})
}

func TestCompactListExistingInstallation(t *testing.T) {
	runDatabaseTest(t, func(ctx context.Context, db *sql.DB) {
		producer := nativeGoalJob(t, ctx, db, "producer")
		nativeGoalJob(t, ctx, db, "consumer")
		deadline := time.Now().UTC().Add(time.Hour).Truncate(time.Second).Add(123456789 * time.Nanosecond)
		wait := nativeGoalWait("precise", producer.GoalID)
		wait.Deadline = &deadline
		require.NoError(t, pgjobdb.RescheduleUnheldJob(ctx, db, "tenant", "consumer", "test", pgjobdb.RescheduleRequest{RouteJobType: "work", WorkKind: pgjobdb.WorkKindJob, DependencyWait: wait}))
		// Simulate format 7 before the compact scalar existed, then reinstall twice.
		_, err := db.Exec(`ALTER TABLE pgjobdb.dependency_waits DROP COLUMN deadline_exact`)
		require.NoError(t, err)
		install := installer.Installer{DB: db}
		require.Error(t, install.Verify(ctx))
		for range 2 {
			require.NoError(t, install.Apply(ctx))
			require.NoError(t, install.Verify(ctx))
			page, err := pgjobdb.ListJobSummaries(ctx, db, pgjobdb.ListJobsOptions{TenantIDs: []pgjobdb.TenantID{"tenant"}, JobKeys: []pgjobdb.JobKey{{TenantID: "tenant", JobID: "consumer"}}})
			require.NoError(t, err)
			require.Len(t, page.Jobs, 1)
			require.Equal(t, &jobdb.DependencyWaitSummary{WaitID: "precise", Deadline: &deadline}, page.Jobs[0].BlockingWaitSummary)
			detail, err := pgjobdb.BlockingDependencyWait(ctx, db, jobdb.JobKey{TenantId: "tenant", JobId: "consumer"})
			require.NoError(t, err)
			require.Equal(t, &deadline, detail.Deadline)
			require.Len(t, detail.Dependencies, 1)
		}
	})
}

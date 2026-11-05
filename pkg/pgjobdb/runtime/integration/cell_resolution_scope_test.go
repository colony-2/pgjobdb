package integration_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/remote"
	"github.com/stretchr/testify/require"
)

// Unrestricted owner leases cover all tenant cells. Explicit principal and
// delegation lists narrow that scope without an exemption for the owning cell.
func TestRemoteLeaseCellResolutionScope(t *testing.T) {
	backend := conformanceRuntimeFactory(t)(t)
	admin := celltest.Context(context.Background())
	ctx := context.Background()
	cells := map[string]jobdb.Cell{}
	for _, name := range []string{"gitvend", "c2j", "root"} {
		c, err := backend.CreateCell(admin, jobdb.CreateCellRequest{TenantID: "c2", Name: name, Repo: "github.com/colony-2/" + name})
		require.NoError(t, err)
		cells[name] = c
	}
	var mode atomic.Int32
	handler := remote.NewServer(backend)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		a, _ := jobdb.CellAuthorityFromContext(admin)
		m := mode.Load()
		if m == 1 {
			a.Cells = map[string]bool{cells["c2j"].ID: true}
		}
		if m == 2 {
			a.Cells = map[string]bool{}
		}
		if m == 3 {
			delete(a.Permissions, "jobs.submit")
		}
		handler.ServeHTTP(w, r.WithContext(jobdb.WithCellAuthority(r.Context(), a)))
	}))
	t.Cleanup(server.Close)
	client, err := remote.New(server.URL, server.Client())
	require.NoError(t, err)
	job, err := client.SubmitJob(ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{
		TenantId: "c2", Cell: "gitvend", JobID: "original", JobType: "recipe",
		Data: jobdb.NewTaskDataOrPanic(map[string]string{"job_id": "original"}),
	}})
	require.NoError(t, err)
	acquire := func(key jobdb.JobKey) jobdb.ExecutionLease {
		l, err := client.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: key, WorkerID: "worker", Routes: []jobdb.Route{{JobType: "recipe"}}})
		require.NoError(t, err)
		require.NotNil(t, l)
		return l
	}
	check := func(t *testing.T, lease jobdb.ExecutionLease) {
		mode.Store(0)
		own, err := lease.ResolveCell(ctx, "gitvend")
		require.NoError(t, err)
		require.Equal(t, cells["gitvend"].Resolved(), own)
		for _, name := range []string{"c2j", "root"} {
			got, err := lease.ResolveCell(ctx, name)
			require.NoError(t, err)
			require.Equal(t, cells[name].Resolved(), got)
			child, err := lease.SubmitJob(ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "c2", Cell: name, JobType: "child", Data: jobdb.NewTaskDataOrPanic(1)}})
			require.NoError(t, err)
			require.Equal(t, cells[name].ID, child.CellID)
		}
		mode.Store(1)
		_, err = lease.ResolveCell(ctx, "root")
		require.ErrorIs(t, err, jobdb.ErrCellNotFound)
		_, err = lease.ResolveCell(ctx, "gitvend")
		require.ErrorIs(t, err, jobdb.ErrCellNotFound, "the owning cell is not exempt from an explicit list")
		_, err = lease.ResolveCell(ctx, "c2j")
		require.NoError(t, err)
		mode.Store(2)
		_, err = lease.ResolveCell(ctx, "c2j")
		require.ErrorIs(t, err, jobdb.ErrCellNotFound)
		mode.Store(3)
		_, err = lease.ResolveCell(ctx, "c2j")
		require.ErrorIs(t, err, jobdb.ErrCellForbidden)
		mode.Store(0)
	}
	owner := acquire(job.JobKey)
	t.Run("owner", func(t *testing.T) { check(t, owner) })
	capability, err := remote.ExportLease(owner)
	require.NoError(t, err)
	supplied, err := client.ImportLease(ctx, capability)
	require.NoError(t, err)
	t.Run("supplied owner", func(t *testing.T) { check(t, supplied) })
	delegated, err := client.CreateDelegation(ctx, supplied, jobdb.CreateDelegationRequest{Permissions: jobdb.DelegationPermissions{
		Submit: &jobdb.DelegationSubmitPermission{JobTypes: jobdb.DelegationNames{Any: true}, JobIDs: jobdb.DelegationNames{Any: true}, CellNames: &jobdb.DelegationNames{Names: []string{"c2j"}}},
	}})
	require.NoError(t, err)
	_, err = delegated.ResolveCell(ctx, "c2j")
	require.NoError(t, err)
	_, err = delegated.ResolveCell(ctx, "root")
	require.ErrorContains(t, err, "lease_permission_denied")
	mode.Store(2) // A delegation cannot override an explicit principal restriction.
	_, err = delegated.ResolveCell(ctx, "c2j")
	require.ErrorIs(t, err, jobdb.ErrCellNotFound)
	mode.Store(0)
	chapter := jobdb.Chapter{Ordinal: 1, TaskType: "recipe", CreatedAt: time.Now().UTC(), Body: jobdb.JobAttemptOutcomeChapter{
		Outcome: jobdb.AppErrorOutcome{Error: jobdb.AppErrorPayload{Message: "restart fixture"}},
	}}
	require.NoError(t, supplied.Complete(ctx, jobdb.CompleteExecutionRequest{Status: "failed_app", Chapter: &chapter}))
	restarted, err := client.SubmitRestartJob(ctx, jobdb.SubmitRestartJobRequest{Job: jobdb.SubmitRestartJob{
		PriorJobKey: job.JobKey, Cell: "gitvend", JobID: "restarted", LastStepToKeep: 0, ShareGoal: true,
	}})
	require.NoError(t, err)
	require.Equal(t, job.GoalID, restarted.GoalID)
	oldChapter, err := client.GetChapter(ctx, jobdb.ChapterRef{JobKey: job.JobKey, Ordinal: 0})
	require.NoError(t, err)
	newChapter, err := client.GetChapter(ctx, jobdb.ChapterRef{JobKey: restarted.JobKey, Ordinal: 0})
	require.NoError(t, err)
	require.Equal(t, oldChapter.Body, newChapter.Body)
	restartedLease := acquire(restarted.JobKey)
	require.Equal(t, "restarted", restartedLease.Job().JobKey.JobId)
	t.Run("restarted with copied input", func(t *testing.T) { check(t, restartedLease) })
}

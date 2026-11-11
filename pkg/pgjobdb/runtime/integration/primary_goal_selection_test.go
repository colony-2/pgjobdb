package integration_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	remoteruntime "github.com/colony-2/jobdb/pkg/jobdb/runtime/remote"
	"github.com/colony-2/jobdb/pkg/jobdb/runtimetest"
	"github.com/stretchr/testify/require"
)

// The host, not the JSON request, supplies verified goal access and selection.
func selectionTestHandler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := runtimetest.PrimaryGoalContext(r.Context())
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func TestPostgresPrimaryGoalSelection(t *testing.T) {
	factory := conformanceRuntimeFactory(t)
	for _, remote := range []bool{false, true} {
		name := "direct"
		if remote {
			name = "remote"
		}
		t.Run(name, func(t *testing.T) {
			rt := factory(t)
			var workflow jobdb.WorkflowRuntime = rt
			if remote {
				server := httptest.NewServer(selectionTestHandler(remoteruntime.NewServer(rt)))
				defer server.Close()
				client, err := remoteruntime.New(server.URL, server.Client())
				require.NoError(t, err)
				workflow = client
			}
			runtimetest.RunPrimaryGoalSelectionConformance(t, workflow)
		})
	}
}

func TestPostgresPrimaryGoalBoundaries(t *testing.T) {
	factory := conformanceRuntimeFactory(t)
	runtimetest.RunPrimaryGoalSelectionBoundaries(t, factory(t))
}

func TestPostgresSelectionDeniedWithoutTrustedHook(t *testing.T) {
	factory := conformanceRuntimeFactory(t)
	rt := factory(t)
	ctx := celltest.Context(context.Background())
	source, err := celltest.Submit(rt, ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "authority", JobID: "source", Cell: "test", CellRepo: "example.test/test", JobType: "work", Data: jobdb.NewTaskDataOrPanic(nil)}})
	require.NoError(t, err)
	_, err = rt.SubmitJob(ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "authority", JobID: "selected", GoalID: source.GoalID, Cell: source.Cell, CellRepo: source.CellRepo, JobType: "work", Data: jobdb.NewTaskDataOrPanic(nil)}})
	require.ErrorIs(t, err, jobdb.ErrPrimaryGoalSelectionForbidden)
	_, err = rt.GetJob(ctx, jobdb.JobKey{TenantId: "authority", JobId: "selected"})
	require.ErrorIs(t, err, jobdb.ErrJobNotFound)
}

func TestPostgresSelectedConcurrentRequests(t *testing.T) {
	factory := conformanceRuntimeFactory(t)
	rt := factory(t)
	ctx := runtimetest.PrimaryGoalContext(context.Background())
	source, err := celltest.Submit(rt, ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "parallel", JobID: "source", Cell: "test", CellRepo: "example.test/test", JobType: "work", Data: jobdb.NewTaskDataOrPanic(nil)}})
	require.NoError(t, err)
	req := jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "parallel", JobID: "selected", GoalID: source.GoalID, Cell: source.Cell, CellRepo: source.CellRepo, JobType: "work", Data: jobdb.NewTaskDataOrPanic("same")}}
	type result struct {
		h   jobdb.JobHandle
		err error
	}
	results := make(chan result, 8)
	start := make(chan struct{})
	for i := 0; i < 8; i++ {
		go func() { <-start; h, e := rt.SubmitJob(ctx, req); results <- result{h, e} }()
	}
	close(start)
	var accepted jobdb.JobHandle
	for i := 0; i < 8; i++ {
		r := <-results
		require.NoError(t, r.err)
		if i == 0 {
			accepted = r.h
		}
		require.Equal(t, accepted, r.h)
	}
	changed := req
	changed.Job.Data = jobdb.NewTaskDataOrPanic("changed")
	_, err = rt.SubmitJob(ctx, changed)
	require.ErrorIs(t, err, jobdb.ErrConflict)
	competing := req
	competing.Job.JobID = "competing"
	alternative := competing
	alternative.Job.Data = jobdb.NewTaskDataOrPanic("alternative")
	begin := make(chan struct{})
	for _, r := range []jobdb.SubmitJobRequest{competing, alternative} {
		go func(r jobdb.SubmitJobRequest) { <-begin; h, e := rt.SubmitJob(ctx, r); results <- result{h, e} }(r)
	}
	close(begin)
	winners, conflicts := 0, 0
	for i := 0; i < 2; i++ {
		r := <-results
		if r.err == nil {
			winners++
		} else {
			require.ErrorIs(t, r.err, jobdb.ErrConflict)
			conflicts++
		}
	}
	require.Equal(t, 1, winners)
	require.Equal(t, 1, conflicts)
	listing, err := rt.ListJobs(ctx, jobdb.ListJobsRequest{TenantIds: []string{"parallel"}})
	require.NoError(t, err)
	require.Len(t, listing.Jobs, 3)
}

package integration_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	remoteruntime "github.com/colony-2/jobdb/pkg/jobdb/runtime/remote"
	"github.com/colony-2/jobdb/pkg/jobdb/runtimetest"
	"github.com/stretchr/testify/require"
)

func TestPostgresSelectedTransportRefusesUnsafeRoutes(t *testing.T) {
	factory := conformanceRuntimeFactory(t)
	rt := factory(t)
	ctx := runtimetest.PrimaryGoalContext(context.Background())
	source, err := celltest.Submit(rt, ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "transport", JobID: "source", Cell: "test", CellRepo: "example.test/test", JobType: "work", Data: jobdb.NewTaskDataOrPanic(nil)}})
	require.NoError(t, err)
	application := selectionTestHandler(remoteruntime.NewServer(rt))
	for _, mode := range []string{"unknown", "unsupported", "route404", "route405", "redirect307", "redirect308", "legacy"} {
		t.Run(mode, func(t *testing.T) {
			var ordinary, selected atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/v1/capabilities/primary-goal-selection" {
					if mode == "unknown" || mode == "legacy" {
						http.NotFound(w, r)
						return
					}
					if mode == "unsupported" {
						w.Header().Set("Content-Type", "application/json")
						fmt.Fprint(w, `{"supported":false}`)
						return
					}
					application.ServeHTTP(w, r)
					return
				}
				if strings.HasSuffix(r.URL.Path, "/primary-goal") {
					selected.Add(1)
					code := http.StatusNotFound
					switch mode {
					case "route405":
						code = 405
					case "redirect307":
						code = 307
					case "redirect308":
						code = 308
					}
					w.Header().Set("Location", "/v1/tenants/transport/jobs/"+mode)
					w.WriteHeader(code)
					return
				}
				if r.Method == http.MethodPost || r.Method == http.MethodPut {
					ordinary.Add(1)
					if mode == "legacy" {
						w.Header().Set("Content-Type", "application/json")
						fmt.Fprint(w, `{}`)
						return
					}
				}
				application.ServeHTTP(w, r)
			}))
			defer server.Close()
			remote, err := remoteruntime.New(server.URL, server.Client())
			require.NoError(t, err)
			req := jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "transport", JobID: mode, GoalID: source.GoalID, Cell: source.Cell, CellRepo: source.CellRepo, JobType: "work", Data: jobdb.NewTaskDataOrPanic(nil)}}
			_, err = remote.SubmitJob(ctx, req)
			require.ErrorIs(t, err, jobdb.ErrPrimaryGoalSelectionUnsupported)
			require.Zero(t, ordinary.Load())
			if mode == "unknown" || mode == "unsupported" || mode == "legacy" {
				require.Zero(t, selected.Load())
			}
			_, err = rt.GetJob(ctx, jobdb.JobKey{TenantId: "transport", JobId: mode})
			require.ErrorIs(t, err, jobdb.ErrJobNotFound)
		})
	}
	jobs, err := rt.ListJobs(ctx, jobdb.ListJobsRequest{TenantIds: []string{"transport"}})
	require.NoError(t, err)
	require.Len(t, jobs.Jobs, 1)
	goals, err := rt.ListGoals(ctx, jobdb.ListGoalsRequest{TenantId: "transport"})
	require.NoError(t, err)
	require.Len(t, goals.Goals, 1)
}

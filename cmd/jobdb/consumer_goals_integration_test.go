//go:build integration

package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/remote"
	"github.com/colony-2/jobdb/pkg/jobdb/runtimetest"
	pgruntime "github.com/colony-2/pgjobdb/pkg/pgjobdb/runtime"
	"github.com/stretchr/testify/require"
)

func TestPostgresConsumerGoalsCommands(t *testing.T) {
	embedded, err := pgruntime.StartEmbeddedRuntime(celltest.Context(context.Background()))
	require.NoError(t, err)
	defer embedded.Shutdown()
	rt := embedded.Runtime
	ctx := runtimetest.PrimaryGoalContext(context.Background())
	submit := func(id, goal string, deps ...jobdb.Dependency) jobdb.JobHandle {
		h, e := celltest.Submit(rt, ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", Cell: "test", CellRepo: "example.test/test", JobID: id, GoalID: goal, JobType: "work", Data: jobdb.NewTaskDataOrPanic(nil), Dependencies: deps}})
		require.NoError(t, e)
		return h
	}
	a := submit("a", "")
	b := submit("b", a.GoalID)
	other := submit("other", "")
	dep := jobdb.Dependency{Target: jobdb.DependencyTarget{GoalID: a.GoalID}, Condition: jobdb.JobPrereqSuccess}
	c := submit("consumer", "", dep)
	_ = submit("consumer-2", "", dep)
	t.Setenv("JOBDB_BEARER_TOKEN", "consumer-cli-token")
	for _, signed := range []bool{false, true} {
		t.Run(map[bool]string{false: "default", true: "signed"}[signed], func(t *testing.T) {
			var handler http.Handler = remote.NewConsumerServer(rt)
			if signed {
				handler, err = remote.NewConsumerServerWithOptions(rt, remote.ServerOptions{LeaseTokenSigningKey: []byte("cli-consumer-key-at-least-32-bytes-long")})
				require.NoError(t, err)
			}
			var routes []string
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				routes = append(routes, r.Method+" "+r.URL.Path)
				require.False(t, strings.Contains(r.URL.Path, "/candidates/"))
				require.Contains(t, r.URL.Path, "/tenants/tenant/")
				if r.Header.Get("Authorization") != "Bearer consumer-cli-token" {
					http.Error(w, "denied", http.StatusForbidden)
					return
				}
				administrativeCellIngress(handler).ServeHTTP(w, r)
			}))
			defer server.Close()
			run := func(args ...string) string {
				t.Helper()
				args = append([]string{"goals"}, args...)
				args = append(args, "--tenant", "tenant", "--url", server.URL)
				out, e := executeRoot(t, args...)
				require.NoError(t, e, "%v: %s", args, out)
				return out
			}
			var goal jobdb.GoalInfo
			require.NoError(t, json.Unmarshal([]byte(run("get", a.GoalID)), &goal))
			require.Equal(t, a.GoalID, goal.GoalKey.GoalID)
			var list jobdb.ListGoalsResponse
			require.NoError(t, json.Unmarshal([]byte(run("list", "--page-size", "1")), &list))
			require.Len(t, list.Goals, 1)
			require.NotEmpty(t, list.NextPageToken)
			require.NoError(t, json.Unmarshal([]byte(run("list", "--page-size", "1", "--page-token", list.NextPageToken)), &list))
			require.Len(t, list.Goals, 1)
			require.NoError(t, json.Unmarshal([]byte(run("list", "--candidate-job", b.JobKey.JobId, "--goal-id", a.GoalID)), &list))
			require.Len(t, list.Goals, 1)
			require.EqualValues(t, 2, list.Goals[0].CandidateCount)
			var jobs jobdb.ListJobsResponse
			require.NoError(t, json.Unmarshal([]byte(run("candidates", a.GoalID, "--page-size", "1", "--include-hidden")), &jobs))
			require.Len(t, jobs.Jobs, 1)
			require.NotEmpty(t, jobs.NextPageToken)
			require.NoError(t, json.Unmarshal([]byte(run("candidates", a.GoalID, "--page-size", "1", "--include-hidden", "--page-token", jobs.NextPageToken)), &jobs))
			require.Len(t, jobs.Jobs, 1)
			require.Equal(t, a.GoalID, jobs.Jobs[0].GoalID)
			var waits jobdb.ListDependencyWaitsResponse
			require.NoError(t, json.Unmarshal([]byte(run("waits", "--target-goal", a.GoalID, "--page-size", "1")), &waits))
			require.Len(t, waits.Waits, 1)
			require.NotEmpty(t, waits.NextPageToken)
			require.NoError(t, json.Unmarshal([]byte(run("waits", "--target-goal", a.GoalID, "--page-size", "1", "--page-token", waits.NextPageToken)), &waits))
			require.Len(t, waits.Waits, 1)
			failedGoal := other
			if signed {
				failedGoal = submit("signed-failure", "")
			}
			failureMessage := "No acceptable solution — reviewed"
			require.NoError(t, json.Unmarshal([]byte(run("fail", failedGoal.GoalID, "--message", failureMessage)), &goal))
			require.Equal(t, failureMessage, goal.Failure.Message)
			frozen, e := rt.GetGoal(ctx, jobdb.GoalKey{TenantId: "tenant", GoalID: failedGoal.GoalID})
			require.NoError(t, e)
			require.Equal(t, goal.Failure, frozen.Failure)
			before := len(routes)
			_, e = executeRoot(t, "goals", "fail", a.GoalID, "--message", " ", "--tenant", "tenant", "--url", server.URL)
			require.Error(t, e)
			require.Len(t, routes, before, "invalid message must fail locally")
			t.Setenv("JOBDB_BEARER_TOKEN", "denied")
			_, e = executeRoot(t, "goals", "get", a.GoalID, "--tenant", "tenant", "--url", server.URL)
			require.Error(t, e)
			_, e = executeRoot(t, "goals", "fail", failedGoal.GoalID, "--message", "denied mutation", "--tenant", "tenant", "--url", server.URL)
			require.Error(t, e)
			unchanged, e := rt.GetGoal(ctx, jobdb.GoalKey{TenantId: "tenant", GoalID: failedGoal.GoalID})
			require.NoError(t, e)
			require.Equal(t, frozen, unchanged)
			t.Setenv("JOBDB_BEARER_TOKEN", "consumer-cli-token")
			hidden := submit(map[bool]string{false: "hidden-default", true: "hidden-signed"}[signed], "")
			_, e = rt.DeleteJobs(ctx, jobdb.DeleteJobsRequest{TenantID: "tenant", JobIDs: []string{hidden.JobKey.JobId}})
			require.NoError(t, e)
			require.NoError(t, json.Unmarshal([]byte(run("candidates", hidden.GoalID)), &jobs))
			require.Empty(t, jobs.Jobs)
			require.NoError(t, json.Unmarshal([]byte(run("candidates", hidden.GoalID, "--include-hidden")), &jobs))
			require.Len(t, jobs.Jobs, 1)
			require.Equal(t, hidden.JobKey, jobs.Jobs[0].JobKey)
			require.Contains(t, routes, "GET /v1/tenants/tenant/goals/"+a.GoalID)
			require.Contains(t, routes, "POST /v1/tenants/tenant/goals/"+failedGoal.GoalID+"/fail")
			current, e := rt.GetJob(ctx, c.JobKey)
			require.NoError(t, e)
			require.Equal(t, jobdb.JobStatusPendingJobs, current.Status)
		})
	}
	// Failure transport through the same tree retains the frozen dependency result.
	_, err = rt.FailGoal(ctx, jobdb.FailGoalRequest{GoalKey: jobdb.GoalKey{TenantId: "tenant", GoalID: a.GoalID}, Message: "frozen failure"})
	require.NoError(t, err)
	server := httptest.NewServer(administrativeCellIngress(remote.NewConsumerServer(rt)))
	defer server.Close()
	output, err := executeRoot(t, "goals", "waits", "--consumer-job", c.JobKey.JobId, "--state", "FAILED", "--tenant", "tenant", "--url", server.URL)
	require.NoError(t, err)
	var waits jobdb.ListDependencyWaitsResponse
	require.NoError(t, json.Unmarshal([]byte(output), &waits))
	require.Len(t, waits.Waits, 1)
	require.ErrorIs(t, waits.Waits[0].Err(), jobdb.ErrGoalFailed)
	require.Equal(t, "frozen failure", waits.Waits[0].Failure.Message)
}

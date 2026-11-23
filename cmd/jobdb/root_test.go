package main

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/stretchr/testify/require"
)

func TestAdministrativeIngressAuthorizesPrimarySelection(t *testing.T) {
	called := false
	handler := administrativeCellIngress(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		ctx := r.Context()
		require.True(t, jobdb.HasPrimaryGoalSelectionAuthorizer(ctx))
		authority, ok := jobdb.CellAuthorityFromContext(ctx)
		require.True(t, ok)
		require.True(t, authority.Permissions["goals.select-primary"])
		require.NoError(t, jobdb.AuthorizePrimaryGoalSelection(ctx, jobdb.GoalKey{TenantId: "c2", GoalID: "goal"}, jobdb.ResolvedCell{CellID: "cell", Cell: "test", CellRepo: "example.test/test"}))
	}))
	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/", nil))
	require.True(t, called)
	// Untrusted request headers cannot install the context hook or permissions.
	request := httptest.NewRequest(http.MethodPost, "/", nil)
	request.Header.Set("goals.select-primary", "true")
	require.False(t, jobdb.HasPrimaryGoalSelectionAuthorizer(request.Context()))
	require.ErrorIs(t, jobdb.AuthorizePrimaryGoalSelection(request.Context(), jobdb.GoalKey{TenantId: "c2", GoalID: "goal"}, jobdb.ResolvedCell{CellID: "cell"}), jobdb.ErrPrimaryGoalSelectionForbidden)
}

// Execute fresh trees so command flags and environment never leak between cases.
func executeRoot(t *testing.T, args ...string) (string, error) {
	t.Helper()
	root := newRootCmd()
	var output bytes.Buffer
	root.SetOut(&output)
	root.SetErr(&output)
	root.SetArgs(args)
	err := root.ExecuteContext(context.Background())
	return output.String(), err
}

func TestConsumerGoalsCommandTree(t *testing.T) {
	t.Setenv("JOBDB_BEARER_TOKEN", "")
	goals, _, err := newRootCmd().Find([]string{"goals"})
	require.NoError(t, err)
	names := []string{}
	for _, child := range goals.Commands() {
		names = append(names, child.Name())
		require.NotContains(t, child.Aliases, "add-candidate")
	}
	require.ElementsMatch(t, []string{"get", "list", "candidates", "waits", "fail"}, names)
	for _, args := range [][]string{{"--help"}, {"goals", "--help"}, {"help", "goals"}, {"goals"}, {"completion", "bash"}, {"completion", "zsh"}, {"completion", "fish"}, {"completion", "powershell"}, {"__complete", "goals", ""}, {"__complete", "goals", "add"}} {
		output, err := executeRoot(t, args...)
		require.NoError(t, err, "%v: %s", args, output)
		require.NotContains(t, output, "add-candidate")
		require.NotContains(t, output, "add candidates")
	}
	for _, name := range names {
		output, err := executeRoot(t, "goals", name, "--help")
		require.NoError(t, err)
		require.Contains(t, output, "--tenant")
		require.Contains(t, output, "--url")
	}
	output, err := executeRoot(t, "goals", "fail", "--help")
	require.NoError(t, err)
	require.Contains(t, output, "--message")
}

func TestConsumerGoalsRejectBeforeHTTP(t *testing.T) {
	var requests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { requests.Add(1); w.WriteHeader(500) }))
	defer server.Close()
	for _, args := range [][]string{
		{"goals", "add-candidate", "goal", "job", "--tenant", "c2", "--url", server.URL},
		{"goals", "unknown", "--tenant", "c2", "--url", server.URL},
	} {
		output, err := executeRoot(t, args...)
		require.ErrorContains(t, err, "unknown command")
		require.NotContains(t, output, "connection refused")
		require.Zero(t, requests.Load())
	}
	for _, args := range [][]string{
		{"goals", "get"}, {"goals", "get", "one", "two"}, {"goals", "get", "goal", "--url", server.URL},
		{"goals", "list", "extra", "--tenant", "c2", "--url", server.URL},
		{"goals", "candidates", "--tenant", "c2", "--url", server.URL},
		{"goals", "waits", "extra", "--tenant", "c2", "--url", server.URL},
		{"goals", "fail", "--tenant", "c2", "--url", server.URL},
		{"goals", "get", "goal", "--tenant", "c2", "--url", "file:///tmp/runtime"},
		{"goals", "list", "--tenant", "c2", "--url", server.URL, "--created-after", "invalid"},
		{"goals", "waits", "--tenant", "c2", "--url", server.URL, "--target-job", "a", "--target-goal", "g"},
	} {
		_, err := executeRoot(t, args...)
		require.Error(t, err, "%v", args)
		require.Zero(t, requests.Load(), "%v", args)
	}
}

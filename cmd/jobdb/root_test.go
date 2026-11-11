package main

import (
	"net/http"
	"net/http/httptest"
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
		require.NoError(t, jobdb.AuthorizePrimaryGoalSelection(ctx, jobdb.GoalKey{TenantId: "tenant", GoalID: "goal"}, jobdb.ResolvedCell{CellID: "cell", Cell: "test", CellRepo: "example.test/test"}))
	}))
	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/", nil))
	require.True(t, called)
	// Untrusted request headers cannot install the context hook or permissions.
	request := httptest.NewRequest(http.MethodPost, "/", nil)
	request.Header.Set("goals.select-primary", "true")
	require.False(t, jobdb.HasPrimaryGoalSelectionAuthorizer(request.Context()))
	require.ErrorIs(t, jobdb.AuthorizePrimaryGoalSelection(request.Context(), jobdb.GoalKey{TenantId: "tenant", GoalID: "goal"}, jobdb.ResolvedCell{CellID: "cell"}), jobdb.ErrPrimaryGoalSelectionForbidden)
}

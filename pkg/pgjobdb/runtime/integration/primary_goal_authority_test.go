package integration_test

import (
	"context"
	"database/sql"
	"testing"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/runtimetest"
	"github.com/colony-2/pgjobdb/pkg/pgjobdb"
	"github.com/stretchr/testify/require"
)

func selectedPolicy(ctx context.Context, tenant string, permissions map[string]bool, cells map[string]bool) context.Context {
	ctx = jobdb.WithCellAuthority(ctx, jobdb.CellAuthority{Principal: "restricted", TenantID: tenant, Permissions: permissions, Cells: cells})
	return jobdb.WithPrimaryGoalSelectionAuthorizer(ctx, func(ctx context.Context, g jobdb.GoalKey, c jobdb.ResolvedCell) error {
		for _, permission := range []string{"goals.get", "goals.select-primary"} {
			if jobdb.AuthorizeCell(ctx, g.TenantId, c.CellID, permission) != nil {
				return jobdb.ErrPrimaryGoalSelectionForbidden
			}
		}
		return nil
	})
}

func TestSelectedNativeCurrentAuthority(t *testing.T) {
	runDatabaseTest(t, func(ctx context.Context, db *sql.DB) {
		admin := runtimetest.PrimaryGoalContext(ctx)
		source := nativeGoalJob(t, admin, db, "source")
		prep, publication := selectedNative(t, admin, db, "selected", source.GoalID, nil)
		policies := []struct {
			name, tenant string
			permissions  map[string]bool
			cells        map[string]bool
		}{
			{name: "read-only", tenant: "tenant", permissions: map[string]bool{"goals.get": true}},
			{name: "submit-only", tenant: "tenant", permissions: map[string]bool{"jobs.submit": true}},
			{name: "selection-without-access", tenant: "tenant", permissions: map[string]bool{"jobs.submit": true, "goals.select-primary": true}},
			{name: "access-without-selection", tenant: "tenant", permissions: map[string]bool{"jobs.submit": true, "goals.get": true}},
			{name: "other-tenant", tenant: "other", permissions: map[string]bool{"jobs.submit": true, "goals.get": true, "goals.select-primary": true}},
			{name: "other-cell", tenant: "tenant", permissions: map[string]bool{"jobs.submit": true, "goals.get": true, "goals.select-primary": true}, cells: map[string]bool{"other": true}},
		}
		for _, policy := range policies {
			t.Run(policy.name, func(t *testing.T) {
				denied := selectedPolicy(ctx, policy.tenant, policy.permissions, policy.cells)
				p, err := pgjobdb.PrepareJob(denied, db, prep)
				require.Error(t, err)
				require.Nil(t, p.Receipt)
				_, err = pgjobdb.SubmitJob(denied, db, publication)
				require.Error(t, err)
				assertNoSelectedScheduler(t, ctx, db, "selected")
			})
		}
		allowed := selectedPolicy(ctx, "tenant", map[string]bool{"jobs.submit": true, "goals.get": true, "goals.select-primary": true}, nil)
		_, err := pgjobdb.SubmitJob(allowed, db, publication)
		require.NoError(t, err)
		receipt, err := pgjobdb.PrepareJob(allowed, db, prep)
		require.NoError(t, err)
		require.NotNil(t, receipt.Receipt)
		for _, policy := range policies {
			denied := selectedPolicy(ctx, policy.tenant, policy.permissions, policy.cells)
			p, err := pgjobdb.PrepareJob(denied, db, prep)
			require.Error(t, err, policy.name)
			require.Nil(t, p.Receipt)
		}
		require.NoError(t, pgjobdb.DiscardJobPreparation(ctx, db, prep.Key))
		same, err := pgjobdb.PrepareJob(allowed, db, prep)
		require.NoError(t, err)
		require.Equal(t, receipt, same)
	})
}

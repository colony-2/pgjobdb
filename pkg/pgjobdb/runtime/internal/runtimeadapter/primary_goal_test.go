package runtimeadapter

import (
	"context"
	"testing"

	"github.com/colony-2/jobdb/pkg/jobdb"
	runtimecore "github.com/colony-2/jobdb/pkg/jobdb/runtime/core"
	"github.com/stretchr/testify/require"
)

func TestPrimaryGoalSelectionContract(t *testing.T) {
	var capability jobdb.PrimaryGoalSelectionCapability = Scheduler{}
	require.True(t, capability.SupportsPrimaryGoalSelection())
	_, err := (Scheduler{}).PrepareJob(context.Background(), runtimecore.JobPreparation{Key: jobdb.JobKey{TenantId: "tenant"}, SelectedGoalID: "goal", Descriptor: "submit"})
	require.ErrorIs(t, err, jobdb.ErrInvalidDependency)
	h := jobdb.JobHandle{Cell: "original", CellID: "immutable", CellRepo: "example.test/original", GoalID: "goal", JobKey: jobdb.JobKey{TenantId: "tenant", JobId: "job"}}
	p := runtimecore.PreparedJob{Key: h.JobKey, GoalID: h.GoalID, Receipt: &h, Published: true}
	got, err := p.AcceptedHandle(true)
	require.NoError(t, err)
	require.Equal(t, h, got)
}

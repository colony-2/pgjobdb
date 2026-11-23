package runtimeadapter

import (
	"context"
	"github.com/colony-2/jobdb/pkg/jobdb/configuration"
	"testing"

	"github.com/colony-2/jobdb/pkg/jobdb"
	runtimecore "github.com/colony-2/jobdb/pkg/jobdb/runtime/core"
	"github.com/stretchr/testify/require"
)

func TestPrimaryGoalSelectionContract(t *testing.T) {
	var capability jobdb.PrimaryGoalSelectionCapability = Scheduler{}
	require.True(t, capability.SupportsPrimaryGoalSelection())
	_, err := (Scheduler{}).PrepareJob(context.Background(), runtimecore.JobPreparation{Key: jobdb.JobKey{TenantId: "c2"}, SelectedGoalID: "goal", Descriptor: "submit"})
	require.ErrorIs(t, err, jobdb.ErrInvalidDependency)
	h := jobdb.JobHandle{Cell: "original", CellID: "immutable", CellRepo: "example.test/original", GoalID: "goal", JobKey: jobdb.JobKey{TenantId: "c2", JobId: "job"}}
	p := runtimecore.PreparedJob{Key: h.JobKey, GoalID: h.GoalID, Receipt: &h, Published: true}
	got, err := p.AcceptedHandle(true)
	require.NoError(t, err)
	require.Equal(t, h, got)
}

// Embed ports with no implementation: unsupported operations must reject before
// any preparation, history access or ordinary submission can reach a backend.
type limitedGoalScheduler struct {
	runtimecore.Scheduler
	guarantees jobdb.JobGoalCapabilities
}

func (s limitedGoalScheduler) JobGoalCapabilities() jobdb.JobGoalCapabilities { return s.guarantees }
func (s limitedGoalScheduler) SupportsPrimaryGoalSelection() bool             { return true }

func TestRetainedGoalIndependentGuarantees(t *testing.T) {
	require.Equal(t, jobdb.JobGoalCapabilities{ImmutableMembership: true, AtomicAdmission: true, TerminalFailureRecords: true}, (Scheduler{}).JobGoalCapabilities())
	for bits := range 8 {
		caps := jobdb.JobGoalCapabilities{ImmutableMembership: bits&1 != 0, AtomicAdmission: bits&2 != 0, TerminalFailureRecords: bits&4 != 0}
		rt, err := runtimecore.NewRuntime(runtimecore.Config{Scheduler: limitedGoalScheduler{guarantees: caps}, Chapters: struct{ runtimecore.ChapterLog }{}, Schemas: struct{ runtimecore.SchemaStore }{}, Configurations: struct{ configuration.Store }{}})
		require.NoError(t, err)
		require.Equal(t, caps, rt.JobGoalCapabilities())
		if !caps.ImmutableMembership || !caps.AtomicAdmission {
			_, err = rt.SubmitJob(context.Background(), jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "c2", JobID: "dest", GoalID: "goal", Cell: "work", CellRepo: "example.test/work", JobType: "work"}})
			require.ErrorIs(t, err, jobdb.ErrJobGoalSemanticsUnsupported)
			_, err = rt.SubmitRestartJob(context.Background(), jobdb.SubmitRestartJobRequest{RequestID: "restart", Job: jobdb.SubmitRestartJob{PriorJobKey: jobdb.JobKey{TenantId: "c2", JobId: "prior"}, ShareGoal: true}})
			require.ErrorIs(t, err, jobdb.ErrJobGoalSemanticsUnsupported)
		}
		if !caps.TerminalFailureRecords {
			_, err = rt.FailGoal(context.Background(), jobdb.FailGoalRequest{GoalKey: jobdb.GoalKey{TenantId: "c2", GoalID: "goal"}, Message: "reason"})
			require.ErrorIs(t, err, jobdb.ErrJobGoalSemanticsUnsupported)
		}
	}
}

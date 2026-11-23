package integration_test

import (
	"context"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb/celltest"

	"github.com/colony-2/jobdb/pkg/jobdb"
	wf "github.com/colony-2/jobdb/pkg/workflow"
	"github.com/stretchr/testify/require"
)

// The pinned JobDB contract classifies live mismatches as infrastructure errors:
// preserve durable history and allow recovery with the original declaration.
func TestPostgresLiveDeterminismMismatchPreservesJob(t *testing.T) {
	for _, transport := range []string{"embedded", "http"} {
		t.Run(transport, func(t *testing.T) {
			ctx := celltest.Context(context.Background())
			rt := configurationTransport(t, conformanceRuntimeFactory(t)(t), transport)
			var taskRuns int
			task := scopeTask{func(_ wf.TaskContext, in jobdb.TaskData) (jobdb.TaskData, error) {
				taskRuns++
				return in, nil
			}}
			worker := func(version uint32) wf.ExecutionWorker {
				return wf.ExecutionWorker{JobType: "determinism-recovery", Execute: func(c wf.ExecutionContext, in jobdb.JobData) (jobdb.JobData, error) {
					call := scopeCall("prefix")
					call.ContractVersion = version
					out, err := c.DoTask(call, func(context.Context) (jobdb.TaskData, error) { return in, nil })
					if err != nil {
						return nil, err
					}
					gate := scopeCall("gate")
					gate.TaskType = "external-gate"
					return c.DoTask(gate, func(context.Context) (jobdb.TaskData, error) { return out, nil })
				}}
			}
			engine, err := wf.NewEngineBuilder().WithRuntime(celltest.WithCells(rt)).BuildEngine()
			require.NoError(t, err)
			handle, err := engine.SubmitJob(ctx, jobdb.SubmitJob{Cell: "test", CellRepo: "example.test/test", TenantId: "c2", JobType: worker(1).Name(), Data: jobdb.NewTaskDataOrPanic("input")})
			require.NoError(t, err)
			require.NotEmpty(t, handle.GoalID)
			key := handle.JobKey
			require.Equal(t, wf.JobRunSuspended, runScoped(t, rt, key, worker(1), task).Status)
			waiting, err := engine.GetWaitingTask(ctx, key)
			require.NoError(t, err)
			require.NoError(t, waiting.Finish(ctx, jobdb.NewTaskDataOrPanic("ready")))
			before, err := rt.ListChapters(ctx, jobdb.ListChaptersRequest{JobKey: key})
			require.NoError(t, err)

			assertMismatch := func(key jobdb.JobKey, goalID string, before []jobdb.Chapter) {
				t.Helper()
				lease, err := rt.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: key, WorkerID: "scopes", Routes: []jobdb.Route{{JobType: worker(2).Name()}}, LeaseDuration: time.Minute})
				require.NoError(t, err)
				require.NotNil(t, lease)
				runnable, err := wf.GetJobForRunWithLease(ctx, rt, lease, wf.GetJobForRunRequest{
					JobKey: key, JobWorker: worker(2), TaskWorkers: []wf.TaskWorker{task},
					WorkerID: "scopes", LeaseDuration: time.Minute,
					ConsumerVersions: map[string]string{"pgjobdb-test": "1"},
				})
				require.NoError(t, err)
				_, err = runnable.Run(nil)
				require.True(t, jobdb.IsExecutionInfrastructureError(err), "expected typed infrastructure failure: %v", err)
				require.ErrorContains(t, err, "logical descriptor changed")
				require.Equal(t, 1, taskRuns, "the mismatching task must not run again")
				info, err := rt.GetJob(ctx, key)
				require.NoError(t, err)
				require.NotEqual(t, jobdb.JobStatusCompleted, info.Status)
				require.Equal(t, goalID, info.GoalID)
				after, err := rt.ListChapters(ctx, jobdb.ListChaptersRequest{JobKey: key})
				require.NoError(t, err)
				require.Equal(t, before, after, "infrastructure failure must not append an application outcome")
				// Explicitly release this test's still-valid lease before retrying.
				// Production admission remains subject to the ordinary lease fence.
				require.NotNil(t, lease)
				require.NoError(t, lease.Reschedule(ctx, jobdb.RescheduleExecutionRequest{NextRoute: jobdb.Route{JobType: worker(1).Name()}}))
			}
			assertMismatch(key, handle.GoalID, before)
			require.Equal(t, wf.JobRunCompleted, runScoped(t, rt, key, worker(1), task).Status)
			require.Equal(t, 1, taskRuns, "valid recovery must replay the retained prefix")

			// A restart may change its target task, but a changed retained prefix
			// remains an infrastructure mismatch with no persisted failure.
			history, err := jobdb.GetJobRun(ctx, rt, jobdb.GetJobRunRequest{JobKey: key})
			require.NoError(t, err)
			var taskIDs []string
			for _, entry := range history.Execution {
				if entry.Record.Kind == "task" {
					taskIDs = append(taskIDs, entry.Record.ID)
				}
			}
			require.Len(t, taskIDs, 2)
			restart, err := engine.SubmitRestartJob(ctx, jobdb.SubmitRestartJobRequest{Job: jobdb.SubmitRestartJob{Cell: "test", PriorJobKey: key, ExecutionRestart: &jobdb.ExecutionRestart{ScopeID: taskIDs[1]}}})
			require.NoError(t, err)
			require.NotEmpty(t, restart.GoalID)
			require.NotEqual(t, handle.GoalID, restart.GoalID)
			before, err = rt.ListChapters(ctx, jobdb.ListChaptersRequest{JobKey: restart.JobKey})
			require.NoError(t, err)
			assertMismatch(restart.JobKey, restart.GoalID, before)
			require.Equal(t, wf.JobRunSuspended, runScoped(t, rt, restart.JobKey, worker(1), task).Status)
			require.Equal(t, 1, taskRuns)
		})
	}
}

package integration_test

import (
	"context"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	wf "github.com/colony-2/jobdb/pkg/workflow"
	"github.com/stretchr/testify/require"
)

// A live history mismatch must persist a failure and release the lease, rather
// than leave the job ACTIVE until a worker or caller times out.
func TestPostgresLiveDeterminismFailureCompletes(t *testing.T) {
	for _, transport := range []string{"embedded", "http"} {
		t.Run(transport, func(t *testing.T) {
			ctx := context.Background()
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
			engine, err := wf.NewEngineBuilder().WithRuntime(rt).BuildEngine()
			require.NoError(t, err)
			handle, err := engine.SubmitJob(ctx, jobdb.SubmitJob{TenantId: "tenant", JobType: worker(1).Name(), Data: jobdb.NewTaskDataOrPanic("input")})
			require.NoError(t, err)
			require.NotEmpty(t, handle.GoalID)
			key := handle.JobKey
			require.Equal(t, wf.JobRunSuspended, runScoped(t, rt, key, worker(1), task).Status)
			waiting, err := engine.GetWaitingTask(ctx, key)
			require.NoError(t, err)
			require.NoError(t, waiting.Finish(ctx, jobdb.NewTaskDataOrPanic("ready")))
			before, err := rt.ListChapters(ctx, jobdb.ListChaptersRequest{JobKey: key})
			require.NoError(t, err)

			out := runScoped(t, rt, key, worker(2), task)
			require.Equal(t, wf.JobRunFailed, out.Status)
			require.ErrorContains(t, out.JobError, "workflow was not deterministic")
			require.Equal(t, 1, taskRuns, "the mismatching task must not run again")
			info, err := rt.GetJob(ctx, key)
			require.NoError(t, err)
			require.Equal(t, jobdb.JobStatusCompleted, info.Status)
			require.Equal(t, handle.GoalID, info.GoalID)
			_, err = info.Data.GetData()
			require.ErrorContains(t, err, "workflow was not deterministic")
			after, err := rt.ListChapters(ctx, jobdb.ListChaptersRequest{JobKey: key})
			require.NoError(t, err)
			require.Greater(t, len(after), len(before))
			require.Equal(t, before, after[:len(before)], "failure must preserve existing history")
			require.IsType(t, jobdb.JobAttemptOutcomeChapter{}, after[len(after)-1].Body)
			lease, err := rt.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: key, WorkerID: "after-failure", Routes: []jobdb.Route{{JobType: worker(2).Name()}}, LeaseDuration: time.Minute})
			require.NoError(t, err)
			require.Nil(t, lease, "terminal job must not be claimable")

			// A restart at the last task may change that task's declaration, but
			// changing the retained prefix must still terminate as a failure.
			history, err := jobdb.GetJobRun(ctx, rt, jobdb.GetJobRunRequest{JobKey: key})
			require.NoError(t, err)
			var taskIDs []string
			for _, entry := range history.Execution {
				if entry.Record.Kind == "task" {
					taskIDs = append(taskIDs, entry.Record.ID)
				}
			}
			require.Len(t, taskIDs, 2)
			restart, err := engine.SubmitRestartJob(ctx, jobdb.SubmitRestartJobRequest{Job: jobdb.SubmitRestartJob{PriorJobKey: key, ExecutionRestart: &jobdb.ExecutionRestart{ScopeID: taskIDs[1]}}})
			require.NoError(t, err)
			require.NotEmpty(t, restart.GoalID)
			require.NotEqual(t, handle.GoalID, restart.GoalID)
			out = runScoped(t, rt, restart.JobKey, worker(2), task)
			require.Equal(t, wf.JobRunFailed, out.Status)
			require.ErrorContains(t, out.JobError, "workflow was not deterministic")
			info, err = rt.GetJob(ctx, restart.JobKey)
			require.NoError(t, err)
			require.Equal(t, jobdb.JobStatusCompleted, info.Status)
			_, err = info.Data.GetData()
			require.ErrorContains(t, err, "workflow was not deterministic")
			require.Equal(t, 1, taskRuns)
		})
	}
}

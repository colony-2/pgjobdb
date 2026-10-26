package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	wf "github.com/colony-2/jobdb/pkg/workflow"
	"github.com/stretchr/testify/require"
)

func TestExecutionGoalWaitRecoveryAndReplay(t *testing.T) {
	for _, backend := range []string{"embedded", "http"} {
		for _, inTask := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/task=%v", backend, inTask), func(t *testing.T) {
				ctx := context.Background()
				rt := configurationTransport(t, conformanceRuntimeFactory(t)(t), backend)
				producer := wf.ExecutionWorker{JobType: "producer", Execute: func(wf.ExecutionContext, jobdb.JobData) (jobdb.JobData, error) {
					return jobdb.NewTaskDataOrPanic("winner"), nil
				}}
				first := submitScoped(t, rt, producer.Name())
				info, err := rt.GetJob(ctx, first)
				require.NoError(t, err)
				var bound string
				var calls atomic.Int32
				task := scopeTask{func(c wf.TaskContext, in jobdb.TaskData) (jobdb.TaskData, error) {
					calls.Add(1)
					bindings, err := c.AwaitGoals(info.GoalID)
					if err != nil {
						return nil, err
					}
					bound = bindings[0].SatisfiedByJobID
					return jobdb.NewTaskDataOrPanic(bound), nil
				}}
				consumer := wf.ExecutionWorker{JobType: "consumer", Execute: func(c wf.ExecutionContext, in jobdb.JobData) (jobdb.JobData, error) {
					if inTask {
						return c.DoTask(scopeCall("wait"), func(context.Context) (jobdb.TaskData, error) { return in, nil })
					}
					bindings, err := c.AwaitGoals(info.GoalID)
					if err != nil {
						return nil, err
					}
					bound = bindings[0].SatisfiedByJobID
					return jobdb.NewTaskDataOrPanic(bound), nil
				}}
				key := submitScoped(t, rt, consumer.Name())
				run := func() wf.JobRunOutcome {
					runnable, err := wf.GetJobForRun(ctx, rt, wf.GetJobForRunRequest{JobKey: key, JobWorker: consumer, TaskWorkers: []wf.TaskWorker{task}, WorkerID: "worker", LeaseDuration: time.Minute})
					require.NoError(t, err)
					out, err := runnable.Run(nil)
					require.NoError(t, err)
					return out
				}
				require.Equal(t, wf.JobRunSuspended, run().Status)
				require.NoError(t, rt.CancelJob(ctx, jobdb.CancelJobRequest{JobKey: first}))
				replacement := submitScoped(t, rt, producer.Name())
				_, err = rt.AddGoalCandidate(ctx, jobdb.AddGoalCandidateRequest{GoalKey: jobdb.GoalKey{TenantId: "tenant", GoalID: info.GoalID}, JobID: replacement.JobId})
				require.NoError(t, err)
				out, err := runGoalWorkflow(t, rt, replacement, producer)
				require.NoError(t, err)
				require.Equal(t, wf.JobRunCompleted, out.Status)
				require.Equal(t, wf.JobRunCompleted, run().Status)
				require.Equal(t, replacement.JobId, bound)
				before := goalExecutionRecords(t, rt, key)
				count := calls.Load()
				replay, err := replayGoalWorkflow(t, rt, key, consumer, task)
				require.NoError(t, err)
				require.Equal(t, jobdb.NewTaskDataOrPanic(replacement.JobId).GetDataOrPanic(), replay.GetDataOrPanic())
				require.Equal(t, count, calls.Load())
				require.Equal(t, before, goalExecutionRecords(t, rt, key))
			})
		}
	}
}
func TestExecutionGoalFailureAdmissionAndReplay(t *testing.T) {
	for _, backend := range []string{"embedded", "http"} {
		for _, submission := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/submission=%v", backend, submission), func(t *testing.T) {
				ctx := context.Background()
				rt := configurationTransport(t, conformanceRuntimeFactory(t)(t), backend)
				a, err := rt.SubmitJob(ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", JobType: "producer", Data: jobdb.NewTaskDataOrPanic(nil)}})
				require.NoError(t, err)
				goal := jobdb.GoalKey{TenantId: "tenant", GoalID: a.GoalID}
				var calls atomic.Int32
				consumer := wf.ExecutionWorker{JobType: "consumer", Execute: func(c wf.ExecutionContext, in jobdb.JobData) (jobdb.JobData, error) {
					calls.Add(1)
					_, err := c.AwaitGoals(goal.GoalID)
					return nil, err
				}}
				request := jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", JobType: "consumer", Data: jobdb.NewTaskDataOrPanic(nil), RunPolicy: jobdb.RunPolicy{Retry: jobdb.RetryPolicy{MaximumAttempts: 3}}}}
				if submission {
					request.Job.Dependencies = []jobdb.Dependency{{Target: jobdb.DependencyTarget{GoalID: goal.GoalID}, Condition: jobdb.JobPrereqSuccess}}
				}
				h, err := rt.SubmitJob(ctx, request)
				require.NoError(t, err)
				if !submission {
					out, err := runGoalWorkflow(t, rt, h.JobKey, consumer)
					require.NoError(t, err)
					require.Equal(t, wf.JobRunSuspended, out.Status)
				}
				failed, err := rt.FailGoal(ctx, jobdb.FailGoalRequest{GoalKey: goal, Message: "human explanation"})
				require.NoError(t, err)
				out, err := runGoalWorkflow(t, rt, h.JobKey, consumer)
				require.NoError(t, err)
				require.Equal(t, wf.JobRunFailed, out.Status)
				if submission {
					require.Zero(t, calls.Load())
				}
				_, err = replayGoalWorkflow(t, rt, h.JobKey, consumer)
				require.ErrorIs(t, err, jobdb.ErrGoalFailed)
				var typed *jobdb.GoalFailedError
				require.True(t, errors.As(err, &typed))
				require.Equal(t, *failed.Failure, typed.Failure)
				job, err := rt.GetJob(ctx, h.JobKey)
				require.NoError(t, err)
				_, err = job.Data.GetData()
				require.ErrorIs(t, err, jobdb.ErrGoalFailed)
				own, err := rt.GetGoal(ctx, jobdb.GoalKey{TenantId: "tenant", GoalID: h.GoalID})
				require.NoError(t, err)
				require.Equal(t, jobdb.GoalOpen, own.State)
			})
		}
	}
}

func TestExecutionRestartGoalAdmission(t *testing.T) {
	for _, backend := range []string{"embedded", "http"} {
		t.Run(backend, func(t *testing.T) {
			ctx := context.Background()
			rt := configurationTransport(t, conformanceRuntimeFactory(t)(t), backend)
			var calls atomic.Int32
			task := scopeTask{func(_ wf.TaskContext, in jobdb.TaskData) (jobdb.TaskData, error) { return in, nil }}
			worker := wf.ExecutionWorker{JobType: "restart-goal", Execute: func(c wf.ExecutionContext, in jobdb.JobData) (jobdb.JobData, error) {
				return c.DoScope("target", wf.ScopeOptions{OnRestart: func(json.RawMessage) error { return nil }}, func(c wf.ExecutionContext) (jobdb.JobData, error) {
					calls.Add(1)
					return c.DoTask(scopeCall("checkpoint"), func(context.Context) (jobdb.TaskData, error) { return in, nil })
				})
			}}
			source := submitScoped(t, rt, worker.Name())
			out, err := runGoalWorkflow(t, rt, source, worker, task)
			require.NoError(t, err)
			require.Equal(t, wf.JobRunCompleted, out.Status)
			var target string
			for _, record := range goalExecutionRecords(t, rt, source) {
				if record.Kind == "scope" && strings.Contains(record.ID, `"target"`) {
					target = record.ID
				}
			}
			require.NotEmpty(t, target)
			upstream, err := rt.SubmitJob(ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", JobType: "upstream", Data: jobdb.NewTaskDataOrPanic(nil)}})
			require.NoError(t, err)
			restart, err := rt.SubmitRestartJob(ctx, jobdb.SubmitRestartJobRequest{Job: jobdb.SubmitRestartJob{PriorJobKey: source, ExecutionRestart: &jobdb.ExecutionRestart{ScopeID: target}, Dependencies: []jobdb.Dependency{{Target: jobdb.DependencyTarget{GoalID: upstream.GoalID}, Condition: jobdb.JobPrereqSuccess}}}})
			require.NoError(t, err)
			_, err = rt.FailGoal(ctx, jobdb.FailGoalRequest{GoalKey: jobdb.GoalKey{TenantId: "tenant", GoalID: upstream.GoalID}, Message: "new dependency failed"})
			require.NoError(t, err)
			out, err = runGoalWorkflow(t, rt, restart.JobKey, worker, task)
			require.NoError(t, err)
			require.Equal(t, wf.JobRunFailed, out.Status)
			require.EqualValues(t, 1, calls.Load())
			_, err = replayGoalWorkflow(t, rt, restart.JobKey, worker, task)
			require.ErrorIs(t, err, jobdb.ErrGoalFailed)
		})
	}
}

func runGoalWorkflow(t *testing.T, rt jobdb.WorkflowRuntime, key jobdb.JobKey, worker wf.ExecutionWorker, tasks ...wf.TaskWorker) (wf.JobRunOutcome, error) {
	t.Helper()
	runnable, err := wf.GetJobForRun(context.Background(), rt, wf.GetJobForRunRequest{JobKey: key, JobWorker: worker, TaskWorkers: tasks, WorkerID: "goals", LeaseDuration: time.Minute})
	if err != nil {
		return wf.JobRunOutcome{}, err
	}
	return runnable.Run(nil)
}
func replayGoalWorkflow(t *testing.T, rt jobdb.WorkflowRuntime, key jobdb.JobKey, worker wf.ExecutionWorker, tasks ...wf.TaskWorker) (jobdb.JobData, error) {
	t.Helper()
	builder := wf.NewEngineBuilder().WithRuntime(rt).WithWorkerTenantId("tenant").PlusWorkers(worker, tasks...)
	engine, err := builder.BuildEngine()
	if err != nil {
		return nil, err
	}
	return engine.ReplayJobRun(context.Background(), wf.ReplayRunRequest{JobKey: key})
}
func goalExecutionRecords(t *testing.T, rt jobdb.WorkflowRuntime, key jobdb.JobKey) []jobdb.ExecutionRecord {
	t.Helper()
	history, err := jobdb.GetJobRun(context.Background(), rt, jobdb.GetJobRunRequest{JobKey: key})
	require.NoError(t, err)
	var records []jobdb.ExecutionRecord
	for _, entry := range history.Execution {
		records = append(records, entry.Record)
	}
	return records
}

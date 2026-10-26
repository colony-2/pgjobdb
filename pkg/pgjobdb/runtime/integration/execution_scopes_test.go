package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	wf "github.com/colony-2/jobdb/pkg/workflow"
	"github.com/stretchr/testify/require"
)

type scopeTask struct {
	run func(wf.TaskContext, jobdb.TaskData) (jobdb.TaskData, error)
}

func (s scopeTask) Name() string { return "echo" }
func (s scopeTask) Run(c wf.TaskContext, in jobdb.TaskData) (jobdb.TaskData, error) {
	return s.run(c, in)
}

func runScoped(t *testing.T, rt jobdb.WorkflowRuntime, key jobdb.JobKey, w wf.ExecutionWorker, tasks ...wf.TaskWorker) wf.JobRunOutcome {
	t.Helper()
	runnable, err := wf.GetJobForRun(context.Background(), rt, wf.GetJobForRunRequest{JobKey: key, JobWorker: w, TaskWorkers: tasks, WorkerID: "scopes", LeaseDuration: time.Minute, ConsumerVersions: map[string]string{"pgjobdb-test": "1"}})
	require.NoError(t, err)
	out, err := runnable.Run(nil)
	require.NoError(t, err)
	return out
}
func submitScoped(t *testing.T, rt jobdb.WorkflowRuntime, name string) jobdb.JobKey {
	t.Helper()
	h, err := rt.SubmitJob(context.Background(), jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", JobType: name, Data: jobdb.NewTaskDataOrPanic("input")}})
	require.NoError(t, err)
	return h.JobKey
}
func scopeCall(site string) wf.TaskCall {
	return wf.TaskCall{Site: site, TaskType: "echo", ContractVersion: 1, Command: json.RawMessage(`{"operation":"echo"}`)}
}

func TestPostgresExecutionScopes(t *testing.T) {
	newRuntime := conformanceRuntimeFactory(t)
	for _, transport := range []string{"embedded", "http"} {
		t.Run(transport, func(t *testing.T) {
			rt := configurationTransport(t, newRuntime(t), transport)
			ctx := context.Background()
			t.Run("retry_history_and_replay", func(t *testing.T) {
				var builds, runs atomic.Int32
				task := scopeTask{func(c wf.TaskContext, in jobdb.TaskData) (jobdb.TaskData, error) {
					if c.InvocationID == "" || c.AttemptNumber < 1 {
						return nil, errors.New("missing task invocation identity")
					}
					if runs.Add(1) == 1 {
						return nil, errors.New("retry")
					}
					return in, nil
				}}
				worker := wf.ExecutionWorker{JobType: "scope-retry", Execute: func(c wf.ExecutionContext, in jobdb.JobData) (jobdb.JobData, error) {
					return c.DoScope("outer", wf.ScopeOptions{}, func(s wf.ExecutionContext) (jobdb.JobData, error) {
						call := scopeCall("echo")
						call.Policy.Retry.MaximumAttempts = 2
						return s.DoTask(call, func(context.Context) (jobdb.TaskData, error) { builds.Add(1); return in, nil })
					})
				}}
				key := submitScoped(t, rt, worker.Name())
				require.Equal(t, wf.JobRunCompleted, runScoped(t, rt, key, worker, task).Status)
				history, err := jobdb.GetJobRun(ctx, rt, jobdb.GetJobRunRequest{JobKey: key, IncludeInputs: true, IncludeOutputs: true})
				require.NoError(t, err)
				require.NotEmpty(t, history.Execution)
				require.Equal(t, "header", history.Execution[0].Record.Kind)
				before, err := rt.ListChapters(ctx, jobdb.ListChaptersRequest{JobKey: key})
				require.NoError(t, err)
				engine, err := wf.NewEngineBuilder().WithRuntime(rt).WithWorkerTenantId("tenant").PlusWorkers(worker, task).BuildEngine()
				require.NoError(t, err)
				replay, err := engine.ReplayJobRun(ctx, wf.ReplayRunRequest{JobKey: key})
				require.NoError(t, err)
				raw, err := replay.GetData()
				require.NoError(t, err)
				require.JSONEq(t, `"input"`, string(raw))
				after, err := rt.ListChapters(ctx, jobdb.ListChaptersRequest{JobKey: key})
				require.NoError(t, err)
				require.Equal(t, before, after)
				require.EqualValues(t, 1, builds.Load())
				require.EqualValues(t, 2, runs.Load())
			})
			t.Run("external_task_recovery", func(t *testing.T) {
				var builds atomic.Int32
				worker := wf.ExecutionWorker{JobType: "scope-external", Execute: func(c wf.ExecutionContext, in jobdb.JobData) (jobdb.JobData, error) {
					return c.DoTask(scopeCall("external"), func(context.Context) (jobdb.TaskData, error) {
						builds.Add(1)
						return jobdb.NewTaskDataOrPanic("explicit-input"), nil
					})
				}}
				key := submitScoped(t, rt, worker.Name())
				require.Equal(t, wf.JobRunSuspended, runScoped(t, rt, key, worker).Status)
				engine, err := wf.NewEngineBuilder().WithRuntime(rt).WithWorkerTenantId("tenant").PlusWorkers(worker).BuildEngine()
				require.NoError(t, err)
				handle, err := engine.GetWaitingTask(ctx, key)
				require.NoError(t, err)
				input, err := handle.Data()
				require.NoError(t, err)
				raw, err := input.GetData()
				require.NoError(t, err)
				require.JSONEq(t, `"explicit-input"`, string(raw))
				require.NoError(t, handle.Finish(ctx, jobdb.NewTaskDataOrPanic("answer")))
				require.Error(t, handle.Finish(ctx, jobdb.NewTaskDataOrPanic("stale")))
				out := runScoped(t, rt, key, worker)
				require.Equal(t, wf.JobRunCompleted, out.Status)
				raw, err = out.Output.GetData()
				require.NoError(t, err)
				require.JSONEq(t, `"answer"`, string(raw))
				_, err = engine.ReplayJobRun(ctx, wf.ReplayRunRequest{JobKey: key})
				require.NoError(t, err)
				require.EqualValues(t, 1, builds.Load())
			})
			t.Run("patched_restart", func(t *testing.T) {
				var runs atomic.Int32
				task := scopeTask{func(_ wf.TaskContext, in jobdb.TaskData) (jobdb.TaskData, error) { runs.Add(1); return in, nil }}
				worker := wf.ExecutionWorker{JobType: "scope-restart", Execute: func(c wf.ExecutionContext, in jobdb.JobData) (jobdb.JobData, error) {
					if _, err := c.DoTask(scopeCall("prefix"), func(context.Context) (jobdb.TaskData, error) { return in, nil }); err != nil {
						return nil, err
					}
					value := "original"
					return c.DoScope("patched", wf.ScopeOptions{OnRestart: func(raw json.RawMessage) error { return json.Unmarshal(raw, &value) }}, func(s wf.ExecutionContext) (jobdb.JobData, error) {
						call := scopeCall("suffix")
						call.Command, _ = json.Marshal(value)
						return s.DoTask(call, func(context.Context) (jobdb.TaskData, error) { return jobdb.NewTaskDataOrPanic(value), nil })
					})
				}}
				key := submitScoped(t, rt, worker.Name())
				require.Equal(t, wf.JobRunCompleted, runScoped(t, rt, key, worker, task).Status)
				before, err := rt.ListChapters(ctx, jobdb.ListChaptersRequest{JobKey: key})
				require.NoError(t, err)
				history, err := jobdb.GetJobRun(ctx, rt, jobdb.GetJobRunRequest{JobKey: key})
				require.NoError(t, err)
				var target string
				for _, entry := range history.Execution {
					if entry.Record.Kind == "scope" {
						target = entry.Record.ID
					}
				}
				require.NotEmpty(t, target)
				restarted, err := rt.SubmitRestartJob(ctx, jobdb.SubmitRestartJobRequest{Job: jobdb.SubmitRestartJob{PriorJobKey: key, ExecutionRestart: &jobdb.ExecutionRestart{ScopeID: target, Payload: json.RawMessage(`"patched"`)}}})
				require.NoError(t, err)
				out := runScoped(t, rt, restarted.JobKey, worker, task)
				require.Equal(t, wf.JobRunCompleted, out.Status)
				raw, err := out.Output.GetData()
				require.NoError(t, err)
				require.JSONEq(t, `"patched"`, string(raw))
				require.EqualValues(t, 3, runs.Load())
				engine, err := wf.NewEngineBuilder().WithRuntime(rt).WithWorkerTenantId("tenant").PlusWorkers(worker, task).BuildEngine()
				require.NoError(t, err)
				_, err = engine.ReplayJobRun(ctx, wf.ReplayRunRequest{JobKey: restarted.JobKey})
				require.NoError(t, err)
				require.EqualValues(t, 3, runs.Load())
				after, err := rt.ListChapters(ctx, jobdb.ListChaptersRequest{JobKey: key})
				require.NoError(t, err)
				require.Equal(t, before, after)
			})
			t.Run("handoff_receipt", func(t *testing.T) {
				worker := wf.ExecutionWorker{JobType: "scope-handoff", Execute: func(c wf.ExecutionContext, in jobdb.JobData) (jobdb.JobData, error) {
					return nil, c.Yield(c.Context(), wf.YieldRequest{Reschedule: jobdb.RescheduleExecutionRequest{NextRoute: jobdb.Route{JobType: "receiver"}}, Metadata: json.RawMessage(`{"environment":"gpu"}`)})
				}}
				key := submitScoped(t, rt, worker.Name())
				out := runScoped(t, rt, key, worker)
				require.Equal(t, wf.JobRunSuspended, out.Status)
				require.NotNil(t, out.Handoff)
				require.Equal(t, "receiver", out.Handoff.NextRoute.JobType)
				require.JSONEq(t, `{"environment":"gpu"}`, string(out.Handoff.Metadata))
				lease, err := rt.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: key, WorkerID: "receiver", Routes: []jobdb.Route{{JobType: "receiver"}}})
				require.NoError(t, err)
				require.NotNil(t, lease)
			})
		})
	}
}

func TestPostgresDependencyDeadline(t *testing.T) {
	newRuntime := conformanceRuntimeFactory(t)
	for _, transport := range []string{"embedded", "http"} {
		for _, scenario := range []string{"child_first", "deadline_first", "already_completed", "no_deadline", "delay_only"} {
			t.Run(transport+"/"+scenario, func(t *testing.T) {
				rt := configurationTransport(t, newRuntime(t), transport)
				ctx := context.Background()
				child := submitScoped(t, rt, "child")
				parent := submitScoped(t, rt, "parent")
				acquire := func(key jobdb.JobKey, name string) jobdb.ExecutionLease {
					l, err := rt.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: key, WorkerID: "test", Routes: []jobdb.Route{{JobType: name}}, LeaseDuration: time.Minute})
					require.NoError(t, err)
					return l
				}
				completeChild := func() {
					l := acquire(child, "child")
					require.NotNil(t, l)
					require.NoError(t, l.Complete(ctx, jobdb.CompleteExecutionRequest{Status: "success", Chapter: &jobdb.Chapter{Ordinal: 1, TaskType: "child", CreatedAt: time.Now().UTC(), Body: jobdb.JobAttemptOutcomeChapter{Outcome: jobdb.ApplicationOutputOutcome{Output: jobdb.ApplicationOutputBytes{Data: []byte(`"done"`)}}}}}))
				}
				if scenario == "already_completed" {
					completeChild()
				}
				l := acquire(parent, "parent")
				require.NotNil(t, l)
				deadline := time.Now().Add(time.Hour)
				if scenario == "deadline_first" {
					deadline = time.Now().Add(-time.Second)
				}
				req := jobdb.RescheduleExecutionRequest{NextRoute: jobdb.Route{JobType: "parent"}, WaitForJobIDs: []string{child.JobId}, WaitUntil: &deadline}
				if scenario == "no_deadline" {
					req.WaitUntil = nil
				}
				if scenario == "delay_only" {
					req.WaitForJobIDs = nil
				}
				require.NoError(t, l.Reschedule(ctx, req))
				if scenario == "child_first" || scenario == "no_deadline" {
					require.Nil(t, acquire(parent, "parent"))
					completeChild()
				}
				if scenario == "delay_only" {
					require.Nil(t, acquire(parent, "parent"))
					return
				}
				info, err := rt.GetJob(ctx, parent)
				require.NoError(t, err)
				require.Equal(t, jobdb.JobStatusReady, info.Status)
				require.NotNil(t, acquire(parent, "parent"))
			})
		}
	}
}

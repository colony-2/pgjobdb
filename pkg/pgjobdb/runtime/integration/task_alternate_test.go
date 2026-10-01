package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/remote"
	wf "github.com/colony-2/jobdb/pkg/workflow"
	pgjobdbruntime "github.com/colony-2/pgjobdb/pkg/pgjobdb/runtime"
	"github.com/stretchr/testify/require"
)

// Upstream task-alternate scenarios exercised against direct and remote PostgreSQL.
func TestPostgresTaskAlternates(t *testing.T) {
	newRuntime := conformanceRuntimeFactory(t)
	t.Run("TestTaskAlternateLifecycle", func(t *testing.T) { runTaskAlternateLifecycle(t, newRuntime) })
	t.Run("TestTaskAlternateHardTimeout", func(t *testing.T) { runTaskAlternateHardTimeout(t, newRuntime) })
	t.Run("TestTaskAlternateHandoffDoesNotScheduleJobInvocationTimeout", func(t *testing.T) { runTaskAlternateHandoffDoesNotScheduleJobInvocationTimeout(t, newRuntime) })
	t.Run("TestTaskAlternateClaimAfterHardDeadline", func(t *testing.T) { runTaskAlternateClaimAfterHardDeadline(t, newRuntime) })
	t.Run("TestTaskAlternateSuppliedLease", func(t *testing.T) { runTaskAlternateSuppliedLease(t, newRuntime) })
	t.Run("TestCompletedExternalTaskReplaysAfterDeadline", func(t *testing.T) { runCompletedExternalTaskReplaysAfterDeadline(t, newRuntime) })
}

type alternateJob struct{ policy jobdb.RunPolicy }

func (alternateJob) Name() string { return "alternate-test" }
func (w alternateJob) Run(ctx wf.JobContext, in jobdb.JobData) (jobdb.JobData, error) {
	prepared, err := ctx.DoTask(jobdb.RunPolicy{}, "prepare", in)
	if err != nil {
		return nil, err
	}
	var alt wf.TaskAlternate
	raw, err := prepared.GetData()
	if err != nil {
		return nil, err
	}
	if err = json.Unmarshal(raw, &alt); err != nil {
		return nil, err
	}
	out, err := ctx.DoTask(w.policy, "external", wf.WithTaskOptions(prepared, wf.TaskOptions{Alternate: &alt}))
	if err != nil {
		return nil, err
	}
	return ctx.DoTask(jobdb.RunPolicy{}, "after", out)
}

type alternateTask struct{ name string }

func (w alternateTask) Name() string { return w.name }
func (w alternateTask) Run(_ wf.TaskContext, in jobdb.TaskData) (jobdb.TaskData, error) {
	if w.name == "fallback" {
		return jobdb.NewTaskDataOrPanic("fallback"), nil
	}
	return in, nil
}

func alternateRuntime(t *testing.T, backend string, newRuntime func(testing.TB) *pgjobdbruntime.Runtime) jobdb.WorkflowRuntime {
	t.Helper()
	rt := newRuntime(t)
	if backend == "remote-postgres" {
		server := httptest.NewServer(remote.NewServer(rt))
		t.Cleanup(server.Close)
		r, err := remote.New(server.URL, server.Client())
		require.NoError(t, err)
		return r
	}
	return rt
}
func runAlternate(t *testing.T, rt jobdb.WorkflowRuntime, key jobdb.JobKey, w alternateJob, includeAlternate bool) wf.JobRunOutcome {
	t.Helper()
	tasks := []wf.TaskWorker{alternateTask{"prepare"}, alternateTask{"after"}}
	if includeAlternate {
		tasks = append(tasks, alternateTask{"fallback"})
	}
	run, err := wf.GetJobForRun(context.Background(), rt, wf.GetJobForRunRequest{JobKey: key, JobWorker: w, TaskWorkers: tasks, WorkerID: "worker", LeaseDuration: 5 * time.Second})
	require.NoError(t, err)
	out, err := run.Run(nil)
	require.NoErrorf(t, err, "underlying error: %v", errors.Unwrap(err))
	return out
}
func waitAlternate(at time.Time) {
	if d := time.Until(at); d > 0 {
		time.Sleep(d)
	}
}

func runTaskAlternateLifecycle(t *testing.T, newRuntime func(testing.TB) *pgjobdbruntime.Runtime) {
	for _, backend := range []string{"postgres", "remote-postgres"} {
		for _, mode := range []string{"fallback", "human-before", "human-late", "race", "crash"} {
			t.Run(backend+"/"+mode, func(t *testing.T) {
				ctx := context.Background()
				rt := alternateRuntime(t, backend, newRuntime)
				at := time.Now().UTC().Add(400 * time.Millisecond)
				policy := jobdb.DefaultRunPolicy()
				policy.Retry.MaximumAttempts = 1
				handle, err := rt.SubmitJob(ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", JobID: "job", JobType: "alternate-test", Data: jobdb.NewTaskDataOrPanic(wf.TaskAlternate{TaskType: "fallback", At: at}), RunPolicy: policy}})
				require.NoError(t, err)
				w := alternateJob{}
				require.Equal(t, wf.JobRunSuspended, runAlternate(t, rt, handle.JobKey, w, true).Status)
				// Merely registering the handler cannot run it early.
				require.False(t, runAlternate(t, rt, handle.JobKey, w, true).LeaseAcquired)
				engine, err := wf.NewEngineBuilder().WithRuntime(rt).BuildEngine()
				require.NoError(t, err)
				pending, err := engine.GetWaitingTask(ctx, handle.JobKey)
				require.NoError(t, err)
				require.Equal(t, "external", pending.TaskType())
				finish := func() error { return pending.Finish(ctx, jobdb.NewTaskDataOrPanic("human")) }
				expected := "fallback"
				switch mode {
				case "human-before":
					require.NoError(t, finish())
					expected = "human"
					waitAlternate(at.Add(10 * time.Millisecond))
				case "human-late":
					waitAlternate(at.Add(10 * time.Millisecond))
					require.False(t, runAlternate(t, rt, handle.JobKey, w, false).LeaseAcquired)
					// A fresh lookup after eligibility still sees the original pending task.
					pending, err = engine.GetWaitingTask(ctx, handle.JobKey)
					require.NoError(t, err)
					require.Equal(t, "external", pending.TaskType())
					require.NoError(t, finish())
					expected = "human"
				case "race":
					waitAlternate(at.Add(10 * time.Millisecond))
					var wg sync.WaitGroup
					wg.Add(1)
					var humanErr error
					go func() { defer wg.Done(); humanErr = finish() }()
					_ = runAlternate(t, rt, handle.JobKey, w, true)
					wg.Wait()
					if humanErr == nil {
						expected = "human"
					}
				case "crash":
					waitAlternate(at.Add(10 * time.Millisecond))
					// PostgreSQL leases have whole-second precision.
					lease, err := rt.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: handle.JobKey, WorkerID: "crashed", Routes: []jobdb.Route{{JobType: w.Name(), TaskType: "fallback"}}, LeaseDuration: time.Second})
					require.NoError(t, err)
					require.NotNil(t, lease)
					require.Error(t, finish())
					// Remote token expiry includes a safety margin and precedes
					// backend expiry. Wait for the scheduler's actual deadline.
					page, err := rt.ListJobs(ctx, jobdb.ListJobsRequest{TenantIds: []string{handle.JobKey.TenantId}, JobKeys: []jobdb.JobKey{handle.JobKey}})
					require.NoError(t, err)
					require.Len(t, page.Jobs, 1)
					require.NotNil(t, page.Jobs[0].LeaseExpiresAt)
					waitAlternate(page.Jobs[0].LeaseExpiresAt.Add(10 * time.Millisecond))
				default:
					waitAlternate(at.Add(10 * time.Millisecond))
				}
				result := runAlternate(t, rt, handle.JobKey, w, true)
				require.True(t, result.Status == wf.JobRunCompleted || (mode == "race" && result.Status == wf.JobRunNotLeaseable), "%+v", result)
				info, err := rt.GetJob(ctx, handle.JobKey)
				require.NoError(t, err)
				data, err := info.Data.GetData()
				require.NoError(t, err)
				require.JSONEq(t, `"`+expected+`"`, string(data))
				require.Error(t, finish(), "completed answer cannot be overwritten")
				chapter, err := rt.GetChapter(ctx, jobdb.ChapterRef{JobKey: handle.JobKey, Ordinal: 2})
				require.NoError(t, err)
				encoded, _ := json.Marshal(chapter)
				require.Contains(t, string(encoded), "external")
				require.NotContains(t, string(encoded), `task_type\":\"fallback`)
			})
		}
	}
}

func runTaskAlternateHardTimeout(t *testing.T, newRuntime func(testing.TB) *pgjobdbruntime.Runtime) {
	for _, backend := range []string{"postgres", "remote-postgres"} {
		for _, scope := range []string{"task-total", "task-invocation", "job-total"} {
			t.Run(backend+"/"+scope, func(t *testing.T) {
				rt := alternateRuntime(t, backend, newRuntime)
				policy := jobdb.DefaultRunPolicy()
				policy.Retry.MaximumAttempts = 1
				taskPolicy := jobdb.RunPolicy{}
				switch scope {
				case "task-total":
					taskPolicy.TotalTimeout = jobdb.AsDuration(500 * time.Millisecond)
				case "task-invocation":
					taskPolicy.InvocationTimeout = jobdb.AsDuration(500 * time.Millisecond)
				case "job-total":
					policy.TotalTimeout = jobdb.AsDuration(500 * time.Millisecond)
				}
				h, err := rt.SubmitJob(context.Background(), jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", JobID: "deadline", JobType: "alternate-test", Data: jobdb.NewTaskDataOrPanic(wf.TaskAlternate{TaskType: "fallback", At: time.Now().Add(2 * time.Second)}), RunPolicy: policy}})
				require.NoError(t, err)
				w := alternateJob{taskPolicy}
				require.Equal(t, wf.JobRunSuspended, runAlternate(t, rt, h.JobKey, w, true).Status)
				waitAlternate(time.Now().Add(600 * time.Millisecond))
				// Timeout wakeups use the primary job route; no alternate task
				// handler is needed to classify an already-expired deadline.
				out := runAlternate(t, rt, h.JobKey, w, false)
				require.Equal(t, wf.JobRunFailed, out.Status)
				var timeout jobdb.TimeoutError
				require.ErrorAs(t, out.JobError, &timeout)
				first, err := rt.GetChapter(context.Background(), jobdb.ChapterRef{JobKey: h.JobKey, Ordinal: 1})
				require.NoError(t, err)
				raw, _ := json.Marshal(first)
				require.Contains(t, string(raw), "prepare")
			})
		}
	}
}

func runTaskAlternateHandoffDoesNotScheduleJobInvocationTimeout(t *testing.T, newRuntime func(testing.TB) *pgjobdbruntime.Runtime) {
	for _, backend := range []string{"postgres", "remote-postgres"} {
		t.Run(backend, func(t *testing.T) {
			rt := alternateRuntime(t, backend, newRuntime)
			policy := jobdb.DefaultRunPolicy()
			policy.Retry.MaximumAttempts = 1
			policy.InvocationTimeout = jobdb.AsDuration(500 * time.Millisecond)
			at := time.Now().Add(1500 * time.Millisecond)
			h, err := rt.SubmitJob(context.Background(), jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{
				TenantId: "tenant", JobType: "alternate-test", RunPolicy: policy,
				Data: jobdb.NewTaskDataOrPanic(wf.TaskAlternate{TaskType: "fallback", At: at}),
			}})
			require.NoError(t, err)
			w := alternateJob{jobdb.RunPolicy{InvocationTimeout: jobdb.AsDuration(3 * time.Second)}}
			require.Equal(t, wf.JobRunSuspended, runAlternate(t, rt, h.JobKey, w, true).Status)
			waitAlternate(time.Now().Add(600 * time.Millisecond))
			require.False(t, runAlternate(t, rt, h.JobKey, w, false).LeaseAcquired)
			waitAlternate(at.Add(10 * time.Millisecond))
			require.Equal(t, wf.JobRunCompleted, runAlternate(t, rt, h.JobKey, w, true).Status)
		})
	}
}

func runTaskAlternateClaimAfterHardDeadline(t *testing.T, newRuntime func(testing.TB) *pgjobdbruntime.Runtime) {
	for _, backend := range []string{"postgres", "remote-postgres"} {
		t.Run(backend, func(t *testing.T) {
			rt := alternateRuntime(t, backend, newRuntime)
			policy := jobdb.DefaultRunPolicy()
			policy.Retry.MaximumAttempts = 1
			h, err := rt.SubmitJob(context.Background(), jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{
				TenantId: "tenant", JobType: "alternate-test", RunPolicy: policy,
				Data: jobdb.NewTaskDataOrPanic(wf.TaskAlternate{TaskType: "fallback", At: time.Now().Add(100 * time.Millisecond)}),
			}})
			require.NoError(t, err)
			w := alternateJob{jobdb.RunPolicy{TotalTimeout: jobdb.AsDuration(300 * time.Millisecond)}}
			require.Equal(t, wf.JobRunSuspended, runAlternate(t, rt, h.JobKey, w, true).Status)
			waitAlternate(time.Now().Add(400 * time.Millisecond))
			out := runAlternate(t, rt, h.JobKey, w, true)
			require.Equal(t, wf.JobRunFailed, out.Status)
			var timeout jobdb.TimeoutError
			require.ErrorAs(t, out.JobError, &timeout)
		})
	}
}

func runTaskAlternateSuppliedLease(t *testing.T, newRuntime func(testing.TB) *pgjobdbruntime.Runtime) {
	ctx := context.Background()
	rt := alternateRuntime(t, "remote-postgres", newRuntime)
	at := time.Now().Add(100 * time.Millisecond)
	h, err := rt.SubmitJob(ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", JobID: "supplied", JobType: "alternate-test", Data: jobdb.NewTaskDataOrPanic(wf.TaskAlternate{TaskType: "fallback", At: at}), RunPolicy: jobdb.DefaultRunPolicy()}})
	require.NoError(t, err)
	w := alternateJob{}
	require.Equal(t, wf.JobRunSuspended, runAlternate(t, rt, h.JobKey, w, true).Status)
	waitAlternate(at.Add(10 * time.Millisecond))
	lease, err := rt.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: h.JobKey, WorkerID: "dispatcher", Routes: []jobdb.Route{{JobType: w.Name(), TaskType: "fallback"}}, LeaseDuration: time.Second})
	require.NoError(t, err)
	require.NotNil(t, lease)
	cap, err := remote.ExportLease(lease)
	require.NoError(t, err)
	raw, err := cap.Encode()
	require.NoError(t, err)
	cap, err = remote.DecodeLeaseCapability(raw)
	require.NoError(t, err)
	imported, err := rt.(*remote.Runtime).ImportLease(ctx, cap)
	require.NoError(t, err)
	require.Equal(t, lease.Route(), imported.Route())
	require.Equal(t, lease.ExecutionState(), imported.ExecutionState())
	runnable, err := wf.GetJobForRunWithLease(ctx, rt, imported, wf.GetJobForRunRequest{JobKey: h.JobKey, JobWorker: w, TaskWorkers: []wf.TaskWorker{alternateTask{"prepare"}, alternateTask{"fallback"}, alternateTask{"after"}}})
	require.NoError(t, err)
	out, err := runnable.Run(nil)
	require.NoError(t, err)
	require.Equal(t, wf.JobRunCompleted, out.Status)
}

func runCompletedExternalTaskReplaysAfterDeadline(t *testing.T, newRuntime func(testing.TB) *pgjobdbruntime.Runtime) {
	for _, backend := range []string{"postgres", "remote-postgres"} {
		t.Run(backend, func(t *testing.T) {
			ctx := context.Background()
			rt := alternateRuntime(t, backend, newRuntime)
			h, err := rt.SubmitJob(ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", JobID: "completed-input", JobType: "alternate-test", Data: jobdb.NewTaskDataOrPanic(wf.TaskAlternate{TaskType: "fallback", At: time.Now().Add(2 * time.Second)}), RunPolicy: jobdb.DefaultRunPolicy()}})
			require.NoError(t, err)
			w := alternateJob{jobdb.RunPolicy{TotalTimeout: jobdb.AsDuration(200 * time.Millisecond)}}
			require.Equal(t, wf.JobRunSuspended, runAlternate(t, rt, h.JobKey, w, true).Status)
			engine, err := wf.NewEngineBuilder().WithRuntime(rt).BuildEngine()
			require.NoError(t, err)
			task, err := engine.GetWaitingTask(ctx, h.JobKey)
			require.NoError(t, err)
			require.NoError(t, task.Finish(ctx, jobdb.NewTaskDataOrPanic("human")))
			waitAlternate(time.Now().Add(250 * time.Millisecond))
			require.Equal(t, wf.JobRunCompleted, runAlternate(t, rt, h.JobKey, w, true).Status)
		})
	}
}

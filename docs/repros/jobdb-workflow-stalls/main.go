// This reproducer imports only JobDB, not pgjobdb, and needs no PostgreSQL server.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/sqlite"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/toy"
	wf "github.com/colony-2/jobdb/pkg/workflow"
)

type stats struct{ complete, reschedule atomic.Int32 }
type tracedRuntime struct {
	jobdb.WorkflowRuntime
	calls *stats
}

func (r *tracedRuntime) GetJobLease(ctx context.Context, req jobdb.GetJobLeaseRequest) (jobdb.ExecutionLease, error) {
	l, err := r.WorkflowRuntime.GetJobLease(ctx, req)
	if err != nil || l == nil {
		return l, err
	}
	r.calls = &stats{}
	return &tracedLease{RenewableExecutionLease: l.(jobdb.RenewableExecutionLease), calls: r.calls}, nil
}

type tracedLease struct {
	jobdb.RenewableExecutionLease
	calls *stats
}

func (l *tracedLease) Renew(ctx context.Context) (jobdb.RenewableExecutionLease, error) {
	next, err := l.RenewableExecutionLease.Renew(ctx)
	if err != nil {
		return nil, err
	}
	return &tracedLease{RenewableExecutionLease: next, calls: l.calls}, nil
}
func (l *tracedLease) Complete(ctx context.Context, req jobdb.CompleteExecutionRequest) error {
	l.calls.complete.Add(1)
	return l.RenewableExecutionLease.Complete(ctx, req)
}
func (l *tracedLease) Reschedule(ctx context.Context, req jobdb.RescheduleExecutionRequest) error {
	l.calls.reschedule.Add(1)
	return l.RenewableExecutionLease.Reschedule(ctx, req)
}

type echoTask struct{}

func (echoTask) Name() string                                                    { return "echo" }
func (echoTask) Run(_ wf.TaskContext, in jobdb.TaskData) (jobdb.TaskData, error) { return in, nil }

type echoJob struct{ forceMismatch, pause bool }

func (echoJob) Name() string { return "echo-job" }
func (j echoJob) Run(ctx wf.ExecutionContext, in jobdb.JobData) (jobdb.JobData, error) {
	version := uint32(1)
	if j.forceMismatch {
		version = 2
	}
	out, err := ctx.DoTask(wf.TaskCall{Command: []byte("{}"), Site: "echo", TaskType: "echo", ContractVersion: version}, func(context.Context) (jobdb.TaskData, error) { return in, nil })
	if err != nil || !j.pause {
		return out, err
	}
	return ctx.DoTask(wf.TaskCall{Command: []byte("{}"), Site: "gate", TaskType: "gate", ContractVersion: 1}, func(context.Context) (jobdb.TaskData, error) { return out, nil })
}

type failJob struct{}

func (failJob) Name() string { return "fail-job" }
func (failJob) Run(wf.ExecutionContext, jobdb.JobData) (jobdb.JobData, error) {
	return nil, jobdb.AppError{Payload: jobdb.AppErrorPayload{Message: "prerequisite failed"}}
}
func main() {
	backend := flag.String("backend", "toy", "toy or sqlite")
	scenario := flag.String("case", "determinism", "determinism, task-restart, or prerequisite")
	flag.Parse()
	ctx := context.Background()
	var runtime jobdb.WorkflowRuntime
	switch *backend {
	case "toy":
		runtime = toy.New()
	case "sqlite":
		dir, err := os.MkdirTemp("", "jobdb-stall-repro-*")
		must(err)
		defer os.RemoveAll(dir)
		rt, err := sqlite.NewFromConfig(ctx, sqlite.Config{DBPath: filepath.Join(dir, "jobs.db")})
		must(err)
		defer rt.Close(ctx)
		runtime = rt
	default:
		panic("unknown backend")
	}
	rt := &tracedRuntime{WorkflowRuntime: runtime}
	submit := func(worker wf.JobWorker, id string) jobdb.JobHandle {
		policy := jobdb.DefaultRunPolicy()
		policy.Retry.MaximumAttempts = 1
		h, err := rt.SubmitJob(ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", JobID: id, JobType: worker.Name(), Data: jobdb.NewTaskDataOrPanic(1), RunPolicy: policy}})
		must(err)
		return h
	}
	fmt.Printf("backend=%s case=%s\n", *backend, *scenario)
	switch *scenario {
	case "determinism":
		w := echoJob{pause: true}
		h := submit(w, "mismatch")
		run(ctx, rt, h.JobKey, w)
		engine, err := wf.NewEngineBuilder().WithRuntime(rt).BuildEngine()
		must(err)
		wait, err := engine.GetWaitingTask(ctx, h.JobKey)
		must(err)
		must(wait.Finish(ctx, jobdb.NewTaskDataOrPanic(1)))
		run(ctx, rt, h.JobKey, echoJob{forceMismatch: true, pause: true})
	case "task-restart", "prerequisite":
		base := submit(echoJob{}, "base")
		run(ctx, rt, base.JobKey, echoJob{})
		history, err := jobdb.GetJobRun(ctx, rt, jobdb.GetJobRunRequest{JobKey: base.JobKey})
		must(err)
		target := ""
		for _, entry := range history.Execution {
			if entry.Record.Kind == "task" {
				target = entry.Record.ID
				break
			}
		}
		if target == "" {
			panic("task restart boundary missing")
		}
		req := jobdb.SubmitRestartJob{PriorJobKey: base.JobKey, JobID: "restart", ExecutionRestart: &jobdb.ExecutionRestart{ScopeID: target}}
		worker := echoJob{forceMismatch: true} // A selected restart boundary may change its task contract.
		if *scenario == "prerequisite" {
			failed := submit(failJob{}, "prerequisite")
			run(ctx, rt, failed.JobKey, failJob{})
			req.Prerequisites = []jobdb.JobPrerequisite{{JobID: failed.JobKey.JobId, Condition: jobdb.JobPrereqSuccess}}
			worker = echoJob{}
		}
		h, err := rt.SubmitRestartJob(ctx, jobdb.SubmitRestartJobRequest{Job: req})
		must(err)
		run(ctx, rt, h.JobKey, worker)
	default:
		panic("unknown case")
	}
}
func run(ctx context.Context, rt *tracedRuntime, key jobdb.JobKey, w wf.JobWorker) {
	run, err := wf.GetJobForRun(ctx, rt, wf.GetJobForRunRequest{JobKey: key, JobWorker: w, TaskWorkers: []wf.TaskWorker{echoTask{}}, WorkerID: "worker", LeaseDuration: time.Minute})
	must(err)
	outcome, err := run.Run(nil)
	info, readErr := rt.GetJob(ctx, key)
	must(readErr)
	fmt.Printf("job=%s outcome=%s status=%s Complete=%d Reschedule=%d\n", key.JobId, outcome.Status, info.Status, rt.calls.complete.Load(), rt.calls.reschedule.Load())
	if err == nil {
		err = outcome.JobError
	}
	for e := err; e != nil; e = errors.Unwrap(e) {
		fmt.Printf("error %T: %v\n", e, e)
	}
	chapters, err := rt.ListChapters(ctx, jobdb.ListChaptersRequest{JobKey: key})
	must(err)
	for _, ch := range chapters {
		fmt.Printf("chapter %d %T\n", ch.Ordinal, ch.Body)
	}
}
func must(err error) {
	if err != nil {
		panic(err)
	}
}

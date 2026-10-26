package workflow_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/workflow"
)

// Changing a recorded task declaration must fail replay without changing history.
func TestExecutionHistoryConstraintsAcrossEngines(t *testing.T) {
	for _, backend := range []string{"toy", "direct"} {
		t.Run(backend, func(t *testing.T) {
			worker := &deterministicJob{}
			var engine workflow.Engine
			configure := func(b *workflow.EngineBuilder) {
				b.WithWorkerTenantId("test-tenant").PlusWorkers(worker, &incrementTask{})
			}
			if backend == "toy" {
				var cancel context.CancelFunc
				engine, cancel = buildToyEngine(t, configure)
				defer cancel()
			} else {
				dsn, stop := startEmbeddedPostgres(t)
				defer stop()
				if err := installPgjobdb(context.Background(), dsn); err != nil {
					t.Fatal(err)
				}
				uri, blobs := startChapterBlobStore(t)
				defer blobs.Shutdown()
				engine = buildDirectEngine(t, dsn, uri, configure)
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				go engine.Run(ctx)
			}
			ctx := context.Background()
			keyHandle, err := engine.SubmitJob(ctx, jobdb.SubmitJob{TenantId: "test-tenant", JobType: worker.Name(), Data: jobdb.NewTaskDataOrPanic(map[string]int{"n": 1})})
			key := keyHandle.JobKey
			if err != nil {
				t.Fatal(err)
			}
			waitForJobToComplete(t, engine, key)
			before, err := engine.GetJobRun(ctx, jobdb.GetJobRunRequest{JobKey: key, IncludeInputs: true, IncludeOutputs: true})
			if err != nil {
				t.Fatal(err)
			}
			if len(before.Execution) == 0 {
				t.Fatal("expected versioned execution history")
			}
			out, err := engine.ReplayJobRun(ctx, workflow.ReplayRunRequest{JobKey: key})
			if err != nil {
				t.Fatal(err)
			}
			raw, err := out.GetData()
			if err != nil {
				t.Fatal(err)
			}
			var data map[string]int
			if err := json.Unmarshal(raw, &data); err != nil {
				t.Fatal(err)
			}
			if data["n"] != 4 {
				t.Fatalf("unexpected replay result: %s", raw)
			}
			worker.version.Add(1)
			if _, err := engine.ReplayJobRun(ctx, workflow.ReplayRunRequest{JobKey: key}); !errors.Is(err, jobdb.ErrWorkflowNotDeterministic) {
				t.Fatalf("expected deterministic-history error, got %v", err)
			}
			after, err := engine.GetJobRun(ctx, jobdb.GetJobRunRequest{JobKey: key, IncludeInputs: true, IncludeOutputs: true})
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(before, after) {
				t.Fatal("replay changed recorded history")
			}
		})
	}
}

type deterministicJob struct{ version atomic.Uint32 }

func (*deterministicJob) Name() string { return "deterministic-job" }
func (j *deterministicJob) Run(ctx workflow.ExecutionContext, data jobdb.JobData) (jobdb.JobData, error) {
	result := data
	for i := 0; i < 3; i++ {
		out, err := ctx.DoTask(workflow.TaskCall{Command: []byte("{}"), Site: fmt.Sprintf("increment-%d", i), TaskType: "increment", ContractVersion: j.version.Load() + 1}, func(context.Context) (jobdb.TaskData, error) { return result, nil })
		if err != nil {
			return nil, err
		}
		result = out
	}
	return result, nil
}

type incrementTask struct{}

func (*incrementTask) Name() string { return "increment" }
func (*incrementTask) Run(_ workflow.TaskContext, input jobdb.TaskData) (jobdb.TaskData, error) {
	raw, err := input.GetData()
	if err != nil {
		return nil, err
	}
	var data map[string]int
	if err := json.Unmarshal(raw, &data); err != nil {
		return nil, err
	}
	data["n"]++
	return jobdb.NewTaskDataOrPanic(data), nil
}
func waitForJobToComplete(t *testing.T, engine workflow.Engine, key jobdb.JobKey) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		status, err := jobStatusForTest(engine, context.Background(), key)
		if err != nil {
			t.Fatal(err)
		}
		if status == jobdb.JobStatusCompleted || status == jobdb.JobStatusCancelled {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for job %v to complete", key)
}

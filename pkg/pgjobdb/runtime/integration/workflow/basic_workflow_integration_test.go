package workflow_test

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/workflow"
	_ "github.com/lib/pq"
)

// TestBasicWorkflowIntegration exercises the simplest end-to-end flow:
// one job worker shared across two engines, with tasks split between them.
func TestBasicWorkflowIntegration(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start embedded Postgres on a random high port to avoid conflicts.
	postgresDSN, stopPG := startEmbeddedPostgres(t)
	defer stopPG()
	if err := installPgjobdb(ctx, postgresDSN); err != nil {
		t.Fatalf("failed to install pgjobdb schema: %v", err)
	}

	blobStoreURI, blobs := startChapterBlobStore(t)
	defer blobs.Shutdown()

	tenantID := "test-tenant"
	logCapture := newCaptureHandler()
	logger := slog.New(logCapture)

	engine1 := buildDirectEngine(t, postgresDSN, blobStoreURI, func(b *workflow.EngineBuilder) {
		b.WithLogger(logger).WithWorkerTenantId(tenantID).PlusWorkers(pipeJob{}, addOneTask{})
	})

	engine2 := buildDirectEngine(t, postgresDSN, blobStoreURI, func(b *workflow.EngineBuilder) {
		b.WithLogger(logger).WithWorkerTenantId(tenantID).PlusWorkers(pipeJob{}, doubleTask{})
	})

	go engine1.Run(ctx)
	go engine2.Run(ctx)
	watcherResult := make(chan error, 1)
	go func() { watcherResult <- userInputWatcher(ctx, engine1, []string{tenantID}) }()
	defer func() {
		cancel()
		if err := <-watcherResult; err != nil {
			t.Error(err)
		}
	}()

	initial := jobdb.NewTaskDataOrPanic(map[string]interface{}{"n": 1})
	jobKeyHandle, err := engine1.SubmitJob(ctx, jobdb.SubmitJob{
		TenantId: tenantID,
		JobType:  pipeJobName,
		Data:     initial,
	})
	jobKey := jobKeyHandle.JobKey
	if err != nil {
		t.Fatalf("failed to start job: %v", err)
	}

	// Assert logical task outcomes independently of physical chapter layout.
	waitForJobToComplete(t, engine1, jobKey)
	history, err := engine1.GetJobRun(ctx, jobdb.GetJobRunRequest{JobKey: jobKey, IncludeOutputs: true})
	if err != nil {
		t.Fatal(err)
	}
	var outputs []int
	for _, entry := range history.Execution {
		r := entry.Record
		if r.Kind == "result" && strings.Contains(r.ID, "task:") && r.ErrorKind == "" && r.Output != nil {
			outputs = append(outputs, decodeNumber(t, r.Output.Data))
		}
	}
	expected := []int{2, 4, 7, 8, 16}
	if len(outputs) != len(expected) {
		t.Fatalf("task outputs: got %v want %v", outputs, expected)
	}
	for i, want := range expected {
		if outputs[i] != want {
			t.Fatalf("task %d: got %d want %d", i, outputs[i], want)
		}
	}

	if errs := logCapture.Errors(); errs > 0 {
		t.Fatalf("saw %d error log(s) during run", errs)
	}
}

// --- Workers used in the integration scenario ---

const pipeJobName = "pipe"

type pipeJob struct{}

func (pipeJob) Name() string { return pipeJobName }

func (pipeJob) Run(ctx workflow.ExecutionContext, data jobdb.JobData) (jobdb.JobData, error) {
	current := taskNumber(data)
	payload := jobdb.NewTaskDataOrPanic(map[string]interface{}{"n": current})
	if ctx.Logger() != nil {
		ctx.Logger().Info("starting job", "current", current)
	}

	steps := []string{addOneTaskName, doubleTaskName, userInputTaskName, addOneTaskName, doubleTaskName}
	var err error
	var out = payload
	for index, step := range steps {
		out, err = ctx.DoTask(workflow.TaskCall{Command: []byte("{}"), Site: fmt.Sprintf("step-%d", index), TaskType: step, ContractVersion: 1, Policy: jobdb.RunPolicy{}}, func(context.Context) (jobdb.TaskData, error) { return out, nil })
		if err != nil {
			return nil, err
		}
		if ctx.Logger() != nil {
			ctx.Logger().Info("completed task", "task", step, "value", taskNumber(out))
		}
	}
	return out, nil
}

const addOneTaskName = "t1"

type addOneTask struct{}

func (addOneTask) Name() string { return addOneTaskName }

func (addOneTask) Run(ctx workflow.TaskContext, input jobdb.TaskData) (jobdb.TaskData, error) {
	n := taskNumber(input)
	if ctx.Logger != nil {
		ctx.Logger.Info("t1 add", "input", n)
	}
	return jobdb.NewTaskDataOrPanic(map[string]interface{}{"n": n + 1}), nil
}

const doubleTaskName = "t2"

type doubleTask struct{}

func (doubleTask) Name() string { return doubleTaskName }

func (doubleTask) Run(ctx workflow.TaskContext, input jobdb.TaskData) (jobdb.TaskData, error) {
	n := taskNumber(input)
	if ctx.Logger != nil {
		ctx.Logger.Info("t2 double", "input", n)
	}
	return jobdb.NewTaskDataOrPanic(map[string]interface{}{"n": n * 2}), nil
}

const userInputTaskName = "userInput"

// userInputWatcher completes externally-handled tasks that no engine claims.
func userInputWatcher(ctx context.Context, engine workflow.Engine, tenantIDs []string) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		handles, err := engine.FindTasksWaitingForRoute(ctx, pipeJobName, userInputTaskName, tenantIDs)
		if err != nil {
			// If the database is shutting down or context will end soon, just back off.
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(100 * time.Millisecond):
				continue
			}
		}
		for _, h := range handles {
			data, err := h.Data()
			if err != nil {
				if ctx.Err() != nil {
					return nil
				}
				return fmt.Errorf("watcher failed to get data: %w", err)
			}
			n := taskNumber(data)
			output := jobdb.NewTaskDataOrPanic(map[string]interface{}{"n": n + 3})
			if err := h.Finish(ctx, output); err != nil {
				if ctx.Err() != nil {
					return nil
				}
				return fmt.Errorf("watcher failed to finish task: %w", err)
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func taskNumber(td jobdb.TaskData) int {
	data, err := td.GetData()
	if err != nil {
		return 0
	}
	var payload map[string]int
	if err := json.Unmarshal(data, &payload); err != nil {
		return 0
	}
	return payload["n"]
}

type captureHandler struct {
	mu      sync.Mutex
	records []slog.Record
}

func newCaptureHandler() *captureHandler {
	return &captureHandler{records: make([]slog.Record, 0)}
}

func (h *captureHandler) Enabled(context.Context, slog.Level) bool {
	return true
}

func (h *captureHandler) Handle(ctx context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.records = append(h.records, r.Clone())
	return nil
}

func (h *captureHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return h
}

func (h *captureHandler) WithGroup(name string) slog.Handler {
	return h
}

func (h *captureHandler) Errors() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	count := 0
	for _, r := range h.records {
		if r.Level >= slog.LevelError {
			count++
		}
	}
	return count
}

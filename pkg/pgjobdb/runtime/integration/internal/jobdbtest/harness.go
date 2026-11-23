package jobdbtest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/colony-2/pgjobdb/internal/projectfixture"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb/celltest"

	"github.com/colony-2/jobdb/pkg/jobdb"
	remoteruntime "github.com/colony-2/jobdb/pkg/jobdb/runtime/remote"
	sqliteruntime "github.com/colony-2/jobdb/pkg/jobdb/runtime/sqlite"
	toyruntime "github.com/colony-2/jobdb/pkg/jobdb/runtime/toy"
	"github.com/colony-2/jobdb/pkg/workflow"
	directruntime "github.com/colony-2/pgjobdb/pkg/pgjobdb/runtime"
)

type RuntimeHarness struct {
	Name                   string
	SupportsLeases         bool
	SupportsRuntimeStorage bool
	StartsWorkerLoop       bool
	New                    func(t *testing.T, workers ...workflow.WorkSet) *BuiltRuntimeHarness
}

type BuiltRuntimeHarness struct {
	Name           string
	Runtime        jobdb.ConsumerRuntime
	Engine         workflow.Engine
	WorkerTenantID string

	cancel   context.CancelFunc
	runDone  <-chan struct{}
	shutdown func()
}

var externalHarnessSeq atomic.Uint64

func (h *BuiltRuntimeHarness) Shutdown(t *testing.T) {
	t.Helper()
	if h == nil {
		return
	}
	if h.cancel != nil {
		h.cancel()
	}
	if h.runDone != nil {
		select {
		case <-h.runDone:
		case <-time.After(5 * time.Second):
			t.Fatalf("%s engine did not stop after cancellation", h.Name)
		}
	}
	if h.shutdown != nil {
		h.shutdown()
	}
}

func BuiltInRuntimeHarnesses() []RuntimeHarness {
	harnesses := []RuntimeHarness{
		{
			Name:                   "toy",
			SupportsLeases:         false,
			SupportsRuntimeStorage: true,
			StartsWorkerLoop:       true,
			New:                    newToyHarness,
		},
		{
			Name:                   "sqlite",
			SupportsLeases:         true,
			SupportsRuntimeStorage: true,
			StartsWorkerLoop:       true,
			New:                    newSQLiteHarness,
		},
		{
			Name:                   "direct",
			SupportsLeases:         true,
			SupportsRuntimeStorage: true,
			StartsWorkerLoop:       true,
			New:                    newDirectHarness,
		},
	}
	if external, ok := externalRemoteRuntimeHarness(); ok {
		if externalOnlyHarnessesEnabled() {
			return []RuntimeHarness{external}
		}
		harnesses = append(harnesses, external)
	}
	return harnesses
}

func RemoteRuntimeHarnesses() []RuntimeHarness {
	harnesses := []RuntimeHarness{
		{
			Name:                   "remote-toy",
			SupportsLeases:         false,
			SupportsRuntimeStorage: true,
			StartsWorkerLoop:       true,
			New:                    newRemoteToyHarness,
		},
		{
			Name:                   "remote-sqlite",
			SupportsLeases:         true,
			SupportsRuntimeStorage: true,
			StartsWorkerLoop:       true,
			New:                    newRemoteSQLiteHarness,
		},
		{
			Name:                   "remote-direct",
			SupportsLeases:         true,
			SupportsRuntimeStorage: true,
			StartsWorkerLoop:       true,
			New:                    newRemoteDirectHarness,
		},
	}
	if external, ok := externalRemoteRuntimeHarness(); ok {
		if externalOnlyHarnessesEnabled() {
			return []RuntimeHarness{external}
		}
		harnesses = append(harnesses, external)
	}
	return harnesses
}

func MustWorkSet(t *testing.T, job workflow.JobWorker, tasks ...workflow.TaskWorker) workflow.WorkSet {
	t.Helper()
	ws, err := workflow.AsWorkSet(job, tasks...)
	if err != nil {
		t.Fatalf("build workset: %v", err)
	}
	return *ws
}

func MustWorkSetWithOptions(t *testing.T, job workflow.JobWorker, opts workflow.WorkRegistrationOptions, tasks ...workflow.TaskWorker) workflow.WorkSet {
	t.Helper()
	ws, err := workflow.AsWorkSetWithOptions(job, opts, tasks...)
	if err != nil {
		t.Fatalf("build workset: %v", err)
	}
	return *ws
}

func WaitForEngineStatus(t *testing.T, ctx context.Context, engine workflow.Engine, jobKey jobdb.JobKey, want jobdb.JobStatus) {
	t.Helper()
	waitForStatus(t, ctx, func(ctx context.Context) (jobdb.JobStatus, error) {
		job, err := engine.GetJob(ctx, jobKey)
		return job.Status, err
	}, want)
}

func WaitForRuntimeStatus(t *testing.T, ctx context.Context, runtime interface {
	GetJob(context.Context, jobdb.JobKey) (jobdb.JobInfo, error)
}, jobKey jobdb.JobKey, want jobdb.JobStatus) {
	t.Helper()
	waitForStatus(t, ctx, func(ctx context.Context) (jobdb.JobStatus, error) {
		job, err := runtime.GetJob(ctx, jobKey)
		return job.Status, err
	}, want)
}

func WaitForTaskHandle(t *testing.T, ctx context.Context, engine workflow.Engine, jobType string, taskType string, tenantIDs []string) workflow.TaskHandle {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		handles, err := engine.FindTasksWaitingForRoute(ctx, jobType, taskType, tenantIDs)
		if err == nil && len(handles) > 0 {
			return handles[0]
		}
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("find waiting tasks: %v", err)
		}
		select {
		case <-ctx.Done():
			t.Fatalf("wait for task handle: %v", ctx.Err())
		case <-time.After(20 * time.Millisecond):
		}
	}
	t.Fatalf("timed out waiting for %s:%s task handle", jobType, taskType)
	return nil
}

func MustDecodeNumberTaskData(t *testing.T, data jobdb.TaskData) int {
	t.Helper()
	if data == nil {
		t.Fatal("missing task data")
	}
	raw, err := data.GetData()
	if err != nil {
		t.Fatalf("get task data: %v", err)
	}
	return decodeNumber(t, raw)
}

func MustDecodeNumberTaskIO(t *testing.T, data *jobdb.TaskIO) int {
	t.Helper()
	if data == nil {
		t.Fatal("missing task io")
	}
	return decodeNumber(t, data.Data)
}

func MustStartJobAsync(t *testing.T, engine workflow.Engine, start jobdb.SubmitJob) <-chan error {
	t.Helper()
	done := make(chan error, 1)
	go func() {
		_, err := engine.SubmitJob(celltest.Context(context.Background()), start)
		done <- err
	}()
	return done
}

func ExpectJobTypeFromNextRoute(t *testing.T, nextRoute *jobdb.Route, wantJobType string) {
	t.Helper()
	if nextRoute == nil {
		t.Fatal("missing runtime next_need")
	}
	if got := nextRoute.JobType; got != wantJobType {
		t.Fatalf("unexpected next_need job type: got %q want %q", got, wantJobType)
	}
}

func ExpectTaskSuffix(t *testing.T, value string, wantSuffix string) {
	t.Helper()
	if !strings.HasSuffix(value, wantSuffix) {
		t.Fatalf("unexpected task value %q, want suffix %q", value, wantSuffix)
	}
}

func NumberTaskData(n int) jobdb.TaskData {
	return jobdb.NewTaskDataOrPanic(map[string]int{"n": n})
}

const (
	SequenceJobName = "seq"
	AddOneTaskName  = "add"
	DoubleTaskName  = "double"
	MissingTaskName = "missing"
	FailingJobName  = "failing"
)

type SequenceJob struct {
	Steps []string
}

func (SequenceJob) Name() string { return SequenceJobName }

func (j SequenceJob) Run(ctx workflow.ExecutionContext, data jobdb.JobData) (jobdb.JobData, error) {
	var (
		out jobdb.TaskData = data
		err error
	)
	for index, step := range j.Steps {
		out, err = ctx.DoTask(workflow.TaskCall{Command: []byte("{}"), Site: fmt.Sprintf("step-%d", index), TaskType: step, ContractVersion: 1, Policy: jobdb.RunPolicy{}}, func(context.Context) (jobdb.TaskData, error) { return out, nil })
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}

type AddOneTask struct{}

func (AddOneTask) Name() string { return AddOneTaskName }

func (AddOneTask) Run(_ workflow.TaskContext, input jobdb.TaskData) (jobdb.TaskData, error) {
	n, err := numberFromTaskData(input)
	if err != nil {
		return nil, err
	}
	return NumberTaskData(n + 1), nil
}

type DoubleTask struct{}

func (DoubleTask) Name() string { return DoubleTaskName }

func (DoubleTask) Run(_ workflow.TaskContext, input jobdb.TaskData) (jobdb.TaskData, error) {
	n, err := numberFromTaskData(input)
	if err != nil {
		return nil, err
	}
	return NumberTaskData(n * 2), nil
}

type FailingJob struct{}

func (FailingJob) Name() string { return FailingJobName }

func (FailingJob) Run(_ workflow.ExecutionContext, _ jobdb.JobData) (jobdb.JobData, error) {
	return nil, errors.New("intentional failure")
}

func decodeNumber(t *testing.T, raw json.RawMessage) int {
	t.Helper()
	payload := map[string]int{}
	if err := json.Unmarshal(raw, &payload); err != nil {
		t.Fatalf("decode numeric payload: %v", err)
	}
	return payload["n"]
}

func waitForStatus(t *testing.T, ctx context.Context, check func(context.Context) (jobdb.JobStatus, error), want jobdb.JobStatus) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	var lastStatus jobdb.JobStatus
	var lastErr error
	for time.Now().Before(deadline) {
		status, err := check(ctx)
		lastStatus, lastErr = status, err
		if err == nil && status == want {
			return
		}
		if err != nil && !errors.Is(err, jobdb.ErrJobNotFound) && !errors.Is(err, context.Canceled) {
			t.Fatalf("check status: %v", err)
		}
		select {
		case <-ctx.Done():
			t.Fatalf("wait for status: %v", ctx.Err())
		case <-time.After(20 * time.Millisecond):
		}
	}
	t.Fatalf("job did not reach status %s (last status %s, last error %v)", want, lastStatus, lastErr)
}

func newToyHarness(t *testing.T, workers ...workflow.WorkSet) *BuiltRuntimeHarness {
	t.Helper()
	runtime := toyruntime.New(toyruntime.WithProjects(celltest.UnrestrictedProjects{}))
	return buildHarness(t, "toy", newNamespacedStorage(runtime, ""), true, func() {}, workers...)
}

func newDirectHarness(t *testing.T, workers ...workflow.WorkSet) *BuiltRuntimeHarness {
	t.Helper()
	embedded, err := directruntime.StartEmbeddedRuntime(celltest.Context(context.Background()), projectfixture.Prepare)
	if err != nil {
		t.Fatalf("start embedded direct runtime: %v", err)
	}
	return buildHarness(t, "direct", NewProjectBackend(embedded.Runtime, ""), true, embedded.Shutdown, workers...)
}

func newSQLiteHarness(t *testing.T, workers ...workflow.WorkSet) *BuiltRuntimeHarness {
	t.Helper()
	embedded, err := sqliteruntime.StartEmbeddedRuntime(celltest.Context(context.Background()), sqliteruntime.WithProjects(celltest.UnrestrictedProjects{}))
	if err != nil {
		t.Fatalf("start embedded sqlite runtime: %v", err)
	}
	return buildHarness(t, "sqlite", newNamespacedDevelopmentBackend(embedded.Runtime, ""), true, embedded.Shutdown, workers...)
}

func newRemoteToyHarness(t *testing.T, workers ...workflow.WorkSet) *BuiltRuntimeHarness {
	t.Helper()
	underlying := toyruntime.New(toyruntime.WithProjects(celltest.UnrestrictedProjects{}))
	server := httptest.NewServer(celltest.Handler(remoteruntime.NewConsumerServer(newNamespacedStorage(underlying, ""))))
	runtime, err := remoteruntime.New(server.URL, server.Client())
	if err != nil {
		server.Close()
		t.Fatalf("build remote toy runtime: %v", err)
	}
	return buildHarness(t, "remote-toy", NewNamespacedClient(runtime, ""), true, server.Close, workers...)
}

func newRemoteDirectHarness(t *testing.T, workers ...workflow.WorkSet) *BuiltRuntimeHarness {
	t.Helper()
	embedded, err := directruntime.StartEmbeddedRuntime(celltest.Context(context.Background()), projectfixture.Prepare)
	if err != nil {
		t.Fatalf("start embedded direct runtime: %v", err)
	}
	server := httptest.NewServer(celltest.Handler(remoteruntime.NewConsumerServer(embedded.Runtime)))
	runtime, err := remoteruntime.New(server.URL, server.Client())
	if err != nil {
		server.Close()
		embedded.Shutdown()
		t.Fatalf("build remote direct runtime: %v", err)
	}
	shutdown := func() {
		server.Close()
		embedded.Shutdown()
	}
	return buildHarness(t, "remote-direct", NewProjectClient(runtime, embedded.Runtime, ""), true, shutdown, workers...)
}

func newRemoteSQLiteHarness(t *testing.T, workers ...workflow.WorkSet) *BuiltRuntimeHarness {
	t.Helper()
	embedded, err := sqliteruntime.StartEmbeddedRuntime(celltest.Context(context.Background()), sqliteruntime.WithProjects(celltest.UnrestrictedProjects{}))
	if err != nil {
		t.Fatalf("start embedded sqlite runtime: %v", err)
	}
	server := httptest.NewServer(celltest.Handler(remoteruntime.NewConsumerServer(newNamespacedDevelopmentBackend(embedded.Runtime, ""))))
	runtime, err := remoteruntime.New(server.URL, server.Client())
	if err != nil {
		server.Close()
		embedded.Shutdown()
		t.Fatalf("build remote sqlite runtime: %v", err)
	}
	shutdown := func() {
		server.Close()
		embedded.Shutdown()
	}
	return buildHarness(t, "remote-sqlite", NewNamespacedClient(runtime, ""), true, shutdown, workers...)
}

func newExternalRemoteHarness(t *testing.T, workers ...workflow.WorkSet) *BuiltRuntimeHarness {
	t.Helper()
	baseURL, name := externalRemoteRuntimeConfig()
	runtime, err := remoteruntime.New(baseURL, &http.Client{Timeout: 30 * time.Second})
	if err != nil {
		t.Fatalf("build %s runtime: %v", name, err)
	}
	prefix := fmt.Sprintf("__jobdbtest_external_%d__", externalHarnessSeq.Add(1))
	return buildHarness(t, name, NewNamespacedClient(runtime, prefix), true, func() {}, workers...)
}

func buildHarness(t *testing.T, name string, runtime jobdb.ConsumerRuntime, startLoop bool, shutdown func(), workers ...workflow.WorkSet) *BuiltRuntimeHarness {
	t.Helper()
	builder := workflow.NewEngineBuilder().WithRuntime(runtime)
	workerTenantID := "tenant-worker-" + name
	if len(workers) > 0 {
		builder.WithWorkerTenantId(workerTenantID)
	}
	for _, ws := range workers {
		tasks := make([]workflow.TaskWorker, 0, len(ws.TaskWorkers))
		for _, task := range ws.TaskWorkers {
			tasks = append(tasks, task)
		}
		builder.PlusWorkersWithOptions(ws.JobWorker, ws.Options, tasks...)
	}
	engine, err := builder.BuildEngine()
	if err != nil {
		shutdown()
		t.Fatalf("build %s engine: %v", name, err)
	}
	// Register fixtures at the submission surface. A mandatory-only runtime
	// wrapper here would hide schema registration and task delegation from the
	// engine, even though the actual consumer supports them.
	engine = fixtureCellEngine{Engine: engine, cells: runtime}

	var (
		cancel  context.CancelFunc
		runDone <-chan struct{}
	)
	if startLoop && len(workers) > 0 {
		runCtx, runCancel := context.WithCancel(celltest.Context(context.Background()))
		cancel = runCancel
		done := make(chan struct{})
		runDone = done
		go func() {
			defer close(done)
			engine.Run(runCtx)
		}()
	}

	return &BuiltRuntimeHarness{
		Name:           name,
		Runtime:        runtime,
		Engine:         engine,
		WorkerTenantID: workerTenantID,
		cancel:         cancel,
		runDone:        runDone,
		shutdown:       shutdown,
	}
}

type fixtureCellEngine struct {
	workflow.Engine
	cells jobdb.CellRuntime
}

func (e fixtureCellEngine) SubmitJob(ctx context.Context, q jobdb.SubmitJob) (jobdb.JobHandle, error) {
	if q.TenantId != "" && q.Cell != "" && q.CellRepo != "" {
		_, err := e.cells.CreateCell(ctx, jobdb.CreateCellRequest{TenantID: q.TenantId, Name: q.Cell, Repo: q.CellRepo})
		if err != nil && !errors.Is(err, jobdb.ErrCellConflict) {
			return jobdb.JobHandle{}, err
		}
	}
	return e.Engine.SubmitJob(ctx, q)
}

func numberFromTaskData(data jobdb.TaskData) (int, error) {
	if data == nil {
		return 0, fmt.Errorf("missing task data")
	}
	raw, err := data.GetData()
	if err != nil {
		return 0, err
	}
	payload := map[string]int{}
	if err := json.Unmarshal(raw, &payload); err != nil {
		return 0, err
	}
	return payload["n"], nil
}

func externalRemoteRuntimeHarness() (RuntimeHarness, bool) {
	baseURL, name := externalRemoteRuntimeConfig()
	if baseURL == "" {
		return RuntimeHarness{}, false
	}
	return RuntimeHarness{
		Name:                   name,
		SupportsLeases:         true,
		SupportsRuntimeStorage: true,
		StartsWorkerLoop:       true,
		New:                    newExternalRemoteHarness,
	}, true
}

func externalRemoteRuntimeConfig() (baseURL string, name string) {
	baseURL = strings.TrimSpace(os.Getenv("JOBDB_EXTERNAL_REMOTE_BASE_URL"))
	name = strings.TrimSpace(os.Getenv("JOBDB_EXTERNAL_REMOTE_NAME"))
	if name == "" {
		name = "remote-worker"
	}
	return baseURL, name
}

func externalOnlyHarnessesEnabled() bool {
	switch strings.TrimSpace(strings.ToLower(os.Getenv("JOBDB_EXTERNAL_REMOTE_ONLY"))) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}

func ExpectTaskType(t *testing.T, route jobdb.Route, want string) {
	t.Helper()
	if route.TaskType != want {
		t.Fatalf("task type = %q; want %q", route.TaskType, want)
	}
}

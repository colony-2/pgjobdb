package workflow_test

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/workflow"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestArtifactStorageOnTaskError verifies that artifacts are stored even when a task errors
func TestArtifactStorageOnTaskError(t *testing.T) {
	t.Run("task error artifacts are stored and retrievable", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// Start embedded Postgres
		postgresDSN, stopPG := startEmbeddedPostgres(t)
		defer stopPG()
		if err := installPgjobdb(ctx, postgresDSN); err != nil {
			t.Fatalf("failed to install pgjobdb schema: %v", err)
		}

		blobStoreURI, blobs := startChapterBlobStore(t)
		defer blobs.Shutdown()

		// Create a temporary file artifact for error diagnostics
		tempDir := t.TempDir()
		errorLogFile := filepath.Join(tempDir, "error-diagnostics.log")
		diagnosticData := []byte("Error occurred at line 42\nStack trace: ...\nState: processing")
		err := os.WriteFile(errorLogFile, diagnosticData, 0644)
		require.NoError(t, err)

		// Track cleanup of error artifact
		var cleanupCalled atomic.Bool
		errorArtifact := jobdb.NewArtifact("error-diagnostics.log", func() (io.ReadCloser, int64, error) {
			f, err := os.Open(errorLogFile)
			if err != nil {
				return nil, 0, err
			}
			info, _ := f.Stat()
			return f, info.Size(), nil
		}, func() error {
			cleanupCalled.Store(true)
			return os.Remove(errorLogFile)
		})

		// Job that calls a task which errors with artifacts
		jobWorker := &taskErrorWithArtifactsJob{artifact: errorArtifact}
		taskWorker := &errorWithArtifactsTask{artifact: errorArtifact}

		// Build engine with the job and task worker
		engine := buildDirectEngine(t, postgresDSN, blobStoreURI, func(b *workflow.EngineBuilder) {
			b.WithWorkerTenantId("test-tenant").PlusWorkers(jobWorker, taskWorker)
		})

		// Run engine in background
		go engine.Run(ctx)

		// Start the job
		jobKeyHandle, err := engine.SubmitJob(ctx, jobdb.SubmitJob{
			TenantId: "test-tenant",
			JobType:  jobWorker.Name(),
			Data:     jobdb.NewTaskDataOrPanic(map[string]string{"input": "data"}),
		})
		jobKey := jobKeyHandle.JobKey
		require.NoError(t, err)

		// Wait for job to complete (with error)
		require.Eventually(t, func() bool {
			status, _ := jobStatusForTest(engine, ctx, jobKey)
			return status == jobdb.JobStatusCompleted
		}, 30*time.Second, 200*time.Millisecond)

		// Verify cleanup was called
		assert.True(t, cleanupCalled.Load(), "error artifact cleanup should be called")

		// Verify file was cleaned up
		_, err = os.Stat(errorLogFile)
		assert.True(t, os.IsNotExist(err), "error artifact file should be cleaned up after storage")

		attempts := executionErrorArtifacts(t, engine, jobKey, true)
		require.Len(t, attempts, 1)
		artifacts := attempts[0]
		require.Len(t, artifacts, 1)
		assert.Equal(t, "error-diagnostics.log", artifacts[0].Name)
		storedData := readExecutionErrorArtifact(t, engine, jobKey.TenantId, artifacts[0])
		assert.Equal(t, diagnosticData, storedData, "stored artifact data should match original")
	})

	t.Run("task error artifacts are stored on retry attempts", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// Start embedded Postgres
		postgresDSN, stopPG := startEmbeddedPostgres(t)
		defer stopPG()
		if err := installPgjobdb(ctx, postgresDSN); err != nil {
			t.Fatalf("failed to install pgjobdb schema: %v", err)
		}

		blobStoreURI, blobs := startChapterBlobStore(t)
		defer blobs.Shutdown()

		// Create a single artifact that will be attached on each retry
		tempDir := t.TempDir()
		logFile := filepath.Join(tempDir, "retry-error.log")
		logData := []byte("Task failed with retry")
		err := os.WriteFile(logFile, logData, 0644)
		require.NoError(t, err)

		artifact := jobdb.NewArtifact("retry-error.log", func() (io.ReadCloser, int64, error) {
			f, err := os.Open(logFile)
			if err != nil {
				return nil, 0, err
			}
			info, _ := f.Stat()
			return f, info.Size(), nil
		}, func() error {
			return nil // Don't delete file so it can be reused
		})

		// Job that calls a task which fails with retry
		jobWorker := &simpleRetryErrorJob{artifact: artifact}
		taskWorker := &simpleRetryErrorTask{artifact: artifact}

		// Build engine with retry policy
		engine := buildDirectEngine(t, postgresDSN, blobStoreURI, func(b *workflow.EngineBuilder) {
			b.WithWorkerTenantId("test-tenant").PlusWorkers(jobWorker, taskWorker)
		})

		// Run engine in background
		go engine.Run(ctx)

		// Start the job with retry policy
		jobKeyHandle, err := engine.SubmitJob(ctx, jobdb.SubmitJob{
			TenantId: "test-tenant",
			JobType:  jobWorker.Name(),
			Data:     jobdb.NewTaskDataOrPanic(map[string]string{"input": "data"}),
			RunPolicy: jobdb.RunPolicy{
				Retry: jobdb.RetryPolicy{
					MaximumAttempts:    3,
					InitialInterval:    jobdb.Duration(100 * time.Millisecond),
					MaximumInterval:    jobdb.Duration(500 * time.Millisecond),
					BackoffCoefficient: 2.0,
				},
			},
		})
		jobKey := jobKeyHandle.JobKey
		require.NoError(t, err)

		// Wait for job to complete (after all retries fail)
		require.Eventually(t, func() bool {
			status, _ := jobStatusForTest(engine, ctx, jobKey)
			return status == jobdb.JobStatusCompleted
		}, 30*time.Second, 200*time.Millisecond)

		attempts := executionErrorArtifacts(t, engine, jobKey, true)
		require.Len(t, attempts, 3, "all failed attempts retain their diagnostics")
		for i, artifacts := range attempts {
			require.Len(t, artifacts, 1, fmt.Sprintf("attempt %d", i+1))
			assert.Equal(t, "retry-error.log", artifacts[0].Name)
			assert.Equal(t, logData, readExecutionErrorArtifact(t, engine, jobKey.TenantId, artifacts[0]))
		}
	})
}

// TestArtifactStorageOnJobError verifies that artifacts are stored even when a job errors
func TestArtifactStorageOnJobError(t *testing.T) {
	t.Run("job error artifacts are stored and retrievable", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// Start embedded Postgres
		postgresDSN, stopPG := startEmbeddedPostgres(t)
		defer stopPG()
		if err := installPgjobdb(ctx, postgresDSN); err != nil {
			t.Fatalf("failed to install pgjobdb schema: %v", err)
		}

		blobStoreURI, blobs := startChapterBlobStore(t)
		defer blobs.Shutdown()

		// Create artifact for job-level error
		tempDir := t.TempDir()
		jobErrorFile := filepath.Join(tempDir, "job-error.log")
		jobErrorData := []byte("Job failed during execution\nPartial results: {...}\nError: validation failed")
		err := os.WriteFile(jobErrorFile, jobErrorData, 0644)
		require.NoError(t, err)

		var cleanupCalled atomic.Bool
		jobErrorArtifact := jobdb.NewArtifact("job-error.log", func() (io.ReadCloser, int64, error) {
			f, err := os.Open(jobErrorFile)
			if err != nil {
				return nil, 0, err
			}
			info, _ := f.Stat()
			return f, info.Size(), nil
		}, func() error {
			cleanupCalled.Store(true)
			return os.Remove(jobErrorFile)
		})

		// Job that errors with artifacts
		jobWorker := &errorJobWithArtifacts{artifact: jobErrorArtifact}

		// Build engine
		engine := buildDirectEngine(t, postgresDSN, blobStoreURI, func(b *workflow.EngineBuilder) {
			b.WithWorkerTenantId("test-tenant").PlusWorkers(jobWorker)
		})

		// Run engine in background
		go engine.Run(ctx)

		// Start the job
		jobKeyHandle, err := engine.SubmitJob(ctx, jobdb.SubmitJob{
			TenantId: "test-tenant",
			JobType:  jobWorker.Name(),
			Data:     jobdb.NewTaskDataOrPanic(map[string]string{"input": "data"}),
		})
		jobKey := jobKeyHandle.JobKey
		require.NoError(t, err)

		// Wait for job to complete (with error)
		require.Eventually(t, func() bool {
			status, _ := jobStatusForTest(engine, ctx, jobKey)
			return status == jobdb.JobStatusCompleted
		}, 30*time.Second, 200*time.Millisecond)

		// Verify cleanup was called
		assert.True(t, cleanupCalled.Load(), "job error artifact cleanup should be called")

		attempts := executionErrorArtifacts(t, engine, jobKey, false)
		require.Len(t, attempts, 1)
		artifacts := attempts[0]
		require.Len(t, artifacts, 1)
		assert.Equal(t, "job-error.log", artifacts[0].Name)
		storedData := readExecutionErrorArtifact(t, engine, jobKey.TenantId, artifacts[0])
		assert.Equal(t, jobErrorData, storedData, "stored job error artifact should match original")
	})
}

// Test workers for artifact storage on error

// taskErrorWithArtifactsJob calls a task that errors with artifacts
type taskErrorWithArtifactsJob struct {
	artifact jobdb.Artifact
}

func (j *taskErrorWithArtifactsJob) Name() string { return "task-error-with-artifacts-job" }

func (j *taskErrorWithArtifactsJob) Run(ctx workflow.ExecutionContext, input jobdb.JobData) (jobdb.JobData, error) {
	return ctx.DoTask(workflow.TaskCall{Command: []byte("{}"), Site: "error-with-artifacts", TaskType: "error-with-artifacts", ContractVersion: 1, Policy: jobdb.RunPolicy{}}, func(context.Context) (jobdb.TaskData, error) { return input, nil })
}

// errorWithArtifactsTask returns an error along with diagnostic artifacts
type errorWithArtifactsTask struct {
	artifact jobdb.Artifact
}

func (t *errorWithArtifactsTask) Name() string { return "error-with-artifacts" }

func (t *errorWithArtifactsTask) Run(ctx workflow.TaskContext, input jobdb.TaskData) (jobdb.TaskData, error) {
	// Return both an error and artifacts (diagnostic information)
	output := &jobdb.SimpleTaskData{
		Data:      []byte(`{"status":"failed","reason":"validation error"}`),
		Artifacts: []jobdb.Artifact{t.artifact},
	}
	return output, jobdb.AppError{Payload: jobdb.AppErrorPayload{Message: "task failed with diagnostics", Level: "error"}}
}

// simpleRetryErrorJob calls a task that fails with retry
type simpleRetryErrorJob struct {
	artifact jobdb.Artifact
}

func (j *simpleRetryErrorJob) Name() string { return "simple-retry-error-job" }

func (j *simpleRetryErrorJob) Run(ctx workflow.ExecutionContext, input jobdb.JobData) (jobdb.JobData, error) {
	return ctx.DoTask(workflow.TaskCall{Command: []byte("{}"), Site: "simple-retry-error", TaskType: "simple-retry-error", ContractVersion: 1, Policy: jobdb.RunPolicy{}}, func(context.Context) (jobdb.TaskData, error) { return input, nil })
}

// simpleRetryErrorTask always fails with the same artifact
type simpleRetryErrorTask struct {
	artifact jobdb.Artifact
}

func (t *simpleRetryErrorTask) Name() string { return "simple-retry-error" }

func (t *simpleRetryErrorTask) Run(ctx workflow.TaskContext, input jobdb.TaskData) (jobdb.TaskData, error) {
	output := &jobdb.SimpleTaskData{
		Data:      []byte(`{"status":"failed"}`),
		Artifacts: []jobdb.Artifact{t.artifact},
	}
	return output, jobdb.AppError{Payload: jobdb.AppErrorPayload{Message: "task failed", Level: "error"}}
}

// errorJobWithArtifacts is a job that errors with artifacts
type errorJobWithArtifacts struct {
	artifact jobdb.Artifact
}

func (j *errorJobWithArtifacts) Name() string { return "error-job-with-artifacts" }

func (j *errorJobWithArtifacts) Run(ctx workflow.ExecutionContext, input jobdb.JobData) (jobdb.JobData, error) {
	// Return both error and artifacts at job level
	output := &jobdb.SimpleTaskData{
		Data:      []byte(`{"status":"job failed","partial_results":"..."}`),
		Artifacts: []jobdb.Artifact{j.artifact},
	}
	return output, jobdb.AppError{Payload: jobdb.AppErrorPayload{Message: "job failed with artifacts", Level: "error"}}
}

func executionErrorArtifacts(t *testing.T, engine workflow.Engine, key jobdb.JobKey, task bool) [][]jobdb.ExecutionArtifact {
	t.Helper()
	history, err := engine.GetJobRun(context.Background(), jobdb.GetJobRunRequest{JobKey: key, IncludeOutputs: true, IncludeArtifacts: true})
	require.NoError(t, err)
	var artifacts [][]jobdb.ExecutionArtifact
	for _, entry := range history.Execution {
		r := entry.Record
		if r.Kind == "result" && r.ErrorKind != "" && strings.Contains(r.ID, "task:") == task {
			require.NotNil(t, r.Output)
			artifacts = append(artifacts, r.Output.Artifacts)
		}
	}
	return artifacts
}
func readExecutionErrorArtifact(t *testing.T, source any, tenant string, artifact jobdb.ExecutionArtifact) []byte {
	t.Helper()
	require.NotNil(t, artifact.Key)
	key := artifact.Key
	reader, err := mustArtifactOpener(t, source).OpenArtifact(context.Background(), jobdb.ArtifactRef{JobKey: jobdb.JobKey{TenantId: tenant, JobId: key.JobId}, Ordinal: key.TaskOrdinal, Name: key.Name})
	require.NoError(t, err)
	stream, err := reader.Open()
	require.NoError(t, err)
	defer stream.Close()
	data, err := io.ReadAll(stream)
	require.NoError(t, err)
	return data
}

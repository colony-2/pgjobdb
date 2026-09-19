package integration_test

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	chapterpostgres "github.com/colony-2/jobdb/pkg/jobdb/chapterstore/postgres"
	runtimecore "github.com/colony-2/jobdb/pkg/jobdb/runtime/core"
	schemapostgres "github.com/colony-2/jobdb/pkg/jobdb/schemastore/postgres"
	"github.com/colony-2/pgjobdb/pkg/pgjobdb"
	"github.com/colony-2/pgjobdb/pkg/pgjobdb/runtime/internal/runtimeadapter"
)

func TestNativeRuntimeCoreLeaseRoutesAndCompletes(t *testing.T) {
	runDatabaseTest(t, func(ctx context.Context, db *sql.DB) {
		chapters, err := chapterpostgres.NewSQLDB(ctx, db, chapterpostgres.Config{
			BlobStoreURI: "blobfs://" + t.TempDir(),
		})
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = chapters.Close(ctx) }()
		schemas, err := schemapostgres.NewSQLDB(ctx, db)
		if err != nil {
			t.Fatal(err)
		}
		runtime, err := runtimecore.NewRuntime(runtimecore.Config{
			Scheduler: runtimeadapter.Scheduler{DB: db}, Chapters: chapters,
			Schemas: schemas,
		})
		if err != nil {
			t.Fatal(err)
		}
		data, err := jobdb.NewTaskData(map[string]any{"item": 1})
		if err != nil {
			t.Fatal(err)
		}
		root, err := runtime.SubmitJob(ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{
			TenantId: "tenant", JobID: "root", JobType: "collect", Data: data, RunPolicy: jobdb.RunPolicy{Retry: jobdb.RetryPolicy{MaximumAttempts: 3}},
		}})
		if err != nil {
			t.Fatal(err)
		}
		lease, err := runtime.GetJobLease(ctx, jobdb.GetJobLeaseRequest{
			JobKey: root.JobKey, WorkerID: "worker", Routes: []jobdb.Route{{JobType: "collect"}},
		})
		if err != nil || lease == nil || lease.Route() != (jobdb.Route{JobType: "collect"}) {
			t.Fatalf("lease root = %+v, %v", lease, err)
		}
		if lease.ExecutionState().RunPolicy.Retry.MaximumAttempts != 3 {
			t.Fatalf("lease state = %+v", lease.ExecutionState())
		}

		child, err := lease.SubmitJob(ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{
			TenantId: "tenant", JobID: "child", JobType: "collect", Data: data,
		}})
		if err != nil || child.JobKey.JobId != "child" {
			t.Fatalf("submit child = %+v, %v", child, err)
		}
		childRow, err := pgjobdb.GetJob(ctx, db, "tenant", "child")
		if err != nil || childRow.ParentJobID != "root" {
			t.Fatalf("child parent = %+v, %v", childRow, err)
		}
		if err := lease.Reschedule(ctx, jobdb.RescheduleExecutionRequest{
			NextRoute:      jobdb.Route{JobType: "collect", TaskType: "download"},
			TaskWait:       &jobdb.TaskWait{InputOrdinal: 0, OutputOrdinal: 1, ResumeJobType: "collect", InputHash: "sha256:input"},
			AlternateRoute: &jobdb.Route{JobType: "collect"}, AlternateAfter: durationPtr(10 * time.Second),
		}); err != nil {
			t.Fatalf("route external task: %v", err)
		}
		taskLease, err := runtime.GetJobLease(ctx, jobdb.GetJobLeaseRequest{
			JobKey: root.JobKey, WorkerID: "task-worker",
			Routes: []jobdb.Route{{JobType: "collect", TaskType: "download"}},
		})
		if err != nil || taskLease == nil || taskLease.Route() != (jobdb.Route{JobType: "collect", TaskType: "download"}) {
			t.Fatalf("task lease = %+v, %v", taskLease, err)
		}
		if err := taskLease.Reschedule(ctx, jobdb.RescheduleExecutionRequest{
			NextRoute: jobdb.Route{JobType: "collect"},
		}); err != nil {
			t.Fatalf("resume job route: %v", err)
		}
		jobLease, err := runtime.GetJobLease(ctx, jobdb.GetJobLeaseRequest{
			JobKey: root.JobKey, WorkerID: "worker", Routes: []jobdb.Route{{JobType: "collect"}},
		})
		if err != nil || jobLease == nil {
			t.Fatalf("resumed job lease = %+v, %v", jobLease, err)
		}
		if err := jobLease.Complete(ctx, jobdb.CompleteExecutionRequest{
			Status: "success", Chapter: &jobdb.Chapter{
				Ordinal: 1, TaskType: "collect", CreatedAt: time.Now().UTC(),
				Body: jobdb.JobAttemptOutcomeChapter{Outcome: jobdb.ApplicationOutputOutcome{
					Output: jobdb.ApplicationOutputBytes{Data: []byte(`{"done":true}`)},
				}},
			},
		}); err != nil {
			t.Fatalf("complete job lease: %v", err)
		}
		info, err := runtime.GetJob(ctx, root.JobKey)
		if err != nil || info.Status != jobdb.JobStatusCompleted {
			t.Fatalf("completed job = %+v, %v", info, err)
		}
		output, err := info.Data.GetData()
		if err != nil || string(output) != `{"done":true}` {
			t.Fatalf("completed output = %s, %v", output, err)
		}
		if _, err := jobLease.SubmitJob(ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{
			TenantId: "tenant", JobID: "late", JobType: "collect", Data: data,
		}}); !errors.Is(err, jobdb.ErrExecutionLeaseLost) {
			t.Fatalf("stale child submission = %v", err)
		}
		polled, err := runtime.PollWork(ctx, jobdb.PollWorkRequest{
			TenantId: "tenant", WorkerID: "worker", Routes: []jobdb.Route{{JobType: "collect"}}, Limit: 2,
		})
		if err != nil || len(polled) != 1 || polled[0].Job().JobKey != child.JobKey {
			t.Fatalf("poll remaining child work = %+v, %v", polled, err)
		}
	})
}

func durationPtr(value time.Duration) *time.Duration { return &value }

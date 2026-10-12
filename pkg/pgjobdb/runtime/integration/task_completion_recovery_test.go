package integration_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/colony-2/jobdb/pkg/jobdb"
	chapterpostgres "github.com/colony-2/jobdb/pkg/jobdb/chapterstore/postgres"
	runtimecore "github.com/colony-2/jobdb/pkg/jobdb/runtime/core"
	remoteruntime "github.com/colony-2/jobdb/pkg/jobdb/runtime/remote"
	schemapostgres "github.com/colony-2/jobdb/pkg/jobdb/schemastore/postgres"
	"github.com/colony-2/jobdb/pkg/workflow"
	"github.com/colony-2/pgjobdb/pkg/pgjobdb/runtime/internal/runtimeadapter"
)

type recoveryJob struct{}

func (recoveryJob) Name() string { return "recover" }
func (recoveryJob) Run(ctx workflow.JobContext, data jobdb.JobData) (jobdb.JobData, error) {
	return ctx.DoTask(jobdb.RunPolicy{}, "external", data)
}

type completionFaultScheduler struct {
	runtimeadapter.Scheduler
	beforeClaim      func(runtimecore.ClaimTaskRequest) error
	afterClaim       func(runtimecore.LeaseIdentity) error
	beforeReschedule func(runtimecore.RescheduleMutation) error
}

func (s *completionFaultScheduler) ClaimTask(ctx context.Context, req runtimecore.ClaimTaskRequest) (runtimecore.LeaseIdentity, error) {
	if s.beforeClaim != nil {
		if err := s.beforeClaim(req); err != nil {
			return runtimecore.LeaseIdentity{}, err
		}
	}
	id, err := s.Scheduler.ClaimTask(ctx, req)
	if err == nil && s.afterClaim != nil {
		err = s.afterClaim(id)
	}
	if err != nil {
		return runtimecore.LeaseIdentity{}, err
	}
	return id, nil
}
func (s *completionFaultScheduler) RescheduleLease(ctx context.Context, req runtimecore.RescheduleMutation) (runtimecore.StoredJob, error) {
	if s.beforeReschedule != nil {
		if err := s.beforeReschedule(req); err != nil {
			return runtimecore.StoredJob{}, err
		}
	}
	return s.Scheduler.RescheduleLease(ctx, req)
}

type completionFaultChapters struct {
	runtimecore.ChapterLog
	appendError error
	appendCalls atomic.Int32
}

func (c *completionFaultChapters) Append(ctx context.Context, key runtimecore.ChapterLogKey, ch runtimecore.EncodedChapter) error {
	c.appendCalls.Add(1)
	if c.appendError != nil {
		return c.appendError
	}
	return c.ChapterLog.Append(ctx, key, ch)
}

func runRecoveryJob(t *testing.T, ctx context.Context, r jobdb.WorkflowRuntime, key jobdb.JobKey) workflow.JobRunOutcome {
	t.Helper()
	runnable, err := workflow.GetJobForRun(ctx, r, workflow.GetJobForRunRequest{JobKey: key, JobWorker: recoveryJob{}, WorkerID: "primary"})
	if err != nil {
		t.Fatal(err)
	}
	outcome, err := runnable.Run(nil)
	if err != nil {
		t.Fatal(err)
	}
	return outcome
}

func TestTaskCompletionRecoveryWithIndependentChapterDatabase(t *testing.T) {
	for _, phase := range []string{"after_claim", "append_failure", "after_append", "lease_lost_after_append"} {
		t.Run(phase, func(t *testing.T) {
			runDatabaseTest(t, func(ctx context.Context, db *sql.DB) {
				// A physically separate database makes a scheduler/chapter
				// transaction impossible, including in the failure paths.
				if _, err := db.ExecContext(ctx, `CREATE DATABASE completion_chapters`); err != nil {
					t.Fatal(err)
				}
				var port string
				if err := db.QueryRowContext(ctx, `SHOW port`).Scan(&port); err != nil {
					t.Fatal(err)
				}
				schedulerDSN := fmt.Sprintf("host=localhost port=%s user=postgres password=postgres dbname=postgres sslmode=disable", port)
				chapterDSN := fmt.Sprintf("host=localhost port=%s user=postgres password=postgres dbname=completion_chapters sslmode=disable", port)
				chapterDB, err := sql.Open("postgres", chapterDSN)
				if err != nil {
					t.Fatal(err)
				}
				defer chapterDB.Close()
				cfg := chapterpostgres.Config{BlobStoreURI: "blobfs://" + t.TempDir(), MaxInlineArtifactBytes: 1}
				chapters, err := chapterpostgres.NewSQLDB(ctx, chapterDB, cfg)
				if err != nil {
					t.Fatal(err)
				}
				defer chapters.Close(ctx)
				schemas, err := schemapostgres.NewSQLDB(ctx, db)
				if err != nil {
					t.Fatal(err)
				}
				scheduler := &completionFaultScheduler{Scheduler: runtimeadapter.Scheduler{DB: db}}
				chapterPort := &completionFaultChapters{ChapterLog: chapters}
				runtime, err := runtimecore.NewRuntime(runtimecore.Config{Configurations: newConfigurationStore(t, ctx, db), Scheduler: scheduler, Chapters: chapterPort, Schemas: schemas})
				if err != nil {
					t.Fatal(err)
				}
				handle, err := runtime.SubmitJob(ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{
					TenantId: "tenant", JobID: "recovery", JobType: "recover", Data: jobdb.NewTaskDataOrPanic(1),
					ClientPayloadUpdate: &jobdb.ClientPayloadUpdate{Mode: "reset", Value: json.RawMessage(`{"keep":true}`)},
				}})
				if err != nil {
					t.Fatal(err)
				}
				if out := runRecoveryJob(t, ctx, runtime, handle.JobKey); out.Status != workflow.JobRunSuspended {
					t.Fatalf("initial run: %+v", out)
				}
				wait, err := scheduler.GetWaitingTask(ctx, handle.JobKey)
				if err != nil {
					t.Fatal(err)
				}
				req := jobdb.CompleteTaskIfWaitingRequest{JobKey: handle.JobKey, Route: jobdb.Route{JobType: wait.JobType, TaskType: wait.Task.TaskType}, InputOrdinal: wait.Task.InputOrdinal, OutputOrdinal: wait.Task.OutputOrdinal, InputHash: wait.Task.InputHash, Data: jobdb.NewTaskDataOrPanic(2, jobdb.NewArtifactFromBytes("result.txt", []byte("result")))}
				injected := errors.New("injected process loss")
				wantErr := injected
				switch phase {
				case "after_claim":
					scheduler.afterClaim = func(runtimecore.LeaseIdentity) error { return injected }
				case "append_failure":
					chapterPort.appendError = injected
				case "after_append":
					scheduler.beforeReschedule = func(runtimecore.RescheduleMutation) error { return injected }
				case "lease_lost_after_append":
					wantErr = jobdb.ErrExecutionLeaseLost
					scheduler.beforeReschedule = func(m runtimecore.RescheduleMutation) error {
						if _, err := db.ExecContext(ctx, `UPDATE pgjobdb.jobs SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE tenant_id=$1 AND job_id=$2`, handle.JobKey.TenantId, handle.JobKey.JobId); err != nil {
							return err
						}
						next, err := scheduler.AcquireJobLease(ctx, runtimecore.JobLeaseRequest{JobKey: handle.JobKey, WorkerID: "takeover", Selector: runtimecore.WorkSelector{JobTypes: []string{"recover"}}})
						if err != nil {
							return err
						}
						if next == nil {
							return errors.New("takeover failed")
						}
						return nil
					}
				}
				if err := runtime.CompleteTaskIfWaiting(ctx, req); !errors.Is(err, wantErr) {
					t.Fatalf("complete: %v, want %v", err, wantErr)
				}
				published := phase == "after_append" || phase == "lease_lost_after_append"
				_, err = runtime.GetChapter(ctx, jobdb.ChapterRef{JobKey: handle.JobKey, Ordinal: wait.Task.OutputOrdinal})
				if published && err != nil || !published && !errors.Is(err, jobdb.ErrChapterNotFound) {
					t.Fatalf("publication boundary: %v", err)
				}
				row, err := scheduler.GetJob(ctx, handle.JobKey)
				if err != nil || row.WorkKind != runtimecore.WorkKindJob || row.TaskWork != nil || row.LeaseExpiresAt == nil || row.ClientPayloadRevision != 1 || string(row.ClientPayload) != `{"keep":true}` {
					t.Fatalf("durable claim: %+v %v", row, err)
				}
				// Reopen both stores through new connections, with no fault hooks.
				if err := chapters.Close(ctx); err != nil {
					t.Fatal(err)
				}
				if err := chapterDB.Close(); err != nil {
					t.Fatal(err)
				}
				if err := db.Close(); err != nil {
					t.Fatal(err)
				}
				reopenedDB, err := sql.Open("postgres", schedulerDSN)
				if err != nil {
					t.Fatal(err)
				}
				defer reopenedDB.Close()
				reopenedChaptersDB, err := sql.Open("postgres", chapterDSN)
				if err != nil {
					t.Fatal(err)
				}
				defer reopenedChaptersDB.Close()
				reopenedChapters, err := chapterpostgres.NewSQLDB(ctx, reopenedChaptersDB, cfg)
				if err != nil {
					t.Fatal(err)
				}
				defer reopenedChapters.Close(ctx)
				reopenedSchemas, err := schemapostgres.NewSQLDB(ctx, reopenedDB)
				if err != nil {
					t.Fatal(err)
				}
				recovered, err := runtimecore.NewRuntime(runtimecore.Config{Configurations: newConfigurationStore(t, ctx, reopenedDB), Scheduler: runtimeadapter.Scheduler{DB: reopenedDB}, Chapters: reopenedChapters, Schemas: reopenedSchemas})
				if err != nil {
					t.Fatal(err)
				}
				if _, err := reopenedDB.ExecContext(ctx, `UPDATE pgjobdb.jobs SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE tenant_id='tenant' AND job_id='recovery'`); err != nil {
					t.Fatal(err)
				}
				taskLease, err := recovered.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: handle.JobKey, WorkerID: "task", Routes: []jobdb.Route{req.Route}})
				if err != nil || taskLease != nil {
					t.Fatalf("task acquired recovery: %v %v", taskLease, err)
				}
				out := runRecoveryJob(t, ctx, recovered, handle.JobKey)
				if !published {
					if out.Status != workflow.JobRunSuspended {
						t.Fatalf("missing output replay: %+v", out)
					}
					rebuilt, err := (runtimeadapter.Scheduler{DB: reopenedDB}).GetWaitingTask(ctx, handle.JobKey)
					if err != nil || rebuilt != wait {
						t.Fatalf("rebuilt wait: %+v %v", rebuilt, err)
					}
					// Re-create task data since the failed caller owns its inputs.
					req.Data = jobdb.NewTaskDataOrPanic(2)
					if err := recovered.CompleteTaskIfWaiting(ctx, req); err != nil {
						t.Fatal(err)
					}
					out = runRecoveryJob(t, ctx, recovered, handle.JobKey)
				}
				if out.Status != workflow.JobRunCompleted {
					t.Fatalf("recovered run: %+v", out)
				}
				final, err := (runtimeadapter.Scheduler{DB: reopenedDB}).GetJob(ctx, handle.JobKey)
				if err != nil || final.ClientPayloadRevision != 1 || string(final.ClientPayload) != `{"keep":true}` {
					t.Fatalf("payload changed: %+v %v", final, err)
				}
			})
		})
	}
}

func TestTaskCompletionHTTPRejectsRemovedPayload(t *testing.T) {
	runDatabaseTest(t, func(ctx context.Context, db *sql.DB) {
		chapters, err := chapterpostgres.NewSQLDB(ctx, db, chapterpostgres.Config{BlobStoreURI: "blobfs://" + t.TempDir()})
		if err != nil {
			t.Fatal(err)
		}
		defer chapters.Close(ctx)
		schemas, err := schemapostgres.NewSQLDB(ctx, db)
		if err != nil {
			t.Fatal(err)
		}
		runtime, err := runtimecore.NewRuntime(runtimecore.Config{Configurations: newConfigurationStore(t, ctx, db), Scheduler: runtimeadapter.Scheduler{DB: db}, Chapters: chapters, Schemas: schemas})
		if err != nil {
			t.Fatal(err)
		}
		server := remoteruntime.NewServer(runtime)
		for _, update := range []string{`null`, `{}`, `{"mode":"reset","expectedRevision":"0","value":{}}`} {
			req := httptest.NewRequest(http.MethodPost, "/v1/tenants/tenant/jobs/job/chapters/1/commit-if-waiting", strings.NewReader(`{"route":{"jobType":"job","taskType":"external"},"data":{"data":2,"artifacts":[]},"clientPayloadUpdate":`+update+`}`))
			req.Header.Set("Content-Type", "application/json")
			response := httptest.NewRecorder()
			server.ServeHTTP(response, req)
			if response.Code != http.StatusBadRequest || !strings.Contains(response.Body.String(), "clientPayloadUpdate") {
				t.Fatalf("response: %d %s", response.Code, response.Body.String())
			}
		}
	})
}

func TestTaskCompletionClaimsBeforeAppending(t *testing.T) {
	runDatabaseTest(t, func(ctx context.Context, db *sql.DB) {
		chapters, err := chapterpostgres.NewSQLDB(ctx, db, chapterpostgres.Config{BlobStoreURI: "blobfs://" + t.TempDir()})
		if err != nil {
			t.Fatal(err)
		}
		defer chapters.Close(ctx)
		schemas, err := schemapostgres.NewSQLDB(ctx, db)
		if err != nil {
			t.Fatal(err)
		}
		scheduler := &completionFaultScheduler{Scheduler: runtimeadapter.Scheduler{DB: db}}
		port := &completionFaultChapters{ChapterLog: chapters}
		runtime, err := runtimecore.NewRuntime(runtimecore.Config{Configurations: newConfigurationStore(t, ctx, db), Scheduler: scheduler, Chapters: port, Schemas: schemas})
		if err != nil {
			t.Fatal(err)
		}
		for _, scenario := range []string{"changed_slot", "unavailable", "cancelled", "concurrent"} {
			t.Run(scenario, func(t *testing.T) {
				handle, err := runtime.SubmitJob(ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", JobID: scenario, JobType: "recover", Data: jobdb.NewTaskDataOrPanic(1)}})
				if err != nil {
					t.Fatal(err)
				}
				if out := runRecoveryJob(t, ctx, runtime, handle.JobKey); out.Status != workflow.JobRunSuspended {
					t.Fatalf("initial run: %+v", out)
				}
				wait, err := scheduler.GetWaitingTask(ctx, handle.JobKey)
				if err != nil {
					t.Fatal(err)
				}
				// Omit optional request guards: ClaimTask still compares the
				// entire observed snapshot after core's preflight checks.
				req := jobdb.CompleteTaskIfWaitingRequest{JobKey: handle.JobKey, Route: jobdb.Route{JobType: wait.JobType, TaskType: wait.Task.TaskType}, OutputOrdinal: wait.Task.OutputOrdinal, Data: jobdb.NewTaskDataOrPanic(2)}
				before := port.appendCalls.Load()
				if scenario != "concurrent" {
					scheduler.beforeClaim = func(runtimecore.ClaimTaskRequest) error {
						updates := map[string]string{"changed_slot": "task_input_hash='changed'", "unavailable": "available_at=clock_timestamp()+interval '1 hour'", "cancelled": "cancel_requested=true"}
						_, err := db.ExecContext(ctx, "UPDATE pgjobdb.jobs SET "+updates[scenario]+" WHERE tenant_id=$1 AND job_id=$2", handle.JobKey.TenantId, handle.JobKey.JobId)
						return err
					}
					err := runtime.CompleteTaskIfWaiting(ctx, req)
					scheduler.beforeClaim = nil
					if !errors.Is(err, jobdb.ErrConflict) {
						t.Fatalf("claim: %v", err)
					}
					if port.appendCalls.Load() != before {
						t.Fatal("rejected claim attempted chapter append")
					}
					if _, err := runtime.GetChapter(ctx, jobdb.ChapterRef{JobKey: handle.JobKey, Ordinal: req.OutputOrdinal}); !errors.Is(err, jobdb.ErrChapterNotFound) {
						t.Fatalf("unexpected chapter: %v", err)
					}
					return
				}
				start := make(chan struct{})
				results := make(chan error, 8)
				for i := 0; i < cap(results); i++ {
					go func() { <-start; results <- runtime.CompleteTaskIfWaiting(ctx, req) }()
				}
				close(start)
				winners := 0
				for i := 0; i < cap(results); i++ {
					err := <-results
					if err == nil {
						winners++
					} else if !errors.Is(err, jobdb.ErrConflict) {
						t.Errorf("completion: %v", err)
					}
				}
				if winners != 1 || port.appendCalls.Load() != before+1 {
					t.Fatalf("winners=%d append calls=%d", winners, port.appendCalls.Load()-before)
				}
				key := runtimecore.ChapterLogKey{JobKey: handle.JobKey}
				encoded, err := chapters.Get(ctx, key, req.OutputOrdinal)
				if err != nil {
					t.Fatal(err)
				}
				// The ordinary store's ordinal constraint also rejects an
				// independent duplicate writer; no scheduler callback is used.
				if err := chapters.Append(ctx, key, encoded); !errors.Is(err, jobdb.ErrConflict) {
					t.Fatalf("duplicate: %v", err)
				}
				count, err := chapters.Count(ctx, key)
				if err != nil || count != req.OutputOrdinal+1 {
					t.Fatalf("chapter count=%d: %v", count, err)
				}
			})
		}
	})
}

package integration_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	chapterpostgres "github.com/colony-2/jobdb/pkg/jobdb/chapterstore/postgres"
	runtimecore "github.com/colony-2/jobdb/pkg/jobdb/runtime/core"
	"github.com/colony-2/jobdb/pkg/jobdb/runtimetest"
	schemapostgres "github.com/colony-2/jobdb/pkg/jobdb/schemastore/postgres"
	"github.com/colony-2/pgjobdb/pkg/pgjobdb/runtime/internal/runtimeadapter"
	"github.com/stretchr/testify/require"
)

type selectedFaultScheduler struct {
	runtimeadapter.Scheduler
	before func(context.Context, runtimecore.CreateJobRequest) error
	after  func() error
}

func (s *selectedFaultScheduler) CreateJob(ctx context.Context, r runtimecore.CreateJobRequest) (runtimecore.StoredJob, error) {
	if s.before != nil {
		if err := s.before(ctx, r); err != nil {
			return runtimecore.StoredJob{}, err
		}
	}
	job, err := s.Scheduler.CreateJob(ctx, r)
	if err == nil && s.after != nil {
		err = s.after()
	}
	return job, err
}

type selectedFaultHistory struct {
	runtimecore.ChapterLog
	fail       bool
	failUpload bool
}

func (s *selectedFaultHistory) Create(ctx context.Context, key runtimecore.ChapterLogKey, ch runtimecore.EncodedChapter) error {
	if s.fail {
		return errors.New("injected history failure")
	}
	if s.failUpload && len(ch.ArtifactUploads) > 0 {
		ch.ArtifactUploads = append([]jobdb.ArtifactUpload(nil), ch.ArtifactUploads...)
		ch.ArtifactUploads[0].Open = func() (io.ReadCloser, error) { return nil, errors.New("injected upload interruption") }
	}
	return s.ChapterLog.Create(ctx, key, ch)
}

func TestSelectedIndependentHistoryBoundaries(t *testing.T) {
	runDatabaseTest(t, func(ctx context.Context, db *sql.DB) {
		ctx = runtimetest.PrimaryGoalContext(ctx)
		_, err := db.ExecContext(ctx, "CREATE DATABASE selected_chapters")
		require.NoError(t, err)
		var port string
		require.NoError(t, db.QueryRowContext(ctx, "SHOW port").Scan(&port))
		historyDB, err := sql.Open("postgres", fmt.Sprintf("host=localhost port=%s user=postgres password=postgres dbname=selected_chapters sslmode=disable", port))
		require.NoError(t, err)
		defer historyDB.Close()
		chapters, err := chapterpostgres.NewSQLDB(ctx, historyDB, chapterpostgres.Config{BlobStoreURI: "blobfs://" + t.TempDir(), MaxInlineArtifactBytes: 1})
		require.NoError(t, err)
		defer chapters.Close(ctx)
		schemas, err := schemapostgres.NewSQLDB(ctx, db)
		require.NoError(t, err)
		scheduler := &selectedFaultScheduler{Scheduler: runtimeadapter.Scheduler{DB: db}}
		history := &selectedFaultHistory{ChapterLog: chapters}
		makeRuntime := func() *runtimecore.Runtime {
			r, e := runtimecore.NewRuntime(runtimecore.Config{Scheduler: scheduler, Chapters: history, Schemas: schemas, Configurations: newConfigurationStore(t, ctx, db)})
			require.NoError(t, e)
			return r
		}
		runtime := makeRuntime()
		source, err := celltest.Submit(runtime, ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", JobID: "source", Cell: "test", CellRepo: "example.test/test", JobType: "work", Data: jobdb.NewTaskDataOrPanic(nil)}})
		require.NoError(t, err)
		request := func(id string) jobdb.SubmitJobRequest {
			return jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", JobID: id, GoalID: source.GoalID, Cell: source.Cell, CellRepo: source.CellRepo, JobType: "work", Data: jobdb.NewTaskDataOrPanic("retained", jobdb.NewArtifactFromBytes("input.txt", []byte("independent blobfs artifact")))}}
		}
		t.Run("history failure", func(t *testing.T) {
			history.fail = true
			_, err := runtime.SubmitJob(ctx, request("history-failure"))
			require.ErrorContains(t, err, "injected history failure")
			history.fail = false
			assertNoSelectedScheduler(t, ctx, db, "history-failure")
			_, err = runtime.SubmitJob(ctx, request("history-failure"))
			require.NoError(t, err)
		})
		t.Run("incomplete story ordinary retry", func(t *testing.T) {
			history.failUpload = true
			_, err := runtime.SubmitJob(ctx, request("incomplete"))
			require.ErrorContains(t, err, "injected upload interruption")
			history.failUpload = false
			assertNoSelectedScheduler(t, ctx, db, "incomplete")
			_, err = makeRuntime().SubmitJob(ctx, request("incomplete"))
			require.Error(t, err)
			assertNoSelectedScheduler(t, ctx, db, "incomplete")
		})
		t.Run("publication rejection retains history", func(t *testing.T) {
			scheduler.before = func(context.Context, runtimecore.CreateJobRequest) error {
				return errors.New("injected scheduler failure")
			}
			_, err := runtime.SubmitJob(ctx, request("rejected"))
			require.ErrorContains(t, err, "injected scheduler failure")
			scheduler.before = nil
			assertNoSelectedScheduler(t, ctx, db, "rejected")
			chapter, err := chapters.Get(ctx, runtimecore.ChapterLogKey{JobKey: jobdb.JobKey{TenantId: "tenant", JobId: "rejected"}}, 0)
			require.NoError(t, err)
			require.Len(t, chapter.Artifacts, 1)
			reader, err := chapters.OpenArtifact(ctx, runtimecore.ArtifactLookup{JobKey: jobdb.JobKey{TenantId: "tenant", JobId: "rejected"}, Ordinal: 0, Name: "input.txt", Digest: chapter.Artifacts[0].Digest})
			require.NoError(t, err)
			stream, err := reader.Open()
			require.NoError(t, err)
			content, err := io.ReadAll(stream)
			require.NoError(t, err)
			require.NoError(t, stream.Close())
			require.Equal(t, "independent blobfs artifact", string(content))
			// Recreate artifact inputs and validate the complete existing initial chapter.
			h, err := makeRuntime().SubmitJob(ctx, request("rejected"))
			require.NoError(t, err)
			require.Equal(t, source.GoalID, h.GoalID)
		})
		t.Run("scheduler rollback retains independent history", func(t *testing.T) {
			_, err := db.ExecContext(ctx, `ALTER TABLE pgjobdb.job_preparations ADD CONSTRAINT injected_receipt_failure CHECK(job_id<>'rolled-back' OR NOT published) NOT VALID`)
			require.NoError(t, err)
			req := request("rolled-back")
			req.Job.Dependencies = []jobdb.Dependency{{Target: jobdb.DependencyTarget{JobID: source.JobKey.JobId}, Condition: jobdb.JobPrereqSuccess}}
			_, err = runtime.SubmitJob(ctx, req)
			require.ErrorContains(t, err, "injected_receipt_failure")
			_, err = db.ExecContext(ctx, `ALTER TABLE pgjobdb.job_preparations DROP CONSTRAINT injected_receipt_failure`)
			require.NoError(t, err)
			assertNoSelectedScheduler(t, ctx, db, "rolled-back")
			chapter, err := chapters.Get(ctx, runtimecore.ChapterLogKey{JobKey: jobdb.JobKey{TenantId: "tenant", JobId: "rolled-back"}}, 0)
			require.NoError(t, err)
			require.Len(t, chapter.Artifacts, 1)
			req = request("rolled-back")
			req.Job.Dependencies = []jobdb.Dependency{{Target: jobdb.DependencyTarget{JobID: source.JobKey.JobId}, Condition: jobdb.JobPrereqSuccess}}
			_, err = makeRuntime().SubmitJob(ctx, req)
			require.NoError(t, err)
		})
		t.Run("lost publication response", func(t *testing.T) {
			scheduler.after = func() error { return errors.New("injected committed response loss") }
			_, err := runtime.SubmitJob(ctx, request("lost-response"))
			require.ErrorContains(t, err, "injected committed response loss")
			scheduler.after = nil
			first, err := makeRuntime().SubmitJob(ctx, request("lost-response"))
			require.NoError(t, err)
			second, err := makeRuntime().SubmitJob(ctx, request("lost-response"))
			require.NoError(t, err)
			require.Equal(t, first, second)
		})
		t.Run("retained history cannot bypass closure", func(t *testing.T) {
			scheduler.before = func(context.Context, runtimecore.CreateJobRequest) error { return errors.New("defer publication") }
			_, err := runtime.SubmitJob(ctx, request("closed"))
			require.Error(t, err)
			scheduler.before = nil
			_, err = runtime.FailGoal(ctx, jobdb.FailGoalRequest{GoalKey: jobdb.GoalKey{TenantId: "tenant", GoalID: source.GoalID}, Message: "closed"})
			require.NoError(t, err)
			_, err = makeRuntime().SubmitJob(ctx, request("closed"))
			require.ErrorIs(t, err, jobdb.ErrGoalConflict)
			assertNoSelectedScheduler(t, ctx, db, "closed")
			_, err = chapters.Get(ctx, runtimecore.ChapterLogKey{JobKey: jobdb.JobKey{TenantId: "tenant", JobId: "closed"}}, 0)
			require.NoError(t, err)
		})
	})
}

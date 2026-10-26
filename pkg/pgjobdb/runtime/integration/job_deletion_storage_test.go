package integration_test

import (
	"context"
	"database/sql"
	"sync"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	chapterpostgres "github.com/colony-2/jobdb/pkg/jobdb/chapterstore/postgres"
	runtimecore "github.com/colony-2/jobdb/pkg/jobdb/runtime/core"
	schemapostgres "github.com/colony-2/jobdb/pkg/jobdb/schemastore/postgres"
	pgruntime "github.com/colony-2/pgjobdb/pkg/pgjobdb/runtime"
	"github.com/colony-2/pgjobdb/pkg/pgjobdb/runtime/internal/runtimeadapter"
	"github.com/stretchr/testify/require"
)

func TestPostgresJobDeletionReopenAndChildren(t *testing.T) {
	newDSN := conformanceDSNFactory(t)
	for _, transport := range []string{"embedded", "http"} {
		t.Run(transport, func(t *testing.T) {
			ctx := context.Background()
			dsn := newDSN(t)
			cfg := pgruntime.Config{BlobStoreURI: "blobfs://" + t.TempDir()}
			backend := openConfigurationRuntime(t, dsn, cfg)
			rt := configurationTransport(t, backend, transport)
			req := jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", JobID: "parent", JobType: "job", Data: jobdb.NewTaskDataOrPanic("input")}}
			parent, err := rt.SubmitJob(ctx, req)
			require.NoError(t, err)
			lease, err := rt.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: parent.JobKey, WorkerID: "parent", Routes: []jobdb.Route{{JobType: "job"}}, LeaseDuration: time.Minute})
			require.NoError(t, err)
			require.NotNil(t, lease)
			child, err := lease.SubmitJob(ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{JobID: "child", JobType: "job", Data: jobdb.NewTaskDataOrPanic(nil)}})
			require.NoError(t, err)
			_, err = rt.(jobdb.JobDeletionRuntime).DeleteJobs(ctx, jobdb.DeleteJobsRequest{TenantID: "tenant", JobIDs: []string{parent.JobKey.JobId}})
			require.NoError(t, err)
			info, err := rt.GetJob(ctx, child.JobKey)
			require.NoError(t, err)
			require.Equal(t, jobdb.JobStatusReady, info.Status)
			require.NoError(t, backend.Close(ctx))
			rt = configurationTransport(t, openConfigurationRuntime(t, dsn, cfg), transport)
			duplicate, err := rt.SubmitJob(ctx, req)
			require.NoError(t, err)
			require.Equal(t, parent.JobKey, duplicate.JobKey)
			visible, err := rt.ListJobs(ctx, jobdb.ListJobsRequest{TenantIds: []string{"tenant"}})
			require.NoError(t, err)
			require.Len(t, visible.Jobs, 1)
			require.Equal(t, child.JobKey, visible.Jobs[0].JobKey)
			query := jobdb.ListJobsRequest{TenantIds: []string{"tenant"}, IncludeHidden: true, PageSize: 1}
			first, err := rt.ListJobs(ctx, query)
			require.NoError(t, err)
			require.Len(t, first.Jobs, 1)
			require.NotEmpty(t, first.NextPageToken)
			query.PageToken = first.NextPageToken
			second, err := rt.ListJobs(ctx, query)
			require.NoError(t, err)
			require.Len(t, second.Jobs, 1)
			require.NotEqual(t, first.Jobs[0].JobKey, second.Jobs[0].JobKey)
			require.Empty(t, second.NextPageToken)
			query.IncludeHidden = false
			_, err = rt.ListJobs(ctx, query)
			require.Error(t, err, "visibility mode is part of cursor identity")
			history, err := jobdb.GetJobRun(ctx, rt, jobdb.GetJobRunRequest{JobKey: parent.JobKey, IncludeInputs: true})
			require.NoError(t, err)
			require.Equal(t, parent.JobKey, history.Job.JobKey)
			require.Equal(t, "cancelled", history.Job.CompletionStatus)
			require.Equal(t, "job deleted", history.Job.CompletionDetail)
		})
	}
}

type deletionCreationGate struct {
	runtimeadapter.Scheduler
	entered chan struct{}
	release chan struct{}
}

func (s deletionCreationGate) CreateJob(ctx context.Context, req runtimecore.CreateJobRequest) (runtimecore.StoredJob, error) {
	if req.ParentJobID != "" {
		close(s.entered)
		select {
		case <-s.release:
		case <-ctx.Done():
			return runtimecore.StoredJob{}, ctx.Err()
		}
	}
	return s.Scheduler.CreateJob(ctx, req)
}

func TestPostgresJobDeletionFencesInFlightChild(t *testing.T) {
	runDatabaseTest(t, func(ctx context.Context, db *sql.DB) {
		chapters, err := chapterpostgres.NewSQLDB(ctx, db, chapterpostgres.Config{BlobStoreURI: "blobfs://" + t.TempDir()})
		require.NoError(t, err)
		defer chapters.Close(ctx)
		schemas, err := schemapostgres.NewSQLDB(ctx, db)
		require.NoError(t, err)
		gate := deletionCreationGate{Scheduler: runtimeadapter.Scheduler{DB: db}, entered: make(chan struct{}), release: make(chan struct{})}
		var release sync.Once
		unblock := func() { release.Do(func() { close(gate.release) }) }
		defer unblock()
		rt, err := runtimecore.NewRuntime(runtimecore.Config{Scheduler: gate, Chapters: chapters, Schemas: schemas, Configurations: newConfigurationStore(t, ctx, db)})
		require.NoError(t, err)
		parent, err := rt.SubmitJob(ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", JobID: "parent", JobType: "job", Data: jobdb.NewTaskDataOrPanic(nil)}})
		require.NoError(t, err)
		lease, err := rt.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: parent.JobKey, WorkerID: "parent", Routes: []jobdb.Route{{JobType: "job"}}, LeaseDuration: time.Minute})
		require.NoError(t, err)
		require.NotNil(t, lease)
		done := make(chan error, 1)
		go func() {
			_, err := lease.SubmitJob(ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{JobID: "child", JobType: "job", Data: jobdb.NewTaskDataOrPanic(nil)}})
			done <- err
		}()
		select {
		case <-gate.entered:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
		_, err = rt.DeleteJobs(ctx, jobdb.DeleteJobsRequest{TenantID: "tenant", JobIDs: []string{"parent"}})
		require.NoError(t, err)
		unblock()
		require.ErrorIs(t, <-done, jobdb.ErrExecutionLeaseLost)
		_, err = rt.GetJob(ctx, jobdb.JobKey{TenantId: "tenant", JobId: "child"})
		require.ErrorIs(t, err, jobdb.ErrJobNotFound)
	})
}

func TestPostgresJobDeletionAcquisitionRace(t *testing.T) {
	newRuntime := conformanceRuntimeFactory(t)
	for _, transport := range []string{"embedded", "http"} {
		t.Run(transport, func(t *testing.T) {
			rt := configurationTransport(t, newRuntime(t), transport)
			ctx := context.Background()
			for range 5 {
				key := submitScoped(t, rt, "job")
				start := make(chan struct{})
				type acquired struct {
					lease jobdb.ExecutionLease
					err   error
				}
				done := make(chan acquired, 1)
				go func() {
					<-start
					l, err := rt.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: key, WorkerID: "racer", Routes: []jobdb.Route{{JobType: "job"}}, LeaseDuration: time.Minute})
					done <- acquired{l, err}
				}()
				close(start)
				result, err := rt.(jobdb.JobDeletionRuntime).DeleteJobs(ctx, jobdb.DeleteJobsRequest{TenantID: "tenant", JobIDs: []string{key.JobId}})
				require.NoError(t, err)
				require.EqualValues(t, 1, result.DeletedCount)
				got := <-done
				require.NoError(t, got.err)
				if got.lease != nil {
					_, err := got.lease.(jobdb.RenewableExecutionLease).Renew(ctx)
					require.ErrorIs(t, err, jobdb.ErrExecutionLeaseLost)
				}
				lease, err := rt.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: key, WorkerID: "after", Routes: []jobdb.Route{{JobType: "job"}}})
				require.NoError(t, err)
				require.Nil(t, lease)
			}
		})
	}
}

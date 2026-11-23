package integration_test

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"io"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	chapterpostgres "github.com/colony-2/jobdb/pkg/jobdb/chapterstore/postgres"
	"github.com/colony-2/jobdb/pkg/jobdb/configuration"
	"github.com/colony-2/jobdb/pkg/jobdb/projectcatalog"
	runtimecore "github.com/colony-2/jobdb/pkg/jobdb/runtime/core"
	schemapostgres "github.com/colony-2/jobdb/pkg/jobdb/schemastore/postgres"
	"github.com/colony-2/pgjobdb/internal/projectfixture"
	"github.com/colony-2/pgjobdb/pkg/pgjobdb"
	"github.com/colony-2/pgjobdb/pkg/pgjobdb/projects"
	pgruntime "github.com/colony-2/pgjobdb/pkg/pgjobdb/runtime"
	"github.com/colony-2/pgjobdb/pkg/pgjobdb/runtime/internal/runtimeadapter"
	"github.com/stretchr/testify/require"
)

func newProject(t *testing.T, ctx context.Context, db *sql.DB, name string) jobdb.ProjectCreation {
	t.Helper()
	created, err := projects.NewControl(db, nil).CreateProject(projectfixture.Context(ctx), jobdb.CreateProjectRequest{Name: name, RequestID: name})
	require.NoError(t, err)
	return created
}
func TestProjectIndependentSharedWriters(t *testing.T) {
	withPreparedDatabase(t, func(ctx context.Context, db *sql.DB) {
		control := projects.NewControl(db, nil)
		created := newProject(t, ctx, db, "independent")
		removed := newProject(t, ctx, db, "removed-project")
		require.NoError(t, projects.NewControl(db, func(context.Context, string) error { return nil }).DeleteProject(projectfixture.Context(ctx), jobdb.DeleteProjectRequest{ID: removed.Project.ID, IfMatch: removed.ETag}))
		var opens int
		upload := jobdb.ArtifactUpload{Name: "opaque.bin", Size: 5, Open: func() (io.ReadCloser, error) { opens++; return io.NopCloser(bytes.NewReader([]byte("bytes"))), nil }}
		for _, configured := range []bool{false, true} {
			var admission jobdb.ProjectAdmission
			if configured {
				admission = control
			}
			chapters, err := chapterpostgres.NewSQLDB(ctx, db, chapterpostgres.Config{Projects: admission, BlobStoreURI: "blobfs://" + t.TempDir(), MaxInlineArtifactBytes: 1})
			require.NoError(t, err)
			defer chapters.Close(ctx)
			schemas, err := schemapostgres.NewSQLDB(ctx, db, admission)
			require.NoError(t, err)
			configs, err := configuration.NewSQLStore(ctx, db, configuration.Postgres, admission)
			require.NoError(t, err)
			for _, id := range []string{"missing-project", removed.Project.ID, created.Project.ID} {
				key := jobdb.JobKey{TenantId: id, JobId: "independent"}
				before := schedulerSnapshot(t, ctx, db)
				beforeOpens := opens
				want := jobdb.ErrProjectUnavailable
				if configured {
					want = jobdb.ErrProjectNotFound
				}
				if !configured || id != created.Project.ID {
					require.ErrorIs(t, chapters.Create(ctx, runtimecore.ChapterLogKey{JobKey: key}, runtimecore.EncodedChapter{Payload: []byte("opaque"), ArtifactUploads: []jobdb.ArtifactUpload{upload}}), want)
					_, err = schemas.StoreJobSchema(ctx, runtimecore.StoredJobSchema{TenantId: id, SchemaHash: "hash", Schema: json.RawMessage(`{"type":"object"}`)})
					require.ErrorIs(t, err, want)
					_, err = configs.Update(ctx, id, func(in jobdb.ConfigurationSnapshot) (jobdb.ConfigurationSnapshot, error) {
						t.Fatal("denied update callback invoked")
						return in, nil
					})
					require.ErrorIs(t, err, want)
					require.Equal(t, beforeOpens, opens)
					require.Equal(t, before, schedulerSnapshot(t, ctx, db))
					continue
				}
				payload := []byte(`{"tenant_id":"opaque","big":90071992547409931234}`)
				require.NoError(t, chapters.Create(ctx, runtimecore.ChapterLogKey{JobKey: key}, runtimecore.EncodedChapter{Payload: payload, ArtifactUploads: []jobdb.ArtifactUpload{upload}}))
				got, err := chapters.Get(ctx, runtimecore.ChapterLogKey{JobKey: key}, 0)
				require.NoError(t, err)
				require.Equal(t, payload, got.Payload)
				require.Greater(t, opens, beforeOpens)
				_, err = schemas.StoreJobSchema(ctx, runtimecore.StoredJobSchema{TenantId: id, SchemaHash: "hash", Schema: json.RawMessage(`{"type":"object"}`)})
				require.NoError(t, err)
				_, err = configs.Update(ctx, id, func(in jobdb.ConfigurationSnapshot) (jobdb.ConfigurationSnapshot, error) { return in, nil })
				require.NoError(t, err)
			}
		}
		// The runtime's separately exposed schema registry shares the same admission.
		rt, err := pgruntime.NewSQLDB(ctx, db, pgruntime.Config{BlobStoreURI: "blobfs://" + t.TempDir()})
		require.NoError(t, err)
		defer rt.Close(ctx)
		_, err = rt.RegisterJobSchema(ctx, jobdb.RegisterJobSchemaRequest{TenantId: "missing-project", Schema: json.RawMessage(`{"chapterShape":{"type":"object"}}`)})
		require.ErrorIs(t, err, jobdb.ErrProjectNotFound)
	})
}
func TestProjectNativeWriterAdmission(t *testing.T) {
	withPreparedDatabase(t, func(ctx context.Context, db *sql.DB) {
		created := newProject(t, ctx, db, "native-deleted")
		require.NoError(t, projects.NewControl(db, func(context.Context, string) error { return nil }).DeleteProject(projectfixture.Context(ctx), jobdb.DeleteProjectRequest{ID: created.Project.ID, IfMatch: created.ETag}))
		for _, id := range []string{"missing-project", created.Project.ID} {
			tenant := pgjobdb.TenantID(id)
			key := jobdb.JobKey{TenantId: id, JobId: "missing"}
			identity := pgjobdb.LeaseIdentity{TenantID: tenant, JobID: "missing", LeaseID: "lease", WorkerID: "worker"}
			writers := map[string]func() error{
				"cell-create": func() error {
					_, e := pgjobdb.CreateCell(ctx, db, jobdb.CreateCellRequest{TenantID: id, Name: "work", Repo: "example.test/work"})
					return e
				},
				"preparation": func() error {
					_, e := pgjobdb.PrepareJob(ctx, db, pgjobdb.JobPreparation{Key: key, Cell: "work", Descriptor: "submit"})
					return e
				},
				"submit": func() error {
					_, e := pgjobdb.SubmitJob(ctx, db, pgjobdb.SubmitJobRequest{TenantID: tenant, JobID: "missing", WorkerID: "worker", JobType: "work", Cell: "work"})
					return e
				},
				"acquire": func() error {
					_, e := pgjobdb.GetWork(ctx, db, "worker", pgjobdb.WorkSelector{JobTypes: []pgjobdb.JobType{"work"}}, pgjobdb.GetWorkOptions{TenantIDs: []pgjobdb.TenantID{tenant}})
					return e
				},
				"renew": func() error { _, e := pgjobdb.KeepAliveLease(ctx, db, identity, time.Minute); return e },
				"completion": func() error {
					return pgjobdb.CompleteJob(ctx, db, identity, pgjobdb.Completion{Status: pgjobdb.CompletionSuccess})
				},
				"unheld-completion": func() error {
					return pgjobdb.CompleteUnheldJob(ctx, db, tenant, "missing", "worker", pgjobdb.Completion{Status: pgjobdb.CompletionSuccess})
				},
				"reschedule": func() error {
					return pgjobdb.RescheduleJob(ctx, db, identity, pgjobdb.RescheduleRequest{RouteJobType: "work", WorkKind: pgjobdb.WorkKindJob})
				},
				"cancel": func() error {
					return pgjobdb.CancelJob(ctx, db, pgjobdb.CancelJobRequest{TenantID: tenant, JobID: "missing", WorkerID: "worker"})
				},
				"delete": func() error {
					_, e := pgjobdb.DeleteJobs(ctx, db, pgjobdb.DeleteJobsRequest{TenantID: tenant, All: true})
					return e
				},
				"fail-goal": func() error {
					_, e := pgjobdb.FailGoal(ctx, db, jobdb.FailGoalRequest{GoalKey: jobdb.GoalKey{TenantId: id, GoalID: "goal"}, Message: "failure"})
					return e
				},
				"crash-reset": func() error {
					return pgjobdb.ClearCrashConcern(ctx, db, jobdb.ClearCrashConcernRequest{JobKey: key, WorkerID: "worker"})
				},
				"append-claim": func() error {
					_, e := (runtimeadapter.Scheduler{DB: db}).ClaimAppend(ctx, runtimecore.ClaimAppendRequest{JobKey: key, WorkerID: "worker", Duration: time.Minute})
					return e
				},
				"append-release": func() error {
					return (runtimeadapter.Scheduler{DB: db}).ReleaseAppend(ctx, runtimecore.LeaseIdentity{JobKey: key, LeaseID: "lease"})
				},
				"task-claim": func() error {
					_, e := pgjobdb.ClaimTask(ctx, db, pgjobdb.ClaimTaskRequest{TenantID: tenant, JobID: "missing", WorkerID: "worker", JobType: "work", Task: pgjobdb.TaskWork{TaskType: "task", ResumeJobType: "work", InputHash: "hash"}, LeaseDuration: time.Minute})
					return e
				},
			}
			for name, write := range writers {
				t.Run(id+"/"+name, func(t *testing.T) {
					before := schedulerSnapshot(t, ctx, db)
					require.ErrorIs(t, write(), jobdb.ErrProjectNotFound)
					require.Equal(t, before, schedulerSnapshot(t, ctx, db))
				})
			}
		}
	})
}
func TestProjectReadDoesNotSeal(t *testing.T) {
	withPreparedDatabase(t, func(ctx context.Context, db *sql.DB) {
		p := newProject(t, ctx, db, "pure-read")
		before := schedulerSnapshot(t, ctx, db)
		_, err := pgjobdb.ListCells(ctx, db, jobdb.ListCellsRequest{TenantID: p.Project.ID})
		require.NoError(t, err)
		_, err = pgjobdb.GetJob(ctx, db, pgjobdb.TenantID(p.Project.ID), "missing")
		require.ErrorIs(t, err, pgjobdb.ErrJobNotFound)
		require.Equal(t, before, schedulerSnapshot(t, ctx, db))
		require.NoError(t, projects.NewControl(db, func(context.Context, string) error { return nil }).DeleteProject(projectfixture.Context(ctx), jobdb.DeleteProjectRequest{ID: p.Project.ID, IfMatch: projectcatalog.ETag(p.Project.Revision)}))
	})
}

type projectSecretProvider func(context.Context, jobdb.SecretResolveRequest) (jobdb.SecretResolveResult, error)

func (f projectSecretProvider) ResolveSecrets(ctx context.Context, q jobdb.SecretResolveRequest) (jobdb.SecretResolveResult, error) {
	return f(ctx, q)
}
func TestProjectAdmissionBeforeSecretProvider(t *testing.T) {
	withPreparedDatabase(t, func(ctx context.Context, db *sql.DB) {
		p := newProject(t, ctx, db, "secret-admission")
		calls := 0
		provider := projectSecretProvider(func(_ context.Context, q jobdb.SecretResolveRequest) (jobdb.SecretResolveResult, error) {
			calls++
			require.Equal(t, p.Project.ID, q.TenantID)
			return jobdb.SecretResolveResult{Secrets: map[string]jobdb.SecretValue{"example": {Value: "fixture"}}}, nil
		})
		rt, err := pgruntime.NewSQLDB(ctx, db, pgruntime.Config{BlobStoreURI: "blobfs://" + t.TempDir(), SecretProvider: provider})
		require.NoError(t, err)
		defer rt.Close(ctx)
		h, err := celltest.Submit(rt, ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: p.Project.ID, JobID: "secret-job", Cell: "work", CellRepo: "example.test/work", JobType: "work", Data: jobdb.NewTaskDataOrPanic(1)}})
		require.NoError(t, err)
		lease, err := rt.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: h.JobKey, WorkerID: "worker", Routes: []jobdb.Route{{JobType: "work"}}, LeaseDuration: time.Minute})
		require.NoError(t, err)
		require.NotNil(t, lease)
		_, err = lease.ResolveSecrets(ctx, []string{"example"})
		require.NoError(t, err)
		require.Equal(t, 1, calls)
		var payload []byte
		require.NoError(t, db.QueryRowContext(ctx, `SELECT payload FROM public.jobdb_project_catalog`).Scan(&payload))
		// Emulate catalog unavailability after a lease was obtained. No provider may
		// run from a cached lease once durable admission can no longer be established.
		_, err = db.ExecContext(ctx, `UPDATE public.jobdb_project_catalog SET payload=NULL`)
		require.NoError(t, err)
		_, err = lease.ResolveSecrets(ctx, []string{"example"})
		require.ErrorIs(t, err, jobdb.ErrSecretResolutionFailed)
		require.Equal(t, 1, calls)
		_, err = db.ExecContext(ctx, `UPDATE public.jobdb_project_catalog SET payload=$1`, payload)
		require.NoError(t, err)
		_, err = lease.ResolveSecrets(ctx, []string{"example"})
		require.NoError(t, err)
		require.Equal(t, 2, calls)
	})
}

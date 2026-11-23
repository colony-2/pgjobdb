package integration_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/colony-2/pgjobdb/internal/projectfixture"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	chapterpostgres "github.com/colony-2/jobdb/pkg/jobdb/chapterstore/postgres"
	runtimecore "github.com/colony-2/jobdb/pkg/jobdb/runtime/core"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/remote"
	"github.com/colony-2/jobdb/pkg/jobdb/runtimetest"
	schemapostgres "github.com/colony-2/jobdb/pkg/jobdb/schemastore/postgres"
	"github.com/colony-2/pgjobdb/pkg/pgjobdb"
	pgruntime "github.com/colony-2/pgjobdb/pkg/pgjobdb/runtime"
	"github.com/colony-2/pgjobdb/pkg/pgjobdb/runtime/internal/runtimeadapter"
	"github.com/stretchr/testify/require"
)

type availabilityFixture struct {
	nativeHistory bool
	native        bool
	t             *testing.T
	ctx           context.Context
	db            *sql.DB
	cfg           pgruntime.Config
	backend       *pgruntime.Runtime
	client        *remote.Runtime
	server        *httptest.Server
	mu            sync.Mutex
	methods       []string
}

func newAvailabilityFixture(t *testing.T, dsn string) *availabilityFixture {
	t.Helper()
	f := &availabilityFixture{t: t, ctx: runtimetest.PrimaryGoalContext(celltest.Context(context.Background())), cfg: pgruntime.Config{BlobStoreURI: "blobfs://" + t.TempDir()}}
	var err error
	f.db, err = sql.Open("postgres", dsn)
	require.NoError(t, err)
	f.reopen()
	t.Cleanup(func() { f.server.Close(); require.NoError(t, f.backend.Close(f.ctx)); require.NoError(t, f.db.Close()) })
	_, err = f.backend.CreateCell(f.ctx, jobdb.CreateCellRequest{TenantID: "c2", Name: "test", Repo: "example.test/test"})
	require.NoError(t, err)
	return f
}

func (f *availabilityFixture) reopen() {
	if f.server != nil {
		f.server.Close()
		require.NoError(f.t, f.backend.Close(f.ctx))
	}
	var err error
	f.backend, err = pgruntime.NewSQLDB(f.ctx, f.db, f.cfg)
	require.NoError(f.t, err)
	handler := selectionTestHandler(remote.NewConsumerServer(f.backend))
	f.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "POST" || r.Method == "PUT" {
			f.mu.Lock()
			f.methods = append(f.methods, r.Method+" "+r.URL.Path)
			f.mu.Unlock()
		}
		handler.ServeHTTP(w, r)
	}))
	f.client, err = remote.New(f.server.URL, f.server.Client())
	require.NoError(f.t, err)
}

func availabilityRequest(id string, at *time.Time) jobdb.SubmitJobRequest {
	return jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "c2", Cell: "test", CellRepo: "example.test/test", JobID: id, JobType: "child", Data: jobdb.NewTaskDataOrPanic(nil), AvailableAt: at}}
}
func effectiveAvailability(at time.Time) time.Time {
	v := at.UTC().Truncate(time.Microsecond)
	if v.Before(at) {
		v = v.Add(time.Microsecond)
	}
	return v
}

// Each submitter uses its real API. Generated modes use POST; explicit remote
// modes use PUT. Lease submitters retain an actually acquired live parent.
func (f *availabilityFixture) submitter(route string) func(context.Context, jobdb.SubmitJobRequest) (jobdb.JobHandle, error) {
	t := f.t
	switch route {
	case "native":
		f.native = true
		return func(ctx context.Context, r jobdb.SubmitJobRequest) (jobdb.JobHandle, error) {
			nativeRequest := pgjobdb.SubmitJobRequest{TenantID: pgjobdb.TenantID(r.Job.TenantId), JobID: pgjobdb.JobID(r.Job.JobID), WorkerID: "submit", JobType: pgjobdb.JobType(r.Job.JobType), Cell: r.Job.Cell, CellRepo: r.Job.CellRepo, AvailableAt: r.Job.AvailableAt, Dependencies: r.Job.Dependencies, AppMetadata: r.Job.Metadata}
			if f.nativeHistory {
				f.stageNative(ctx, r, &nativeRequest)
			}
			_, err := pgjobdb.SubmitJob(ctx, f.db, nativeRequest)
			if err != nil {
				return jobdb.JobHandle{}, err
			}
			j, err := pgjobdb.GetJob(ctx, f.db, pgjobdb.TenantID(r.Job.TenantId), pgjobdb.JobID(r.Job.JobID))
			return jobdb.JobHandle{JobKey: jobdb.JobKey{TenantId: r.Job.TenantId, JobId: r.Job.JobID}, GoalID: j.GoalID, Cell: j.Cell, CellID: j.CellID, CellRepo: j.CellRepo}, err
		}
	case "direct-explicit", "direct-generated":
		return func(ctx context.Context, r jobdb.SubmitJobRequest) (jobdb.JobHandle, error) {
			if strings.HasSuffix(route, "generated") {
				r.Job.JobID = ""
			}
			return f.backend.SubmitJob(ctx, r)
		}
	case "http-put", "http-post":
		return func(ctx context.Context, r jobdb.SubmitJobRequest) (jobdb.JobHandle, error) {
			if route == "http-post" {
				r.Job.JobID = ""
			}
			return f.client.SubmitJob(ctx, r)
		}
	case "selected-direct", "selected-http":
		source, err := f.backend.SubmitJob(f.ctx, availabilityRequest("selected-source", nil))
		require.NoError(t, err)
		retireNativeMember(t, f.ctx, f.db, source.JobKey.JobId)
		return func(ctx context.Context, r jobdb.SubmitJobRequest) (jobdb.JobHandle, error) {
			r.Job.GoalID = source.GoalID
			if route == "selected-http" {
				return f.client.SubmitJob(ctx, r)
			}
			return f.backend.SubmitJob(ctx, r)
		}
	case "native-parent", "http-parent", "imported-submit-only":
		ownerReq := availabilityRequest("owner", nil)
		ownerReq.Job.JobType = "owner"
		owner, err := f.backend.SubmitJob(f.ctx, ownerReq)
		require.NoError(t, err)
		var rt jobdb.ConsumerRuntime = f.backend
		if route != "native-parent" {
			rt = f.client
		}
		lease, err := rt.GetJobLease(f.ctx, jobdb.GetJobLeaseRequest{JobKey: owner.JobKey, WorkerID: "owner", Routes: []jobdb.Route{{JobType: "owner"}}, LeaseDuration: time.Minute})
		require.NoError(t, err)
		require.NotNil(t, lease)
		if route == "imported-submit-only" {
			grant, err := f.client.CreateDelegation(f.ctx, lease, jobdb.CreateDelegationRequest{Permissions: jobdb.DelegationPermissions{Submit: &jobdb.DelegationSubmitPermission{JobTypes: jobdb.DelegationNames{Names: []string{"child"}}, JobIDs: jobdb.DelegationNames{Any: true}, CellNames: &jobdb.DelegationNames{Names: []string{"test"}}}}})
			require.NoError(t, err)
			capability, err := remote.ExportLease(grant)
			require.NoError(t, err)
			encoded, err := capability.Encode()
			require.NoError(t, err)
			decoded, err := remote.DecodeLeaseCapability(encoded)
			require.NoError(t, err)
			lease, err = f.client.ImportLease(context.Background(), decoded)
			require.NoError(t, err)
			// The imported submit-only token cannot read even its parent's chapter.
			token := lease.(interface{ LeaseToken() string }).LeaseToken()
			_, err = f.client.GetChapter(context.Background(), jobdb.ChapterRef{JobKey: owner.JobKey, Ordinal: 0, LeaseID: lease.LeaseID(), LeaseToken: token})
			require.ErrorIs(t, err, jobdb.ErrLeasePermissionDenied)
			probe, err := f.backend.SubmitJob(f.ctx, availabilityRequest("prerequisite-read-probe", nil))
			require.NoError(t, err)
			read, err := http.NewRequest("GET", f.server.URL+"/v1/tenants/c2/jobs/"+probe.JobKey.JobId, nil)
			require.NoError(t, err)
			read.Header.Set("X-JobDB-Lease-Token", token)
			response, err := f.server.Client().Do(read)
			require.NoError(t, err)
			require.Equal(t, http.StatusForbidden, response.StatusCode)
			require.NoError(t, response.Body.Close())

			forbidden := availabilityRequest("forbidden", nil)
			forbidden.Job.Cell = "elsewhere"
			_, err = lease.SubmitJob(context.Background(), forbidden)
			require.ErrorIs(t, err, jobdb.ErrLeasePermissionDenied)
		}
		selected := availabilityRequest("unsupported", nil)
		selected.Job.GoalID = owner.GoalID
		_, err = lease.SubmitJob(f.ctx, selected)
		require.ErrorIs(t, err, jobdb.ErrPrimaryGoalSelectionUnsupported)
		return lease.SubmitJob
	default:
		t.Fatalf("unknown route %s", route)
		return nil
	}
}

var availabilityRoutes = []string{"native", "direct-explicit", "direct-generated", "http-put", "http-post", "selected-direct", "selected-http", "native-parent", "http-parent", "imported-submit-only"}

func (f *availabilityFixture) noPublication(id string) {
	f.t.Helper()
	for _, table := range []string{"jobs", "job_facts", "dependency_waits"} {
		var n int
		require.NoError(f.t, f.db.QueryRowContext(f.ctx, `SELECT count(*) FROM pgjobdb.`+table+` WHERE tenant_id='c2' AND job_id=$1`, id).Scan(&n))
		require.Zero(f.t, n, table)
	}
	var n int
	require.NoError(f.t, f.db.QueryRowContext(f.ctx, `SELECT count(*) FROM pgjobdb.job_preparations WHERE tenant_id='c2' AND job_id=$1 AND (published OR receipt IS NOT NULL)`, id).Scan(&n))
	require.Zero(f.t, n)
}

func TestSubmissionAvailabilityPublicationRoutes(t *testing.T) {
	newDSN := conformanceDSNFactory(t)
	for _, route := range availabilityRoutes {
		t.Run(route, func(t *testing.T) {
			t.Parallel()
			f := newAvailabilityFixture(t, newDSN(t))
			submit := f.submitter(route)
			producer, err := f.backend.SubmitJob(f.ctx, availabilityRequest("producer", nil))
			require.NoError(t, err)
			for i, bad := range []time.Time{time.Time{}.Add(-time.Nanosecond), time.Date(9999, 12, 31, 23, 59, 59, 999999001, time.UTC), time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC)} {
				id := fmt.Sprintf("invalid-%d", i)
				req := availabilityRequest(id, &bad)
				req.Job.Dependencies = []jobdb.Dependency{{Target: jobdb.DependencyTarget{JobID: producer.JobKey.JobId}, Condition: jobdb.JobPrereqSuccess}}
				var before string
				require.NoError(t, f.db.QueryRowContext(f.ctx, `SELECT jsonb_build_array((SELECT count(*) FROM pgjobdb.job_facts),(SELECT count(*) FROM pgjobdb.retained_goal_projection),(SELECT count(*) FROM pgjobdb.dependency_waits),(SELECT count(*) FROM pgjobdb.job_preparations WHERE published OR receipt IS NOT NULL))::text`).Scan(&before))
				_, err = submit(f.ctx, req)
				require.Error(t, err)
				f.noPublication(id)
				var after string
				require.NoError(t, f.db.QueryRowContext(f.ctx, `SELECT jsonb_build_array((SELECT count(*) FROM pgjobdb.job_facts),(SELECT count(*) FROM pgjobdb.retained_goal_projection),(SELECT count(*) FROM pgjobdb.dependency_waits),(SELECT count(*) FROM pgjobdb.job_preparations WHERE published OR receipt IS NOT NULL))::text`).Scan(&after))
				require.Equal(t, before, after)
				good := time.Date(2300, 1, 1, 0, 0, 0, 123456789, time.UTC)
				req.Job.AvailableAt = &good
				h, err := submit(f.ctx, req)
				require.NoError(t, err)
				f.assertReads(h.JobKey, effectiveAvailability(good))
				if route != "native" {
					var descriptor string
					require.NoError(t, f.db.QueryRowContext(f.ctx, `SELECT descriptor FROM pgjobdb.job_preparations WHERE job_id=$1`, h.JobKey.JobId).Scan(&descriptor))
					require.Contains(t, descriptor, "2300-01-01T00:00:00.123456789Z")
				}
				if strings.HasPrefix(route, "selected-") {
					require.NoError(t, pgjobdb.CancelJob(f.ctx, f.db, pgjobdb.CancelJobRequest{TenantID: "c2", JobID: pgjobdb.JobID(h.JobKey.JobId), WorkerID: "fixture-drain"}))
				}
			}
			if strings.HasPrefix(route, "http-") {
				f.mu.Lock()
				defer f.mu.Unlock()
				verb := "PUT"
				if route == "http-post" {
					verb = "POST"
				}
				require.Contains(t, strings.Join(f.methods, "\n"), verb+" /v1/tenants/c2/jobs")
			}
		})
	}
}

func (f *availabilityFixture) assertReads(key jobdb.JobKey, want time.Time) {
	t := f.t
	t.Helper()
	native, err := pgjobdb.GetJob(f.ctx, f.db, pgjobdb.TenantID(key.TenantId), pgjobdb.JobID(key.JobId))
	require.NoError(t, err)
	full, err := pgjobdb.ListJobs(f.ctx, f.db, pgjobdb.ListJobsOptions{TenantIDs: []pgjobdb.TenantID{"c2"}, JobKeys: []pgjobdb.JobKey{{TenantID: "c2", JobID: pgjobdb.JobID(key.JobId)}}})
	require.NoError(t, err)
	require.Len(t, full.Jobs, 1)
	for _, rt := range []jobdb.ConsumerRuntime{f.backend, f.client} {
		detailAt := want
		if !f.native {
			detail, err := jobdb.GetJobRun(f.ctx, rt, jobdb.GetJobRunRequest{JobKey: key})
			require.NoError(t, err)
			detailAt = detail.Job.AvailableAt
		}
		page, err := rt.ListJobs(f.ctx, jobdb.ListJobsRequest{TenantIds: []string{"c2"}, JobKeys: []jobdb.JobKey{key}})
		require.NoError(t, err)
		require.Len(t, page.Jobs, 1)
		for _, v := range []time.Time{native.AvailableAt, full.Jobs[0].AvailableAt, detailAt, page.Jobs[0].AvailableAt} {
			require.True(t, want.Equal(v), "got %s want %s", v, want)
			require.Same(t, time.UTC, v.Location())
		}
	}
}

// Decode immutable identity independently of the effective scheduler time.
func (f *availabilityFixture) descriptor(key jobdb.JobKey) string {
	var descriptor string
	require.NoError(f.t, f.db.QueryRowContext(f.ctx, `SELECT descriptor FROM pgjobdb.job_preparations WHERE job_id=$1`, key.JobId).Scan(&descriptor))
	var envelope struct {
		Job struct{ AvailableAt *time.Time }
	}
	require.NoError(f.t, json.Unmarshal([]byte(descriptor), &envelope))
	require.NotNil(f.t, envelope.Job.AvailableAt)
	return descriptor
}

// Native scheduler callers normally own history separately. Stage a real first
// chapter for workflow settlement tests, then publish with the typed native API.
func (f *availabilityFixture) stageNative(ctx context.Context, req jobdb.SubmitJobRequest, native *pgjobdb.SubmitJobRequest) {
	chapters, err := chapterpostgres.NewSQLDB(ctx, f.db, chapterpostgres.Config{Projects: projectfixture.Control(f.db), BlobStoreURI: f.cfg.BlobStoreURI})
	require.NoError(f.t, err)
	defer chapters.Close(ctx)
	schemas, err := schemapostgres.NewSQLDB(ctx, f.db, projectfixture.Control(f.db))
	require.NoError(f.t, err)
	var staged runtimecore.CreateJobRequest
	scheduler := &selectedFaultScheduler{Scheduler: runtimeadapter.Scheduler{DB: f.db}, before: func(_ context.Context, r runtimecore.CreateJobRequest) error {
		staged = r
		return jobdb.ErrExecutionLeaseLost
	}}
	core, err := runtimecore.NewRuntime(runtimecore.Config{Projects: projectfixture.Control(f.db), Scheduler: scheduler, Chapters: chapters, Schemas: schemas, Configurations: newConfigurationStore(f.t, ctx, f.db)})
	require.NoError(f.t, err)
	_, err = core.SubmitJob(ctx, req)
	require.ErrorIs(f.t, err, jobdb.ErrExecutionLeaseLost)
	native.GoalID = staged.GoalID
	native.Target = staged.Cell
	native.AppMetadata = staged.AppMetadata
	native.ClientPayloadUpdate = &pgjobdb.ClientPayloadUpdate{Mode: "reset", Value: staged.ClientPayload}
	native.Runtime.SchemaHash = staged.SchemaHash
}

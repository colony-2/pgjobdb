package integration_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/remote"
	"github.com/colony-2/jobdb/pkg/workflow"
	"github.com/colony-2/pgjobdb/pkg/pgjobdb"
	pgjobdbruntime "github.com/colony-2/pgjobdb/pkg/pgjobdb/runtime"
	"github.com/lib/pq"
	"github.com/stretchr/testify/require"
)

// All cases share a server; each fixture gets a fresh database and artifact store.
func TestPostgresSuppliedLease(t *testing.T) {
	newDSN := conformanceDSNFactory(t)
	newFixture := func(t *testing.T) *suppliedFixture { t.Helper(); return newSuppliedFixture(t, newDSN(t)) }
	t.Run("SuppliedRemoteLeaseRenewsAndPersists", func(t *testing.T) { runSuppliedRemoteLeaseRenewsAndPersists(t, newFixture) })
	t.Run("SuppliedLeaseAdmissionReschedulesWithoutWork", func(t *testing.T) { runSuppliedLeaseAdmissionReschedulesWithoutWork(t, newFixture) })
	t.Run("SuppliedLeaseLossCancelsWithoutFailureChapter", func(t *testing.T) { runSuppliedLeaseLossCancelsWithoutFailureChapter(t, newFixture) })
	t.Run("SuppliedLeaseInvalidBeforeExecution", func(t *testing.T) { runSuppliedLeaseInvalidBeforeExecution(t, newFixture) })
	t.Run("SuppliedLeaseTaskHandoffAndResume", func(t *testing.T) { runSuppliedLeaseTaskHandoffAndResume(t, newFixture) })
	t.Run("SuppliedLeaseCallerCancellationStopsHeartbeat", func(t *testing.T) { runSuppliedLeaseCallerCancellationStopsHeartbeat(t, newFixture) })
	t.Run("SuppliedLeaseInvalidBeforeImport", func(t *testing.T) { runSuppliedLeaseInvalidBeforeImport(t, newFixture) })
	t.Run("RenewalDatabaseErrorsAndDeadlines", func(t *testing.T) { runRenewalDatabaseErrorsAndDeadlines(t, newFixture) })
	t.Run("SuppliedLeaseDatabaseFailureStopsExecution", func(t *testing.T) { runSuppliedLeaseDatabaseFailureStopsExecution(t, newFixture) })
	t.Run("SuppliedLeaseReleaseDuringRenewal", func(t *testing.T) { runSuppliedLeaseReleaseDuringRenewal(t, newFixture) })
	t.Run("SuppliedLeaseRouteSnapshots", func(t *testing.T) { runSuppliedLeaseRouteSnapshots(t, newFixture) })
}

type suppliedJob struct {
	run func(workflow.ExecutionContext, jobdb.JobData) (jobdb.JobData, error)
}

func (suppliedJob) Name() string { return "supplied" }
func (j suppliedJob) Run(ctx workflow.ExecutionContext, data jobdb.JobData) (jobdb.JobData, error) {
	return j.run(ctx, data)
}

type suppliedTask struct{}

func (suppliedTask) Name() string { return "echo" }
func (suppliedTask) Run(_ workflow.TaskContext, data jobdb.TaskData) (jobdb.TaskData, error) {
	return data, nil
}

type suppliedFixture struct {
	backend                *pgjobdbruntime.Runtime
	dispatcher, receiver   *remote.Runtime
	key                    jobdb.JobKey
	lease                  jobdb.ExecutionLease
	acquisitions, renewals atomic.Int64
	failRenew              atomic.Bool
	db                     *sql.DB
	blockRenew             atomic.Bool
	renewEntered           chan struct{}
	renewRelease           chan struct{}
}

func newSuppliedFixture(t *testing.T, dsn string) *suppliedFixture {
	t.Helper()
	ctx := context.Background()
	f := &suppliedFixture{renewEntered: make(chan struct{}, 1), renewRelease: make(chan struct{})}
	db, err := sql.Open("postgres", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	backend, err := pgjobdbruntime.NewSQLDB(ctx, db, pgjobdbruntime.Config{BlobStoreURI: "blobfs://" + t.TempDir(), MaxInlineArtifactBytes: 1})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, backend.Close(context.Background())) })
	f.backend = backend
	f.db = db
	handler := remote.NewServer(backend)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Receiver") == "yes" {
			if r.URL.Path == "/v1/jobs/poll" || strings.HasSuffix(r.URL.Path, "/lease") {
				f.acquisitions.Add(1)
				http.Error(w, "acquisition forbidden", http.StatusForbidden)
				return
			}
			if strings.HasSuffix(r.URL.Path, "/keepalive") {
				f.renewals.Add(1)
				if f.blockRenew.Load() {
					select {
					case f.renewEntered <- struct{}{}:
					default:
					}
					select {
					case <-f.renewRelease:
					case <-r.Context().Done():
						return
					}
				}
				if f.failRenew.Load() {
					http.Error(w, "injected failure", http.StatusServiceUnavailable)
					return
				}
			}
		}
		handler.ServeHTTP(w, r)
	}))
	t.Cleanup(server.Close)
	f.dispatcher, err = remote.New(server.URL, server.Client())
	require.NoError(t, err)
	f.receiver, err = remote.New(server.URL, &http.Client{Transport: receiverTransport{base: server.Client().Transport}})
	require.NoError(t, err)
	handle, err := f.dispatcher.SubmitJob(ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", JobType: "supplied", Data: jobdb.NewTaskDataOrPanic(7, jobdb.NewArtifactFromBytes("input.txt", []byte("artifact"))), ClientPayloadUpdate: &jobdb.ClientPayloadUpdate{Mode: "reset", Value: json.RawMessage(`{"cursor":1}`)}}})
	require.NoError(t, err)
	f.key = handle.JobKey
	f.lease, err = f.dispatcher.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: f.key, WorkerID: "dispatcher", Routes: []jobdb.Route{{JobType: "supplied"}}, LeaseDuration: time.Second})
	require.NoError(t, err)
	require.NotNil(t, f.lease)
	return f
}

type receiverTransport struct{ base http.RoundTripper }

func (r receiverTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	req = req.Clone(req.Context())
	req.Header.Set("Receiver", "yes")
	return r.base.RoundTrip(req)
}
func (f *suppliedFixture) imported(t *testing.T) jobdb.RenewableExecutionLease {
	t.Helper()
	capability, err := remote.ExportLease(f.lease)
	require.NoError(t, err)
	encoded, err := capability.Encode()
	require.NoError(t, err)
	decoded, err := remote.DecodeLeaseCapability(encoded)
	require.NoError(t, err)
	lease, err := f.receiver.ImportLease(context.Background(), decoded)
	require.NoError(t, err)
	require.Equal(t, f.lease.LeaseID(), lease.LeaseID())
	require.Equal(t, "dispatcher", lease.LeaseWorkerID())
	return lease
}
func runSuppliedRemoteLeaseRenewsAndPersists(t *testing.T, newFixture func(*testing.T) *suppliedFixture) {
	f := newFixture(t)
	lease := f.imported(t)
	job := suppliedJob{run: func(ctx workflow.ExecutionContext, data jobdb.JobData) (jobdb.JobData, error) {
		if err := ctx.AwaitDuration(jobdb.Duration(1400 * time.Millisecond)); err != nil {
			return nil, err
		}
		if !time.Now().After(f.lease.(jobdb.RenewableExecutionLease).LeaseExpiry()) {
			return nil, errors.New("expected execution past original token expiry")
		}
		if _, err := ctx.SubmitJob(context.Background(), jobdb.SubmitJob{JobType: "child", Data: jobdb.NewTaskDataOrPanic(1)}); err != nil {
			return nil, err
		}
		return ctx.DoTask(workflow.TaskCall{Command: []byte("{}"), Site: "echo", TaskType: "echo", ContractVersion: 1, Policy: jobdb.RunPolicy{}}, func(context.Context) (jobdb.TaskData, error) { return data, nil })
	}}
	runnable, err := workflow.GetJobForRunWithLease(context.Background(), f.receiver, lease, workflow.GetJobForRunRequest{JobKey: f.key, JobWorker: job, TaskWorkers: []workflow.TaskWorker{suppliedTask{}}, BeforeRun: func(_ context.Context, l jobdb.ExecutionLease) error {
		require.JSONEq(t, `{"cursor":1}`, string(l.ClientPayload()))
		_, err := remote.ExportLease(l)
		return err
	}})
	require.NoError(t, err)
	outcome, err := runnable.Run(nil)
	require.NoError(t, err)
	require.Equal(t, workflow.JobRunCompleted, outcome.Status)
	require.Zero(t, f.acquisitions.Load())
	require.GreaterOrEqual(t, f.renewals.Load(), int64(4))
	artifacts, err := outcome.Output.GetArtifacts()
	require.NoError(t, err)
	require.Len(t, artifacts, 1)
	raw, err := artifacts[0].Bytes(context.Background())
	require.NoError(t, err)
	require.Equal(t, "artifact", string(raw))
	var completedLease, completedOwner string
	require.NoError(t, f.db.QueryRow(`SELECT lease_id,final_lease_worker_id FROM pgjobdb.jobs_archive WHERE job_id=$1`, f.key.JobId).Scan(&completedLease, &completedOwner))
	require.Equal(t, f.lease.LeaseID(), completedLease)
	require.Equal(t, "dispatcher", completedOwner)
	count := f.renewals.Load()
	time.Sleep(100 * time.Millisecond)
	require.Equal(t, count, f.renewals.Load())
}
func runSuppliedLeaseAdmissionReschedulesWithoutWork(t *testing.T, newFixture func(*testing.T) *suppliedFixture) {
	f := newFixture(t)
	var called atomic.Bool
	runnable, err := workflow.GetJobForRunWithLease(context.Background(), f.receiver, f.imported(t), workflow.GetJobForRunRequest{JobKey: f.key, JobWorker: suppliedJob{run: func(_ workflow.ExecutionContext, data jobdb.JobData) (jobdb.JobData, error) {
		called.Store(true)
		return data, nil
	}}, BeforeRun: func(ctx context.Context, l jobdb.ExecutionLease) error {
		return l.Reschedule(ctx, jobdb.RescheduleExecutionRequest{NextRoute: jobdb.Route{JobType: "elsewhere"}})
	}})
	require.NoError(t, err)
	out, err := runnable.Run(nil)
	require.NoError(t, err)
	require.Equal(t, workflow.JobRunSuspended, out.Status)
	require.False(t, called.Load())
	require.Zero(t, f.acquisitions.Load())
	chapters, err := f.backend.ListChapters(context.Background(), jobdb.ListChaptersRequest{JobKey: f.key})
	require.NoError(t, err)
	require.Len(t, chapters, 1)
}
func runSuppliedLeaseLossCancelsWithoutFailureChapter(t *testing.T, newFixture func(*testing.T) *suppliedFixture) {
	for _, transport := range []bool{false, true} {
		t.Run(map[bool]string{false: "cancelled", true: "transport"}[transport], func(t *testing.T) {
			f := newFixture(t)
			entered := make(chan struct{})
			exited := make(chan struct{})
			runnable, err := workflow.GetJobForRunWithLease(context.Background(), f.receiver, f.imported(t), workflow.GetJobForRunRequest{JobKey: f.key, JobWorker: suppliedJob{run: func(ctx workflow.ExecutionContext, data jobdb.JobData) (jobdb.JobData, error) {
				close(entered)
				defer close(exited)
				return nil, ctx.AwaitDuration(jobdb.Duration(time.Minute))
			}}})
			require.NoError(t, err)
			result := make(chan error, 1)
			go func() { _, err := runnable.Run(nil); result <- err }()
			<-entered
			if transport {
				f.failRenew.Store(true)
			} else {
				require.NoError(t, f.backend.CancelJob(context.Background(), jobdb.CancelJobRequest{JobKey: f.key}))
			}
			select {
			case err := <-result:
				var renewal *jobdb.LeaseRenewalError
				require.ErrorAs(t, err, &renewal)
				require.Equal(t, !transport, errors.Is(err, jobdb.ErrExecutionLeaseLost))
			case <-time.After(3 * time.Second):
				t.Fatal("lease loss did not stop runner")
			}
			select {
			case <-exited:
			case <-time.After(time.Second):
				t.Fatal("execution context not cancelled")
			}
			chapters, err := f.backend.ListChapters(context.Background(), jobdb.ListChaptersRequest{JobKey: f.key})
			require.NoError(t, err)
			requireNoExecutionOutcome(t, chapters)
			require.Zero(t, f.acquisitions.Load())
		})
	}
}
func runSuppliedLeaseInvalidBeforeExecution(t *testing.T, newFixture func(*testing.T) *suppliedFixture) {
	for _, kind := range []string{"cancelled", "expired", "completed", "superseded", "revoked", "initial_transport", "wrong_job", "wrong_worker", "unsupported_route"} {
		t.Run(kind, func(t *testing.T) {
			f := newFixture(t)
			lease := f.imported(t)
			var called atomic.Bool
			req := workflow.GetJobForRunRequest{JobKey: f.key, JobWorker: suppliedJob{run: func(_ workflow.ExecutionContext, d jobdb.JobData) (jobdb.JobData, error) {
				called.Store(true)
				return d, nil
			}}}
			if kind == "wrong_job" {
				req.JobKey.JobId = "different"
			}
			if kind == "wrong_worker" {
				req.WorkerID = "receiver"
			}
			if kind == "unsupported_route" {
				require.NoError(t, lease.Reschedule(context.Background(), jobdb.RescheduleExecutionRequest{NextRoute: jobdb.Route{JobType: "other"}}))
				var err error
				f.lease, err = f.dispatcher.GetJobLease(context.Background(), jobdb.GetJobLeaseRequest{JobKey: f.key, WorkerID: "dispatcher", Routes: []jobdb.Route{{JobType: "other"}}})
				require.NoError(t, err)
				lease = f.imported(t)
			}
			runnable, err := workflow.GetJobForRunWithLease(context.Background(), f.receiver, lease, req)
			if kind == "wrong_job" || kind == "wrong_worker" {
				require.ErrorIs(t, err, jobdb.ErrExecutionLeaseLost)
				return
			}
			require.NoError(t, err)
			switch kind {
			case "expired":
				time.Sleep(time.Until(lease.LeaseExpiry()) + 10*time.Millisecond)
			case "cancelled", "completed", "revoked", "superseded":
				f.invalidate(t, kind, lease)
			}
			if kind == "initial_transport" {
				f.failRenew.Store(true)
			}
			_, err = runnable.Run(nil)
			require.Error(t, err)
			require.False(t, called.Load())
			require.Zero(t, f.acquisitions.Load())
		})
	}
}
func runSuppliedLeaseTaskHandoffAndResume(t *testing.T, newFixture func(*testing.T) *suppliedFixture) {
	f := newFixture(t)
	job := suppliedJob{run: func(ctx workflow.ExecutionContext, d jobdb.JobData) (jobdb.JobData, error) {
		return ctx.DoTask(workflow.TaskCall{Command: []byte("{}"), Site: "echo", TaskType: "echo", ContractVersion: 1, Policy: jobdb.RunPolicy{}}, func(context.Context) (jobdb.TaskData, error) { return d, nil })
	}}
	first, err := workflow.GetJobForRunWithLease(context.Background(), f.receiver, f.imported(t), workflow.GetJobForRunRequest{JobKey: f.key, JobWorker: job})
	require.NoError(t, err)
	out, err := first.Run(nil)
	require.NoError(t, err)
	require.Equal(t, workflow.JobRunSuspended, out.Status)
	require.Zero(t, f.acquisitions.Load())
	f.lease, err = f.dispatcher.GetJobLease(context.Background(), jobdb.GetJobLeaseRequest{JobKey: f.key, WorkerID: "task-owner", Routes: []jobdb.Route{{JobType: "supplied", TaskType: "echo"}}})
	require.NoError(t, err)
	require.NotNil(t, f.lease)
	cap, err := remote.ExportLease(f.lease)
	require.NoError(t, err)
	imported, err := f.receiver.ImportLease(context.Background(), cap)
	require.NoError(t, err)
	require.Equal(t, f.lease.ExecutionState(), imported.ExecutionState())
	require.Equal(t, f.lease.Route(), imported.Route())
	require.NotNil(t, imported.ExecutionState().TaskWait)
	require.Equal(t, f.lease.ClientPayloadRevision(), imported.ClientPayloadRevision())
	require.JSONEq(t, string(f.lease.ClientPayload()), string(imported.ClientPayload()))
	second, err := workflow.GetJobForRunWithLease(context.Background(), f.receiver, imported, workflow.GetJobForRunRequest{JobKey: f.key, JobWorker: job, TaskWorkers: []workflow.TaskWorker{suppliedTask{}}})
	require.NoError(t, err)
	out, err = second.Run(nil)
	require.NoError(t, err)
	require.Equal(t, workflow.JobRunCompleted, out.Status)
	require.Zero(t, f.acquisitions.Load())
}

func runSuppliedLeaseCallerCancellationStopsHeartbeat(t *testing.T, newFixture func(*testing.T) *suppliedFixture) {
	f := newFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	entered := make(chan struct{})
	exited := make(chan struct{})
	runnable, err := workflow.GetJobForRunWithLease(ctx, f.receiver, f.imported(t), workflow.GetJobForRunRequest{JobKey: f.key, JobWorker: suppliedJob{run: func(ctx workflow.ExecutionContext, d jobdb.JobData) (jobdb.JobData, error) {
		close(entered)
		defer close(exited)
		return nil, ctx.AwaitDuration(jobdb.Duration(time.Minute))
	}}})
	require.NoError(t, err)
	result := make(chan error, 1)
	go func() { _, err := runnable.Run(nil); result <- err }()
	<-entered
	cancel()
	select {
	case err := <-result:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("runner ignored cancellation")
	}
	<-exited
	count := f.renewals.Load()
	time.Sleep(100 * time.Millisecond)
	require.Equal(t, count, f.renewals.Load())
	require.Zero(t, f.acquisitions.Load())
	chapters, err := f.backend.ListChapters(context.Background(), jobdb.ListChaptersRequest{JobKey: f.key})
	require.NoError(t, err)
	requireNoExecutionOutcome(t, chapters)
}

func (f *suppliedFixture) invalidate(t *testing.T, kind string, lease jobdb.RenewableExecutionLease) {
	t.Helper()
	ctx := context.Background()
	switch kind {
	case "cancelled":
		require.NoError(t, f.backend.CancelJob(ctx, jobdb.CancelJobRequest{JobKey: f.key}))
	case "expired", "superseded":
		_, err := f.db.Exec(`UPDATE pgjobdb.jobs SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE job_id=$1`, f.key.JobId)
		require.NoError(t, err)
		if kind == "superseded" {
			next, err := f.dispatcher.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: f.key, WorkerID: "replacement", Routes: []jobdb.Route{{JobType: "supplied"}}})
			require.NoError(t, err)
			require.NotNil(t, next)
			require.NotEqual(t, lease.LeaseID(), next.LeaseID())
		}
	case "revoked":
		require.NoError(t, lease.Reschedule(ctx, jobdb.RescheduleExecutionRequest{NextRoute: jobdb.Route{JobType: "supplied"}}))
	case "completed":
		require.NoError(t, pgjobdb.CompleteJob(ctx, f.db, pgjobdb.LeaseIdentity{TenantID: pgjobdb.TenantID(f.key.TenantId), JobID: pgjobdb.JobID(f.key.JobId), LeaseID: lease.LeaseID(), WorkerID: pgjobdb.WorkerID(lease.LeaseWorkerID())}, pgjobdb.Completion{Status: pgjobdb.CompletionSuccess}))
	default:
		t.Fatalf("unknown invalidation: %s", kind)
	}
}

func runSuppliedLeaseInvalidBeforeImport(t *testing.T, newFixture func(*testing.T) *suppliedFixture) {
	for _, kind := range []string{"cancelled", "expired", "superseded", "revoked", "completed"} {
		t.Run(kind, func(t *testing.T) {
			f := newFixture(t)
			capability, err := remote.ExportLease(f.lease)
			require.NoError(t, err)
			f.invalidate(t, kind, f.lease.(jobdb.RenewableExecutionLease))
			_, err = f.receiver.ImportLease(context.Background(), capability)
			require.ErrorIs(t, err, jobdb.ErrExecutionLeaseLost)
			require.Zero(t, f.acquisitions.Load())
		})
	}
}

// Inject actual PostgreSQL failures at the boundary between the renewal update
// and the independent validation query. The committed extension is observable.
func validationFault(t *testing.T, db *sql.DB, statement string) {
	t.Helper()
	var definition string
	require.NoError(t, db.QueryRow(`SELECT pg_get_functiondef('pgjobdb.validate_native_lease(text,text,text,text)'::regprocedure)`).Scan(&definition))
	_, err := db.Exec(strings.Replace(definition, "BEGIN", "BEGIN\n"+statement, 1))
	require.NoError(t, err)
	t.Cleanup(func() { _, err := db.Exec(definition); require.NoError(t, err) })
}

func runRenewalDatabaseErrorsAndDeadlines(t *testing.T, newFixture func(*testing.T) *suppliedFixture) {
	for _, kind := range []string{"database", "deadline", "cancelled-context"} {
		t.Run(kind, func(t *testing.T) {
			f := newFixture(t)
			lease := f.lease.(jobdb.RenewableExecutionLease)
			var originalExpiry time.Time
			require.NoError(t, f.db.QueryRow(`SELECT lease_expires_at FROM pgjobdb.jobs WHERE job_id=$1`, f.key.JobId).Scan(&originalExpiry))
			ctx := context.Background()
			var cancel context.CancelFunc
			switch kind {
			case "database":
				validationFault(t, f.db, `RAISE EXCEPTION 'injected validation failure' USING ERRCODE='08006';`)
			case "deadline":
				validationFault(t, f.db, `PERFORM pg_sleep(5);`)
				ctx, cancel = context.WithTimeout(ctx, 100*time.Millisecond)
				defer cancel()
			case "cancelled-context":
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			start := time.Now()
			_, err := f.backend.RenewExecutionLeaseByID(ctx, f.key, lease.LeaseID(), lease.LeaseWorkerID(), 5*time.Second)
			require.Error(t, err)
			require.NotErrorIs(t, err, jobdb.ErrExecutionLeaseLost)
			require.Less(t, time.Since(start), 2*time.Second)
			if kind == "database" {
				var pgErr *pq.Error
				require.ErrorAs(t, err, &pgErr)
				require.Equal(t, pq.ErrorCode("08006"), pgErr.Code)
			} else {
				require.Error(t, ctx.Err())
			}
			var expiry time.Time
			require.NoError(t, f.db.QueryRow(`SELECT lease_expires_at FROM pgjobdb.jobs WHERE job_id=$1`, f.key.JobId).Scan(&expiry))
			if kind == "cancelled-context" {
				require.True(t, expiry.Equal(originalExpiry))
			} else {
				require.True(t, expiry.After(originalExpiry))
			}
			require.Zero(t, f.acquisitions.Load())
		})
	}
}

func runSuppliedLeaseDatabaseFailureStopsExecution(t *testing.T, newFixture func(*testing.T) *suppliedFixture) {
	for _, kind := range []string{"before-update", "after-update", "deadline"} {
		t.Run(kind, func(t *testing.T) {
			f := newFixture(t)
			lease := f.imported(t)
			releaseRenewal := sync.OnceFunc(func() { close(f.renewRelease) })
			defer releaseRenewal()
			runCtx, cancel := context.WithCancel(context.Background())
			defer cancel()
			entered, exited := make(chan struct{}), make(chan struct{})
			runnable, err := workflow.GetJobForRunWithLease(runCtx, f.receiver, lease, workflow.GetJobForRunRequest{JobKey: f.key, JobWorker: suppliedJob{run: func(ctx workflow.ExecutionContext, d jobdb.JobData) (jobdb.JobData, error) {
				// Admission renews before invoking the worker. Block only subsequent
				// heartbeats so fault setup cannot race with an expiry update.
				f.blockRenew.Store(true)
				close(entered)
				defer close(exited)
				// AwaitDuration persists execution history and validates the lease.
				// It can encounter the injected fault before the heartbeat does.
				// Wait only for cancellation so this test isolates renewal failure.
				<-ctx.Context().Done()
				return nil, ctx.Context().Err()
			}}})
			require.NoError(t, err)
			result := make(chan error, 1)
			go func() { _, err := runnable.Run(nil); result <- err }()
			select {
			case <-entered:
			case <-time.After(5 * time.Second):
				t.Fatal("application did not start")
			}
			select {
			case <-f.renewEntered:
			case <-time.After(5 * time.Second):
				t.Fatal("heartbeat did not start")
			}
			var before time.Time
			require.NoError(t, f.db.QueryRow(`SELECT lease_expires_at FROM pgjobdb.jobs WHERE job_id=$1`, f.key.JobId).Scan(&before))
			switch kind {
			case "before-update":
				_, err = f.db.Exec(`CREATE FUNCTION pgjobdb.fail_renewal() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected update failure' USING ERRCODE='08006'; END $$;
     CREATE TRIGGER fail_renewal BEFORE UPDATE OF lease_expires_at ON pgjobdb.jobs FOR EACH ROW EXECUTE FUNCTION pgjobdb.fail_renewal()`)
				require.NoError(t, err)
			case "after-update":
				validationFault(t, f.db, `RAISE EXCEPTION 'injected validation failure' USING ERRCODE='08006';`)
			case "deadline":
				validationFault(t, f.db, `PERFORM pg_sleep(5);`)
			}
			releaseRenewal()
			select {
			case err := <-result:
				var renewal *jobdb.LeaseRenewalError
				require.ErrorAs(t, err, &renewal)
				require.NotErrorIs(t, err, jobdb.ErrExecutionLeaseLost)
			case <-time.After(5 * time.Second):
				t.Fatal("renewal failure did not stop execution")
			}
			select {
			case <-exited:
			case <-time.After(time.Second):
				t.Fatal("application was not cancelled")
			}
			chapters, err := f.backend.ListChapters(context.Background(), jobdb.ListChaptersRequest{JobKey: f.key})
			require.NoError(t, err)
			requireNoExecutionOutcome(t, chapters)
			var after time.Time
			require.NoError(t, f.db.QueryRow(`SELECT lease_expires_at FROM pgjobdb.jobs WHERE job_id=$1`, f.key.JobId).Scan(&after))
			if kind == "before-update" {
				require.True(t, before.Equal(after))
			} else {
				require.True(t, after.After(before))
			}
			count := f.renewals.Load()
			require.Positive(t, count, "the injected failure must be observed by renewal")
			time.Sleep(100 * time.Millisecond)
			require.Equal(t, count, f.renewals.Load())
			require.Zero(t, f.acquisitions.Load())
		})
	}
}

func runSuppliedLeaseReleaseDuringRenewal(t *testing.T, newFixture func(*testing.T) *suppliedFixture) {
	for _, release := range []string{"complete", "reschedule"} {
		t.Run(release, func(t *testing.T) {
			f := newFixture(t)
			lease := f.imported(t)
			entered, finish := make(chan struct{}), make(chan struct{})
			runnable, err := workflow.GetJobForRunWithLease(context.Background(), f.receiver, lease, workflow.GetJobForRunRequest{JobKey: f.key, JobWorker: suppliedJob{run: func(ctx workflow.ExecutionContext, d jobdb.JobData) (jobdb.JobData, error) {
				close(entered)
				<-finish
				if release == "reschedule" {
					return ctx.DoTask(workflow.TaskCall{Command: []byte("{}"), Site: "elsewhere", TaskType: "elsewhere", ContractVersion: 1, Policy: jobdb.RunPolicy{}}, func(context.Context) (jobdb.TaskData, error) { return d, nil })
				}
				return d, nil
			}}})
			require.NoError(t, err)
			type result struct {
				out workflow.JobRunOutcome
				err error
			}
			results := make(chan result, 1)
			go func() { out, err := runnable.Run(nil); results <- result{out, err} }()
			select {
			case <-entered:
			case <-time.After(5 * time.Second):
				t.Fatal("application did not start")
			}
			f.blockRenew.Store(true)
			select {
			case <-f.renewEntered:
			case <-time.After(2 * time.Second):
				t.Fatal("heartbeat did not start")
			}
			close(finish)
			// A terminal operation must serialize behind the in-flight renewal.
			select {
			case got := <-results:
				t.Fatalf("released while renewal blocked: %+v", got)
			case <-time.After(30 * time.Millisecond):
			}
			close(f.renewRelease)
			select {
			case got := <-results:
				require.NoError(t, got.err)
				expected := workflow.JobRunCompleted
				if release == "reschedule" {
					expected = workflow.JobRunSuspended
				}
				require.Equal(t, expected, got.out.Status)
			case <-time.After(3 * time.Second):
				t.Fatal("release did not finish")
			}
			count := f.renewals.Load()
			time.Sleep(400 * time.Millisecond)
			require.Equal(t, count, f.renewals.Load())
			require.Zero(t, f.acquisitions.Load())
		})
	}
}

func runSuppliedLeaseRouteSnapshots(t *testing.T, newFixture func(*testing.T) *suppliedFixture) {
	for _, kind := range []string{"job", "task", "job-alternate", "task-alternate"} {
		t.Run(kind, func(t *testing.T) {
			f := newFixture(t)
			ctx := context.Background()
			req := jobdb.RescheduleExecutionRequest{NextRoute: jobdb.Route{JobType: "supplied"}}
			expected := req.NextRoute
			if kind != "job" {
				req.NextRoute.TaskType = "echo"
				req.TaskWait = &jobdb.TaskWait{ResumeJobType: "supplied", InputOrdinal: 2, OutputOrdinal: 3, InputHash: "input-hash"}
				expected = req.NextRoute
			}
			if kind == "job-alternate" || kind == "task-alternate" {
				expected = jobdb.Route{JobType: "fallback"}
				if kind == "task-alternate" {
					expected.TaskType = "fallback-echo"
				}
				req.AlternateRoute = &expected
				req.AlternateAfter = durationPtr(time.Second)
			}
			require.NoError(t, f.lease.Reschedule(ctx, req))
			if req.AlternateRoute != nil {
				early, err := f.dispatcher.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: f.key, WorkerID: "dispatcher", Routes: []jobdb.Route{expected}})
				require.NoError(t, err)
				require.Nil(t, early)
				_, err = f.db.Exec(`UPDATE pgjobdb.jobs SET alternate_at=clock_timestamp()-interval '1 second' WHERE job_id=$1`, f.key.JobId)
				require.NoError(t, err)
			}
			// Populate metadata whose authoritative values must survive transport.
			_, err := f.db.Exec(`UPDATE pgjobdb.job_facts SET schema_hash='schema-hash', run_policy='{"retry":{"maximum_attempts":3}}' WHERE job_id=$1`, f.key.JobId)
			require.NoError(t, err)
			f.lease, err = f.dispatcher.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: f.key, WorkerID: "dispatcher", Routes: []jobdb.Route{expected}, LeaseDuration: 1500 * time.Millisecond})
			require.NoError(t, err)
			require.NotNil(t, f.lease)
			require.Equal(t, expected, f.lease.Route())
			imported := f.imported(t)
			for i := 0; i < 3; i++ {
				require.Equal(t, expected, imported.Route())
				require.Equal(t, f.lease.ExecutionState(), imported.ExecutionState())
				require.Equal(t, f.lease.ClientPayloadRevision(), imported.ClientPayloadRevision())
				require.JSONEq(t, string(f.lease.ClientPayload()), string(imported.ClientPayload()))
				direct, err := f.backend.RenewExecutionLeaseByID(ctx, f.key, imported.LeaseID(), "dispatcher", 1500*time.Millisecond)
				require.NoError(t, err)
				require.Equal(t, expected, direct.Route())
				require.Equal(t, "schema-hash", direct.(interface{ LeaseSchemaHash() string }).LeaseSchemaHash())
				require.Equal(t, int32(3), direct.ExecutionState().RunPolicy.Retry.MaximumAttempts)
				var expiry time.Time
				require.NoError(t, f.db.QueryRow(`SELECT lease_expires_at FROM pgjobdb.jobs WHERE job_id=$1`, f.key.JobId).Scan(&expiry))
				require.True(t, expiry.Equal(direct.LeaseExpiry()))
				imported, err = imported.Renew(ctx)
				require.NoError(t, err)
				require.Equal(t, f.lease.LeaseID(), imported.LeaseID())
				require.Equal(t, "dispatcher", imported.LeaseWorkerID())
			}
			_, err = f.backend.RenewExecutionLeaseByID(ctx, f.key, imported.LeaseID(), "wrong-owner", time.Second)
			require.ErrorIs(t, err, jobdb.ErrExecutionLeaseLost)
			_, err = f.db.Exec(`UPDATE pgjobdb.job_client_state SET client_payload='{"cursor":2}',revision=revision+1 WHERE job_id=$1`, f.key.JobId)
			require.NoError(t, err)
			fresh, err := imported.Renew(ctx)
			require.NoError(t, err)
			require.Equal(t, imported.ClientPayloadRevision()+1, fresh.ClientPayloadRevision())
			require.JSONEq(t, `{"cursor":2}`, string(fresh.ClientPayload()))
			require.JSONEq(t, `{"cursor":1}`, string(imported.ClientPayload()))
			require.Zero(t, f.acquisitions.Load())
		})
	}
}

// Lease loss may leave execution declarations and waits, but no committed outcome.
func requireNoExecutionOutcome(t *testing.T, chapters []jobdb.Chapter) {
	t.Helper()
	require.NotEmpty(t, chapters)
	for _, chapter := range chapters {
		switch body := chapter.Body.(type) {
		case jobdb.JobAttemptOutcomeChapter:
			t.Fatal("lease loss committed a job outcome")
		case jobdb.ExecutionChapter:
			record, err := jobdb.DecodeExecutionRecord(body.Record)
			require.NoError(t, err)
			require.NotEqual(t, "result", record.Kind, "lease loss committed an execution outcome")
		}
	}
}

package jobdbtest

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/remote"
	"github.com/colony-2/jobdb/pkg/workflow"
	pgruntime "github.com/colony-2/pgjobdb/pkg/pgjobdb/runtime"
	"github.com/stretchr/testify/require"
)

type taskDelegationResult struct {
	lease jobdb.RenewableExecutionLease
	err   error
}
type consumerDelegationTask struct {
	observed chan taskDelegationResult
	release  <-chan struct{}
	deadline time.Time
}

func (consumerDelegationTask) Name() string { return "consumer-delegation-task" }
func (s consumerDelegationTask) Run(tc workflow.TaskContext, in jobdb.TaskData) (jobdb.TaskData, error) {
	lease, err := tc.CreateDelegation(context.Background(), jobdb.CreateDelegationRequest{
		NotAfter:    &s.deadline,
		Permissions: jobdb.DelegationPermissions{CreateCells: &jobdb.DelegationNames{Names: []string{"child"}}},
	})
	s.observed <- taskDelegationResult{lease, err}
	<-s.release
	return in, nil
}
func consumerDelegationWorker() workflow.ExecutionWorker {
	return workflow.ExecutionWorker{JobType: "consumer-delegation", Execute: func(c workflow.ExecutionContext, in jobdb.JobData) (jobdb.JobData, error) {
		return c.DoTask(workflow.TaskCall{Site: "delegate", TaskType: "consumer-delegation-task", ContractVersion: 1, Command: json.RawMessage(`{}`)}, func(context.Context) (jobdb.TaskData, error) { return in, nil })
	}}
}

// Exercise real workflow lease sessions and the actual harness construction
// boundary. A direct CreateDelegation call on a leased handle misses both bugs.
func TestPostgresTaskDelegation(t *testing.T) {
	for _, transport := range []string{"default", "signed"} {
		for _, path := range []string{"raw-runner", "namespaced-runner", "harness-engine"} {
			t.Run(transport+"/"+path, func(t *testing.T) {
				ctx := celltest.Context(context.Background())
				embedded, err := pgruntime.StartEmbeddedRuntime(ctx)
				require.NoError(t, err) // Required PostgreSQL failures must fail, never skip.
				t.Cleanup(embedded.Shutdown)
				backend := NewNamespacedBackend(embedded.Runtime, "")
				handler := remote.NewConsumerServer(backend)
				if transport == "signed" {
					handler, err = remote.NewConsumerServerWithOptions(backend, remote.ServerOptions{
						LeaseTokenSigningKey: []byte("task-delegation-key-at-least-32-bytes"), LeaseTokenAudience: "task-delegation",
					})
					require.NoError(t, err)
				}
				server := httptest.NewServer(celltest.Handler(handler))
				t.Cleanup(server.Close)
				client, err := remote.New(server.URL, server.Client())
				require.NoError(t, err)
				var rt jobdb.ConsumerRuntime = client
				if path != "raw-runner" {
					rt = NewNamespacedClient(client, "client-")
					assertConsumerOnly(t, rt)
				}
				var factory func(workflow.WorkSet) *BuiltRuntimeHarness
				if path == "harness-engine" {
					factory = func(ws workflow.WorkSet) *BuiltRuntimeHarness {
						return buildHarness(t, "delegation", rt, true, func() {}, ws)
					}
				}
				runTaskDelegation(t, rt, factory)
			})
		}
	}
	t.Run("standard-remote-postgres-harness", func(t *testing.T) {
		// Cover the normal factory as well as the nonempty namespace/signed matrix.
		runTaskDelegation(t, nil, func(ws workflow.WorkSet) *BuiltRuntimeHarness { return newRemoteDirectHarness(t, ws) })
	})
}

func runTaskDelegation(t *testing.T, rt jobdb.ConsumerRuntime, factory func(workflow.WorkSet) *BuiltRuntimeHarness) {
	t.Helper()
	ctx := celltest.Context(context.Background())
	worker := consumerDelegationWorker()
	release := make(chan struct{})
	task := consumerDelegationTask{make(chan taskDelegationResult, 1), release, time.Now().Add(time.Minute)}
	var handle jobdb.JobHandle
	var done <-chan error
	var shutdown func()
	released := false
	defer func() {
		if !released {
			close(release)
		}
		if shutdown != nil {
			shutdown()
		}
	}()
	q := jobdb.SubmitJob{TenantId: "tenant", Cell: "test", CellRepo: "example.test/test", JobType: worker.Name(), Data: jobdb.NewTaskDataOrPanic(nil), Schema: &jobdb.JobSchemaSelector{Schema: json.RawMessage(`{"chapterShape":{"type":"object"}}`)}}
	if factory != nil {
		h := factory(MustWorkSet(t, worker, task))
		rt = h.Runtime
		shutdown = func() { h.Shutdown(t) }
		q.TenantId = h.WorkerTenantID
		var err error
		handle, err = h.Engine.SubmitJob(ctx, q)
		require.NoError(t, err)
	} else {
		var err error
		handle, err = celltest.Submit(rt, ctx, jobdb.SubmitJobRequest{Job: q})
		require.NoError(t, err)
		run, err := workflow.GetJobForRun(ctx, rt, workflow.GetJobForRunRequest{JobKey: handle.JobKey, JobWorker: worker, TaskWorkers: []workflow.TaskWorker{task}, WorkerID: "delegation-worker", LeaseDuration: time.Minute})
		require.NoError(t, err)
		result := make(chan error, 1)
		done = result
		go func() { _, err := run.Run(nil); result <- err }()
		shutdown = func() {
			select {
			case err := <-done:
				require.NoError(t, err)
			case <-time.After(5 * time.Second):
				t.Error("runner did not stop")
			}
		}
	}
	var result taskDelegationResult
	select {
	case result = <-task.observed:
	case <-time.After(10 * time.Second):
		t.Fatal("delegation task did not run")
	}
	require.NoError(t, result.err)
	child := result.lease
	require.NotNil(t, child)
	require.Equal(t, handle, child.Job())
	require.NotEmpty(t, handle.GoalID)
	require.NotEmpty(t, handle.CellID)
	require.Equal(t, jobdb.Route{JobType: worker.Name()}, child.Route())
	require.False(t, child.LeaseExpiry().After(task.deadline))
	require.NotNil(t, child.(delegationSource).Delegation())
	renewed, err := child.Renew(ctx)
	require.NoError(t, err)
	require.Equal(t, handle, renewed.Job())
	require.Equal(t, child.(delegationSource).Delegation(), renewed.(delegationSource).Delegation())
	require.NotEmpty(t, renewed.(tokenSource).LeaseToken())
	created, err := renewed.CreateCell(ctx, jobdb.CreateCellRequest{Name: "child", Repo: "example.test/child"})
	require.NoError(t, err)
	require.Equal(t, handle.JobKey.TenantId, created.TenantID)
	_, err = renewed.CreateCell(ctx, jobdb.CreateCellRequest{Name: "denied", Repo: "example.test/denied"})
	require.ErrorIs(t, err, jobdb.ErrLeasePermissionDenied)
	_, err = renewed.SubmitJob(ctx, jobdb.SubmitJobRequest{Job: q})
	require.ErrorIs(t, err, jobdb.ErrLeasePermissionDenied)
	issuer := rt.(jobdb.LeaseDelegator)
	_, err = issuer.CreateDelegation(ctx, renewed, jobdb.CreateDelegationRequest{Permissions: jobdb.DelegationPermissions{CreateCells: &jobdb.DelegationNames{Any: true}}})
	require.ErrorIs(t, err, jobdb.ErrLeasePermissionDenied)
	parentDeadline := renewed.(delegationSource).Delegation().NotAfter
	require.NotNil(t, parentDeadline)
	later := parentDeadline.Add(time.Minute)
	_, err = issuer.CreateDelegation(ctx, renewed, jobdb.CreateDelegationRequest{NotAfter: &later,
		Permissions: jobdb.DelegationPermissions{CreateCells: &jobdb.DelegationNames{Names: []string{"child"}}}})
	require.ErrorIs(t, err, jobdb.ErrLeasePermissionDenied)
	// Hide private ownership just as the engine session does. The signed token
	// must still reject a different namespace's translated tenant.
	public := struct {
		jobdb.ExecutionLease
		tokenSource
	}{renewed, renewed.(tokenSource)}
	if ns, ok := rt.(*namespacedClient); ok {
		foreign := NewNamespacedClient(ns.runtime.(clientPorts), "foreign-")
		_, err = foreign.(jobdb.LeaseDelegator).CreateDelegation(ctx, public, jobdb.CreateDelegationRequest{})
		require.Error(t, err)
	}
	past := time.Now().Add(-time.Second)
	_, err = issuer.CreateDelegation(ctx, renewed, jobdb.CreateDelegationRequest{NotAfter: &past})
	require.Error(t, err)
	shortDeadline := time.Now().Add(500 * time.Millisecond)
	short, err := issuer.CreateDelegation(ctx, renewed, jobdb.CreateDelegationRequest{NotAfter: &shortDeadline,
		Permissions: jobdb.DelegationPermissions{CreateCells: &jobdb.DelegationNames{Names: []string{"child"}}}})
	require.NoError(t, err)
	time.Sleep(time.Until(shortDeadline) + 10*time.Millisecond)
	_, err = short.Renew(ctx)
	require.ErrorIs(t, err, jobdb.ErrLeasePermissionDenied)
	close(release)
	released = true
	waitForStatus(t, ctx, func(ctx context.Context) (jobdb.JobStatus, error) {
		info, err := rt.GetJob(ctx, handle.JobKey)
		return info.Status, err
	}, jobdb.JobStatusCompleted)
	_, err = renewed.Renew(ctx)
	require.ErrorIs(t, err, jobdb.ErrExecutionLeaseLost)
	_, err = renewed.CreateCell(ctx, jobdb.CreateCellRequest{Name: "child", Repo: "example.test/child"})
	require.ErrorIs(t, err, jobdb.ErrExecutionLeaseLost)
}

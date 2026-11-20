package integration_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/remote"
	"github.com/colony-2/jobdb/pkg/jobdb/runtimetest"
	"github.com/colony-2/pgjobdb/pkg/pgjobdb/runtime/integration/internal/jobdbtest"
	"github.com/stretchr/testify/require"
)

func TestConsumerCapabilities(t *testing.T) {
	factory := conformanceRuntimeFactory(t)
	for _, transport := range []string{"direct", "default", "signed"} {
		t.Run(transport, func(t *testing.T) {
			rt := factory(t)
			var consumer jobdb.ConsumerRuntime = jobdbtest.NewNamespacedBackend(rt, "identity-")
			if transport != "direct" {
				handler := remote.NewConsumerServer(jobdbtest.NewNamespacedBackend(rt, ""))
				if transport == "signed" {
					var err error
					handler, err = remote.NewConsumerServerWithOptions(jobdbtest.NewNamespacedBackend(rt, ""), remote.ServerOptions{LeaseTokenSigningKey: []byte("capability-signing-key-more-than-32-bytes"), LeaseTokenAudience: "consumer-capabilities"})
					require.NoError(t, err)
				}
				server := httptest.NewServer(selectionTestHandler(handler))
				t.Cleanup(server.Close)
				client, err := remote.New(server.URL, server.Client())
				require.NoError(t, err)
				consumer = jobdbtest.NewNamespacedClient(client, "identity-")
			}
			_, ok := consumer.(jobdb.GoalCandidateMutator)
			require.False(t, ok)
			_, ok = consumer.(jobdb.WorkflowRuntime)
			require.False(t, ok)
			ctx := runtimetest.PrimaryGoalContext(context.Background())
			// Administrative authority belongs to the test caller, never the decorator.
			noAuthority := context.Background()
			_, err := consumer.CreateCell(noAuthority, jobdb.CreateCellRequest{TenantID: "tenant", Name: "denied", Repo: "example.test/denied"})
			if transport == "direct" {
				require.ErrorIs(t, err, jobdb.ErrCellForbidden)
			}
			admin := consumer.(jobdb.ConfigurationAdmin)
			_, err = admin.SetTenantConfigurationValue(ctx, jobdb.SetTenantConfigurationValueRequest{TenantID: "tenant", Name: "setting", Value: json.RawMessage(`"one"`)})
			require.NoError(t, err)
			config, err := admin.GetTenantConfiguration(ctx, "tenant")
			require.NoError(t, err)
			require.JSONEq(t, `"one"`, string(config.Values["setting"]))
			wrong := "wrong"
			_, err = admin.RemoveTenantConfigurationValue(ctx, jobdb.RemoveTenantConfigurationValueRequest{TenantID: "tenant", Name: "setting", ExpectedRevision: &wrong})
			require.ErrorIs(t, err, jobdb.ErrConfigurationRevisionMismatch)
			schema, err := consumer.(jobdb.JobSchemaRegistry).RegisterJobSchema(ctx, jobdb.RegisterJobSchemaRequest{TenantId: "tenant", Schema: json.RawMessage(`{"chapterShape":{"type":"object"}}`)})
			require.NoError(t, err)
			require.Equal(t, "tenant", schema.TenantId)
			got, err := consumer.(jobdb.JobSchemaRegistry).GetJobSchema(ctx, jobdb.JobSchemaKey{TenantId: "tenant", SchemaHash: schema.SchemaHash})
			require.NoError(t, err)
			require.Equal(t, schema, got)
			submit := func(id, goal string, deps ...jobdb.Dependency) jobdb.JobHandle {
				t.Helper()
				h, e := celltest.Submit(consumer, ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", Cell: "test", CellRepo: "example.test/test", JobID: id, GoalID: goal, JobType: "work", Data: jobdb.NewTaskDataOrPanic("payload"), Schema: &jobdb.JobSchemaSelector{Hash: schema.SchemaHash}, Dependencies: deps}})
				require.NoError(t, e)
				return h
			}
			a := submit("source", "")
			selected := submit("selected", a.GoalID)
			require.NotEmpty(t, a.CellID)
			require.Equal(t, a.CellID, selected.CellID)
			require.Equal(t, a.CellRepo, selected.CellRepo)
			dependent := submit("dependent", "", jobdb.Dependency{Target: jobdb.DependencyTarget{GoalID: a.GoalID}, Condition: jobdb.JobPrereqSuccess})
			info, err := consumer.GetJob(ctx, dependent.JobKey)
			require.NoError(t, err)
			require.Equal(t, dependent.GoalID, info.GoalID)
			require.Empty(t, info.OtherGoalIDs)
			require.Equal(t, dependent.JobKey, info.BlockingWait.JobKey)
			listed, err := consumer.ListJobs(ctx, jobdb.ListJobsRequest{TenantIds: []string{"tenant"}, JobKeys: []jobdb.JobKey{selected.JobKey}})
			require.NoError(t, err)
			require.Len(t, listed.Jobs, 1)
			require.Equal(t, selected.JobKey, listed.Jobs[0].JobKey)
			require.Equal(t, a.GoalID, listed.Jobs[0].GoalID)
			lease, err := consumer.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: a.JobKey, WorkerID: "worker", Routes: []jobdb.Route{{JobType: "work"}}, LeaseDuration: time.Minute})
			require.NoError(t, err)
			require.NotNil(t, lease)
			require.Equal(t, a, lease.Job())
			renewed, err := lease.(jobdb.RenewableExecutionLease).Renew(ctx)
			require.NoError(t, err)
			require.Equal(t, a, renewed.Job())
			require.Equal(t, schema.SchemaHash, renewed.(interface{ LeaseSchemaHash() string }).LeaseSchemaHash())
			require.Equal(t, "worker", renewed.LeaseWorkerID())
			require.True(t, renewed.LeaseExpiry().After(time.Now()))
			if transport != "direct" {
				require.NotEmpty(t, renewed.(interface{ LeaseToken() string }).LeaseToken())
			}
			if transport != "direct" {
				until := time.Now().Add(30 * time.Second)
				grant := jobdb.CreateDelegationRequest{NotAfter: &until, Permissions: jobdb.DelegationPermissions{CreateCells: &jobdb.DelegationNames{Names: []string{"delegated-cell"}}}}
				delegated, e := consumer.(jobdb.LeaseDelegator).CreateDelegation(ctx, renewed, grant)
				require.NoError(t, e)
				require.Equal(t, a, delegated.Job())
				require.False(t, delegated.LeaseExpiry().After(until))
				_, e = delegated.CreateCell(ctx, jobdb.CreateCellRequest{Name: "delegated-cell", Repo: "example.test/delegated"})
				require.NoError(t, e)
				_, e = delegated.CreateCell(ctx, jobdb.CreateCellRequest{Name: "denied-cell", Repo: "example.test/denied"})
				require.ErrorIs(t, e, jobdb.ErrLeasePermissionDenied)
				_, e = consumer.(jobdb.LeaseDelegator).CreateDelegation(ctx, delegated, jobdb.CreateDelegationRequest{Permissions: jobdb.DelegationPermissions{CreateCells: &jobdb.DelegationNames{Any: true}}})
				require.ErrorIs(t, e, jobdb.ErrLeasePermissionDenied)
				refreshed, e := delegated.Renew(ctx)
				require.NoError(t, e)
				require.Equal(t, a, refreshed.Job())
				require.False(t, refreshed.LeaseExpiry().After(until))
			}
			snapshot, err := renewed.GetConfiguration(ctx)
			require.NoError(t, err)
			require.Equal(t, config, snapshot)
			cell, err := renewed.LookupCell(ctx, "test")
			require.NoError(t, err)
			require.Equal(t, a.CellID, cell.CellID)
			require.Equal(t, a.CellRepo, cell.CellRepo)
			created, err := renewed.CreateCell(ctx, jobdb.CreateCellRequest{Name: "child-cell", Repo: "example.test/child"})
			require.NoError(t, err)
			require.Equal(t, "tenant", created.TenantID)
			child, err := renewed.SubmitJob(ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", Cell: "test", CellRepo: "example.test/test", JobID: "child", JobType: "work", Data: jobdb.NewTaskDataOrPanic(nil)}})
			require.NoError(t, err)
			require.Equal(t, "tenant", child.JobKey.TenantId)
			require.NotEmpty(t, child.GoalID)
			require.Equal(t, a.CellID, child.CellID)
			group := jobdb.StartOrAppendJobRequest{Job: jobdb.StartOrAppendJob{AppendKey: "group", Append: json.RawMessage(`1`), Job: jobdb.SubmitJob{TenantId: "tenant", Cell: "test", CellRepo: "example.test/test", JobType: "work", Data: jobdb.NewTaskDataOrPanic(nil)}}}
			grouped, err := renewed.StartOrAppendJob(ctx, group)
			require.NoError(t, err)
			require.Equal(t, "tenant", grouped.JobKey.TenantId)
			require.NotEmpty(t, grouped.GoalID)
			repeated, err := renewed.StartOrAppendJob(ctx, group)
			require.NoError(t, err)
			require.Equal(t, grouped, repeated)
			require.NoError(t, renewed.Reschedule(ctx, jobdb.RescheduleExecutionRequest{NextRoute: jobdb.Route{JobType: "work"}}))
			_, err = renewed.Renew(ctx)
			require.ErrorIs(t, err, jobdb.ErrExecutionLeaseLost)
			again, err := consumer.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: a.JobKey, WorkerID: "worker-2", Routes: []jobdb.Route{{JobType: "work"}}, LeaseDuration: time.Minute})
			require.NoError(t, err)
			require.NotNil(t, again)
			chapter := jobdb.Chapter{Ordinal: 1, TaskType: "work", CreatedAt: time.Now().UTC(), Body: jobdb.JobAttemptOutcomeChapter{Outcome: jobdb.AppErrorOutcome{Error: jobdb.AppErrorPayload{Message: "ordinary failure"}}}}
			require.NoError(t, again.Complete(ctx, jobdb.CompleteExecutionRequest{Status: "failed_app", Chapter: &chapter}))
			restart, err := consumer.SubmitRestartJob(ctx, jobdb.SubmitRestartJobRequest{RequestID: "restart", Job: jobdb.SubmitRestartJob{PriorJobKey: a.JobKey, Cell: "test", ShareGoal: true}})
			require.NoError(t, err)
			require.Equal(t, a.GoalID, restart.GoalID)
			require.Equal(t, a.CellID, restart.CellID)
			// A live child can also submit a restart through the transported port.
			childLease, err := consumer.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: child.JobKey, WorkerID: "child-worker", Routes: []jobdb.Route{{JobType: "work"}}})
			require.NoError(t, err)
			require.NotNil(t, childLease)
			childRestart, err := childLease.SubmitRestartJob(ctx, jobdb.SubmitRestartJobRequest{Job: jobdb.SubmitRestartJob{PriorJobKey: a.JobKey, Cell: "test"}})
			require.NoError(t, err)
			require.Equal(t, "tenant", childRestart.JobKey.TenantId)
			require.NotEqual(t, a.GoalID, childRestart.GoalID)
			history, err := jobdb.GetJobRun(ctx, struct{ jobdb.JobRunReader }{consumer}, jobdb.GetJobRunRequest{JobKey: restart.JobKey, IncludeInputs: true})
			require.NoError(t, err)
			require.Equal(t, restart.JobKey, history.Job.JobKey)
			require.Equal(t, a.GoalID, history.Job.GoalID)
			deleted, err := consumer.(jobdb.JobDeletionRuntime).DeleteJobs(ctx, jobdb.DeleteJobsRequest{TenantID: "tenant", JobIDs: []string{child.JobKey.JobId}})
			require.NoError(t, err)
			require.EqualValues(t, 1, deleted.DeletedCount)
			_, err = childLease.(jobdb.RenewableExecutionLease).Renew(ctx)
			require.ErrorIs(t, err, jobdb.ErrExecutionLeaseLost)
			hidden, err := jobdb.GetJobRun(ctx, struct{ jobdb.JobRunReader }{consumer}, jobdb.GetJobRunRequest{JobKey: child.JobKey})
			require.NoError(t, err)
			require.Equal(t, child.GoalID, hidden.Job.GoalID)
		})
	}
}

func TestConsumerReadOnlyHistory(t *testing.T) {
	goalRuntimes(t, func(t *testing.T, consumer jobdb.ConsumerRuntime) {
		ctx := celltest.Context(context.Background())
		h := goalSubmit(t, consumer, "reader")
		goalComplete(t, consumer, h, true)
		reader := struct{ jobdb.JobRunReader }{consumer}
		_, mutator := any(reader).(jobdb.ConsumerRuntime)
		require.False(t, mutator)
		run, err := jobdb.GetJobRun(ctx, reader, jobdb.GetJobRunRequest{JobKey: h.JobKey, IncludeInputs: true, IncludeOutputs: true})
		require.NoError(t, err)
		require.Equal(t, h.JobKey, run.Job.JobKey)
		require.Equal(t, h.GoalID, run.Job.GoalID)
		require.Empty(t, run.Job.OtherGoalIDs)
		require.NotNil(t, run.Start.Input)
		_, err = jobdb.GetJobRun(ctx, reader, jobdb.GetJobRunRequest{JobKey: jobdb.JobKey{TenantId: "other", JobId: "reader"}})
		require.ErrorIs(t, err, jobdb.ErrJobNotFound)
	})
}

// The outer namespace translates tenant identities without supplying authority.
// Each HTTP ingress installs the same verified policy as the embedded caller.
func TestConsumerNamespacedAuthority(t *testing.T) {
	factory := conformanceRuntimeFactory(t)
	for _, transport := range []string{"direct", "default", "signed"} {
		t.Run(transport, func(t *testing.T) {
			rt := factory(t)
			admin := runtimetest.PrimaryGoalContext(context.Background())
			source, err := celltest.Submit(rt, admin, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "identity-tenant", Cell: "test", CellRepo: "example.test/test", JobID: "source", JobType: "work", Data: jobdb.NewTaskDataOrPanic(nil)}})
			require.NoError(t, err)
			for _, policy := range []struct {
				name, tenant       string
				permissions, cells map[string]bool
			}{
				{"allowed", "identity-tenant", map[string]bool{"jobs.submit": true, "goals.get": true, "goals.select-primary": true}, nil},
				{"no-selection", "identity-tenant", map[string]bool{"jobs.submit": true, "goals.get": true}, nil},
				{"wrong-tenant", "identity-other", map[string]bool{"jobs.submit": true, "goals.get": true, "goals.select-primary": true}, nil},
				{"wrong-cell", "identity-tenant", map[string]bool{"jobs.submit": true, "goals.get": true, "goals.select-primary": true}, map[string]bool{"other-cell": true}},
			} {
				t.Run(policy.name, func(t *testing.T) {
					ctx := selectedPolicy(context.Background(), policy.tenant, policy.permissions, policy.cells)
					var consumer jobdb.ConsumerRuntime = jobdbtest.NewNamespacedBackend(rt, "identity-")
					if transport != "direct" {
						var handler http.Handler = remote.NewConsumerServer(jobdbtest.NewNamespacedBackend(rt, ""))
						if transport == "signed" {
							handler, err = remote.NewConsumerServerWithOptions(jobdbtest.NewNamespacedBackend(rt, ""), remote.ServerOptions{LeaseTokenSigningKey: []byte("namespaced-authority-key-at-least-32-bytes")})
							require.NoError(t, err)
						}
						server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
							verified := selectedPolicy(r.Context(), policy.tenant, policy.permissions, policy.cells)
							handler.ServeHTTP(w, r.WithContext(verified))
						}))
						defer server.Close()
						client, e := remote.New(server.URL, server.Client())
						require.NoError(t, e)
						consumer = jobdbtest.NewNamespacedClient(client, "identity-")
					}
					before, e := rt.GetGoal(admin, jobdb.GoalKey{TenantId: "identity-tenant", GoalID: source.GoalID})
					require.NoError(t, e)
					h, e := consumer.SubmitJob(ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", JobID: policy.name, GoalID: source.GoalID, Cell: source.Cell, CellRepo: source.CellRepo, JobType: "work", Data: jobdb.NewTaskDataOrPanic(nil)}})
					if policy.name == "allowed" {
						require.NoError(t, e)
						require.Equal(t, "tenant", h.JobKey.TenantId)
						require.Equal(t, source.GoalID, h.GoalID)
						return
					}
					require.Error(t, e)
					_, e = rt.GetJob(admin, jobdb.JobKey{TenantId: "identity-tenant", JobId: policy.name})
					require.ErrorIs(t, e, jobdb.ErrJobNotFound)
					after, e := rt.GetGoal(admin, jobdb.GoalKey{TenantId: "identity-tenant", GoalID: source.GoalID})
					require.NoError(t, e)
					require.Equal(t, before, after)
				})
			}
		})
	}
}

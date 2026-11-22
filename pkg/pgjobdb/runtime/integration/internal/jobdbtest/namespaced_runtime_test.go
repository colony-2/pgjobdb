package jobdbtest

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/remote"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/sqlite"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/toy"
	"github.com/colony-2/jobdb/pkg/workflow"
	"github.com/stretchr/testify/require"
)

func assertConsumerOnly(t *testing.T, value any) {
	t.Helper()
	_, attachment := reflect.TypeOf(value).MethodByName("AddGoalCandidate")
	require.False(t, attachment)
	_, legacy := value.(jobdb.WorkflowRuntime)
	require.True(t, legacy)
	_, goals := value.(jobdb.GoalRuntime)
	require.True(t, goals)
}
func TestNamespacedAbsentCapabilities(t *testing.T) {
	value := newTenantNamespacedRuntime(toy.New(), "ns-")
	assertConsumerOnly(t, value)
	_, ok := value.(jobdb.ConfigurationAdmin)
	require.False(t, ok)
	_, ok = value.(jobdb.JobSchemaRegistry)
	require.False(t, ok)
	_, ok = value.(jobdb.JobDeletionRuntime)
	require.False(t, ok)
	_, ok = value.(jobdb.LeaseDelegator)
	require.False(t, ok)
	_, ok = value.(jobdb.PrimaryGoalSelectionCapability)
	require.False(t, ok)
	_, ok = value.(devSecretAdmin)
	require.False(t, ok)
	_, ok = value.(LeaseTransport)
	require.False(t, ok)
	assertConsumerOnly(t, celltest.WithCells(toy.New()))
}

func TestNamespacedConfigurationAndDevelopment(t *testing.T) {
	ctx := celltest.Context(context.Background())
	embedded, err := sqlite.StartEmbeddedRuntime(ctx)
	require.NoError(t, err)
	defer embedded.Shutdown()
	rt := embedded.Runtime
	ns := &tenantNamespacedRuntime{runtime: rt, prefix: "ns-"}
	// This fixture explicitly composes only supported SQLite administration ports.
	value := struct {
		*tenantNamespacedRuntime
		configurationAdapter
		schemaAdapter
		deletionAdapter
		developmentAdapter
	}{ns, configurationAdapter{ns, rt}, schemaAdapter{ns, rt}, deletionAdapter{ns, rt}, developmentAdapter{ns, rt}}
	assertConsumerOnly(t, value)
	_, err = value.SetTenantConfigurationValue(ctx, jobdb.SetTenantConfigurationValueRequest{TenantID: "tenant", Name: "setting", Value: json.RawMessage(`true`)})
	require.NoError(t, err)
	snapshot, err := value.GetTenantConfiguration(ctx, "tenant")
	require.NoError(t, err)
	require.JSONEq(t, `true`, string(snapshot.Values["setting"]))
	physical, err := rt.GetTenantConfiguration(ctx, "ns-tenant")
	require.NoError(t, err)
	require.Equal(t, snapshot, physical)
	wrong := "wrong"
	_, err = value.RemoveTenantConfigurationValue(ctx, jobdb.RemoveTenantConfigurationValueRequest{TenantID: "tenant", Name: "setting", ExpectedRevision: &wrong})
	require.ErrorIs(t, err, jobdb.ErrConfigurationRevisionMismatch)
	_, err = value.RemoveTenantConfigurationValue(ctx, jobdb.RemoveTenantConfigurationValueRequest{TenantID: "tenant", Name: "setting", ExpectedRevision: &snapshot.Revision})
	require.NoError(t, err)
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	_, err = value.GetTenantConfiguration(canceled, "tenant")
	require.ErrorIs(t, err, jobdb.ErrConfigurationUnavailable)
	schema, err := value.RegisterJobSchema(ctx, jobdb.RegisterJobSchemaRequest{TenantId: "tenant", Schema: json.RawMessage(`{"chapterShape":{"type":"object"}}`)})
	require.NoError(t, err)
	require.Equal(t, "tenant", schema.TenantId)
	schemas, err := value.ListJobSchemas(ctx, jobdb.ListJobSchemasRequest{TenantId: "tenant"})
	require.NoError(t, err)
	require.Len(t, schemas.Schemas, 1)
	require.Equal(t, "tenant", schemas.Schemas[0].TenantId)
	_, err = value.GetJobSchema(ctx, jobdb.JobSchemaKey{TenantId: "tenant", SchemaHash: schema.SchemaHash})
	require.NoError(t, err)
	_, err = value.ArchiveJobSchema(ctx, jobdb.JobSchemaKey{TenantId: "tenant", SchemaHash: schema.SchemaHash})
	require.NoError(t, err)
	_, err = value.GetJobSchema(canceled, jobdb.JobSchemaKey{TenantId: "tenant", SchemaHash: schema.SchemaHash})
	require.ErrorIs(t, err, context.Canceled)
	server := httptest.NewServer(celltest.Handler(remote.NewConsumerServer(value)))
	defer server.Close()
	response, err := server.Client().Post(server.URL+"/dev/v1/tenants/tenant/secrets/set", "application/json", strings.NewReader(`{"name":"example","value":"private-value"}`))
	require.NoError(t, err)
	response.Body.Close()
	require.Equal(t, http.StatusNoContent, response.StatusCode)
	h, err := celltest.Submit(value, ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", Cell: "test", CellRepo: "example.test/test", JobType: "work", Data: jobdb.NewTaskDataOrPanic(nil)}})
	require.NoError(t, err)
	lease, err := value.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: h.JobKey, WorkerID: "worker", Routes: []jobdb.Route{{JobType: "work"}}, LeaseDuration: time.Minute})
	require.NoError(t, err)
	require.NotNil(t, lease)
	result, err := lease.ResolveSecrets(ctx, []string{"example"})
	require.NoError(t, err)
	require.Len(t, result.Secrets, 1)
	response, err = server.Client().Post(server.URL+"/dev/v1/tenants/tenant/secrets/remove", "application/json", strings.NewReader(`{"name":"example"}`))
	require.NoError(t, err)
	response.Body.Close()
	require.Equal(t, http.StatusNoContent, response.StatusCode)
	result, err = lease.ResolveSecrets(ctx, []string{"example"})
	require.NoError(t, err)
	require.Empty(t, result.Secrets)
	deleted, err := value.DeleteJobs(ctx, jobdb.DeleteJobsRequest{TenantID: "tenant", JobIDs: []string{h.JobKey.JobId}})
	require.NoError(t, err)
	require.EqualValues(t, 1, deleted.DeletedCount)
	require.ErrorIs(t, lease.KeepAlive(ctx), jobdb.ErrExecutionLeaseLost)
}

func TestNamespacedClientDelegation(t *testing.T) {
	ctx := celltest.Context(context.Background())
	embedded, err := sqlite.StartEmbeddedRuntime(ctx)
	require.NoError(t, err)
	defer embedded.Shutdown()
	server := httptest.NewServer(celltest.Handler(remote.NewConsumerServer(embedded.Runtime)))
	defer server.Close()
	client, err := remote.New(server.URL, server.Client())
	require.NoError(t, err)
	value := NewNamespacedClient(client, "delegated-")
	assertConsumerOnly(t, value)
	h, err := celltest.Submit(value, ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", Cell: "test", CellRepo: "example.test/test", JobType: "work", Data: jobdb.NewTaskDataOrPanic(nil)}})
	require.NoError(t, err)
	lease, err := value.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: h.JobKey, WorkerID: "worker", Routes: []jobdb.Route{{JobType: "work"}}, LeaseDuration: time.Minute})
	require.NoError(t, err)
	require.NotNil(t, lease)
	deadline := time.Now().Add(30 * time.Second)
	grant := jobdb.CreateDelegationRequest{NotAfter: &deadline, Permissions: jobdb.DelegationPermissions{CreateCells: &jobdb.DelegationNames{Names: []string{"child"}}}}
	delegated, err := value.(jobdb.LeaseDelegator).CreateDelegation(ctx, lease, grant)
	require.NoError(t, err)
	require.Equal(t, h, delegated.Job())
	require.NotNil(t, delegated.(delegationSource).Delegation())
	require.False(t, delegated.LeaseExpiry().After(deadline))
	_, err = delegated.CreateCell(ctx, jobdb.CreateCellRequest{Name: "child", Repo: "example.test/child"})
	require.NoError(t, err)
	_, err = delegated.CreateCell(ctx, jobdb.CreateCellRequest{Name: "denied", Repo: "example.test/denied"})
	require.ErrorIs(t, err, jobdb.ErrLeasePermissionDenied)
	_, err = delegated.SubmitJob(ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", Cell: "test", JobType: "work", Data: jobdb.NewTaskDataOrPanic(nil)}})
	require.ErrorIs(t, err, jobdb.ErrLeasePermissionDenied)
	renewed, err := delegated.Renew(ctx)
	require.NoError(t, err)
	require.Equal(t, h, renewed.Job())
	require.NotEmpty(t, renewed.(tokenSource).LeaseToken())
	foreign := NewNamespacedClient(client, "foreign-")
	_, err = foreign.(jobdb.LeaseDelegator).CreateDelegation(ctx, lease, grant)
	require.ErrorIs(t, err, jobdb.ErrLeasePermissionDenied)
	_, err = value.(jobdb.LeaseDelegator).CreateDelegation(ctx, delegated, jobdb.CreateDelegationRequest{Permissions: jobdb.DelegationPermissions{CreateCells: &jobdb.DelegationNames{Any: true}}})
	require.ErrorIs(t, err, jobdb.ErrLeasePermissionDenied)
}

func TestNamespacedEnginePreservesCapabilities(t *testing.T) {
	ctx := celltest.Context(context.Background())
	embedded, err := sqlite.StartEmbeddedRuntime(ctx)
	require.NoError(t, err)
	defer embedded.Shutdown()
	ns := &tenantNamespacedRuntime{runtime: embedded.Runtime, prefix: "engine-"}
	value := struct {
		*tenantNamespacedRuntime
		schemaAdapter
	}{ns, schemaAdapter{ns, embedded.Runtime}}
	_, err = value.CreateCell(ctx, jobdb.CreateCellRequest{TenantID: "tenant", Name: "test", Repo: "example.test/test"})
	require.NoError(t, err)
	engine, err := workflow.NewEngineBuilder().WithRuntime(value).BuildEngine()
	require.NoError(t, err)
	h, err := engine.SubmitJob(ctx, jobdb.SubmitJob{TenantId: "tenant", Cell: "test", CellRepo: "example.test/test", JobType: "work", Data: jobdb.NewTaskDataOrPanic(nil), Schema: &jobdb.JobSchemaSelector{Schema: json.RawMessage(`{"chapterShape":{"type":"object"}}`)}})
	require.NoError(t, err)
	require.NotEmpty(t, h.GoalID)
	assertConsumerOnly(t, value)
}

// These ports are independent: adding one never installs the others.
func TestNamespacedIndividualPorts(t *testing.T) {
	ns := &tenantNamespacedRuntime{runtime: toy.New(), prefix: "n-"}
	for _, v := range []any{
		struct {
			*tenantNamespacedRuntime
			configurationAdapter
		}{ns, configurationAdapter{ns, toy.New()}},
		struct {
			*tenantNamespacedRuntime
			schemaAdapter
		}{ns, schemaAdapter{ns, toy.New()}},
		struct {
			*tenantNamespacedRuntime
			deletionAdapter
		}{ns, deletionAdapter{ns, toy.New()}},
	} {
		assertConsumerOnly(t, v)
		_, ok := v.(jobdb.LeaseDelegator)
		require.False(t, ok)
		_, ok = v.(devSecretAdmin)
		require.False(t, ok)
	}
}

type mandatoryLease struct{ jobdb.ExecutionLease }
type tokenOnlyLease struct{ jobdb.ExecutionLease }

func (tokenOnlyLease) LeaseToken() string { return "test-token" }
func TestNamespacedLeaseAbsentCapabilities(t *testing.T) {
	ns := &tenantNamespacedRuntime{}
	base := ns.wrapLease(mandatoryLease{})
	_, ok := base.(tokenSource)
	require.False(t, ok)
	_, ok = base.(jobdb.RenewableExecutionLease)
	require.False(t, ok)
	signed := ns.wrapLease(tokenOnlyLease{})
	require.Equal(t, "test-token", signed.(tokenSource).LeaseToken())
	_, ok = signed.(jobdb.RenewableExecutionLease)
	require.False(t, ok)
}

func TestNamespacedAbsentSelectionAndSchema(t *testing.T) {
	ctx := celltest.Context(context.Background())
	underlying := toy.New()
	ns := &tenantNamespacedRuntime{runtime: underlying, prefix: "selection-"}
	value := struct {
		*tenantNamespacedRuntime
		configurationAdapter
	}{ns, configurationAdapter{ns, underlying}}
	server := httptest.NewServer(celltest.Handler(remote.NewConsumerServer(value)))
	defer server.Close()
	client, err := remote.New(server.URL, server.Client())
	require.NoError(t, err)
	h, err := celltest.Submit(client, ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", Cell: "test", CellRepo: "example.test/test", JobID: "source", JobType: "work", Data: jobdb.NewTaskDataOrPanic(nil)}})
	require.NoError(t, err)
	_, err = client.SubmitJob(ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", Cell: "test", GoalID: h.GoalID, JobID: "unsupported", JobType: "work", Data: jobdb.NewTaskDataOrPanic(nil)}})
	require.ErrorIs(t, err, jobdb.ErrPrimaryGoalSelectionUnsupported)
	_, err = underlying.GetJob(ctx, jobdb.JobKey{TenantId: "selection-tenant", JobId: "unsupported"})
	require.ErrorIs(t, err, jobdb.ErrJobNotFound)
	_, err = client.RegisterJobSchema(ctx, jobdb.RegisterJobSchemaRequest{TenantId: "tenant", Schema: json.RawMessage(`{}`)})
	require.Error(t, err)
	_, ok := any(value).(jobdb.JobSchemaRegistry)
	require.False(t, ok)
	_, ok = any(value).(jobdb.PrimaryGoalSelectionCapability)
	require.False(t, ok)
}

func TestNamespacedHarnessConsumers(t *testing.T) {
	for _, harness := range append(BuiltInRuntimeHarnesses(), RemoteRuntimeHarnesses()...) {
		t.Run(harness.Name, func(t *testing.T) {
			built := harness.New(t)
			defer built.Shutdown(t)
			assertConsumerOnly(t, built.Runtime)
			ctx := celltest.Context(context.Background())
			config, ok := built.Runtime.(jobdb.ConfigurationAdmin)
			require.True(t, ok)
			_, err := config.SetTenantConfigurationValue(ctx, jobdb.SetTenantConfigurationValueRequest{TenantID: "tenant", Name: "harness-port", Value: json.RawMessage(`true`)})
			require.NoError(t, err)
			values, err := config.GetTenantConfiguration(ctx, "tenant")
			require.NoError(t, err)
			require.JSONEq(t, `true`, string(values.Values["harness-port"]))
			_, err = config.RemoveTenantConfigurationValue(ctx, jobdb.RemoveTenantConfigurationValueRequest{TenantID: "tenant", Name: "harness-port", ExpectedRevision: &values.Revision})
			require.NoError(t, err)
			registry, ok := built.Runtime.(jobdb.JobSchemaRegistry)
			require.True(t, ok)
			schema, err := registry.RegisterJobSchema(ctx, jobdb.RegisterJobSchemaRequest{TenantId: "tenant", Schema: json.RawMessage(`{"chapterShape":{"type":"object"}}`)})
			require.NoError(t, err)
			got, err := registry.GetJobSchema(ctx, jobdb.JobSchemaKey{TenantId: "tenant", SchemaHash: schema.SchemaHash})
			require.NoError(t, err)
			require.Equal(t, schema, got)
			_, err = registry.ArchiveJobSchema(ctx, jobdb.JobSchemaKey{TenantId: "tenant", SchemaHash: schema.SchemaHash})
			require.NoError(t, err)
			deletion, ok := built.Runtime.(jobdb.JobDeletionRuntime)
			require.True(t, ok)
			h, err := celltest.Submit(built.Runtime, ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", Cell: "test", CellRepo: "example.test/test", JobType: "harness-port", Data: jobdb.NewTaskDataOrPanic(nil)}})
			require.NoError(t, err)
			deleted, err := deletion.DeleteJobs(ctx, jobdb.DeleteJobsRequest{TenantID: "tenant", JobIDs: []string{h.JobKey.JobId}})
			require.NoError(t, err)
			require.EqualValues(t, 1, deleted.DeletedCount)
			history, err := jobdb.GetJobRun(ctx, struct{ jobdb.JobRunReader }{built.Runtime}, jobdb.GetJobRunRequest{JobKey: h.JobKey})
			require.NoError(t, err)
			require.Equal(t, h.GoalID, history.Job.GoalID)

		})
	}
}

// An empty but present history page must stay an empty array through the facade.
// Copy nonempty pages before translating tenants so shared backend data is safe.
type leaseEventPageRuntime struct {
	jobdb.ConsumerRuntime
	page jobdb.ListLeaseEventsResponse
}

func (r leaseEventPageRuntime) ListLeaseEvents(context.Context, jobdb.ListLeaseEventsRequest) (jobdb.ListLeaseEventsResponse, error) {
	return r.page, nil
}
func TestNamespacedLeaseEventPages(t *testing.T) {
	for _, events := range [][]jobdb.LeaseEvent{nil, {}, {{JobKey: jobdb.JobKey{TenantId: "ns-tenant", JobId: "job"}}}} {
		base := leaseEventPageRuntime{page: jobdb.ListLeaseEventsResponse{Events: events}}
		rt := newTenantNamespacedRuntime(base, "ns-")
		page, err := rt.ListLeaseEvents(context.Background(), jobdb.ListLeaseEventsRequest{TenantID: "tenant"})
		require.NoError(t, err)
		require.Equal(t, events == nil, page.Events == nil)
		if len(events) > 0 {
			require.Equal(t, "tenant", page.Events[0].JobKey.TenantId)
			require.Equal(t, "ns-tenant", events[0].JobKey.TenantId)
		}
	}
}

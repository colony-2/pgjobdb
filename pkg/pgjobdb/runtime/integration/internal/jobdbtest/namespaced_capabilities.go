package jobdbtest

import (
	"context"
	"slices"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
)

// Each adapter requires its real port. The mandatory facade never advertises
// an optional capability merely to return an unsupported error.
type configurationAdapter struct {
	ns   *tenantNamespacedRuntime
	port jobdb.ConfigurationAdmin
}

func (a configurationAdapter) GetTenantConfiguration(ctx context.Context, tenant string) (jobdb.ConfigurationSnapshot, error) {
	ctx = a.ns.prefixContext(ctx)
	return a.port.GetTenantConfiguration(ctx, a.ns.prefixTenant(tenant))
}
func (a configurationAdapter) SetTenantConfigurationValue(ctx context.Context, q jobdb.SetTenantConfigurationValueRequest) (string, error) {
	ctx = a.ns.prefixContext(ctx)
	q.TenantID = a.ns.prefixTenant(q.TenantID)
	return a.port.SetTenantConfigurationValue(ctx, q)
}
func (a configurationAdapter) RemoveTenantConfigurationValue(ctx context.Context, q jobdb.RemoveTenantConfigurationValueRequest) (string, error) {
	ctx = a.ns.prefixContext(ctx)
	q.TenantID = a.ns.prefixTenant(q.TenantID)
	return a.port.RemoveTenantConfigurationValue(ctx, q)
}

type schemaAdapter struct {
	ns   *tenantNamespacedRuntime
	port jobdb.JobSchemaRegistry
}

func (a schemaAdapter) RegisterJobSchema(ctx context.Context, q jobdb.RegisterJobSchemaRequest) (jobdb.JobSchemaInfo, error) {
	ctx = a.ns.prefixContext(ctx)
	q.TenantId = a.ns.prefixTenant(q.TenantId)
	v, e := a.port.RegisterJobSchema(ctx, q)
	v.TenantId = a.ns.stripTenant(v.TenantId)
	return v, e
}
func (a schemaAdapter) GetJobSchema(ctx context.Context, q jobdb.JobSchemaKey) (jobdb.JobSchemaInfo, error) {
	ctx = a.ns.prefixContext(ctx)
	q.TenantId = a.ns.prefixTenant(q.TenantId)
	v, e := a.port.GetJobSchema(ctx, q)
	v.TenantId = a.ns.stripTenant(v.TenantId)
	return v, e
}
func (a schemaAdapter) ArchiveJobSchema(ctx context.Context, q jobdb.JobSchemaKey) (jobdb.JobSchemaInfo, error) {
	ctx = a.ns.prefixContext(ctx)
	q.TenantId = a.ns.prefixTenant(q.TenantId)
	v, e := a.port.ArchiveJobSchema(ctx, q)
	v.TenantId = a.ns.stripTenant(v.TenantId)
	return v, e
}
func (a schemaAdapter) ListJobSchemas(ctx context.Context, q jobdb.ListJobSchemasRequest) (jobdb.ListJobSchemasResponse, error) {
	ctx = a.ns.prefixContext(ctx)
	q.TenantId = a.ns.prefixTenant(q.TenantId)
	v, e := a.port.ListJobSchemas(ctx, q)
	v.Schemas = slices.Clone(v.Schemas)
	for i := range v.Schemas {
		v.Schemas[i].TenantId = a.ns.stripTenant(v.Schemas[i].TenantId)
	}
	return v, e
}

type deletionAdapter struct {
	ns   *tenantNamespacedRuntime
	port jobdb.JobDeletionRuntime
}

func (a deletionAdapter) DeleteJobs(ctx context.Context, q jobdb.DeleteJobsRequest) (jobdb.DeleteJobsResponse, error) {
	ctx = a.ns.prefixContext(ctx)
	q.TenantID = a.ns.prefixTenant(q.TenantID)
	return a.port.DeleteJobs(ctx, q)
}

type delegationAdapter struct {
	ns   *tenantNamespacedRuntime
	port jobdb.LeaseDelegator
}

func (a delegationAdapter) CreateDelegation(ctx context.Context, parent jobdb.ExecutionLease, q jobdb.CreateDelegationRequest) (jobdb.RenewableExecutionLease, error) {
	if parent == nil {
		return nil, jobdb.ErrLeasePermissionDenied
	}
	if owned, ok := parent.(interface {
		ownedLease() (*tenantNamespacedRuntime, jobdb.ExecutionLease)
	}); ok {
		ns, _ := owned.ownedLease()
		if ns != a.ns {
			return nil, jobdb.ErrLeasePermissionDenied
		}
	}
	// The workflow runner supplies a lease session, not our private wrapper.
	// Translate only its public job identity; the remote port checks that the
	// signed token binds that physical tenant/job/lease and attenuates the grant.
	// Never substitute an owner credential or a cached pre-renewal lease.
	token, ok := parent.(tokenSource)
	if !ok {
		return nil, jobdb.ErrLeaseRenewalUnsupported
	}
	base := delegationParent{parent, token, a.ns}
	var translated jobdb.ExecutionLease = base
	if expiry, ok := parent.(expirySource); ok {
		translated = expiringDelegationParent{base, expiry}
	}
	v, e := a.port.CreateDelegation(ctx, translated, q)
	if e != nil {
		return nil, e
	}
	return a.ns.wrapLease(v).(jobdb.RenewableExecutionLease), nil
}

type expirySource interface {
	LeaseExpiry() time.Time
}

type delegationParent struct {
	jobdb.ExecutionLease
	tokenSource
	ns *tenantNamespacedRuntime
}

type expiringDelegationParent struct {
	delegationParent
	expirySource
}

func (p delegationParent) Job() jobdb.JobHandle {
	h := p.ExecutionLease.Job()
	h.JobKey = p.ns.prefixJobKey(h.JobKey)
	return h
}

type clientPorts interface {
	jobdb.ConsumerRuntime
	jobdb.ConfigurationAdmin
	jobdb.JobSchemaRegistry
	jobdb.JobDeletionRuntime
	jobdb.LeaseDelegator
}
type namespacedClient struct {
	*tenantNamespacedRuntime
	configurationAdapter
	schemaAdapter
	deletionAdapter
	delegationAdapter
}

// NewNamespacedClient preserves the optional ports of a remote client. Remote
// selection negotiation still belongs to that client's submission protocol.
func NewNamespacedClient(rt clientPorts, prefix string) jobdb.ConsumerRuntime {
	ns := &tenantNamespacedRuntime{runtime: rt, prefix: prefix}
	return &namespacedClient{ns, configurationAdapter{ns, rt}, schemaAdapter{ns, rt}, deletionAdapter{ns, rt}, delegationAdapter{ns, rt}}
}

// NewProjectClient maps symbolic conformance identities before HTTP signs or
// validates lease credentials. The production server always sees real IDs.
func NewProjectClient(rt clientPorts, catalog jobdb.ProjectCatalog, prefix string) jobdb.ConsumerRuntime {
	ns := &tenantNamespacedRuntime{runtime: rt, prefix: prefix, projects: catalog, projectIDs: map[string]string{}, projectNames: map[string]string{}}
	return &namespacedClient{ns, configurationAdapter{ns, rt}, schemaAdapter{ns, rt}, deletionAdapter{ns, rt}, delegationAdapter{ns, rt}}
}

type LeaseTransport interface {
	RenewExecutionLeaseByID(context.Context, jobdb.JobKey, string, string, time.Duration) (jobdb.RenewableExecutionLease, error)
	KeepAliveLeaseByID(context.Context, jobdb.JobKey, string, string, time.Duration) error
	KeepAliveLeaseByIDWithExpiry(context.Context, jobdb.JobKey, string, string, time.Duration) (time.Time, error)
	ValidateExecutionLeaseByID(context.Context, jobdb.JobKey, string, string) (jobdb.LeaseValidity, error)
	CompleteJobWithLeaseByID(context.Context, jobdb.JobKey, string, string, jobdb.CompleteExecutionRequest) error
	RescheduleJobWithLeaseByID(context.Context, jobdb.JobKey, string, string, jobdb.RescheduleExecutionRequest) error
	SubmitJobWithLeaseByID(context.Context, jobdb.JobKey, string, string, jobdb.SubmitJobRequest) (jobdb.JobHandle, error)
	SubmitRestartJobWithLeaseByID(context.Context, jobdb.JobKey, string, string, jobdb.SubmitRestartJobRequest) (jobdb.JobHandle, error)
	StartOrAppendJobWithLeaseByID(context.Context, jobdb.JobKey, string, string, jobdb.StartOrAppendJobRequest) (jobdb.JobHandle, error)
	CreateCellWithLeaseByID(context.Context, jobdb.JobKey, string, string, jobdb.CreateCellRequest) (jobdb.Cell, error)
	LookupCellWithLeaseByID(context.Context, jobdb.JobKey, string, string, string) (jobdb.ResolvedCell, error)
	ResolveCellWithLeaseByID(context.Context, jobdb.JobKey, string, string, string) (jobdb.ResolvedCell, error)
}
type backendPorts interface {
	jobdb.ConsumerRuntime
	jobdb.ConfigurationAdmin
	jobdb.JobSchemaRegistry
	jobdb.JobDeletionRuntime
	jobdb.PrimaryGoalSelectionCapability
	jobdb.JobGoalCapability
	LeaseTransport
	ConfigurationLimits() jobdb.ConfigurationLimits
	SecretResolutionLimits() jobdb.SecretLimits
}
type transportAdapter struct {
	ns   *tenantNamespacedRuntime
	port LeaseTransport
}
type namespacedBackend struct {
	*tenantNamespacedRuntime
	configurationAdapter
	schemaAdapter
	deletionAdapter
	transportAdapter
	capability jobdb.PrimaryGoalSelectionCapability
	limits     backendPorts
}

// NewNamespacedBackend is the concrete PostgreSQL server composition. It requires
// all ports it exposes; development administration and delegation are absent.
func NewNamespacedBackend(rt backendPorts, prefix string) jobdb.ConsumerRuntime {
	ns := &tenantNamespacedRuntime{runtime: rt, prefix: prefix}
	return &namespacedBackend{ns, configurationAdapter{ns, rt}, schemaAdapter{ns, rt}, deletionAdapter{ns, rt}, transportAdapter{ns, rt}, rt, rt}
}
func (r *namespacedBackend) SupportsPrimaryGoalSelection() bool {
	return r.capability.SupportsPrimaryGoalSelection()
}
func (r *namespacedBackend) ConfigurationLimits() jobdb.ConfigurationLimits {
	return r.limits.ConfigurationLimits()
}
func (r *namespacedBackend) SecretResolutionLimits() jobdb.SecretLimits {
	return r.limits.SecretResolutionLimits()
}
func (a transportAdapter) KeepAliveLeaseByID(ctx context.Context, k jobdb.JobKey, l, w string, d time.Duration) error {
	ctx = a.ns.prefixContext(ctx)
	return a.port.KeepAliveLeaseByID(ctx, a.ns.prefixJobKey(k), l, w, d)
}
func (a transportAdapter) KeepAliveLeaseByIDWithExpiry(ctx context.Context, k jobdb.JobKey, l, w string, d time.Duration) (time.Time, error) {
	ctx = a.ns.prefixContext(ctx)
	return a.port.KeepAliveLeaseByIDWithExpiry(ctx, a.ns.prefixJobKey(k), l, w, d)
}
func (a transportAdapter) ValidateExecutionLeaseByID(ctx context.Context, k jobdb.JobKey, l, w string) (jobdb.LeaseValidity, error) {
	ctx = a.ns.prefixContext(ctx)
	return a.port.ValidateExecutionLeaseByID(ctx, a.ns.prefixJobKey(k), l, w)
}
func (a transportAdapter) CompleteJobWithLeaseByID(ctx context.Context, k jobdb.JobKey, l, w string, q jobdb.CompleteExecutionRequest) error {
	ctx = a.ns.prefixContext(ctx)
	return a.port.CompleteJobWithLeaseByID(ctx, a.ns.prefixJobKey(k), l, w, q)
}
func (a transportAdapter) RescheduleJobWithLeaseByID(ctx context.Context, k jobdb.JobKey, l, w string, q jobdb.RescheduleExecutionRequest) error {
	ctx = a.ns.prefixContext(ctx)
	return a.port.RescheduleJobWithLeaseByID(ctx, a.ns.prefixJobKey(k), l, w, q)
}
func (a transportAdapter) SubmitJobWithLeaseByID(ctx context.Context, k jobdb.JobKey, l, w string, q jobdb.SubmitJobRequest) (jobdb.JobHandle, error) {
	ctx = a.ns.prefixContext(ctx)
	q.Job.TenantId = a.ns.prefixTenant(q.Job.TenantId)
	v, e := a.port.SubmitJobWithLeaseByID(ctx, a.ns.prefixJobKey(k), l, w, q)
	v.JobKey = a.ns.stripJobKey(v.JobKey)
	return v, e
}
func (a transportAdapter) SubmitRestartJobWithLeaseByID(ctx context.Context, k jobdb.JobKey, l, w string, q jobdb.SubmitRestartJobRequest) (jobdb.JobHandle, error) {
	ctx = a.ns.prefixContext(ctx)
	q.Job.PriorJobKey = a.ns.prefixJobKey(q.Job.PriorJobKey)
	v, e := a.port.SubmitRestartJobWithLeaseByID(ctx, a.ns.prefixJobKey(k), l, w, q)
	v.JobKey = a.ns.stripJobKey(v.JobKey)
	return v, e
}
func (a transportAdapter) StartOrAppendJobWithLeaseByID(ctx context.Context, k jobdb.JobKey, l, w string, q jobdb.StartOrAppendJobRequest) (jobdb.JobHandle, error) {
	ctx = a.ns.prefixContext(ctx)
	q.Job.Job.TenantId = a.ns.prefixTenant(q.Job.Job.TenantId)
	v, e := a.port.StartOrAppendJobWithLeaseByID(ctx, a.ns.prefixJobKey(k), l, w, q)
	v.JobKey = a.ns.stripJobKey(v.JobKey)
	return v, e
}
func (a transportAdapter) CreateCellWithLeaseByID(ctx context.Context, k jobdb.JobKey, l, w string, q jobdb.CreateCellRequest) (jobdb.Cell, error) {
	ctx = a.ns.prefixContext(ctx)
	q.TenantID = a.ns.prefixTenant(q.TenantID)
	v, e := a.port.CreateCellWithLeaseByID(ctx, a.ns.prefixJobKey(k), l, w, q)
	v.TenantID = a.ns.stripTenant(v.TenantID)
	return v, e
}
func (a transportAdapter) LookupCellWithLeaseByID(ctx context.Context, k jobdb.JobKey, l, w, n string) (jobdb.ResolvedCell, error) {
	ctx = a.ns.prefixContext(ctx)
	return a.port.LookupCellWithLeaseByID(ctx, a.ns.prefixJobKey(k), l, w, n)
}
func (a transportAdapter) ResolveCellWithLeaseByID(ctx context.Context, k jobdb.JobKey, l, w, n string) (jobdb.ResolvedCell, error) {
	ctx = a.ns.prefixContext(ctx)
	return a.port.ResolveCellWithLeaseByID(ctx, a.ns.prefixJobKey(k), l, w, n)
}

type devSecretAdmin interface {
	SetDevSecret(context.Context, string, string, string) error
	RemoveDevSecret(context.Context, string, string) error
}
type developmentAdapter struct {
	ns   *tenantNamespacedRuntime
	port devSecretAdmin
}

func (a developmentAdapter) SetDevSecret(ctx context.Context, t, n, v string) error {
	return a.port.SetDevSecret(ctx, a.ns.prefixTenant(t), n, v)
}
func (a developmentAdapter) RemoveDevSecret(ctx context.Context, t, n string) error {
	return a.port.RemoveDevSecret(ctx, a.ns.prefixTenant(t), n)
}

func (a transportAdapter) RenewExecutionLeaseByID(ctx context.Context, k jobdb.JobKey, l, w string, d time.Duration) (jobdb.RenewableExecutionLease, error) {
	ctx = a.ns.prefixContext(ctx)
	lease, err := a.port.RenewExecutionLeaseByID(ctx, a.ns.prefixJobKey(k), l, w, d)
	if err != nil {
		return nil, err
	}
	return a.ns.wrapLease(lease).(jobdb.RenewableExecutionLease), nil
}

type storagePorts interface {
	jobdb.ConsumerRuntime
	jobdb.ConfigurationAdmin
	jobdb.JobSchemaRegistry
	jobdb.JobDeletionRuntime
	jobdb.PrimaryGoalSelectionCapability
	jobdb.JobGoalCapability
	ConfigurationLimits() jobdb.ConfigurationLimits
	SecretResolutionLimits() jobdb.SecretLimits
}
type namespacedStorage struct {
	*tenantNamespacedRuntime
	configurationAdapter
	schemaAdapter
	deletionAdapter
	capability jobdb.PrimaryGoalSelectionCapability
	limits     storagePorts
}

func (r *namespacedStorage) SupportsPrimaryGoalSelection() bool {
	return r.capability.SupportsPrimaryGoalSelection()
}
func (r *namespacedStorage) ConfigurationLimits() jobdb.ConfigurationLimits {
	return r.limits.ConfigurationLimits()
}
func (r *namespacedStorage) SecretResolutionLimits() jobdb.SecretLimits {
	return r.limits.SecretResolutionLimits()
}

// The toy fixture has storage capabilities but no transported PostgreSQL leases.
func newNamespacedStorage(rt storagePorts, prefix string) jobdb.ConsumerRuntime {
	ns := &tenantNamespacedRuntime{runtime: rt, prefix: prefix}
	return &namespacedStorage{ns, configurationAdapter{ns, rt}, schemaAdapter{ns, rt}, deletionAdapter{ns, rt}, rt, rt}
}

type developmentBackendPorts interface {
	backendPorts
	devSecretAdmin
}
type namespacedDevelopmentBackend struct {
	*namespacedBackend
	developmentAdapter
}

// SQLite fixtures use their existing functioning development provider.
func newNamespacedDevelopmentBackend(rt developmentBackendPorts, prefix string) jobdb.ConsumerRuntime {
	backend := NewNamespacedBackend(rt, prefix).(*namespacedBackend)
	return &namespacedDevelopmentBackend{backend, developmentAdapter{backend.tenantNamespacedRuntime, rt}}
}

// Forward actual backend guarantees independently of selection support.
func (r *namespacedBackend) JobGoalCapabilities() jobdb.JobGoalCapabilities {
	return r.limits.JobGoalCapabilities()
}
func (r *namespacedStorage) JobGoalCapabilities() jobdb.JobGoalCapabilities {
	return r.limits.JobGoalCapabilities()
}

// NewProjectBackend explicitly creates/maps real catalog identities for upstream
// conformance cases that fix symbolic tenant names. Production never uses this
// adapter; project acceptance tests call the actual PostgreSQL runtime directly.
func NewProjectBackend(rt interface {
	backendPorts
	jobdb.ProjectCatalog
}, prefix string) jobdb.ConsumerRuntime {
	ns := &tenantNamespacedRuntime{runtime: rt, prefix: prefix, projects: rt, projectIDs: map[string]string{}, projectNames: map[string]string{}}
	return &namespacedBackend{ns, configurationAdapter{ns, rt}, schemaAdapter{ns, rt}, deletionAdapter{ns, rt}, transportAdapter{ns, rt}, rt, rt}
}

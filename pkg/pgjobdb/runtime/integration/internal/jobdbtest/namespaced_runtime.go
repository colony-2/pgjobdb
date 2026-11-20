package jobdbtest

import (
	"context"
	"encoding/json"
	"github.com/colony-2/jobdb/pkg/jobdb"
	"slices"
	"strings"
	"time"
)

type tenantNamespacedRuntime struct {
	runtime jobdb.ConsumerRuntime
	prefix  string
}

func newTenantNamespacedRuntime(runtime jobdb.ConsumerRuntime, prefix string) jobdb.ConsumerRuntime {
	return &tenantNamespacedRuntime{runtime: runtime, prefix: prefix}
}

func (r *tenantNamespacedRuntime) SubmitJob(ctx context.Context, req jobdb.SubmitJobRequest) (jobdb.JobHandle, error) {
	req.Job.TenantId = r.prefixTenant(req.Job.TenantId)
	handle, err := r.runtime.SubmitJob(ctx, req)
	if err != nil {
		return jobdb.JobHandle{}, err
	}
	handle.JobKey = r.stripJobKey(handle.JobKey)
	return handle, nil
}

func (r *tenantNamespacedRuntime) SubmitRestartJob(ctx context.Context, req jobdb.SubmitRestartJobRequest) (jobdb.JobHandle, error) {
	req.Job.PriorJobKey = r.prefixJobKey(req.Job.PriorJobKey)
	handle, err := r.runtime.SubmitRestartJob(ctx, req)
	if err != nil {
		return jobdb.JobHandle{}, err
	}
	handle.JobKey = r.stripJobKey(handle.JobKey)
	return handle, nil
}

func (r *tenantNamespacedRuntime) CancelJob(ctx context.Context, req jobdb.CancelJobRequest) error {
	req.JobKey = r.prefixJobKey(req.JobKey)
	return r.runtime.CancelJob(ctx, req)
}

func (r *tenantNamespacedRuntime) ClearCrashConcern(ctx context.Context, req jobdb.ClearCrashConcernRequest) error {
	req.JobKey = r.prefixJobKey(req.JobKey)
	return r.runtime.ClearCrashConcern(ctx, req)
}

func (r *tenantNamespacedRuntime) PollWork(ctx context.Context, req jobdb.PollWorkRequest) ([]jobdb.ExecutionLease, error) {
	if req.TenantId != "" {
		req.TenantId = r.prefixTenant(req.TenantId)
	}
	leases, err := r.runtime.PollWork(ctx, req)
	if err != nil {
		return nil, err
	}
	return r.wrapLeases(leases), nil
}

func (r *tenantNamespacedRuntime) GetJobLease(ctx context.Context, req jobdb.GetJobLeaseRequest) (jobdb.ExecutionLease, error) {
	req.JobKey = r.prefixJobKey(req.JobKey)
	lease, err := r.runtime.GetJobLease(ctx, req)
	if err != nil || lease == nil {
		return lease, err
	}
	return r.wrapLease(lease), nil
}

func (r *tenantNamespacedRuntime) CompleteTaskIfWaiting(ctx context.Context, req jobdb.CompleteTaskIfWaitingRequest) error {
	req.JobKey = r.prefixJobKey(req.JobKey)
	return r.runtime.CompleteTaskIfWaiting(ctx, req)
}

func (r *tenantNamespacedRuntime) GetJob(ctx context.Context, jobKey jobdb.JobKey) (jobdb.JobInfo, error) {
	info, err := r.runtime.GetJob(ctx, r.prefixJobKey(jobKey))
	if info.BlockingWait != nil {
		wait := *info.BlockingWait
		wait.JobKey = r.stripJobKey(wait.JobKey)
		info.BlockingWait = &wait
	}
	return info, err
}

func (r *tenantNamespacedRuntime) ListJobs(ctx context.Context, req jobdb.ListJobsRequest) (jobdb.ListJobsResponse, error) {
	req.TenantIds = r.prefixTenantIDs(req.TenantIds)
	if len(req.JobKeys) > 0 {
		jobKeys := make([]jobdb.JobKey, 0, len(req.JobKeys))
		for _, jobKey := range req.JobKeys {
			jobKeys = append(jobKeys, r.prefixJobKey(jobKey))
		}
		req.JobKeys = jobKeys
	}
	resp, err := r.runtime.ListJobs(ctx, req)
	if err != nil {
		return jobdb.ListJobsResponse{}, err
	}
	resp.Jobs = slices.Clone(resp.Jobs)
	for i := range resp.Jobs {
		resp.Jobs[i].JobKey = r.stripJobKey(resp.Jobs[i].JobKey)
	}
	return resp, nil
}

func (r *tenantNamespacedRuntime) GetChapter(ctx context.Context, ref jobdb.ChapterRef) (jobdb.Chapter, error) {
	ref.JobKey = r.prefixJobKey(ref.JobKey)
	return r.runtime.GetChapter(ctx, ref)
}

func (r *tenantNamespacedRuntime) ListChapters(ctx context.Context, req jobdb.ListChaptersRequest) ([]jobdb.Chapter, error) {
	req.JobKey = r.prefixJobKey(req.JobKey)
	return r.runtime.ListChapters(ctx, req)
}

func (r *tenantNamespacedRuntime) PutChapter(ctx context.Context, req jobdb.PutChapterRequest) error {
	req.Ref.JobKey = r.prefixJobKey(req.Ref.JobKey)
	return r.runtime.PutChapter(ctx, req)
}

func (r *tenantNamespacedRuntime) OpenArtifact(ctx context.Context, ref jobdb.ArtifactRef) (jobdb.ArtifactReader, error) {
	ref.JobKey = r.prefixJobKey(ref.JobKey)
	return r.runtime.OpenArtifact(ctx, ref)
}

func (r *tenantNamespacedRuntime) wrapLeases(leases []jobdb.ExecutionLease) []jobdb.ExecutionLease {
	if len(leases) == 0 {
		return leases
	}
	wrapped := make([]jobdb.ExecutionLease, 0, len(leases))
	for _, lease := range leases {
		wrapped = append(wrapped, r.wrapLease(lease))
	}
	return wrapped
}

func (r *tenantNamespacedRuntime) prefixTenantIDs(tenantIDs []string) []string {
	if len(tenantIDs) == 0 {
		return tenantIDs
	}
	out := make([]string, 0, len(tenantIDs))
	for _, tenantID := range tenantIDs {
		out = append(out, r.prefixTenant(tenantID))
	}
	return out
}

func (r *tenantNamespacedRuntime) prefixJobKey(jobKey jobdb.JobKey) jobdb.JobKey {
	jobKey.TenantId = r.prefixTenant(jobKey.TenantId)
	return jobKey
}

func (r *tenantNamespacedRuntime) stripJobKey(jobKey jobdb.JobKey) jobdb.JobKey {
	jobKey.TenantId = r.stripTenant(jobKey.TenantId)
	return jobKey
}

func (r *tenantNamespacedRuntime) prefixTenant(tenantID string) string {
	if tenantID == "" {
		return ""
	}
	return r.prefix + tenantID
}

func (r *tenantNamespacedRuntime) stripTenant(tenantID string) string {
	return strings.TrimPrefix(tenantID, r.prefix)
}

type tenantNamespacedLease struct {
	lease   jobdb.ExecutionLease
	runtime *tenantNamespacedRuntime
}

func (l *tenantNamespacedLease) LeaseID() string                { return l.lease.LeaseID() }
func (l *tenantNamespacedLease) Route() jobdb.Route             { return l.lease.Route() }
func (l *tenantNamespacedLease) ClientPayload() json.RawMessage { return l.lease.ClientPayload() }
func (l *tenantNamespacedLease) ClientPayloadRevision() int64   { return l.lease.ClientPayloadRevision() }
func (l *tenantNamespacedLease) ExecutionState() jobdb.ExecutionState {
	return l.lease.ExecutionState()
}
func (l *tenantNamespacedLease) KeepAlive(ctx context.Context) error { return l.lease.KeepAlive(ctx) }
func (l *tenantNamespacedLease) StopKeepAlive()                      { l.lease.StopKeepAlive() }
func (l *tenantNamespacedLease) Complete(ctx context.Context, req jobdb.CompleteExecutionRequest) error {
	return l.lease.Complete(ctx, req)
}
func (l *tenantNamespacedLease) Reschedule(ctx context.Context, req jobdb.RescheduleExecutionRequest) error {
	return l.lease.Reschedule(ctx, req)
}
func (l *tenantNamespacedLease) SubmitJob(ctx context.Context, req jobdb.SubmitJobRequest) (jobdb.JobHandle, error) {
	req.Job.TenantId = l.runtime.prefixTenant(req.Job.TenantId)
	handle, err := l.lease.SubmitJob(ctx, req)
	if err != nil {
		return jobdb.JobHandle{}, err
	}
	handle.JobKey = l.runtime.stripJobKey(handle.JobKey)
	return handle, nil
}
func (l *tenantNamespacedLease) SubmitRestartJob(ctx context.Context, req jobdb.SubmitRestartJobRequest) (jobdb.JobHandle, error) {
	req.Job.PriorJobKey = l.runtime.prefixJobKey(req.Job.PriorJobKey)
	handle, err := l.lease.SubmitRestartJob(ctx, req)
	if err != nil {
		return jobdb.JobHandle{}, err
	}
	handle.JobKey = l.runtime.stripJobKey(handle.JobKey)
	return handle, nil
}
func (l *tenantNamespacedLease) Job() jobdb.JobHandle {
	handle := l.lease.Job()
	handle.JobKey = l.runtime.stripJobKey(handle.JobKey)
	return handle
}

func (r *tenantNamespacedRuntime) GetConfiguration(ctx context.Context, req jobdb.GetConfigurationRequest) (jobdb.ConfigurationSnapshot, error) {
	req.JobKey = r.prefixJobKey(req.JobKey)
	return r.runtime.GetConfiguration(ctx, req)
}
func (l *tenantNamespacedLease) GetConfiguration(ctx context.Context) (jobdb.ConfigurationSnapshot, error) {
	return l.lease.GetConfiguration(ctx)
}

func (r *tenantNamespacedRuntime) ResolveSecrets(ctx context.Context, req jobdb.ResolveSecretsRequest) (jobdb.SecretResolveResult, error) {
	req.JobKey = r.prefixJobKey(req.JobKey)
	return r.runtime.ResolveSecrets(ctx, req)
}
func (l *tenantNamespacedLease) ResolveSecrets(ctx context.Context, names []string) (jobdb.SecretResolveResult, error) {
	return l.lease.ResolveSecrets(ctx, names)
}

func (r *tenantNamespacedRuntime) GetGoal(ctx context.Context, key jobdb.GoalKey) (jobdb.GoalInfo, error) {
	key.TenantId = r.prefixTenant(key.TenantId)
	info, err := r.runtime.GetGoal(ctx, key)
	info.GoalKey.TenantId = r.stripTenant(info.GoalKey.TenantId)
	return info, err
}
func (r *tenantNamespacedRuntime) ListGoals(ctx context.Context, req jobdb.ListGoalsRequest) (jobdb.ListGoalsResponse, error) {
	req.TenantId = r.prefixTenant(req.TenantId)
	result, err := r.runtime.ListGoals(ctx, req)
	result.Goals = slices.Clone(result.Goals)
	for i := range result.Goals {
		result.Goals[i].GoalKey.TenantId = r.stripTenant(result.Goals[i].GoalKey.TenantId)
	}
	return result, err
}
func (r *tenantNamespacedRuntime) ListDependencyWaits(ctx context.Context, req jobdb.ListDependencyWaitsRequest) (jobdb.ListDependencyWaitsResponse, error) {
	req.TenantId = r.prefixTenant(req.TenantId)
	result, err := r.runtime.ListDependencyWaits(ctx, req)
	result.Waits = slices.Clone(result.Waits)
	for i := range result.Waits {
		result.Waits[i].JobKey = r.stripJobKey(result.Waits[i].JobKey)
	}
	return result, err
}
func (r *tenantNamespacedRuntime) FailGoal(ctx context.Context, req jobdb.FailGoalRequest) (jobdb.GoalInfo, error) {
	req.GoalKey.TenantId = r.prefixTenant(req.GoalKey.TenantId)
	info, err := r.runtime.FailGoal(ctx, req)
	info.GoalKey.TenantId = r.stripTenant(info.GoalKey.TenantId)
	return info, err
}
func (r *tenantNamespacedRuntime) GetDependencyWait(ctx context.Context, key jobdb.DependencyWaitKey) (jobdb.DependencyWaitInfo, error) {
	key.JobKey = r.prefixJobKey(key.JobKey)
	info, err := r.runtime.GetDependencyWait(ctx, key)
	info.JobKey = r.stripJobKey(info.JobKey)
	return info, err
}

func (r *tenantNamespacedRuntime) CreateCell(ctx context.Context, req jobdb.CreateCellRequest) (out jobdb.Cell, err error) {
	req.TenantID = r.prefixTenant(req.TenantID)
	out, err = r.runtime.CreateCell(ctx, req)
	out.TenantID = r.stripTenant(out.TenantID)
	return
}
func (r *tenantNamespacedRuntime) GetCell(ctx context.Context, req jobdb.CellKey) (out jobdb.Cell, err error) {
	req.TenantID = r.prefixTenant(req.TenantID)
	out, err = r.runtime.GetCell(ctx, req)
	out.TenantID = r.stripTenant(out.TenantID)
	return
}
func (r *tenantNamespacedRuntime) ListCells(ctx context.Context, req jobdb.ListCellsRequest) (out jobdb.ListCellsResponse, err error) {
	req.TenantID = r.prefixTenant(req.TenantID)
	out, err = r.runtime.ListCells(ctx, req)
	out.Cells = slices.Clone(out.Cells)
	for i := range out.Cells {
		out.Cells[i].TenantID = r.stripTenant(out.Cells[i].TenantID)
	}
	return
}
func (r *tenantNamespacedRuntime) UpdateCell(ctx context.Context, req jobdb.UpdateCellRequest) (out jobdb.Cell, err error) {
	req.TenantID = r.prefixTenant(req.TenantID)
	out, err = r.runtime.UpdateCell(ctx, req)
	out.TenantID = r.stripTenant(out.TenantID)
	return
}
func (r *tenantNamespacedRuntime) RetireCell(ctx context.Context, req jobdb.RetireCellRequest) (out jobdb.Cell, err error) {
	req.TenantID = r.prefixTenant(req.TenantID)
	out, err = r.runtime.RetireCell(ctx, req)
	out.TenantID = r.stripTenant(out.TenantID)
	return
}
func (l *tenantNamespacedLease) ResolveCell(ctx context.Context, name string) (jobdb.ResolvedCell, error) {
	return l.lease.ResolveCell(ctx, name)
}

func (l *tenantNamespacedLease) LookupCell(ctx context.Context, name string) (jobdb.ResolvedCell, error) {
	return l.lease.LookupCell(ctx, name)
}

func (l *tenantNamespacedLease) CreateCell(ctx context.Context, req jobdb.CreateCellRequest) (jobdb.Cell, error) {
	if req.TenantID != "" {
		req.TenantID = l.runtime.prefixTenant(req.TenantID)
	}
	c, err := l.lease.CreateCell(ctx, req)
	c.TenantID = l.runtime.stripTenant(c.TenantID)
	return c, err
}

func (r *tenantNamespacedRuntime) ListLeaseEvents(ctx context.Context, req jobdb.ListLeaseEventsRequest) (out jobdb.ListLeaseEventsResponse, err error) {
	req.TenantID = r.prefixTenant(req.TenantID)
	out, err = r.runtime.ListLeaseEvents(ctx, req)
	out.Events = slices.Clone(out.Events)
	for i := range out.Events {
		out.Events[i].JobKey.TenantId = r.stripTenant(out.Events[i].JobKey.TenantId)
	}
	return
}

func (r *tenantNamespacedRuntime) StartOrAppendJob(ctx context.Context, req jobdb.StartOrAppendJobRequest) (jobdb.JobHandle, error) {
	req.Job.Job.TenantId = r.prefixTenant(req.Job.Job.TenantId)
	handle, err := r.runtime.StartOrAppendJob(ctx, req)
	if err != nil {
		return jobdb.JobHandle{}, err
	}
	handle.JobKey = r.stripJobKey(handle.JobKey)
	return handle, nil
}

func (l *tenantNamespacedLease) StartOrAppendJob(ctx context.Context, req jobdb.StartOrAppendJobRequest) (jobdb.JobHandle, error) {
	req.Job.Job.TenantId = l.runtime.prefixTenant(req.Job.Job.TenantId)
	handle, err := l.lease.StartOrAppendJob(ctx, req)
	if err != nil {
		return jobdb.JobHandle{}, err
	}
	handle.JobKey = l.runtime.stripJobKey(handle.JobKey)
	return handle, nil
}

// NewNamespacedRuntime exposes only mandatory consumer operations. Optional ports
// are composed below at boundaries with statically verified underlying support.
func NewNamespacedRuntime(rt jobdb.ConsumerRuntime, prefix string) jobdb.ConsumerRuntime {
	return newTenantNamespacedRuntime(rt, prefix)
}

type tokenSource interface{ LeaseToken() string }
type namespacedTokenLease struct {
	*tenantNamespacedLease
	token tokenSource
}

func (l *namespacedTokenLease) LeaseToken() string { return l.token.LeaseToken() }

type namespacedRenewableLease struct {
	*tenantNamespacedLease
	renewable jobdb.RenewableExecutionLease
}

func (l *namespacedRenewableLease) LeaseWorkerID() string  { return l.renewable.LeaseWorkerID() }
func (l *namespacedRenewableLease) LeaseExpiry() time.Time { return l.renewable.LeaseExpiry() }
func (l *namespacedRenewableLease) Renew(ctx context.Context) (jobdb.RenewableExecutionLease, error) {
	lease, err := l.renewable.Renew(ctx)
	if err != nil {
		return nil, err
	}
	return l.runtime.wrapLease(lease).(jobdb.RenewableExecutionLease), nil
}

type namespacedRenewableTokenLease struct {
	*namespacedRenewableLease
	token tokenSource
}

func (l *namespacedRenewableTokenLease) LeaseToken() string { return l.token.LeaseToken() }

type schemaSource interface{ LeaseSchemaHash() string }
type delegationSource interface{ Delegation() *jobdb.DelegationInfo }
type namespacedSchemaLease struct {
	*namespacedRenewableLease
	schema schemaSource
}

func (l *namespacedSchemaLease) LeaseSchemaHash() string { return l.schema.LeaseSchemaHash() }

type namespacedSchemaTokenLease struct {
	*namespacedRenewableTokenLease
	schema schemaSource
}

func (l *namespacedSchemaTokenLease) LeaseSchemaHash() string { return l.schema.LeaseSchemaHash() }

type namespacedRemoteLease struct {
	*namespacedSchemaTokenLease
	delegation delegationSource
}

func (l *namespacedRemoteLease) Delegation() *jobdb.DelegationInfo { return l.delegation.Delegation() }
func (l *tenantNamespacedLease) ownedLease() (*tenantNamespacedRuntime, jobdb.ExecutionLease) {
	return l.runtime, l.lease
}
func (r *tenantNamespacedRuntime) wrapLease(lease jobdb.ExecutionLease) jobdb.ExecutionLease {
	if lease == nil {
		return nil
	}
	base := &tenantNamespacedLease{lease: lease, runtime: r}
	renewable, renews := lease.(jobdb.RenewableExecutionLease)
	token, signed := lease.(tokenSource)
	if renews {
		renewed := &namespacedRenewableLease{base, renewable}
		schema, hasSchema := lease.(schemaSource)
		if signed {
			signedLease := &namespacedRenewableTokenLease{renewed, token}
			if hasSchema {
				withSchema := &namespacedSchemaTokenLease{signedLease, schema}
				if delegation, ok := lease.(delegationSource); ok {
					return &namespacedRemoteLease{withSchema, delegation}
				}
				return withSchema
			}
			return signedLease
		}
		if hasSchema {
			return &namespacedSchemaLease{renewed, schema}
		}
		return renewed
	}
	if signed {
		return &namespacedTokenLease{base, token}
	}
	return base
}

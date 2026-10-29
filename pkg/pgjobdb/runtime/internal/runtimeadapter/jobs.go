package runtimeadapter

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	runtimecore "github.com/colony-2/jobdb/pkg/jobdb/runtime/core"
	"github.com/colony-2/pgjobdb/pkg/pgjobdb"
)

// Scheduler adapts pgjobdb's native SQL API to JobDB's scheduler port.
type Scheduler struct {
	DB *sql.DB
}

var _ runtimecore.Scheduler = Scheduler{}

func (s Scheduler) GetJob(ctx context.Context, key jobdb.JobKey) (runtimecore.StoredJob, error) {
	detail, err := pgjobdb.GetJob(ctx, s.DB, pgjobdb.TenantID(key.TenantId), pgjobdb.JobID(key.JobId))
	if errors.Is(err, pgjobdb.ErrJobNotFound) {
		return runtimecore.StoredJob{}, jobdb.ErrJobNotFound
	}
	if err != nil {
		return runtimecore.StoredJob{}, err
	}
	row, err := storedJobFromDetail(*detail)
	if err != nil {
		return runtimecore.StoredJob{}, err
	}
	if row.Store == jobdb.JobStoreActive && row.LeaseWorkerID != "" {
		// The public job projection deliberately omits lease IDs. JobDB's
		// secrets broker needs the identity internally and then independently
		// validates it with ValidateLease before granting access.
		identity := runtimecore.LeaseIdentity{JobKey: key}
		err = s.DB.QueryRowContext(ctx, `SELECT lease_id, lease_worker_id, lease_expires_at
			FROM pgjobdb.jobs WHERE tenant_id = $1 AND job_id = $2
			AND lease_id IS NOT NULL AND lease_worker_id IS NOT NULL`,
			key.TenantId, key.JobId).Scan(&identity.LeaseID, &identity.WorkerID, &identity.ExpiresAt)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return runtimecore.StoredJob{}, err
		}
		if err == nil {
			row.Lease = &identity
		}
	}
	row.BlockingWait, err = pgjobdb.BlockingDependencyWait(ctx, s.DB, key)
	if err != nil {
		return runtimecore.StoredJob{}, err
	}
	return row, nil
}

func (s Scheduler) ListJobs(ctx context.Context, req runtimecore.ListJobsRequest) (runtimecore.ListJobsResponse, error) {
	opts := pgjobdb.ListJobsOptions{
		GoalIDs:  req.GoalIDs,
		RootOnly: req.RootOnly, CreatedAfter: req.CreatedAfter, IncludeHidden: req.IncludeHidden,
		CreatedBefore: req.CreatedBefore, PageSize: req.PageSize,
		PageToken: req.PageToken, Now: req.Now,
	}
	for _, route := range req.AvailableRoutes {
		opts.AvailableRoutes = append(opts.AvailableRoutes, pgjobdb.Route{JobType: pgjobdb.JobType(route.JobType), TaskType: pgjobdb.TaskType(route.TaskType)})
	}
	for _, id := range req.TenantIds {
		opts.TenantIDs = append(opts.TenantIDs, pgjobdb.TenantID(id))
	}
	for _, status := range req.Statuses {
		opts.Statuses = append(opts.Statuses, pgjobdb.JobStatus(status))
	}
	for _, store := range req.Stores {
		opts.Stores = append(opts.Stores, pgjobdb.JobStore(store))
	}
	for _, jobType := range req.JobTypes {
		opts.JobTypes = append(opts.JobTypes, pgjobdb.JobType(jobType))
	}
	for _, task := range req.JobTasks {
		opts.TaskSelectors = append(opts.TaskSelectors, pgjobdb.TaskSelector{
			JobType: pgjobdb.JobType(task.JobType), TaskType: pgjobdb.TaskType(task.TaskType),
		})
	}
	for _, key := range req.JobKeys {
		opts.JobKeys = append(opts.JobKeys, pgjobdb.JobKey{
			TenantID: pgjobdb.TenantID(key.TenantId), JobID: pgjobdb.JobID(key.JobId),
		})
	}
	for _, id := range req.ParentJobIDs {
		opts.ParentJobIDs = append(opts.ParentJobIDs, pgjobdb.JobID(id))
	}
	predicates, err := metadataPredicatesToPgjobdb(req.MetadataEquals)
	if err != nil {
		return runtimecore.ListJobsResponse{}, err
	}
	opts.MetadataPredicates = predicates
	listed, err := pgjobdb.ListJobSummaries(ctx, s.DB, opts)
	if err != nil {
		return runtimecore.ListJobsResponse{}, err
	}
	result := runtimecore.ListJobsResponse{
		Jobs:          make([]runtimecore.StoredJob, 0, len(listed.Jobs)),
		NextPageToken: listed.NextPageToken,
	}
	for _, summary := range listed.Jobs {
		row := runtimecore.StoredJob{
			JobKey: jobdb.JobKey{TenantId: string(summary.TenantID), JobId: string(summary.JobID)},
			GoalID: summary.GoalID, Store: jobdb.JobStore(summary.Store), Status: jobdb.JobStatus(summary.Status),
			JobType: string(summary.JobType), RouteJobType: string(summary.RouteJobType), WorkKind: runtimecore.WorkKind(summary.WorkKind),
			AppMetadata: summary.AppMetadata, SchemaHash: summary.SchemaHash, ParentJobID: string(summary.ParentJobID),
			CreatedAt: summary.CreatedAt, AvailableAt: summary.AvailableAt, ExpiresAt: summary.ExpiresAt,
			LeaseExpiresAt: summary.LeaseExpiresAt, ArchivedAt: summary.ArchivedAt, CancelRequested: summary.CancelRequested,
			ClientPayload:         summary.ClientPayload,
			ClientPayloadRevision: summary.ClientPayloadRevision, BlockingWaitSummary: summary.BlockingWaitSummary,
		}
		policy, err := runPolicyFromPgjobdb(summary.RunPolicy)
		if err != nil {
			return runtimecore.ListJobsResponse{}, err
		}
		row.RunPolicy = policy
		for _, id := range summary.WaitFor {
			row.WaitForJobIDs = append(row.WaitForJobIDs, string(id))
		}
		if summary.WorkKind == pgjobdb.WorkKindTask {
			if summary.TaskType == "" || summary.ResumeJobType == "" || summary.TaskInputOrdinal == nil || summary.TaskOutputOrdinal == nil || summary.TaskInputHash == "" {
				return runtimecore.ListJobsResponse{}, fmt.Errorf("task route for %s lacks coordinates", row.JobKey)
			}
			row.TaskWork = &runtimecore.TaskWork{TaskType: string(summary.TaskType), ResumeJobType: string(summary.ResumeJobType),
				InputOrdinal: *summary.TaskInputOrdinal, OutputOrdinal: *summary.TaskOutputOrdinal, InputHash: summary.TaskInputHash}
		}
		if summary.CompletionStatus != nil {
			row.Completion = &runtimecore.CompletionSnapshot{Status: string(*summary.CompletionStatus)}
			if summary.CompletionDetail != nil {
				row.Completion.Detail = *summary.CompletionDetail
			}
		}
		result.Jobs = append(result.Jobs, row)
	}
	return result, nil
}

func metadataPredicatesToPgjobdb(input []jobdb.MetadataPredicate) ([]pgjobdb.MetadataPredicate, error) {
	out := make([]pgjobdb.MetadataPredicate, 0, len(input))
	for _, predicate := range input {
		converted := pgjobdb.MetadataPredicate{Path: append([]string(nil), predicate.Path...)}
		for _, value := range predicate.Values {
			raw, err := json.Marshal(value)
			if err != nil {
				return nil, fmt.Errorf("encode metadata predicate: %w", err)
			}
			converted.Values = append(converted.Values, raw)
		}
		out = append(out, converted)
	}
	return out, nil
}

func storedJobFromDetail(detail pgjobdb.JobDetail) (runtimecore.StoredJob, error) {
	row := runtimecore.StoredJob{
		GoalID: detail.GoalID, OtherGoalIDs: detail.OtherGoalIDs,
		JobKey: jobdb.JobKey{TenantId: string(detail.TenantID), JobId: string(detail.JobID)},
		Store:  jobdb.JobStore(detail.Store), Status: jobdb.JobStatus(detail.Status),
		JobType: string(detail.JobType), RouteJobType: string(detail.RouteJobType),
		WorkKind:      runtimecore.WorkKind(detail.WorkKind),
		ClientPayload: append(json.RawMessage(nil), detail.ClientPayload...), ClientPayloadRevision: detail.ClientPayloadRevision, InitialPayloadDigest: detail.InitialPayloadDigest,
		AppMetadata: append(json.RawMessage(nil), detail.AppMetadata...),
		SchemaHash:  detail.SchemaHash, ParentJobID: string(detail.ParentJobID),
		AvailableAt: detail.AvailableAt, ExpiresAt: detail.ExpiresAt,
		LeaseExpiresAt: detail.LeaseExpiresAt, LeaseWorkerID: string(detail.LeaseWorkerID),
		CreatedAt: detail.CreatedAt, ArchivedAt: detail.ArchivedAt,
		CancelRequested: detail.CancelRequested,
	}
	for _, id := range detail.WaitFor {
		row.WaitForJobIDs = append(row.WaitForJobIDs, string(id))
	}
	policy, err := runPolicyFromPgjobdb(detail.RunPolicy)
	if err != nil {
		return runtimecore.StoredJob{}, err
	}
	row.RunPolicy = policy
	if detail.WorkKind == pgjobdb.WorkKindTask {
		if detail.TaskType == "" || detail.ResumeJobType == "" ||
			detail.TaskInputOrdinal == nil || detail.TaskOutputOrdinal == nil ||
			detail.TaskInputHash == "" {
			return runtimecore.StoredJob{}, fmt.Errorf("task route for %s lacks coordinates", row.JobKey)
		}
		row.TaskWork = &runtimecore.TaskWork{
			TaskType: string(detail.TaskType), ResumeJobType: string(detail.ResumeJobType),
			InputOrdinal: *detail.TaskInputOrdinal, OutputOrdinal: *detail.TaskOutputOrdinal,
			InputHash: detail.TaskInputHash,
		}
	}
	if detail.AlternateJobType != "" {
		if detail.AlternateAfterSeconds == nil {
			return runtimecore.StoredJob{}, fmt.Errorf("alternate route for %s lacks delay", row.JobKey)
		}
		row.AlternateRoute = &runtimecore.AlternateRoute{
			JobType: string(detail.AlternateJobType), TaskType: string(detail.AlternateTaskType),
			After: time.Duration(*detail.AlternateAfterSeconds * float64(time.Second)),
		}
	}
	if detail.ScheduleID != "" {
		if detail.ScheduleGeneration == nil || detail.ScheduledAt == nil {
			return runtimecore.StoredJob{}, fmt.Errorf("schedule occurrence for %s is incomplete", row.JobKey)
		}
		occ := &jobdb.ScheduleOccurrenceMetadata{
			ScheduleId: detail.ScheduleID, Kind: jobdb.ScheduleMetadataKind,
			Generation: *detail.ScheduleGeneration, SpecHash: detail.ScheduleSpecHash,
			ScheduledAt: *detail.ScheduledAt, RunId: detail.ScheduleRunID,
			Manual: detail.ScheduleManual, BackfillId: detail.ScheduleBackfillID,
			PreviousJobId: string(detail.SchedulePreviousJobID),
		}
		if len(detail.ScheduleFailureHistory) > 0 {
			if err := json.Unmarshal(detail.ScheduleFailureHistory, &occ.FailureHistory); err != nil {
				return runtimecore.StoredJob{}, fmt.Errorf("decode schedule failure history: %w", err)
			}
		}
		row.Schedule = occ
	}
	if detail.CompletionStatus != nil {
		completion := &runtimecore.CompletionSnapshot{Status: string(*detail.CompletionStatus)}
		if detail.CompletionDetail != nil {
			completion.Detail = *detail.CompletionDetail
		}
		if detail.CompletionErrorKind != nil {
			completion.ErrorKind = *detail.CompletionErrorKind
		}
		completion.Retryable = detail.CompletionRetryable
		row.Completion = completion
	}
	return row, nil
}

func runPolicyFromPgjobdb(policy pgjobdb.RunPolicy) (jobdb.RunPolicy, error) {
	initial, err := durationFromMillis(policy.Retry.InitialIntervalMillis)
	if err != nil {
		return jobdb.RunPolicy{}, err
	}
	maximum, err := durationFromMillis(policy.Retry.MaximumIntervalMillis)
	if err != nil {
		return jobdb.RunPolicy{}, err
	}
	out := jobdb.RunPolicy{Retry: jobdb.RetryPolicy{
		InitialInterval:        jobdb.Duration(initial),
		BackoffCoefficient:     policy.Retry.BackoffCoefficient,
		MaximumInterval:        jobdb.Duration(maximum),
		MaximumAttempts:        policy.Retry.MaximumAttempts,
		NonRetryableErrorTypes: append([]string(nil), policy.Retry.NonRetryableErrorTypes...),
	}}
	if policy.InvocationTimeoutMillis != nil {
		value, err := durationFromMillis(*policy.InvocationTimeoutMillis)
		if err != nil {
			return jobdb.RunPolicy{}, err
		}
		out.InvocationTimeout = jobdb.AsDuration(value)
	}
	if policy.TotalTimeoutMillis != nil {
		value, err := durationFromMillis(*policy.TotalTimeoutMillis)
		if err != nil {
			return jobdb.RunPolicy{}, err
		}
		out.TotalTimeout = jobdb.AsDuration(value)
	}
	return out, nil
}

func durationFromMillis(millis int64) (time.Duration, error) {
	if millis < 0 || millis > math.MaxInt64/int64(time.Millisecond) {
		return 0, fmt.Errorf("run policy duration in milliseconds is out of range")
	}
	return time.Duration(millis) * time.Millisecond, nil
}

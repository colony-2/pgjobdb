package pgjobdb

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math"
	"time"

	"github.com/lib/pq"
)

// GetWork atomically claims one eligible native job or task route.
func GetWork(ctx context.Context, db DB, worker WorkerID, selector WorkSelector, opts GetWorkOptions) (*JobLease, error) {
	return getWork(ctx, db, worker, selector, opts, "", "")
}

// GetJobLease claims a specific job when it is eligible for the selector.
func GetJobLease(ctx context.Context, db DB, tenant TenantID, job JobID, worker WorkerID,
	selector WorkSelector, opts GetJobLeaseOptions) (*JobLease, error) {
	if tenant == "" || job == "" {
		return nil, fmt.Errorf("pgjobdb: tenant id and job id are required")
	}
	return getWork(ctx, db, worker, selector, GetWorkOptions{
		TenantIDs: []TenantID{tenant}, AppMetadataContains: opts.AppMetadataContains,
		LeaseDuration: opts.LeaseDuration,
	}, tenant, job)
}

func getWork(ctx context.Context, db DB, worker WorkerID, selector WorkSelector,
	opts GetWorkOptions, targetTenant TenantID, targetJob JobID) (*JobLease, error) {
	if err := validateScheduleDB(ctx, db); err != nil {
		return nil, err
	}
	if worker == "" {
		return nil, fmt.Errorf("pgjobdb: worker id is required")
	}
	jobTypes := make([]string, 0, len(selector.JobTypes))
	for _, jobType := range selector.JobTypes {
		if jobType == "" {
			return nil, fmt.Errorf("pgjobdb: selector job type is required")
		}
		jobTypes = append(jobTypes, string(jobType))
	}
	for _, task := range selector.Tasks {
		if task.JobType == "" || task.TaskType == "" {
			return nil, fmt.Errorf("pgjobdb: task selector job and task types are required")
		}
	}
	if len(jobTypes) == 0 && len(selector.Tasks) == 0 {
		return nil, fmt.Errorf("pgjobdb: work selector is empty")
	}
	taskSelectors := selector.Tasks
	if taskSelectors == nil {
		taskSelectors = []TaskSelector{}
	}
	tasks, err := json.Marshal(taskSelectors)
	if err != nil {
		return nil, fmt.Errorf("pgjobdb: encode task selectors: %w", err)
	}
	metadata := opts.AppMetadataContains
	if len(metadata) == 0 {
		metadata = json.RawMessage(`{}`)
	}
	if !isJSONObject(metadata) {
		return nil, fmt.Errorf("pgjobdb: metadata filter must be a JSON object")
	}
	seconds, err := leaseSeconds(opts.LeaseDuration)
	if err != nil {
		return nil, err
	}
	var tenantIDs []string
	for _, tenant := range opts.TenantIDs {
		if tenant == "" {
			return nil, fmt.Errorf("pgjobdb: tenant filter cannot contain an empty id")
		}
		tenantIDs = append(tenantIDs, string(tenant))
	}
	row := db.QueryRowContext(ctx, `SELECT * FROM pgjobdb.get_native_work(
		$1, $2, $3, $4, $5, $6, $7, $8)`, string(worker),
		pq.Array(tenantIDs), pq.Array(jobTypes), string(tasks), string(metadata),
		seconds, nilIfBlank(string(targetTenant)), nilIfBlank(string(targetJob)))
	lease, err := scanJobLease(row, worker)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return lease, err
}

func leaseSeconds(duration time.Duration) (int, error) {
	if duration == 0 {
		return 60, nil
	}
	if duration < 0 || duration > 24*time.Hour {
		return 0, fmt.Errorf("pgjobdb: lease duration must be between 1 second and 24 hours")
	}
	seconds := int(math.Ceil(duration.Seconds()))
	if seconds < 1 {
		seconds = 1
	}
	return seconds, nil
}

func scanJobLease(row scheduleScanner, worker WorkerID) (*JobLease, error) {
	var lease JobLease
	var tenant, job, jobType, route, workKind string
	var taskType, resumeType, inputHash, schemaHash sql.NullString
	var inputOrdinal, outputOrdinal sql.NullInt64
	var policy, payload []byte
	if err := row.Scan(&tenant, &job, &lease.LeaseID, &lease.ExpiresAt,
		&jobType, &route, &workKind, &taskType, &resumeType,
		&inputOrdinal, &outputOrdinal, &inputHash, &policy, &payload,
		&schemaHash); err != nil {
		return nil, err
	}
	lease.TenantID, lease.JobID, lease.WorkerID = TenantID(tenant), JobID(job), worker
	lease.JobType, lease.RouteJobType, lease.WorkKind = JobType(jobType), JobType(route), WorkKind(workKind)
	lease.ExpiresAt = lease.ExpiresAt.UTC()
	lease.SchemaHash = schemaHash.String
	lease.LeasePayload = append(json.RawMessage(nil), payload...)
	if err := json.Unmarshal(policy, &lease.RunPolicy); err != nil {
		return nil, fmt.Errorf("pgjobdb: decode run policy: %w", err)
	}
	if lease.WorkKind == WorkKindTask {
		lease.Task = &TaskWork{
			TaskType: TaskType(taskType.String), ResumeJobType: JobType(resumeType.String),
			InputOrdinal: inputOrdinal.Int64, OutputOrdinal: outputOrdinal.Int64,
			InputHash: inputHash.String,
		}
	}
	return &lease, nil
}

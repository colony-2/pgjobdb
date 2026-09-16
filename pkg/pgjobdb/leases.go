package pgjobdb

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/lib/pq"
)

var ErrLeaseLost = errors.New("pgjobdb: lease lost")

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
		MetadataPredicates: opts.MetadataPredicates,
		LeaseDuration:      opts.LeaseDuration,
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
	predicates := opts.MetadataPredicates
	if predicates == nil {
		predicates = []MetadataPredicate{}
	}
	if err := validateMetadataPredicates(predicates); err != nil {
		return nil, err
	}
	predicateJSON, err := json.Marshal(predicates)
	if err != nil {
		return nil, fmt.Errorf("pgjobdb: encode metadata predicates: %w", err)
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
		$1, $2, $3, $4, $5, $6, $7, $8, $9)`, string(worker),
		pq.Array(tenantIDs), pq.Array(jobTypes), string(tasks), string(metadata),
		seconds, nilIfBlank(string(targetTenant)), nilIfBlank(string(targetJob)),
		string(predicateJSON))
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
		&lease.LeasePayloadVisible,
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
	if taskType.Valid {
		lease.Task = &TaskWork{
			TaskType: TaskType(taskType.String), ResumeJobType: JobType(resumeType.String),
			InputOrdinal: inputOrdinal.Int64, OutputOrdinal: outputOrdinal.Int64,
			InputHash: inputHash.String,
		}
	}
	return &lease, nil
}

// ValidateLease loads the current live native lease for an exact identity.
func ValidateLease(ctx context.Context, db DB, identity LeaseIdentity) (*JobLease, error) {
	if err := validateScheduleDB(ctx, db); err != nil {
		return nil, err
	}
	if err := validateLeaseIdentity(identity); err != nil {
		return nil, err
	}
	row := db.QueryRowContext(ctx, `SELECT * FROM pgjobdb.validate_native_lease(
		$1, $2, $3, $4)`, string(identity.TenantID), string(identity.JobID),
		identity.LeaseID, string(identity.WorkerID))
	lease, err := scanJobLease(row, identity.WorkerID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrLeaseLost
	}
	return lease, err
}

// KeepAliveLease extends a live native lease and returns its new snapshot.
func KeepAliveLease(ctx context.Context, db DB, identity LeaseIdentity,
	additional time.Duration) (*JobLease, error) {
	if err := validateScheduleDB(ctx, db); err != nil {
		return nil, err
	}
	if err := validateLeaseIdentity(identity); err != nil {
		return nil, err
	}
	if additional <= 0 {
		return nil, fmt.Errorf("pgjobdb: additional lease duration must be positive")
	}
	seconds, err := leaseSeconds(additional)
	if err != nil {
		return nil, err
	}
	var expiresAt time.Time
	if err := db.QueryRowContext(ctx, `SELECT pgjobdb.renew_native_lease(
		$1, $2, $3, $4, $5)`, string(identity.TenantID),
		string(identity.JobID), identity.LeaseID, string(identity.WorkerID),
		seconds).Scan(&expiresAt); err != nil {
		return nil, err
	}
	return ValidateLease(ctx, db, identity)
}

func (l *JobLease) KeepAlive(ctx context.Context, db DB, additional time.Duration) error {
	updated, err := KeepAliveLease(ctx, db, l.Identity(), additional)
	if err != nil {
		return err
	}
	l.ExpiresAt = updated.ExpiresAt
	return nil
}

func validateLeaseIdentity(identity LeaseIdentity) error {
	if identity.TenantID == "" || identity.JobID == "" ||
		identity.LeaseID == "" || identity.WorkerID == "" {
		return fmt.Errorf("pgjobdb: tenant, job, lease, and worker ids are required")
	}
	return nil
}

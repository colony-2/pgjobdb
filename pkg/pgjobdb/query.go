package pgjobdb

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"time"

	"github.com/lib/pq"
)

// JobStatus represents the possible states returned by pgjobdb.jobs_with_status view
type JobStatus string

const (
	JobStatusActive         JobStatus = "ACTIVE"          // Currently leased (lease_expires_at > NOW())
	JobStatusCancelled      JobStatus = "CANCELLED"       // Cancellation requested
	JobStatusAwaitingFuture JobStatus = "AWAITING_FUTURE" // Waiting for available_at timestamp
	JobStatusPendingJobs    JobStatus = "PENDING_JOBS"    // Blocked by wait_for dependencies
	JobStatusCrashConcern   JobStatus = "CRASH_CONCERN"   // Too many consecutive lease expirations
	JobStatusExpired        JobStatus = "EXPIRED"         // Hit expires_at timestamp
	JobStatusReady          JobStatus = "READY"           // Ready to be leased
	JobStatusCompleted      JobStatus = "COMPLETED"       // Archived and not cancelled
)

// JobStatusInfo contains the current status and metadata of a job
// Maps directly to fields from pgjobdb.jobs_with_status view and pgjobdb.jobs_archive table
type JobStatusInfo struct {
	// Core identification
	TenantID string
	JobID    string

	// Status (computed by the view for active jobs)
	Status JobStatus

	// Job configuration
	NextNeed string
	WaitFor  []string
	Payload  json.RawMessage
	Metadata json.RawMessage

	// Timing
	AvailableAt time.Time
	ExpiresAt   *time.Time // nil if 'infinity'
	CreatedAt   time.Time
	ArchivedAt  *time.Time // Only populated if job is archived
	// Completion (archived jobs only)
	CompletionStatus *CompletionStatus
	CompletionDetail *string

	// Lease information
	LeaseID                *string
	LeaseExpiresAt         *time.Time // nil if '-infinity' (no active lease)
	LeaseExpirationCount   int64
	ConsecutiveExpirations int64

	// Cancellation
	CancelRequested   bool
	CancelRequestedBy *string
	CancelRequestedAt *time.Time
}

// JobExistence contains information about whether a job exists
type JobExistence struct {
	Exists   bool
	TenantID string
	JobID    string
}

// JobListItem contains summary information about a job (excluding payload for efficiency)
type JobListItem struct {
	TenantID               string
	JobID                  string
	Status                 JobStatus
	NextNeed               string
	WaitFor                []string
	Metadata               json.RawMessage
	AvailableAt            time.Time
	ExpiresAt              *time.Time // nil if 'infinity'
	CreatedAt              time.Time
	ArchivedAt             *time.Time
	CompletionStatus       *CompletionStatus
	CompletionDetail       *string
	LeaseID                *string
	LeaseExpiresAt         *time.Time // nil if '-infinity'
	LeaseExpirationCount   int64
	ConsecutiveExpirations int64
	CancelRequested        bool
	CancelRequestedBy      *string
	CancelRequestedAt      *time.Time
}

// ListJobsOptions specifies filtering and pagination options for listing jobs
type ListJobsOptions struct {
	// Filtering - Tenant
	TenantID  string   // DEPRECATED: Use TenantIDs for single or multiple tenants
	TenantIDs []string // Filter by tenant IDs (if both TenantID and TenantIDs are provided, TenantIDs takes precedence)

	// Filtering - Status and Type
	Statuses           []JobStatus // Filter by status (empty = all statuses)
	JobTypePattern     string      // DEPRECATED: Use JobTypePatterns for single or multiple patterns
	JobTypePatterns    []string    // Filter by job type patterns (SQL LIKE patterns, OR semantics)
	CreatedAfter       *time.Time  // Filter by creation time
	CreatedBefore      *time.Time
	CompletionStatuses []CompletionStatus  // Filter by completion status (archived jobs only)
	IncludeArchived    bool                // Whether to include archived jobs (default: false, only active)
	MetadataEquals     []MetadataPredicate // Equality filters for JSON metadata fields

	// Pagination
	Limit  int    // Max results to return (default: 100, max: 1000)
	Cursor string // Opaque cursor for pagination (empty for first page)

	// Sorting
	SortBy    SortField     // Field to sort by (default: CreatedAt)
	SortOrder SortDirection // ASC or DESC (default: DESC)
}

// SortField specifies which field to sort by
type SortField string

const (
	SortByCreatedAt   SortField = "created_at"
	SortByAvailableAt SortField = "available_at"
	SortByJobID       SortField = "job_id"
)

// SortDirection specifies sort direction
type SortDirection string

const (
	SortAsc  SortDirection = "ASC"
	SortDesc SortDirection = "DESC"
)

// MetadataPredicate matches a JSON metadata value at a specific path.
// Path elements map to successive object keys (metadata #> path).
type MetadataPredicate struct {
	Path   []string
	Values []any
}

// ListJobsResult contains the results of a job listing query
type ListJobsResult struct {
	Jobs       []JobListItem
	NextCursor string // Empty if no more results
	HasMore    bool
}

// FindJobsOptions specifies criteria for finding jobs
type FindJobsOptions struct {
	TenantIDs []string  // Filter by tenant IDs (empty = all tenants)
	Status    JobStatus // Required: status to match
	NextNeed  string    // Required: the capability the job is waiting for
	Limit     int       // Max results (default: 100, max: 1000)
}

// JobInfo contains detailed information about a job (without payload)
type JobInfo struct {
	TenantID               string
	JobID                  string
	Status                 JobStatus
	NextNeed               string
	WaitFor                []string
	Metadata               json.RawMessage
	AvailableAt            time.Time
	ExpiresAt              *time.Time // nil if 'infinity'
	CreatedAt              time.Time
	LeaseID                *string
	LeaseExpiresAt         *time.Time // nil if '-infinity'
	LeaseExpirationCount   int64
	ConsecutiveExpirations int64
	CancelRequested        bool
	CancelRequestedBy      *string
	CancelRequestedAt      *time.Time
}

// GetJobOptions specifies options for retrieving a job
type GetJobOptions struct {
	IncludePayload bool // Whether to include the job payload (default: false)
}

// JobDetail contains complete information about a job including payload
type JobDetail struct {
	TenantID               string
	JobID                  string
	Status                 JobStatus
	NextNeed               string
	WaitFor                []string
	Payload                json.RawMessage // Only populated if IncludePayload option is true
	Metadata               json.RawMessage
	AvailableAt            time.Time
	ExpiresAt              *time.Time // nil if 'infinity'
	CreatedAt              time.Time
	ArchivedAt             *time.Time // Only populated if querying archive
	CompletionStatus       *CompletionStatus
	CompletionDetail       *string
	LeaseID                *string
	LeaseExpiresAt         *time.Time // nil if '-infinity'
	LeaseExpirationCount   int64
	ConsecutiveExpirations int64
	CancelRequested        bool
	CancelRequestedBy      *string
	CancelRequestedAt      *time.Time
}

// ListArchivedJobsOptions specifies options for listing archived jobs
type ListArchivedJobsOptions struct {
	TenantID           string
	JobTypePattern     string
	CompletionStatuses []CompletionStatus
	ArchivedAfter      *time.Time
	ArchivedBefore     *time.Time
	Limit              int
	Cursor             string
}

var (
	// ErrTenantMismatch is returned when a job exists but belongs to a different tenant
	ErrTenantMismatch = fmt.Errorf("pgjobdb: job belongs to different tenant")

	// ErrInvalidCursor is returned when a pagination cursor is invalid or expired
	ErrInvalidCursor = fmt.Errorf("pgjobdb: invalid or expired cursor")

	// ErrInvalidOptions is returned when query options are invalid
	ErrInvalidOptions = fmt.Errorf("pgjobdb: invalid query options")
)

// GetJobStatus retrieves the status and metadata of a job by ID.
// Returns ErrJobNotFound if the job doesn't exist in either active or archived tables.
// Searches both pgjobdb.jobs_with_status (active) and pgjobdb.jobs_archive (archived).
func GetJobStatus(ctx context.Context, db DB, tenantID TenantID, jobID JobID) (*JobStatusInfo, error) {
	if db == nil {
		return nil, fmt.Errorf("pgjobdb: nil DB")
	}
	if ctx == nil {
		return nil, fmt.Errorf("pgjobdb: nil context")
	}
	if tenantID == "" {
		return nil, fmt.Errorf("pgjobdb: tenant id is required")
	}
	if jobID == "" {
		return nil, fmt.Errorf("pgjobdb: job id is required")
	}

	// Try active jobs first
	const activeQuery = `
		SELECT
			tenant_id, job_id, status, next_need, wait_for, payload, metadata,
			available_at,
			CASE WHEN expires_at = 'infinity' THEN NULL ELSE expires_at END,
			created_at,
			NULL as archived_at,
			NULL as completion_status,
			NULL as completion_detail,
			lease_id,
			CASE WHEN lease_expires_at = '-infinity' THEN NULL ELSE lease_expires_at END,
			lease_expiration_count, consecutive_expirations,
			cancel_requested, cancel_requested_by, cancel_requested_at
		FROM pgjobdb.jobs_with_status
		WHERE tenant_id = $1 AND job_id = $2
	`

	info, err := scanJobStatusInfo(db.QueryRowContext(ctx, activeQuery, string(tenantID), string(jobID)))
	if err == nil {
		return info, nil
	}
	if err != sql.ErrNoRows {
		return nil, annotateError(err)
	}

	// Try archived jobs
	const archiveQuery = `
		SELECT
			tenant_id, job_id, next_need, wait_for, payload, metadata,
			CASE WHEN expires_at = 'infinity' THEN NULL ELSE expires_at END,
			created_at, archived_at,
			completion_status, completion_detail,
			lease_id, lease_expiration_count, consecutive_expirations,
			cancel_requested, cancel_requested_by, cancel_requested_at
		FROM pgjobdb.jobs_archive
		WHERE tenant_id = $1 AND job_id = $2
	`

	info, err = scanJobStatusInfoArchive(db.QueryRowContext(ctx, archiveQuery, string(tenantID), string(jobID)))
	if err == sql.ErrNoRows {
		return nil, wrap(ErrJobNotFound, fmt.Errorf("job %s not found in tenant %s", jobID, tenantID))
	}
	if err != nil {
		return nil, annotateError(err)
	}

	return info, nil
}

// scanJobStatusInfo scans a row from jobs_with_status into JobStatusInfo
func scanJobStatusInfo(row *sql.Row) (*JobStatusInfo, error) {
	var info JobStatusInfo
	var status string
	var waitFor pq.StringArray
	var archivedAt sql.NullTime
	var completionStatus sql.NullString
	var completionDetail sql.NullString
	var expiresAt sql.NullTime
	var leaseID sql.NullString
	var leaseExpiresAt sql.NullTime
	var cancelRequestedBy sql.NullString
	var cancelRequestedAt sql.NullTime

	err := row.Scan(
		&info.TenantID,
		&info.JobID,
		&status,
		&info.NextNeed,
		(*pq.StringArray)(&waitFor),
		&info.Payload,
		&info.Metadata,
		&info.AvailableAt,
		&expiresAt,
		&info.CreatedAt,
		&archivedAt,
		&completionStatus,
		&completionDetail,
		&leaseID,
		&leaseExpiresAt,
		&info.LeaseExpirationCount,
		&info.ConsecutiveExpirations,
		&info.CancelRequested,
		&cancelRequestedBy,
		&cancelRequestedAt,
	)
	if err != nil {
		return nil, err
	}

	info.Status = JobStatus(status)
	info.WaitFor = waitFor
	if expiresAt.Valid {
		info.ExpiresAt = &expiresAt.Time
	}
	if archivedAt.Valid {
		info.ArchivedAt = &archivedAt.Time
	}
	if completionStatus.Valid && completionStatus.String != "" {
		cs := CompletionStatus(completionStatus.String)
		info.CompletionStatus = &cs
	}
	if completionDetail.Valid {
		info.CompletionDetail = &completionDetail.String
	}
	if leaseID.Valid {
		info.LeaseID = &leaseID.String
	}
	if leaseExpiresAt.Valid {
		info.LeaseExpiresAt = &leaseExpiresAt.Time
	}
	if cancelRequestedBy.Valid {
		info.CancelRequestedBy = &cancelRequestedBy.String
	}
	if cancelRequestedAt.Valid {
		info.CancelRequestedAt = &cancelRequestedAt.Time
	}

	return &info, nil
}

// scanJobStatusInfoArchive scans a row from jobs_archive into JobStatusInfo
func scanJobStatusInfoArchive(row *sql.Row) (*JobStatusInfo, error) {
	var info JobStatusInfo
	var waitFor pq.StringArray
	var expiresAt sql.NullTime
	var archivedAt sql.NullTime
	var completionStatus sql.NullString
	var completionDetail sql.NullString
	var leaseID sql.NullString
	var cancelRequestedBy sql.NullString
	var cancelRequestedAt sql.NullTime

	err := row.Scan(
		&info.TenantID,
		&info.JobID,
		&info.NextNeed,
		(*pq.StringArray)(&waitFor),
		&info.Payload,
		&info.Metadata,
		&expiresAt,
		&info.CreatedAt,
		&archivedAt,
		&completionStatus,
		&completionDetail,
		&leaseID,
		&info.LeaseExpirationCount,
		&info.ConsecutiveExpirations,
		&info.CancelRequested,
		&cancelRequestedBy,
		&cancelRequestedAt,
	)
	if err != nil {
		return nil, err
	}

	if completionStatus.Valid && completionStatus.String != "" {
		cs := CompletionStatus(completionStatus.String)
		info.CompletionStatus = &cs
		if cs == CompletionStatusCancelled {
			info.Status = JobStatusCancelled
		} else {
			info.Status = JobStatusCompleted
		}
	} else if info.CancelRequested {
		info.Status = JobStatusCancelled
	} else {
		info.Status = JobStatusCompleted
	}

	info.WaitFor = waitFor
	if expiresAt.Valid {
		info.ExpiresAt = &expiresAt.Time
	}
	if archivedAt.Valid {
		info.ArchivedAt = &archivedAt.Time
	}
	if completionDetail.Valid {
		info.CompletionDetail = &completionDetail.String
	}
	if leaseID.Valid {
		info.LeaseID = &leaseID.String
	}
	if cancelRequestedBy.Valid {
		info.CancelRequestedBy = &cancelRequestedBy.String
	}
	if cancelRequestedAt.Valid {
		info.CancelRequestedAt = &cancelRequestedAt.Time
	}

	// Archived jobs: AvailableAt not stored, use CreatedAt as fallback
	info.AvailableAt = info.CreatedAt
	// Archived jobs have no active lease, leave LeaseExpiresAt as nil

	return &info, nil
}

// CheckJobExists checks if a job exists in either active or archived state.
func CheckJobExists(ctx context.Context, db DB, tenantID TenantID, jobID JobID) (*JobExistence, error) {
	if db == nil {
		return nil, fmt.Errorf("pgjobdb: nil DB")
	}
	if ctx == nil {
		return nil, fmt.Errorf("pgjobdb: nil context")
	}
	if tenantID == "" {
		return nil, fmt.Errorf("pgjobdb: tenant id is required")
	}
	if jobID == "" {
		return nil, fmt.Errorf("pgjobdb: job id is required")
	}

	const query = `
		SELECT EXISTS(SELECT 1 FROM pgjobdb.jobs WHERE tenant_id = $1 AND job_id = $2)
		   OR EXISTS(SELECT 1 FROM pgjobdb.jobs_archive WHERE tenant_id = $1 AND job_id = $2)
	`

	var exists bool
	err := db.QueryRowContext(ctx, query, string(tenantID), string(jobID)).Scan(&exists)
	if err != nil {
		return nil, annotateError(err)
	}

	return &JobExistence{
		Exists:   exists,
		TenantID: string(tenantID),
		JobID:    string(jobID),
	}, nil
}

// CheckJobExistsWithTenant checks if a job exists and validates it belongs to the expected tenant.
// Returns ErrJobNotFound if job doesn't exist.
// Returns ErrTenantMismatch if job exists but belongs to different tenant.
func CheckJobExistsWithTenant(ctx context.Context, db DB, jobID JobID, expectedTenantID TenantID) (*JobExistence, error) {
	if db == nil {
		return nil, fmt.Errorf("pgjobdb: nil DB")
	}
	if ctx == nil {
		return nil, fmt.Errorf("pgjobdb: nil context")
	}
	if expectedTenantID == "" {
		return nil, fmt.Errorf("pgjobdb: tenant id is required")
	}
	if jobID == "" {
		return nil, fmt.Errorf("pgjobdb: job id is required")
	}

	const query = `
		SELECT tenant_id FROM pgjobdb.jobs WHERE job_id = $1
		UNION ALL
		SELECT tenant_id FROM pgjobdb.jobs_archive WHERE job_id = $1
		LIMIT 1
	`

	var actualTenantID string
	err := db.QueryRowContext(ctx, query, string(jobID)).Scan(&actualTenantID)
	if err == sql.ErrNoRows {
		return nil, wrap(ErrJobNotFound, fmt.Errorf("job %s not found", jobID))
	}
	if err != nil {
		return nil, annotateError(err)
	}

	if actualTenantID != string(expectedTenantID) {
		return nil, wrap(ErrTenantMismatch, fmt.Errorf("job %s belongs to tenant %s, expected %s", jobID, actualTenantID, expectedTenantID))
	}

	return &JobExistence{
		Exists:   true,
		TenantID: actualTenantID,
		JobID:    string(jobID),
	}, nil
}

// GetJob retrieves a job by ID without leasing it.
// Returns ErrJobNotFound if the job doesn't exist.
// Searches both active and archived jobs.
func GetJob(ctx context.Context, db DB, tenantID TenantID, jobID JobID, opts GetJobOptions) (*JobDetail, error) {
	if db == nil {
		return nil, fmt.Errorf("pgjobdb: nil DB")
	}
	if ctx == nil {
		return nil, fmt.Errorf("pgjobdb: nil context")
	}
	if tenantID == "" {
		return nil, fmt.Errorf("pgjobdb: tenant id is required")
	}
	if jobID == "" {
		return nil, fmt.Errorf("pgjobdb: job id is required")
	}

	payloadCol := "NULL as payload"
	if opts.IncludePayload {
		payloadCol = "payload"
	}

	// Try active jobs first
	activeQuery := fmt.Sprintf(`
		SELECT
			tenant_id, job_id, status, next_need, wait_for, %s, metadata,
			available_at,
			CASE WHEN expires_at = 'infinity' THEN NULL ELSE expires_at END,
			created_at,
			NULL as archived_at,
			NULL as completion_status,
			NULL as completion_detail,
			lease_id,
			CASE WHEN lease_expires_at = '-infinity' THEN NULL ELSE lease_expires_at END,
			lease_expiration_count, consecutive_expirations,
			cancel_requested, cancel_requested_by, cancel_requested_at
		FROM pgjobdb.jobs_with_status
		WHERE tenant_id = $1 AND job_id = $2
	`, payloadCol)

	detail, err := scanJobDetail(db.QueryRowContext(ctx, activeQuery, string(tenantID), string(jobID)))
	if err == nil {
		return detail, nil
	}
	if err != sql.ErrNoRows {
		return nil, annotateError(err)
	}

	// Try archived jobs
	archiveQuery := fmt.Sprintf(`
		SELECT
			tenant_id, job_id, next_need, wait_for, %s, metadata,
			CASE WHEN expires_at = 'infinity' THEN NULL ELSE expires_at END,
			created_at, archived_at,
			completion_status, completion_detail,
			lease_id, lease_expiration_count, consecutive_expirations,
			cancel_requested, cancel_requested_by, cancel_requested_at
		FROM pgjobdb.jobs_archive
		WHERE tenant_id = $1 AND job_id = $2
	`, payloadCol)

	detail, err = scanJobDetailArchive(db.QueryRowContext(ctx, archiveQuery, string(tenantID), string(jobID)))
	if err == sql.ErrNoRows {
		return nil, wrap(ErrJobNotFound, fmt.Errorf("job %s not found in tenant %s", jobID, tenantID))
	}
	if err != nil {
		return nil, annotateError(err)
	}

	return detail, nil
}

// scanJobDetail scans a row from jobs_with_status into JobDetail
func scanJobDetail(row *sql.Row) (*JobDetail, error) {
	var detail JobDetail
	var status string
	var waitFor pq.StringArray
	var payload sql.NullString
	var metadata json.RawMessage
	var expiresAt sql.NullTime
	var archivedAt sql.NullTime
	var completionStatus sql.NullString
	var completionDetail sql.NullString
	var leaseID sql.NullString
	var leaseExpiresAt sql.NullTime
	var cancelRequestedBy sql.NullString
	var cancelRequestedAt sql.NullTime

	err := row.Scan(
		&detail.TenantID,
		&detail.JobID,
		&status,
		&detail.NextNeed,
		(*pq.StringArray)(&waitFor),
		&payload,
		&metadata,
		&detail.AvailableAt,
		&expiresAt,
		&detail.CreatedAt,
		&archivedAt,
		&completionStatus,
		&completionDetail,
		&leaseID,
		&leaseExpiresAt,
		&detail.LeaseExpirationCount,
		&detail.ConsecutiveExpirations,
		&detail.CancelRequested,
		&cancelRequestedBy,
		&cancelRequestedAt,
	)
	if err != nil {
		return nil, err
	}

	detail.Status = JobStatus(status)
	detail.WaitFor = waitFor
	if payload.Valid {
		detail.Payload = json.RawMessage(payload.String)
	}
	detail.Metadata = metadata
	if expiresAt.Valid {
		detail.ExpiresAt = &expiresAt.Time
	}
	if archivedAt.Valid {
		detail.ArchivedAt = &archivedAt.Time
	}
	if completionStatus.Valid && completionStatus.String != "" {
		cs := CompletionStatus(completionStatus.String)
		detail.CompletionStatus = &cs
	}
	if completionDetail.Valid {
		detail.CompletionDetail = &completionDetail.String
	}
	if leaseID.Valid {
		detail.LeaseID = &leaseID.String
	}
	if leaseExpiresAt.Valid {
		detail.LeaseExpiresAt = &leaseExpiresAt.Time
	}
	if cancelRequestedBy.Valid {
		detail.CancelRequestedBy = &cancelRequestedBy.String
	}
	if cancelRequestedAt.Valid {
		detail.CancelRequestedAt = &cancelRequestedAt.Time
	}

	return &detail, nil
}

// scanJobDetailArchive scans a row from jobs_archive into JobDetail
func scanJobDetailArchive(row *sql.Row) (*JobDetail, error) {
	var detail JobDetail
	var waitFor pq.StringArray
	var payload sql.NullString
	var metadata json.RawMessage
	var expiresAt sql.NullTime
	var archivedAt sql.NullTime
	var completionStatus sql.NullString
	var completionDetail sql.NullString
	var leaseID sql.NullString
	var cancelRequestedBy sql.NullString
	var cancelRequestedAt sql.NullTime

	err := row.Scan(
		&detail.TenantID,
		&detail.JobID,
		&detail.NextNeed,
		(*pq.StringArray)(&waitFor),
		&payload,
		&metadata,
		&expiresAt,
		&detail.CreatedAt,
		&archivedAt,
		&completionStatus,
		&completionDetail,
		&leaseID,
		&detail.LeaseExpirationCount,
		&detail.ConsecutiveExpirations,
		&detail.CancelRequested,
		&cancelRequestedBy,
		&cancelRequestedAt,
	)
	if err != nil {
		return nil, err
	}

	if completionStatus.Valid && completionStatus.String != "" {
		cs := CompletionStatus(completionStatus.String)
		detail.CompletionStatus = &cs
		if cs == CompletionStatusCancelled {
			detail.Status = JobStatusCancelled
		} else {
			detail.Status = JobStatusCompleted
		}
	} else if detail.CancelRequested {
		detail.Status = JobStatusCancelled
	} else {
		detail.Status = JobStatusCompleted
	}

	detail.WaitFor = waitFor
	if payload.Valid {
		detail.Payload = json.RawMessage(payload.String)
	}
	detail.Metadata = metadata
	if expiresAt.Valid {
		detail.ExpiresAt = &expiresAt.Time
	}
	if archivedAt.Valid {
		detail.ArchivedAt = &archivedAt.Time
	}
	if completionDetail.Valid {
		detail.CompletionDetail = &completionDetail.String
	}
	if leaseID.Valid {
		detail.LeaseID = &leaseID.String
	}
	if cancelRequestedBy.Valid {
		detail.CancelRequestedBy = &cancelRequestedBy.String
	}
	if cancelRequestedAt.Valid {
		detail.CancelRequestedAt = &cancelRequestedAt.Time
	}

	// Archived jobs: AvailableAt not stored, use CreatedAt as fallback
	detail.AvailableAt = detail.CreatedAt
	// Archived jobs have no active lease, leave LeaseExpiresAt as nil

	return &detail, nil
}

// IsJobArchived checks if a specific job has been archived (completed).
// Returns true if job is in archive, false if in active jobs or doesn't exist.
func IsJobArchived(ctx context.Context, db DB, tenantID TenantID, jobID JobID) (bool, error) {
	if db == nil {
		return false, fmt.Errorf("pgjobdb: nil DB")
	}
	if ctx == nil {
		return false, fmt.Errorf("pgjobdb: nil context")
	}
	if tenantID == "" {
		return false, fmt.Errorf("pgjobdb: tenant id is required")
	}
	if jobID == "" {
		return false, fmt.Errorf("pgjobdb: job id is required")
	}

	const query = `
		SELECT EXISTS(SELECT 1 FROM pgjobdb.jobs_archive WHERE tenant_id = $1 AND job_id = $2)
	`

	var archived bool
	err := db.QueryRowContext(ctx, query, string(tenantID), string(jobID)).Scan(&archived)
	if err != nil {
		return false, annotateError(err)
	}

	return archived, nil
}

// FindJobs finds jobs matching specific criteria.
// This is useful for discovering work without leasing it (e.g., external task workers).
// Returns jobs sorted by AvailableAt ascending.
func FindJobs(ctx context.Context, db DB, opts FindJobsOptions) ([]JobInfo, error) {
	if db == nil {
		return nil, fmt.Errorf("pgjobdb: nil DB")
	}
	if ctx == nil {
		return nil, fmt.Errorf("pgjobdb: nil context")
	}
	if opts.NextNeed == "" {
		return nil, wrap(ErrInvalidOptions, fmt.Errorf("next_need is required"))
	}

	limit := opts.Limit
	if limit == 0 {
		limit = 100
	}
	if limit > 1000 {
		limit = 1000
	}

	// Build WHERE clause for tenant filtering
	var tenantFilter string
	var args []interface{}
	argIdx := 1

	if len(opts.TenantIDs) > 0 {
		tenantFilter = fmt.Sprintf("tenant_id = ANY($%d) AND ", argIdx)
		args = append(args, pq.Array(opts.TenantIDs))
		argIdx++
	}

	// Add status and next_need filters
	query := fmt.Sprintf(`
		SELECT
			tenant_id, job_id, status, next_need, wait_for, metadata,
			available_at,
			CASE WHEN expires_at = 'infinity' THEN NULL ELSE expires_at END,
			created_at,
			lease_id,
			CASE WHEN lease_expires_at = '-infinity' THEN NULL ELSE lease_expires_at END,
			lease_expiration_count, consecutive_expirations,
			cancel_requested, cancel_requested_by, cancel_requested_at
		FROM pgjobdb.jobs_with_status
		WHERE %sstatus = $%d AND next_need = $%d
		ORDER BY available_at ASC
		LIMIT $%d
	`, tenantFilter, argIdx, argIdx+1, argIdx+2)

	args = append(args, string(opts.Status), opts.NextNeed, limit)

	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, annotateError(err)
	}
	defer rows.Close()

	var jobs []JobInfo
	for rows.Next() {
		job, err := scanJobInfo(rows)
		if err != nil {
			return nil, annotateError(err)
		}
		jobs = append(jobs, *job)
	}

	if err := rows.Err(); err != nil {
		return nil, annotateError(err)
	}

	return jobs, nil
}

// scanJobInfo scans a row into JobInfo
func scanJobInfo(rows interface{ Scan(...interface{}) error }) (*JobInfo, error) {
	var job JobInfo
	var status string
	var waitFor pq.StringArray
	var metadata json.RawMessage
	var expiresAt sql.NullTime
	var leaseID sql.NullString
	var leaseExpiresAt sql.NullTime
	var cancelRequestedBy sql.NullString
	var cancelRequestedAt sql.NullTime

	err := rows.Scan(
		&job.TenantID,
		&job.JobID,
		&status,
		&job.NextNeed,
		(*pq.StringArray)(&waitFor),
		&metadata,
		&job.AvailableAt,
		&expiresAt,
		&job.CreatedAt,
		&leaseID,
		&leaseExpiresAt,
		&job.LeaseExpirationCount,
		&job.ConsecutiveExpirations,
		&job.CancelRequested,
		&cancelRequestedBy,
		&cancelRequestedAt,
	)
	if err != nil {
		return nil, err
	}

	job.Status = JobStatus(status)
	job.WaitFor = waitFor
	job.Metadata = metadata
	if expiresAt.Valid {
		job.ExpiresAt = &expiresAt.Time
	}
	if leaseID.Valid {
		job.LeaseID = &leaseID.String
	}
	if leaseExpiresAt.Valid {
		job.LeaseExpiresAt = &leaseExpiresAt.Time
	}
	if cancelRequestedBy.Valid {
		job.CancelRequestedBy = &cancelRequestedBy.String
	}
	if cancelRequestedAt.Valid {
		job.CancelRequestedAt = &cancelRequestedAt.Time
	}

	return &job, nil
}

// GetJobStatusBatch retrieves status for multiple jobs in a single query.
// Returns a map of jobID -> JobStatusInfo.
// Jobs that don't exist are omitted from the result.
func GetJobStatusBatch(ctx context.Context, db DB, tenantID TenantID, jobIDs []JobID) (map[string]*JobStatusInfo, error) {
	if db == nil {
		return nil, fmt.Errorf("pgjobdb: nil DB")
	}
	if ctx == nil {
		return nil, fmt.Errorf("pgjobdb: nil context")
	}
	if tenantID == "" {
		return nil, fmt.Errorf("pgjobdb: tenant id is required")
	}
	if len(jobIDs) == 0 {
		return make(map[string]*JobStatusInfo), nil
	}

	// Convert JobID slice to string slice
	jobIDStrings := make([]string, len(jobIDs))
	for i, id := range jobIDs {
		jobIDStrings[i] = string(id)
	}

	// Query active jobs
	const activeQuery = `
		SELECT
			tenant_id, job_id, status, next_need, wait_for, payload, metadata,
			available_at,
			CASE WHEN expires_at = 'infinity' THEN NULL ELSE expires_at END,
			created_at,
			NULL as archived_at,
			NULL as completion_status,
			NULL as completion_detail,
			lease_id,
			CASE WHEN lease_expires_at = '-infinity' THEN NULL ELSE lease_expires_at END,
			lease_expiration_count, consecutive_expirations,
			cancel_requested, cancel_requested_by, cancel_requested_at
		FROM pgjobdb.jobs_with_status
		WHERE tenant_id = $1 AND job_id = ANY($2)
	`

	result := make(map[string]*JobStatusInfo)

	rows, err := db.QueryContext(ctx, activeQuery, string(tenantID), pq.Array(jobIDStrings))
	if err != nil {
		return nil, annotateError(err)
	}
	defer rows.Close()

	for rows.Next() {
		info, err := scanJobStatusInfoRows(rows)
		if err != nil {
			return nil, annotateError(err)
		}
		result[info.JobID] = info
	}

	if err := rows.Err(); err != nil {
		return nil, annotateError(err)
	}

	// Query archived jobs for any not found in active
	const archiveQuery = `
		SELECT
			tenant_id, job_id, next_need, wait_for, payload, metadata,
			CASE WHEN expires_at = 'infinity' THEN NULL ELSE expires_at END,
			created_at, archived_at,
			completion_status, completion_detail,
			lease_id, lease_expiration_count, consecutive_expirations,
			cancel_requested, cancel_requested_by, cancel_requested_at
		FROM pgjobdb.jobs_archive
		WHERE tenant_id = $1 AND job_id = ANY($2)
	`

	rows, err = db.QueryContext(ctx, archiveQuery, string(tenantID), pq.Array(jobIDStrings))
	if err != nil {
		return nil, annotateError(err)
	}
	defer rows.Close()

	for rows.Next() {
		info, err := scanJobStatusInfoArchiveRows(rows)
		if err != nil {
			return nil, annotateError(err)
		}
		// Only add if not already found in active jobs
		if _, exists := result[info.JobID]; !exists {
			result[info.JobID] = info
		}
	}

	if err := rows.Err(); err != nil {
		return nil, annotateError(err)
	}

	return result, nil
}

// scanJobStatusInfoRows scans a row from jobs_with_status into JobStatusInfo
func scanJobStatusInfoRows(rows interface{ Scan(...interface{}) error }) (*JobStatusInfo, error) {
	var info JobStatusInfo
	var status string
	var waitFor pq.StringArray
	var expiresAt sql.NullTime
	var archivedAt sql.NullTime
	var completionStatus sql.NullString
	var completionDetail sql.NullString
	var leaseID sql.NullString
	var leaseExpiresAt sql.NullTime
	var cancelRequestedBy sql.NullString
	var cancelRequestedAt sql.NullTime

	err := rows.Scan(
		&info.TenantID,
		&info.JobID,
		&status,
		&info.NextNeed,
		(*pq.StringArray)(&waitFor),
		&info.Payload,
		&info.Metadata,
		&info.AvailableAt,
		&expiresAt,
		&info.CreatedAt,
		&archivedAt,
		&completionStatus,
		&completionDetail,
		&leaseID,
		&leaseExpiresAt,
		&info.LeaseExpirationCount,
		&info.ConsecutiveExpirations,
		&info.CancelRequested,
		&cancelRequestedBy,
		&cancelRequestedAt,
	)
	if err != nil {
		return nil, err
	}

	info.Status = JobStatus(status)
	info.WaitFor = waitFor
	if expiresAt.Valid {
		info.ExpiresAt = &expiresAt.Time
	}
	if archivedAt.Valid {
		info.ArchivedAt = &archivedAt.Time
	}
	if completionStatus.Valid && completionStatus.String != "" {
		cs := CompletionStatus(completionStatus.String)
		info.CompletionStatus = &cs
	}
	if completionDetail.Valid {
		info.CompletionDetail = &completionDetail.String
	}
	if leaseID.Valid {
		info.LeaseID = &leaseID.String
	}
	if leaseExpiresAt.Valid {
		info.LeaseExpiresAt = &leaseExpiresAt.Time
	}
	if cancelRequestedBy.Valid {
		info.CancelRequestedBy = &cancelRequestedBy.String
	}
	if cancelRequestedAt.Valid {
		info.CancelRequestedAt = &cancelRequestedAt.Time
	}

	return &info, nil
}

// scanJobStatusInfoArchiveRows scans a row from jobs_archive into JobStatusInfo
func scanJobStatusInfoArchiveRows(rows interface{ Scan(...interface{}) error }) (*JobStatusInfo, error) {
	var info JobStatusInfo
	var waitFor pq.StringArray
	var expiresAt sql.NullTime
	var archivedAt sql.NullTime
	var completionStatus sql.NullString
	var completionDetail sql.NullString
	var leaseID sql.NullString
	var cancelRequestedBy sql.NullString
	var cancelRequestedAt sql.NullTime

	err := rows.Scan(
		&info.TenantID,
		&info.JobID,
		&info.NextNeed,
		(*pq.StringArray)(&waitFor),
		&info.Payload,
		&info.Metadata,
		&expiresAt,
		&info.CreatedAt,
		&archivedAt,
		&completionStatus,
		&completionDetail,
		&leaseID,
		&info.LeaseExpirationCount,
		&info.ConsecutiveExpirations,
		&info.CancelRequested,
		&cancelRequestedBy,
		&cancelRequestedAt,
	)
	if err != nil {
		return nil, err
	}

	if completionStatus.Valid && completionStatus.String != "" {
		cs := CompletionStatus(completionStatus.String)
		info.CompletionStatus = &cs
		if cs == CompletionStatusCancelled {
			info.Status = JobStatusCancelled
		} else {
			info.Status = JobStatusCompleted
		}
	} else if info.CancelRequested {
		info.Status = JobStatusCancelled
	} else {
		info.Status = JobStatusCompleted
	}

	info.WaitFor = waitFor
	if expiresAt.Valid {
		info.ExpiresAt = &expiresAt.Time
	}
	if archivedAt.Valid {
		info.ArchivedAt = &archivedAt.Time
	}
	if completionDetail.Valid {
		info.CompletionDetail = &completionDetail.String
	}
	if leaseID.Valid {
		info.LeaseID = &leaseID.String
	}
	if cancelRequestedBy.Valid {
		info.CancelRequestedBy = &cancelRequestedBy.String
	}
	if cancelRequestedAt.Valid {
		info.CancelRequestedAt = &cancelRequestedAt.Time
	}

	// Archived jobs: AvailableAt not stored, use CreatedAt as fallback
	info.AvailableAt = info.CreatedAt
	// Archived jobs have no active lease, leave LeaseExpiresAt as nil

	return &info, nil
}

// paginationCursor encodes the position in result set for stateless pagination
type paginationCursor struct {
	LastSortValue string        `json:"last_sort_value"` // Last value of sort field from previous page
	LastJobID     string        `json:"last_job_id"`     // Last job_id from previous page
	QueryHash     string        `json:"query_hash"`      // Hash of query parameters for validation
	SortBy        SortField     `json:"sort_by"`
	SortOrder     SortDirection `json:"sort_order"`
}

// encodeCursor encodes a cursor to an opaque base64 string
func encodeCursor(lastJob *JobListItem, opts ListJobsOptions) (string, error) {
	if lastJob == nil {
		return "", nil
	}

	cursor := paginationCursor{
		LastJobID: lastJob.JobID,
		SortBy:    opts.SortBy,
		SortOrder: opts.SortOrder,
		QueryHash: hashListJobsOptions(opts),
	}

	// Extract sort field value based on SortBy
	switch opts.SortBy {
	case SortByCreatedAt:
		cursor.LastSortValue = lastJob.CreatedAt.Format(time.RFC3339Nano)
	case SortByAvailableAt:
		cursor.LastSortValue = lastJob.AvailableAt.Format(time.RFC3339Nano)
	case SortByJobID:
		cursor.LastSortValue = lastJob.JobID
	default:
		cursor.LastSortValue = lastJob.CreatedAt.Format(time.RFC3339Nano)
	}

	// Encode to opaque string
	data, err := json.Marshal(cursor)
	if err != nil {
		return "", fmt.Errorf("failed to encode cursor: %w", err)
	}
	return base64.URLEncoding.EncodeToString(data), nil
}

// decodeCursor decodes and validates a cursor string
func decodeCursor(cursorStr string, opts ListJobsOptions) (*paginationCursor, error) {
	if cursorStr == "" {
		return nil, nil
	}

	data, err := base64.URLEncoding.DecodeString(cursorStr)
	if err != nil {
		return nil, wrap(ErrInvalidCursor, fmt.Errorf("failed to decode cursor: %w", err))
	}

	var cursor paginationCursor
	if err := json.Unmarshal(data, &cursor); err != nil {
		return nil, wrap(ErrInvalidCursor, fmt.Errorf("failed to unmarshal cursor: %w", err))
	}

	// Validate cursor matches current query parameters
	currentHash := hashListJobsOptions(opts)
	if cursor.QueryHash != currentHash {
		return nil, wrap(ErrInvalidCursor, fmt.Errorf("cursor does not match current query parameters"))
	}

	// Validate sort parameters match
	if cursor.SortBy != opts.SortBy || cursor.SortOrder != opts.SortOrder {
		return nil, wrap(ErrInvalidCursor, fmt.Errorf("cursor sort parameters do not match current query"))
	}

	return &cursor, nil
}

type normalizedMetadataPredicate struct {
	Path       []string
	ValuesJSON []string
}

func normalizeMetadataPredicates(predicates []MetadataPredicate) ([]normalizedMetadataPredicate, error) {
	if len(predicates) == 0 {
		return nil, nil
	}
	normalized := make([]normalizedMetadataPredicate, 0, len(predicates))
	for i, predicate := range predicates {
		if len(predicate.Path) == 0 {
			return nil, wrap(ErrInvalidOptions, fmt.Errorf("metadata predicate %d path is required", i))
		}
		for _, segment := range predicate.Path {
			if segment == "" {
				return nil, wrap(ErrInvalidOptions, fmt.Errorf("metadata predicate %d path contains empty segment", i))
			}
		}
		if len(predicate.Values) == 0 {
			return nil, wrap(ErrInvalidOptions, fmt.Errorf("metadata predicate %d values are required", i))
		}
		valuesJSON := make([]string, 0, len(predicate.Values))
		for _, value := range predicate.Values {
			if value == nil {
				return nil, wrap(ErrInvalidOptions, fmt.Errorf("metadata predicate %d values cannot contain nil", i))
			}
			valueJSON, err := encodeMetadataPredicateValue(value)
			if err != nil {
				return nil, wrap(ErrInvalidOptions, err)
			}
			valuesJSON = append(valuesJSON, valueJSON)
		}
		normalized = append(normalized, normalizedMetadataPredicate{
			Path:       predicate.Path,
			ValuesJSON: valuesJSON,
		})
	}
	return normalized, nil
}

func encodeMetadataPredicateValue(value any) (string, error) {
	switch v := value.(type) {
	case json.RawMessage:
		if !json.Valid(v) {
			return "", fmt.Errorf("metadata predicate value must be valid JSON")
		}
		return string(v), nil
	case []byte:
		if !json.Valid(v) {
			return "", fmt.Errorf("metadata predicate value must be valid JSON")
		}
		return string(v), nil
	default:
		encoded, err := json.Marshal(v)
		if err != nil {
			return "", fmt.Errorf("metadata predicate value must be JSON-serializable: %w", err)
		}
		return string(encoded), nil
	}
}

// hashListJobsOptions creates a hash of the query parameters to validate cursor consistency
func hashListJobsOptions(opts ListJobsOptions) string {
	h := sha256.New()

	// Include all filter parameters that affect query results
	// Note: We don't include Limit or Cursor as those are pagination controls
	fmt.Fprintf(h, "tenant_id:%s;", opts.TenantID)

	// Include TenantIDs if present
	for _, tid := range opts.TenantIDs {
		fmt.Fprintf(h, "tid:%s;", tid)
	}

	for _, status := range opts.Statuses {
		fmt.Fprintf(h, "status:%s;", status)
	}

	fmt.Fprintf(h, "job_type_pattern:%s;", opts.JobTypePattern)

	// Include JobTypePatterns if present
	for _, pattern := range opts.JobTypePatterns {
		fmt.Fprintf(h, "jtp:%s;", pattern)
	}

	fmt.Fprintf(h, "include_archived:%t;", opts.IncludeArchived)
	for _, status := range opts.CompletionStatuses {
		fmt.Fprintf(h, "completion_status:%s;", status)
	}
	for _, status := range opts.CompletionStatuses {
		fmt.Fprintf(h, "completion_status:%s;", status)
	}

	if opts.CreatedAfter != nil {
		fmt.Fprintf(h, "created_after:%s;", opts.CreatedAfter.Format(time.RFC3339Nano))
	}
	if opts.CreatedBefore != nil {
		fmt.Fprintf(h, "created_before:%s;", opts.CreatedBefore.Format(time.RFC3339Nano))
	}

	for _, predicate := range opts.MetadataEquals {
		fmt.Fprintf(h, "metadata_path_len:%d;", len(predicate.Path))
		for _, segment := range predicate.Path {
			fmt.Fprintf(h, "metadata_path:%s;", segment)
		}
		for _, value := range predicate.Values {
			if valueJSON, err := encodeMetadataPredicateValue(value); err == nil {
				fmt.Fprintf(h, "metadata_value:%s;", valueJSON)
			} else {
				fmt.Fprintf(h, "metadata_value_error:%s;", err.Error())
			}
		}
	}

	// Sort parameters are validated separately, so we include them
	fmt.Fprintf(h, "sort_by:%s;sort_order:%s", opts.SortBy, opts.SortOrder)

	return fmt.Sprintf("%x", h.Sum(nil))
}

func normalizeCompletionStatuses(statuses []CompletionStatus) []string {
	if len(statuses) == 0 {
		return nil
	}
	out := make([]string, 0, len(statuses))
	for _, status := range statuses {
		if status == "" {
			continue
		}
		out = append(out, string(status))
	}
	return out
}

// buildCursorCondition builds a WHERE clause condition for cursor-based pagination
// Returns the condition string and the args to append
func buildCursorCondition(cursor *paginationCursor, sortBy SortField, sortOrder SortDirection, argIdx int) (string, []interface{}) {
	if cursor == nil {
		return "", nil
	}

	var args []interface{}

	// Parse the last sort value back to appropriate type
	var lastSortValue interface{}
	switch sortBy {
	case SortByCreatedAt, SortByAvailableAt:
		// Parse timestamp
		t, err := time.Parse(time.RFC3339Nano, cursor.LastSortValue)
		if err != nil {
			// If parsing fails, return empty condition to avoid errors
			return "", nil
		}
		lastSortValue = t
	case SortByJobID:
		lastSortValue = cursor.LastSortValue
	default:
		// Default to created_at
		t, err := time.Parse(time.RFC3339Nano, cursor.LastSortValue)
		if err != nil {
			return "", nil
		}
		lastSortValue = t
	}

	// Build row comparison condition
	// For DESC: WHERE (sort_field, job_id) < (cursor_value, cursor_job_id)
	// For ASC:  WHERE (sort_field, job_id) > (cursor_value, cursor_job_id)
	sortField := string(sortBy)
	operator := ">"
	if sortOrder == SortDesc {
		operator = "<"
	}

	condition := fmt.Sprintf("(%s, job_id) %s ($%d, $%d)", sortField, operator, argIdx, argIdx+1)
	args = append(args, lastSortValue, cursor.LastJobID)

	return condition, args
}

// joinStrings joins strings with a separator
func joinStrings(strs []string, sep string) string {
	if len(strs) == 0 {
		return ""
	}
	result := strs[0]
	for i := 1; i < len(strs); i++ {
		result += sep + strs[i]
	}
	return result
}

// ListJobs queries jobs with filtering and pagination.
// The cursor returned in ListJobsResult can be used for the next page.
func ListJobs(ctx context.Context, db DB, opts ListJobsOptions) (*ListJobsResult, error) {
	if db == nil {
		return nil, fmt.Errorf("pgjobdb: nil DB")
	}
	if ctx == nil {
		return nil, fmt.Errorf("pgjobdb: nil context")
	}

	// Determine which tenant filter to use
	tenantIDs := opts.TenantIDs
	if len(tenantIDs) == 0 && opts.TenantID != "" {
		// Backwards compatibility: use TenantID if TenantIDs is empty
		tenantIDs = []string{opts.TenantID}
	}
	if len(tenantIDs) == 0 {
		return nil, wrap(ErrInvalidOptions, fmt.Errorf("tenant_id or tenant_ids is required"))
	}

	if len(opts.CompletionStatuses) > 0 && !opts.IncludeArchived {
		return nil, wrap(ErrInvalidOptions, fmt.Errorf("completion_status filter requires include_archived"))
	}

	// Determine which job type filter to use
	jobTypePatterns := opts.JobTypePatterns
	if len(jobTypePatterns) == 0 && opts.JobTypePattern != "" {
		// Backwards compatibility: use JobTypePattern if JobTypePatterns is empty
		jobTypePatterns = []string{opts.JobTypePattern}
	}

	limit := opts.Limit
	if limit == 0 {
		limit = 100
	}
	if limit > 1000 {
		limit = 1000
	}

	sortBy := opts.SortBy
	if sortBy == "" {
		sortBy = SortByCreatedAt
	}

	sortOrder := opts.SortOrder
	if sortOrder == "" {
		sortOrder = SortDesc
	}

	// Update opts with normalized values for cursor hashing
	opts.SortBy = sortBy
	opts.SortOrder = sortOrder
	opts.TenantIDs = tenantIDs
	opts.JobTypePatterns = jobTypePatterns

	metadataPredicates, err := normalizeMetadataPredicates(opts.MetadataEquals)
	if err != nil {
		return nil, err
	}
	completionStatusStrings := normalizeCompletionStatuses(opts.CompletionStatuses)

	// Decode cursor if present
	cursor, err := decodeCursor(opts.Cursor, opts)
	if err != nil {
		return nil, err
	}

	// Build WHERE clause
	var conditions []string
	var args []interface{}
	argIdx := 1

	// Multi-tenant filter
	conditions = append(conditions, fmt.Sprintf("tenant_id = ANY($%d)", argIdx))
	args = append(args, pq.Array(tenantIDs))
	argIdx++

	// Status filter
	if len(opts.Statuses) > 0 {
		statusStrings := make([]string, len(opts.Statuses))
		for i, s := range opts.Statuses {
			statusStrings[i] = string(s)
		}
		conditions = append(conditions, fmt.Sprintf("status = ANY($%d)", argIdx))
		args = append(args, pq.Array(statusStrings))
		argIdx++
	}

	// Multi-pattern job type filter
	if len(jobTypePatterns) > 0 {
		patterns := make([]string, len(jobTypePatterns))
		for i := 0; i < len(jobTypePatterns); i++ {
			patterns[i] = fmt.Sprintf("next_need LIKE $%d", argIdx)
			args = append(args, jobTypePatterns[i])
			argIdx++
		}
		conditions = append(conditions, "("+joinStrings(patterns, " OR ")+")")
	}

	// Created after filter
	if opts.CreatedAfter != nil {
		conditions = append(conditions, fmt.Sprintf("created_at > $%d", argIdx))
		args = append(args, *opts.CreatedAfter)
		argIdx++
	}

	// Created before filter
	if opts.CreatedBefore != nil {
		conditions = append(conditions, fmt.Sprintf("created_at < $%d", argIdx))
		args = append(args, *opts.CreatedBefore)
		argIdx++
	}

	for _, predicate := range metadataPredicates {
		conditions = append(conditions, fmt.Sprintf("metadata #> $%d = ANY($%d::jsonb[])", argIdx, argIdx+1))
		args = append(args, pq.Array(predicate.Path), pq.Array(predicate.ValuesJSON))
		argIdx += 2
	}

	// Add cursor-based pagination condition
	if cursor != nil {
		cursorCondition, cursorArgs := buildCursorCondition(cursor, sortBy, sortOrder, argIdx)
		if cursorCondition != "" {
			conditions = append(conditions, cursorCondition)
			args = append(args, cursorArgs...)
			argIdx += len(cursorArgs)
		}
	}

	whereClauseActive := "WHERE " + conditions[0]
	for i := 1; i < len(conditions); i++ {
		whereClauseActive += " AND " + conditions[i]
	}
	if len(completionStatusStrings) > 0 {
		whereClauseActive = "WHERE FALSE"
	}

	// Build query - query active jobs only by default unless IncludeArchived is true
	var query string
	// Include job_id in ORDER BY for stable pagination (tie-breaking)
	orderByClause := fmt.Sprintf("%s %s, job_id %s", sortBy, sortOrder, sortOrder)

	if opts.IncludeArchived {
		archiveConditions := append([]string{}, conditions...)
		archiveArgs := append([]interface{}{}, args...)
		if len(completionStatusStrings) > 0 {
			archiveConditions = append(archiveConditions, fmt.Sprintf("completion_status = ANY($%d)", len(archiveArgs)+1))
			archiveArgs = append(archiveArgs, pq.Array(completionStatusStrings))
		}
		whereClauseArchive := "WHERE " + archiveConditions[0]
		for i := 1; i < len(archiveConditions); i++ {
			whereClauseArchive += " AND " + archiveConditions[i]
		}

		// Query both active and archived with UNION
		query = fmt.Sprintf(`
			SELECT
				tenant_id, job_id, status, next_need, wait_for, metadata,
				available_at,
				CASE WHEN expires_at = 'infinity' THEN NULL ELSE expires_at END,
				created_at,
				NULL as archived_at,
				NULL as completion_status,
				NULL as completion_detail,
				lease_id,
				CASE WHEN lease_expires_at = '-infinity' THEN NULL ELSE lease_expires_at END,
				lease_expiration_count, consecutive_expirations,
				cancel_requested, cancel_requested_by, cancel_requested_at
			FROM pgjobdb.jobs_with_status
			%s
			UNION ALL
			SELECT
				tenant_id, job_id,
				CASE WHEN completion_status = 'cancelled' THEN 'CANCELLED' ELSE 'COMPLETED' END as status,
				next_need, wait_for, metadata,
				created_at as available_at,
				CASE WHEN expires_at = 'infinity' THEN NULL ELSE expires_at END,
				created_at,
				archived_at,
				completion_status, completion_detail,
				lease_id,
				NULL as lease_expires_at,
				lease_expiration_count, consecutive_expirations,
				cancel_requested, cancel_requested_by, cancel_requested_at
			FROM pgjobdb.jobs_archive
			%s
			ORDER BY %s
			LIMIT $%d
		`, whereClauseActive, whereClauseArchive, orderByClause, len(archiveArgs)+1)
		args = archiveArgs
	} else {
		// Query only active jobs
		query = fmt.Sprintf(`
			SELECT
				tenant_id, job_id, status, next_need, wait_for, metadata,
				available_at,
				CASE WHEN expires_at = 'infinity' THEN NULL ELSE expires_at END,
				created_at,
				NULL as archived_at,
				NULL as completion_status,
				NULL as completion_detail,
				lease_id,
				CASE WHEN lease_expires_at = '-infinity' THEN NULL ELSE lease_expires_at END,
				lease_expiration_count, consecutive_expirations,
				cancel_requested, cancel_requested_by, cancel_requested_at
			FROM pgjobdb.jobs_with_status
			%s
			ORDER BY %s
			LIMIT $%d
		`, whereClauseActive, orderByClause, argIdx)
	}

	args = append(args, limit+1) // Fetch one extra to determine if there are more results

	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, annotateError(err)
	}
	defer rows.Close()

	var jobs []JobListItem
	for rows.Next() {
		job, err := scanJobListItem(rows)
		if err != nil {
			return nil, annotateError(err)
		}
		jobs = append(jobs, *job)
	}

	if err := rows.Err(); err != nil {
		return nil, annotateError(err)
	}

	// Determine if there are more results
	hasMore := len(jobs) > limit
	if hasMore {
		jobs = jobs[:limit] // Remove the extra row
	}

	result := &ListJobsResult{
		Jobs:    jobs,
		HasMore: hasMore,
	}

	// Generate next cursor if there are more results
	if hasMore && len(jobs) > 0 {
		lastJob := &jobs[len(jobs)-1]
		nextCursor, err := encodeCursor(lastJob, opts)
		if err != nil {
			return nil, fmt.Errorf("failed to encode next cursor: %w", err)
		}
		result.NextCursor = nextCursor
	}

	return result, nil
}

// scanJobListItem scans a row into JobListItem
func scanJobListItem(rows interface{ Scan(...interface{}) error }) (*JobListItem, error) {
	var job JobListItem
	var status string
	var waitFor pq.StringArray
	var metadata json.RawMessage
	var expiresAt sql.NullTime
	var archivedAt sql.NullTime
	var completionStatus sql.NullString
	var completionDetail sql.NullString
	var leaseID sql.NullString
	var leaseExpiresAt sql.NullTime
	var cancelRequestedBy sql.NullString
	var cancelRequestedAt sql.NullTime

	err := rows.Scan(
		&job.TenantID,
		&job.JobID,
		&status,
		&job.NextNeed,
		(*pq.StringArray)(&waitFor),
		&metadata,
		&job.AvailableAt,
		&expiresAt,
		&job.CreatedAt,
		&archivedAt,
		&completionStatus,
		&completionDetail,
		&leaseID,
		&leaseExpiresAt,
		&job.LeaseExpirationCount,
		&job.ConsecutiveExpirations,
		&job.CancelRequested,
		&cancelRequestedBy,
		&cancelRequestedAt,
	)
	if err != nil {
		return nil, err
	}

	job.Status = JobStatus(status)
	job.WaitFor = waitFor
	job.Metadata = metadata
	if expiresAt.Valid {
		job.ExpiresAt = &expiresAt.Time
	}
	if archivedAt.Valid {
		job.ArchivedAt = &archivedAt.Time
	}
	if completionStatus.Valid && completionStatus.String != "" {
		cs := CompletionStatus(completionStatus.String)
		job.CompletionStatus = &cs
	}
	if completionDetail.Valid {
		job.CompletionDetail = &completionDetail.String
	}
	if leaseID.Valid {
		job.LeaseID = &leaseID.String
	}
	if leaseExpiresAt.Valid {
		job.LeaseExpiresAt = &leaseExpiresAt.Time
	}
	if cancelRequestedBy.Valid {
		job.CancelRequestedBy = &cancelRequestedBy.String
	}
	if cancelRequestedAt.Valid {
		job.CancelRequestedAt = &cancelRequestedAt.Time
	}

	return &job, nil
}

// ListArchivedJobs queries only archived jobs with pagination.
// This is more efficient than ListJobs when you only need completed jobs.
func ListArchivedJobs(ctx context.Context, db DB, opts ListArchivedJobsOptions) (*ListJobsResult, error) {
	if db == nil {
		return nil, fmt.Errorf("pgjobdb: nil DB")
	}
	if ctx == nil {
		return nil, fmt.Errorf("pgjobdb: nil context")
	}
	if opts.TenantID == "" {
		return nil, wrap(ErrInvalidOptions, fmt.Errorf("tenant_id is required"))
	}

	limit := opts.Limit
	if limit == 0 {
		limit = 100
	}
	if limit > 1000 {
		limit = 1000
	}

	completionStatusStrings := normalizeCompletionStatuses(opts.CompletionStatuses)

	// Build WHERE clause
	var conditions []string
	var args []interface{}
	argIdx := 1

	// Always filter by tenant
	conditions = append(conditions, fmt.Sprintf("tenant_id = $%d", argIdx))
	args = append(args, opts.TenantID)
	argIdx++

	// Job type pattern filter
	if opts.JobTypePattern != "" {
		conditions = append(conditions, fmt.Sprintf("next_need LIKE $%d", argIdx))
		args = append(args, opts.JobTypePattern)
		argIdx++
	}

	if len(completionStatusStrings) > 0 {
		conditions = append(conditions, fmt.Sprintf("completion_status = ANY($%d)", argIdx))
		args = append(args, pq.Array(completionStatusStrings))
		argIdx++
	}

	// Archived after filter
	if opts.ArchivedAfter != nil {
		conditions = append(conditions, fmt.Sprintf("archived_at > $%d", argIdx))
		args = append(args, *opts.ArchivedAfter)
		argIdx++
	}

	// Archived before filter
	if opts.ArchivedBefore != nil {
		conditions = append(conditions, fmt.Sprintf("archived_at < $%d", argIdx))
		args = append(args, *opts.ArchivedBefore)
		argIdx++
	}

	whereClause := "WHERE " + conditions[0]
	for i := 1; i < len(conditions); i++ {
		whereClause += " AND " + conditions[i]
	}

	query := fmt.Sprintf(`
		SELECT
			tenant_id, job_id,
			CASE WHEN completion_status = 'cancelled' THEN 'CANCELLED' ELSE 'COMPLETED' END as status,
			next_need, wait_for, metadata,
			created_at as available_at,
			CASE WHEN expires_at = 'infinity' THEN NULL ELSE expires_at END,
			created_at, archived_at,
			completion_status, completion_detail,
			lease_id, lease_expiration_count, consecutive_expirations,
			cancel_requested, cancel_requested_by, cancel_requested_at
		FROM pgjobdb.jobs_archive
		%s
		ORDER BY archived_at DESC
		LIMIT $%d
	`, whereClause, argIdx)

	args = append(args, limit+1) // Fetch one extra to determine if there are more results

	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, annotateError(err)
	}
	defer rows.Close()

	var jobs []JobListItem
	for rows.Next() {
		job, err := scanJobListItemArchive(rows)
		if err != nil {
			return nil, annotateError(err)
		}
		jobs = append(jobs, *job)
	}

	if err := rows.Err(); err != nil {
		return nil, annotateError(err)
	}

	// Determine if there are more results
	hasMore := len(jobs) > limit
	if hasMore {
		jobs = jobs[:limit]
	}

	result := &ListJobsResult{
		Jobs:    jobs,
		HasMore: hasMore,
	}

	if hasMore {
		result.NextCursor = "next-page" // Placeholder
	}

	return result, nil
}

// scanJobListItemArchive scans a row from jobs_archive into JobListItem
func scanJobListItemArchive(rows interface{ Scan(...interface{}) error }) (*JobListItem, error) {
	var job JobListItem
	var status string
	var waitFor pq.StringArray
	var metadata json.RawMessage
	var expiresAt sql.NullTime
	var completionStatus sql.NullString
	var completionDetail sql.NullString
	var leaseID sql.NullString
	var cancelRequestedBy sql.NullString
	var cancelRequestedAt sql.NullTime

	err := rows.Scan(
		&job.TenantID,
		&job.JobID,
		&status,
		&job.NextNeed,
		(*pq.StringArray)(&waitFor),
		&metadata,
		&job.AvailableAt,
		&expiresAt,
		&job.CreatedAt,
		&job.ArchivedAt,
		&completionStatus,
		&completionDetail,
		&leaseID,
		&job.LeaseExpirationCount,
		&job.ConsecutiveExpirations,
		&job.CancelRequested,
		&cancelRequestedBy,
		&cancelRequestedAt,
	)
	if err != nil {
		return nil, err
	}

	job.Status = JobStatus(status)
	job.WaitFor = waitFor
	job.Metadata = metadata
	if expiresAt.Valid {
		job.ExpiresAt = &expiresAt.Time
	}
	if leaseID.Valid {
		job.LeaseID = &leaseID.String
	}
	if completionStatus.Valid && completionStatus.String != "" {
		cs := CompletionStatus(completionStatus.String)
		job.CompletionStatus = &cs
	}
	if completionDetail.Valid {
		job.CompletionDetail = &completionDetail.String
	}
	if cancelRequestedBy.Valid {
		job.CancelRequestedBy = &cancelRequestedBy.String
	}
	if cancelRequestedAt.Valid {
		job.CancelRequestedAt = &cancelRequestedAt.Time
	}

	// Archived jobs have no active lease, leave LeaseExpiresAt as nil

	return &job, nil
}

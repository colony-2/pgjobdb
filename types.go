package pgjobdb

import (
	"context"
	"database/sql"
	"encoding/json"
	"time"
)

// DB is the database surface required by pgjobdb's typed operations.
type DB interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

type TenantID string
type JobID string
type WorkerID string
type JobType string

type RetryPolicy struct {
	InitialIntervalMillis  int64    `json:"initial_interval_millis,omitempty"`
	BackoffCoefficient     float64  `json:"backoff_coefficient,omitempty"`
	MaximumIntervalMillis  int64    `json:"maximum_interval_millis,omitempty"`
	MaximumAttempts        int32    `json:"maximum_attempts,omitempty"`
	NonRetryableErrorTypes []string `json:"non_retryable_error_types,omitempty"`
}

type RunPolicy struct {
	Retry                   RetryPolicy `json:"retry,omitempty"`
	InvocationTimeoutMillis *int64      `json:"invocation_timeout_millis,omitempty"`
	TotalTimeoutMillis      *int64      `json:"total_timeout_millis,omitempty"`
}

type ScheduleOccurrence struct {
	ScheduleID     string          `json:"schedule_id"`
	Generation     int64           `json:"generation"`
	SpecHash       string          `json:"spec_hash"`
	ScheduledAt    time.Time       `json:"scheduled_at"`
	RunID          string          `json:"run_id"`
	Reason         string          `json:"reason,omitempty"`
	Manual         bool            `json:"manual,omitempty"`
	BackfillID     string          `json:"backfill_id,omitempty"`
	PreviousJobID  string          `json:"previous_job_id,omitempty"`
	FailureHistory json.RawMessage `json:"failure_history,omitempty"`
}

type RuntimeMetadata struct {
	SchemaHash  string
	ParentJobID JobID
	Schedule    *ScheduleOccurrence
}

type SubmitJobRequest struct {
	TenantID     TenantID
	JobID        JobID
	WorkerID     WorkerID
	JobType      JobType
	RunPolicy    RunPolicy
	AppMetadata  json.RawMessage
	Runtime      RuntimeMetadata
	WaitFor      []JobID
	LeasePayload json.RawMessage
	AvailableAt  *time.Time
	ExpiresAt    *time.Time
}

type SubmitJobResult struct {
	JobID   JobID
	Created bool
}

type TaskType string

type WorkKind string

const (
	WorkKindJob  WorkKind = "JOB"
	WorkKindTask WorkKind = "TASK"
)

type TaskSelector struct {
	JobType  JobType  `json:"job_type"`
	TaskType TaskType `json:"task_type"`
}

type WorkSelector struct {
	JobTypes []JobType
	Tasks    []TaskSelector
}

type GetWorkOptions struct {
	TenantIDs           []TenantID
	AppMetadataContains json.RawMessage
	LeaseDuration       time.Duration
}

type GetJobLeaseOptions struct {
	AppMetadataContains json.RawMessage
	LeaseDuration       time.Duration
}

type TaskWork struct {
	TaskType      TaskType
	ResumeJobType JobType
	InputOrdinal  int64
	OutputOrdinal int64
	InputHash     string
}

type JobLease struct {
	TenantID     TenantID
	JobID        JobID
	LeaseID      string
	WorkerID     WorkerID
	ExpiresAt    time.Time
	JobType      JobType
	RouteJobType JobType
	WorkKind     WorkKind
	Task         *TaskWork
	RunPolicy    RunPolicy
	LeasePayload json.RawMessage
	SchemaHash   string
}

type LeaseIdentity struct {
	TenantID TenantID
	JobID    JobID
	LeaseID  string
	WorkerID WorkerID
}

type CompletionStatus string

const (
	CompletionSuccess       CompletionStatus = "success"
	CompletionFailedApp     CompletionStatus = "failed_app"
	CompletionFailedSystem  CompletionStatus = "failed_system"
	CompletionFailedTimeout CompletionStatus = "failed_timeout"
	CompletionCancelled     CompletionStatus = "cancelled"
)

type Completion struct {
	Status    CompletionStatus
	Detail    string
	ErrorKind string
	Retryable *bool
}

type AlternateRoute struct {
	JobType  JobType
	TaskType TaskType
	After    time.Duration
}

type RescheduleRequest struct {
	RouteJobType JobType
	WorkKind     WorkKind
	Task         *TaskWork
	WaitFor      []JobID
	AvailableAt  *time.Time
	LeasePayload json.RawMessage
	Alternate    *AlternateRoute
}

type CompleteTaskWorkRequest struct {
	TenantID     TenantID
	JobID        JobID
	WorkerID     WorkerID
	JobType      JobType
	Task         TaskWork
	LeasePayload json.RawMessage
}

type JobStore string

const (
	JobStoreActive   JobStore = "ACTIVE"
	JobStoreArchived JobStore = "ARCHIVED"
)

type JobStatus string

const (
	JobStatusReady          JobStatus = "READY"
	JobStatusExpired        JobStatus = "EXPIRED"
	JobStatusPendingJobs    JobStatus = "PENDING_JOBS"
	JobStatusAwaitingFuture JobStatus = "AWAITING_FUTURE"
	JobStatusActive         JobStatus = "ACTIVE"
	JobStatusCrashConcern   JobStatus = "CRASH_CONCERN"
	JobStatusCancelled      JobStatus = "CANCELLED"
	JobStatusCompleted      JobStatus = "COMPLETED"
)

// JobDetail joins immutable job facts with current or final scheduler state.
type JobDetail struct {
	TenantID               TenantID          `json:"tenant_id"`
	JobID                  JobID             `json:"job_id"`
	Store                  JobStore          `json:"store"`
	Status                 JobStatus         `json:"status"`
	JobType                JobType           `json:"job_type"`
	RouteJobType           JobType           `json:"route_job_type"`
	WorkKind               WorkKind          `json:"work_kind"`
	TaskType               TaskType          `json:"task_type"`
	ResumeJobType          JobType           `json:"resume_job_type"`
	TaskInputOrdinal       *int64            `json:"task_input_ordinal"`
	TaskOutputOrdinal      *int64            `json:"task_output_ordinal"`
	TaskInputHash          string            `json:"task_input_hash"`
	AlternateJobType       JobType           `json:"alternate_job_type"`
	AlternateTaskType      TaskType          `json:"alternate_task_type"`
	AlternateAfterSeconds  *int32            `json:"alternate_after_seconds"`
	WaitFor                []JobID           `json:"wait_for"`
	AvailableAt            time.Time         `json:"available_at"`
	LeaseExpiresAt         *time.Time        `json:"lease_expires_at"`
	LeaseWorkerID          WorkerID          `json:"lease_worker_id"`
	CancelRequested        bool              `json:"cancel_requested"`
	LeasePayload           json.RawMessage   `json:"lease_payload"`
	RunPolicy              RunPolicy         `json:"run_policy"`
	AppMetadata            json.RawMessage   `json:"app_metadata"`
	SchemaHash             string            `json:"schema_hash"`
	ParentJobID            JobID             `json:"parent_job_id"`
	CreatedAt              time.Time         `json:"created_at"`
	ExpiresAt              *time.Time        `json:"expires_at"`
	ArchivedAt             *time.Time        `json:"archived_at"`
	CompletionStatus       *CompletionStatus `json:"completion_status"`
	CompletionDetail       *string           `json:"completion_detail"`
	CompletionErrorKind    *string           `json:"completion_error_kind"`
	CompletionRetryable    *bool             `json:"completion_retryable"`
	ScheduleID             string            `json:"schedule_id"`
	ScheduleGeneration     *int64            `json:"schedule_generation"`
	ScheduleSpecHash       string            `json:"schedule_spec_hash"`
	ScheduledAt            *time.Time        `json:"scheduled_at"`
	ScheduleRunID          string            `json:"schedule_run_id"`
	ScheduleReason         string            `json:"schedule_reason"`
	ScheduleManual         bool              `json:"schedule_manual"`
	ScheduleBackfillID     string            `json:"schedule_backfill_id"`
	SchedulePreviousJobID  JobID             `json:"schedule_previous_job_id"`
	ScheduleFailureHistory json.RawMessage   `json:"schedule_failure_history"`
}

type JobStatusInfo struct {
	TenantID         TenantID          `json:"tenant_id"`
	JobID            JobID             `json:"job_id"`
	Store            JobStore          `json:"store"`
	Status           JobStatus         `json:"status"`
	JobType          JobType           `json:"job_type"`
	CreatedAt        time.Time         `json:"created_at"`
	ArchivedAt       *time.Time        `json:"archived_at"`
	CompletionStatus *CompletionStatus `json:"completion_status"`
	CompletionDetail *string           `json:"completion_detail"`
	CancelRequested  bool              `json:"cancel_requested"`
}

type JobKey struct {
	TenantID TenantID `json:"tenant_id"`
	JobID    JobID    `json:"job_id"`
}

type MetadataPredicate struct {
	Path   []string          `json:"path"`
	Values []json.RawMessage `json:"values"`
}

type ListJobsOptions struct {
	TenantIDs          []TenantID
	Statuses           []JobStatus
	Stores             []JobStore
	JobTypes           []JobType
	TaskSelectors      []TaskSelector
	JobKeys            []JobKey
	ParentJobIDs       []JobID
	RootOnly           bool
	MetadataPredicates []MetadataPredicate
	CreatedAfter       *time.Time
	CreatedBefore      *time.Time
	PageSize           int
	PageToken          string
}

type ListJobsResult struct {
	Jobs          []JobDetail
	NextPageToken string
}

type ListScheduleRunsOptions struct {
	TenantID        TenantID
	ScheduleID      string
	ScheduledAfter  *time.Time
	ScheduledBefore *time.Time
	Statuses        []JobStatus
	PageSize        int
	PageToken       string
}

type ListScheduleRunsResult struct {
	Runs          []JobDetail
	NextPageToken string
}

type ScheduleState string

const (
	ScheduleStateActive   ScheduleState = "ACTIVE"
	ScheduleStatePaused   ScheduleState = "PAUSED"
	ScheduleStateArchived ScheduleState = "ARCHIVED"
)

// Schedule is the durable definition and control state of a JobDB schedule.
// TargetSnapshot is opaque to pgjobdb; JobDB core owns its encoding.
type Schedule struct {
	TenantID       TenantID
	ScheduleID     string
	State          ScheduleState
	Generation     int64
	SpecHash       string
	Trigger        json.RawMessage
	TargetJobType  JobType
	TargetSnapshot json.RawMessage
	OverlapPolicy  string
	FailurePolicy  json.RawMessage
	NextFireAt     *time.Time
	NextJobID      *JobID
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

type UpsertScheduleRequest struct {
	TenantID           TenantID
	ScheduleID         string
	State              ScheduleState
	SpecHash           string
	Trigger            json.RawMessage
	TargetJobType      JobType
	TargetSnapshot     json.RawMessage
	OverlapPolicy      string
	FailurePolicy      json.RawMessage
	NextFireAt         *time.Time
	NextJobID          *JobID
	ExpectedGeneration *int64
}

type ScheduleMutationRequest struct {
	TenantID           TenantID
	ScheduleID         string
	NextFireAt         *time.Time
	NextJobID          *JobID
	ExpectedGeneration *int64
}

type ListSchedulesOptions struct {
	TenantID       TenantID
	ScheduleIDs    []string
	States         []ScheduleState
	TargetJobTypes []JobType
	PageSize       int
	PageToken      string
}

type ListSchedulesResult struct {
	Schedules     []Schedule
	NextPageToken string
}

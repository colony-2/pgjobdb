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

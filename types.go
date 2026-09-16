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
type JobType string

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

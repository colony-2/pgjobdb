package pgjobdb

import (
	"context"
	"database/sql"
	_ "embed"
	"encoding/json"
	"fmt"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
)

// JobListSummary is deliberately independent of JobDetail: listing never needs
// payloads, policies, task coordinates, memberships, or dependency explanations.
type JobListSummary struct {
	TenantID              TenantID                     `json:"tenant_id"`
	JobID                 JobID                        `json:"job_id"`
	GoalID                string                       `json:"goal_id"`
	Store                 JobStore                     `json:"store"`
	Status                JobStatus                    `json:"status"`
	JobType               JobType                      `json:"job_type"`
	RouteJobType          JobType                      `json:"route_job_type"`
	WorkKind              WorkKind                     `json:"work_kind"`
	TaskType              TaskType                     `json:"task_type"`
	AppMetadata           json.RawMessage              `json:"app_metadata"`
	SchemaHash            string                       `json:"schema_hash"`
	ParentJobID           JobID                        `json:"parent_job_id"`
	CreatedAt             time.Time                    `json:"created_at"`
	AvailableAt           time.Time                    `json:"available_at"`
	ExpiresAt             *time.Time                   `json:"expires_at"`
	LeaseExpiresAt        *time.Time                   `json:"lease_expires_at"`
	ArchivedAt            *time.Time                   `json:"archived_at"`
	CancelRequested       bool                         `json:"cancel_requested"`
	CompletionStatus      *CompletionStatus            `json:"completion_status"`
	ClientPayloadRevision int64                        `json:"client_payload_revision"`
	BlockingWaitSummary   *jobdb.DependencyWaitSummary `json:"blocking_wait_summary"`
}

type ListJobSummariesResult struct {
	Jobs          []JobListSummary
	NextPageToken string
}

//go:embed compact_job_lists.sql
var compactJobListSQL string

// ListJobSummaries fetches a page with one set-based query. Detail-only storage
// is never read, and dependency deadlines use the same time as job eligibility.
func ListJobSummaries(ctx context.Context, db DB, opts ListJobsOptions) (*ListJobSummariesResult, error) {
	page, err := listJobs(ctx, db, opts, compactJobListSQL, func(rows *sql.Rows) (JobListSummary, error) {
		var item JobListSummary
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			return item, err
		}
		if err := json.Unmarshal(raw, &item); err != nil {
			return item, fmt.Errorf("pgjobdb: decode job summary: %w", err)
		}
		if item.BlockingWaitSummary != nil && item.BlockingWaitSummary.Deadline != nil {
			deadline := item.BlockingWaitSummary.Deadline.UTC()
			item.BlockingWaitSummary.Deadline = &deadline
		}
		return item, nil
	}, func(j JobListSummary) jobCursor {
		return jobCursor{CreatedAt: j.CreatedAt, TenantID: j.TenantID, JobID: j.JobID}
	})
	if err != nil {
		return nil, err
	}
	return &ListJobSummariesResult{Jobs: page.Jobs, NextPageToken: page.NextPageToken}, nil
}

package pgjobdb

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"slices"
	"time"

	"github.com/lib/pq"
)

type scheduleRunCursor struct {
	Filter      string    `json:"f"`
	ScheduledAt time.Time `json:"s"`
	JobID       JobID     `json:"j"`
}

func ListScheduleRuns(ctx context.Context, db DB,
	opts ListScheduleRunsOptions) (*ListScheduleRunsResult, error) {
	if err := validateScheduleDB(ctx, db); err != nil {
		return nil, err
	}
	if err := validateScheduleKey(opts.TenantID, opts.ScheduleID); err != nil {
		return nil, err
	}
	if opts.ScheduledAfter != nil && opts.ScheduledBefore != nil &&
		opts.ScheduledAfter.After(*opts.ScheduledBefore) {
		return nil, fmt.Errorf("pgjobdb: scheduled-after must not exceed scheduled-before")
	}
	if opts.PageSize == 0 {
		opts.PageSize = 100
	}
	if opts.PageSize < 1 || opts.PageSize > 200 {
		return nil, fmt.Errorf("pgjobdb: schedule run page size must be between 1 and 200")
	}
	statuses := make([]string, 0, len(opts.Statuses))
	for _, status := range opts.Statuses {
		switch status {
		case JobStatusReady, JobStatusExpired, JobStatusPendingJobs,
			JobStatusAwaitingFuture, JobStatusActive, JobStatusCrashConcern,
			JobStatusCancelled, JobStatusCompleted:
		default:
			return nil, fmt.Errorf("pgjobdb: invalid job status %q", status)
		}
		statuses = append(statuses, string(status))
	}
	slices.Sort(statuses)
	filterData, err := json.Marshal([]any{
		opts.TenantID, opts.ScheduleID, opts.ScheduledAfter,
		opts.ScheduledBefore, statuses,
	})
	if err != nil {
		return nil, err
	}
	sum := sha256.Sum256(filterData)
	fingerprint := hex.EncodeToString(sum[:])
	var beforeScheduled, beforeJob any
	if opts.PageToken != "" {
		if len(opts.PageToken) > 4096 {
			return nil, fmt.Errorf("pgjobdb: invalid schedule run page token")
		}
		data, err := base64.RawURLEncoding.DecodeString(opts.PageToken)
		if err != nil {
			return nil, fmt.Errorf("pgjobdb: invalid schedule run page token")
		}
		var cursor scheduleRunCursor
		if json.Unmarshal(data, &cursor) != nil || cursor.Filter != fingerprint ||
			cursor.ScheduledAt.IsZero() || cursor.JobID == "" {
			return nil, fmt.Errorf("pgjobdb: invalid schedule run page token")
		}
		beforeScheduled, beforeJob = cursor.ScheduledAt, string(cursor.JobID)
	}
	rows, err := db.QueryContext(ctx, `SELECT * FROM pgjobdb.list_native_schedule_runs(
		$1, $2, $3, $4, $5, $6, $7, $8)`,
		string(opts.TenantID), opts.ScheduleID, optionalTime(opts.ScheduledAfter),
		optionalTime(opts.ScheduledBefore), pq.Array(nilIfEmpty(statuses)),
		beforeScheduled, beforeJob, opts.PageSize+1)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := &ListScheduleRunsResult{Runs: make([]JobDetail, 0, opts.PageSize)}
	for rows.Next() {
		var raw, payload []byte
		var revision int64
		var digest string
		if err := rows.Scan(&raw, &payload, &revision, &digest); err != nil {
			return nil, err
		}
		var run JobDetail
		if err := json.Unmarshal(raw, &run); err != nil {
			return nil, fmt.Errorf("pgjobdb: decode schedule run: %w", err)
		}
		run.ClientPayload = append(json.RawMessage(nil), payload...)
		run.ClientPayloadRevision = revision
		run.InitialPayloadDigest = digest
		result.Runs = append(result.Runs, run)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(result.Runs) > opts.PageSize {
		result.Runs = result.Runs[:opts.PageSize]
		last := result.Runs[len(result.Runs)-1]
		if last.ScheduledAt == nil {
			return nil, fmt.Errorf("pgjobdb: listed schedule run is missing scheduled time")
		}
		data, err := json.Marshal(scheduleRunCursor{
			Filter: fingerprint, ScheduledAt: *last.ScheduledAt, JobID: last.JobID,
		})
		if err != nil {
			return nil, err
		}
		result.NextPageToken = base64.RawURLEncoding.EncodeToString(data)
	}
	return result, nil
}

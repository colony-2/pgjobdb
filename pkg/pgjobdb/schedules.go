package pgjobdb

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/lib/pq"
)

var ErrScheduleNotFound = errors.New("pgjobdb: schedule not found")

func UpsertSchedule(ctx context.Context, db DB, req UpsertScheduleRequest) (*Schedule, error) {
	if err := validateScheduleDB(ctx, db); err != nil {
		return nil, err
	}
	if err := validateScheduleKey(req.TenantID, req.ScheduleID); err != nil {
		return nil, err
	}
	if req.State != ScheduleStateActive && req.State != ScheduleStatePaused {
		return nil, fmt.Errorf("pgjobdb: schedule upsert requires ACTIVE or PAUSED state")
	}
	if req.SpecHash == "" || req.TargetJobType == "" || req.OverlapPolicy == "" {
		return nil, fmt.Errorf("pgjobdb: schedule spec hash, target job type, and overlap policy are required")
	}
	for name, raw := range map[string]json.RawMessage{
		"trigger": req.Trigger, "target snapshot": req.TargetSnapshot,
		"failure policy": req.FailurePolicy,
	} {
		if !isJSONObject(raw) {
			return nil, fmt.Errorf("pgjobdb: %s must be a JSON object", name)
		}
	}
	if err := validateNextFire(req.State, req.NextFireAt, req.NextJobID); err != nil {
		return nil, err
	}
	if req.ExpectedGeneration != nil && *req.ExpectedGeneration < 1 {
		return nil, fmt.Errorf("pgjobdb: expected generation must be positive")
	}
	row := db.QueryRowContext(ctx, `SELECT * FROM pgjobdb.upsert_schedule(
		$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)`,
		string(req.TenantID), req.ScheduleID, string(req.State), req.SpecHash,
		string(req.Trigger), string(req.TargetJobType), string(req.TargetSnapshot),
		req.OverlapPolicy, string(req.FailurePolicy), optionalTime(req.NextFireAt),
		optionalJobID(req.NextJobID), optionalInt64(req.ExpectedGeneration))
	return scanSchedule(row)
}

func GetSchedule(ctx context.Context, db DB, tenantID TenantID, scheduleID string) (*Schedule, error) {
	if err := validateScheduleDB(ctx, db); err != nil {
		return nil, err
	}
	if err := validateScheduleKey(tenantID, scheduleID); err != nil {
		return nil, err
	}
	row := db.QueryRowContext(ctx, `SELECT * FROM pgjobdb.get_schedule($1, $2)`,
		string(tenantID), scheduleID)
	schedule, err := scanSchedule(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrScheduleNotFound
	}
	return schedule, err
}

func PauseSchedule(ctx context.Context, db DB, req ScheduleMutationRequest) (*Schedule, error) {
	return mutateSchedule(ctx, db, req, ScheduleStatePaused)
}

func ResumeSchedule(ctx context.Context, db DB, req ScheduleMutationRequest) (*Schedule, error) {
	return mutateSchedule(ctx, db, req, ScheduleStateActive)
}

func ArchiveSchedule(ctx context.Context, db DB, req ScheduleMutationRequest) (*Schedule, error) {
	return mutateSchedule(ctx, db, req, ScheduleStateArchived)
}

func mutateSchedule(ctx context.Context, db DB, req ScheduleMutationRequest, state ScheduleState) (*Schedule, error) {
	if err := validateScheduleDB(ctx, db); err != nil {
		return nil, err
	}
	if err := validateScheduleKey(req.TenantID, req.ScheduleID); err != nil {
		return nil, err
	}
	if err := validateNextFire(state, req.NextFireAt, req.NextJobID); err != nil {
		return nil, err
	}
	if req.ExpectedGeneration != nil && *req.ExpectedGeneration < 1 {
		return nil, fmt.Errorf("pgjobdb: expected generation must be positive")
	}
	var row *sql.Row
	switch state {
	case ScheduleStatePaused:
		row = db.QueryRowContext(ctx, `SELECT * FROM pgjobdb.pause_schedule($1, $2, $3)`,
			string(req.TenantID), req.ScheduleID, optionalInt64(req.ExpectedGeneration))
	case ScheduleStateActive:
		row = db.QueryRowContext(ctx, `SELECT * FROM pgjobdb.resume_schedule($1, $2, $3, $4, $5)`,
			string(req.TenantID), req.ScheduleID, optionalTime(req.NextFireAt),
			optionalJobID(req.NextJobID), optionalInt64(req.ExpectedGeneration))
	case ScheduleStateArchived:
		row = db.QueryRowContext(ctx, `SELECT * FROM pgjobdb.archive_schedule($1, $2, $3)`,
			string(req.TenantID), req.ScheduleID, optionalInt64(req.ExpectedGeneration))
	}
	return scanSchedule(row)
}

func ListSchedules(ctx context.Context, db DB, opts ListSchedulesOptions) (*ListSchedulesResult, error) {
	if err := validateScheduleDB(ctx, db); err != nil {
		return nil, err
	}
	if opts.TenantID == "" {
		return nil, fmt.Errorf("pgjobdb: tenant id is required")
	}
	if opts.PageSize == 0 {
		opts.PageSize = 100
	}
	if opts.PageSize < 1 || opts.PageSize > 999 {
		return nil, fmt.Errorf("pgjobdb: schedule page size must be between 1 and 999")
	}
	ids := slices.Clone(opts.ScheduleIDs)
	states := make([]string, 0, len(opts.States))
	for _, state := range opts.States {
		if state != ScheduleStateActive && state != ScheduleStatePaused && state != ScheduleStateArchived {
			return nil, fmt.Errorf("pgjobdb: invalid schedule state %q", state)
		}
		states = append(states, string(state))
	}
	jobTypes := make([]string, 0, len(opts.TargetJobTypes))
	for _, jobType := range opts.TargetJobTypes {
		jobTypes = append(jobTypes, string(jobType))
	}
	slices.Sort(ids)
	slices.Sort(states)
	slices.Sort(jobTypes)
	fingerprint, err := scheduleFilterHash(opts.TenantID, ids, states, jobTypes)
	if err != nil {
		return nil, err
	}
	var beforeTime any
	var beforeID any
	if opts.PageToken != "" {
		cursor, err := decodeScheduleCursor(opts.PageToken)
		if err != nil || cursor.Filter != fingerprint || cursor.UpdatedAt.IsZero() || cursor.ScheduleID == "" {
			return nil, fmt.Errorf("pgjobdb: invalid schedule page token")
		}
		beforeTime, beforeID = cursor.UpdatedAt, cursor.ScheduleID
	}
	rows, err := db.QueryContext(ctx, `SELECT * FROM pgjobdb.list_schedules(
		$1, $2, $3, $4, $5, $6, $7)`, string(opts.TenantID),
		pq.Array(nilIfEmpty(states)), pq.Array(nilIfEmpty(jobTypes)), beforeTime,
		beforeID, opts.PageSize+1, pq.Array(nilIfEmpty(ids)))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := &ListSchedulesResult{Schedules: make([]Schedule, 0, opts.PageSize)}
	for rows.Next() {
		schedule, err := scanSchedule(rows)
		if err != nil {
			return nil, err
		}
		result.Schedules = append(result.Schedules, *schedule)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(result.Schedules) > opts.PageSize {
		result.Schedules = result.Schedules[:opts.PageSize]
		last := result.Schedules[len(result.Schedules)-1]
		token, err := encodeScheduleCursor(scheduleCursor{
			Filter: fingerprint, UpdatedAt: last.UpdatedAt, ScheduleID: last.ScheduleID,
		})
		if err != nil {
			return nil, err
		}
		result.NextPageToken = token
	}
	return result, nil
}

type scheduleScanner interface{ Scan(...any) error }

func scanSchedule(row scheduleScanner) (*Schedule, error) {
	var result Schedule
	var state, tenant, jobType string
	var trigger, target, failure []byte
	var nextFire sql.NullTime
	var nextJob sql.NullString
	if err := row.Scan(&tenant, &result.ScheduleID, &state, &result.Generation,
		&result.SpecHash, &trigger, &jobType, &target, &result.OverlapPolicy,
		&failure, &nextFire, &nextJob, &result.CreatedAt, &result.UpdatedAt); err != nil {
		return nil, err
	}
	result.TenantID = TenantID(tenant)
	result.State = ScheduleState(state)
	result.TargetJobType = JobType(jobType)
	result.Trigger = append(json.RawMessage(nil), trigger...)
	result.TargetSnapshot = append(json.RawMessage(nil), target...)
	result.FailurePolicy = append(json.RawMessage(nil), failure...)
	if nextFire.Valid {
		t := nextFire.Time.UTC()
		result.NextFireAt = &t
	}
	if nextJob.Valid {
		id := JobID(nextJob.String)
		result.NextJobID = &id
	}
	return &result, nil
}

func validateScheduleDB(ctx context.Context, db DB) error {
	if ctx == nil {
		return fmt.Errorf("pgjobdb: context is nil")
	}
	if db == nil {
		return fmt.Errorf("pgjobdb: DB is nil")
	}
	return nil
}

func validateScheduleKey(tenant TenantID, scheduleID string) error {
	if tenant == "" || scheduleID == "" {
		return fmt.Errorf("pgjobdb: tenant id and schedule id are required")
	}
	return nil
}

func validateNextFire(state ScheduleState, next *time.Time, jobID *JobID) error {
	if (next == nil) != (jobID == nil) || (jobID != nil && *jobID == "") {
		return fmt.Errorf("pgjobdb: next fire and next job id must be provided together")
	}
	if state != ScheduleStateActive && next != nil {
		return fmt.Errorf("pgjobdb: inactive schedule cannot have next fire state")
	}
	return nil
}

func isJSONObject(raw json.RawMessage) bool {
	if !json.Valid(raw) {
		return false
	}
	var value any
	if json.Unmarshal(raw, &value) != nil {
		return false
	}
	_, ok := value.(map[string]any)
	return ok
}

func optionalTime(value *time.Time) any {
	if value == nil {
		return nil
	}
	return value.UTC()
}
func optionalJobID(value *JobID) any {
	if value == nil {
		return nil
	}
	return string(*value)
}
func optionalInt64(value *int64) any {
	if value == nil {
		return nil
	}
	return *value
}
func nilIfEmpty(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	return values
}

type scheduleCursor struct {
	Filter     string    `json:"f"`
	UpdatedAt  time.Time `json:"u"`
	ScheduleID string    `json:"s"`
}

func scheduleFilterHash(tenant TenantID, ids, states, jobTypes []string) (string, error) {
	data, err := json.Marshal([]any{tenant, ids, states, jobTypes})
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}

func encodeScheduleCursor(cursor scheduleCursor) (string, error) {
	data, err := json.Marshal(cursor)
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(data), nil
}

func decodeScheduleCursor(token string) (scheduleCursor, error) {
	var cursor scheduleCursor
	data, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return cursor, err
	}
	err = json.Unmarshal(data, &cursor)
	return cursor, err
}

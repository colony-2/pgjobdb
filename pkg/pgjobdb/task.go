package pgjobdb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/lib/pq"
)

var ErrTaskConflict = errors.New("pgjobdb: waiting task conflict")

// ClaimTask atomically claims an eligible waiting task and installs its job-only
// resume route. The claim commits before returning; chapter storage is independent.
// Client payload and its revision are preserved.
func ClaimTask(ctx context.Context, db *sql.DB, req ClaimTaskRequest) (TaskClaim, error) {
	if db == nil {
		return TaskClaim{}, fmt.Errorf("pgjobdb: database is required")
	}
	if err := validateScheduleDB(ctx, db); err != nil {
		return TaskClaim{}, err
	}
	if req.TenantID == "" || req.JobID == "" || req.WorkerID == "" ||
		!validTypeName(req.JobType) || !validTypeName(req.Task.TaskType) ||
		!validTypeName(req.Task.ResumeJobType) || req.Task.InputOrdinal < 0 ||
		req.Task.OutputOrdinal < 0 || req.Task.InputHash == "" {
		return TaskClaim{}, fmt.Errorf("pgjobdb: claim task requires full route and coordinates")
	}
	if req.LeaseDuration <= 0 {
		return TaskClaim{}, fmt.Errorf("pgjobdb: claim task requires a positive lease duration")
	}
	seconds, err := leaseSeconds(req.LeaseDuration)
	if err != nil {
		return TaskClaim{}, err
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return TaskClaim{}, err
	}
	defer tx.Rollback()
	claim := TaskClaim{Identity: LeaseIdentity{
		TenantID: req.TenantID, JobID: req.JobID, WorkerID: req.WorkerID,
	}}
	err = tx.QueryRowContext(ctx, `SELECT lease_id, lease_expires_at
        FROM pgjobdb.claim_native_task($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`,
		string(req.TenantID), string(req.JobID), string(req.WorkerID),
		string(req.JobType), string(req.Task.TaskType), string(req.Task.ResumeJobType),
		req.Task.InputOrdinal, req.Task.OutputOrdinal, req.Task.InputHash, seconds,
	).Scan(&claim.Identity.LeaseID, &claim.ExpiresAt)
	if err != nil {
		var pgErr *pq.Error
		if errors.As(err, &pgErr) && pgErr.Code == "JTC01" {
			return TaskClaim{}, fmt.Errorf("%w: %s", ErrTaskConflict, pgErr.Message)
		}
		return TaskClaim{}, err
	}
	if err := tx.Commit(); err != nil {
		// An ambiguous commit must not authorize this caller to append.
		return TaskClaim{}, err
	}
	return claim, nil
}

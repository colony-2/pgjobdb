package pgjobdb

import (
	"context"
	"fmt"
)

// CompleteTaskWork atomically compares task coordinates and routes an unheld
// waiting job back to its resume job type. JobDB core writes the task chapter
// before calling this mutation.
func CompleteTaskWork(ctx context.Context, db DB, req CompleteTaskWorkRequest) error {
	if err := validateScheduleDB(ctx, db); err != nil {
		return err
	}
	if req.TenantID == "" || req.JobID == "" || req.WorkerID == "" ||
		req.JobType == "" || req.Task.TaskType == "" ||
		req.Task.ResumeJobType == "" || req.Task.InputOrdinal < 0 ||
		req.Task.OutputOrdinal < 0 || req.Task.InputHash == "" {
		return fmt.Errorf("pgjobdb: complete task requires full route and coordinates")
	}
	mode, value, revision, err := clientUpdateArgs(req.ClientPayloadUpdate, false)
	if err != nil {
		return err
	}
	var completed bool
	if err := db.QueryRowContext(ctx, `SELECT pgjobdb.complete_native_task_work(
		$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)`,
		string(req.TenantID), string(req.JobID), string(req.WorkerID),
		string(req.JobType), string(req.Task.TaskType),
		string(req.Task.ResumeJobType), req.Task.InputOrdinal,
		req.Task.OutputOrdinal, req.Task.InputHash, mode, value, revision).Scan(&completed); err != nil {
		return clientPayloadError(err)
	}
	if !completed {
		return fmt.Errorf("pgjobdb: complete native task work returned false")
	}
	return nil
}

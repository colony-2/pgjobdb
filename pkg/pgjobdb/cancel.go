package pgjobdb

import (
	"context"
	"fmt"
)

type CancelJobRequest struct {
	TenantID TenantID
	JobID    JobID
	WorkerID WorkerID
	Reason   string
}

func CancelJob(ctx context.Context, db DB, req CancelJobRequest) error {
	if err := validateScheduleDB(ctx, db); err != nil {
		return err
	}
	if req.TenantID == "" || req.JobID == "" || req.WorkerID == "" {
		return fmt.Errorf("pgjobdb: cancel requires tenant, job, and worker ids")
	}
	var cancelled bool
	if err := db.QueryRowContext(ctx, `SELECT pgjobdb.cancel_native_job(
		$1, $2, $3, $4)`, string(req.TenantID), string(req.JobID),
		string(req.WorkerID), nilIfBlank(req.Reason)).Scan(&cancelled); err != nil {
		return err
	}
	if !cancelled {
		return fmt.Errorf("pgjobdb: cancel native job returned false")
	}
	return nil
}

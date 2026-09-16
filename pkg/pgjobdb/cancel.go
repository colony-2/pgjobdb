package pgjobdb

import (
	"context"
	"fmt"
)

const cancelStmt = `
SELECT cancel_requested
FROM pgjobdb.cancel_job($1, $2, $3, $4)
`

// CancelJob requests cancellation for the given job_id via pgjobdb.cancel_job.
// If the job is already cancelled, the function succeeds without error.
func CancelJob(ctx context.Context, db DB, tenantID TenantID, jobID JobID, worker WorkerID, reason string) error {
	if db == nil {
		return fmt.Errorf("pgjobdb: nil DB")
	}
	if ctx == nil {
		return fmt.Errorf("pgjobdb: nil context")
	}
	if tenantID == "" {
		return fmt.Errorf("pgjobdb: tenant id is required")
	}
	if jobID == "" {
		return fmt.Errorf("pgjobdb: job id is required")
	}
	if worker == "" {
		return fmt.Errorf("pgjobdb: worker id is required")
	}

	var reasonArg any
	if reason != "" {
		reasonArg = reason
	}

	row := db.QueryRowContext(ctx, cancelStmt, string(tenantID), string(jobID), string(worker), reasonArg)
	var cancelled bool
	if err := row.Scan(&cancelled); err != nil {
		return annotateError(err)
	}
	if !cancelled {
		return fmt.Errorf("pgjobdb: cancel_job returned false")
	}
	return nil
}

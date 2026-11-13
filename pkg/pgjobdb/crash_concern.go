package pgjobdb

import (
	"context"
	"fmt"

	"github.com/colony-2/jobdb/pkg/jobdb"
)

// ClearCrashConcern clears only the consecutive crash count. The guard and the
// existing traced SQL operation execute under the same row lock, including when
// called inside a borrowed transaction. Other run eligibility constraints remain.
func ClearCrashConcern(ctx context.Context, db DB, req jobdb.ClearCrashConcernRequest) error {
	if err := req.Validate(); err != nil {
		return err
	}
	if err := validateDB(ctx, db); err != nil {
		return err
	}
	worker := req.WorkerID
	if worker == "" {
		worker = "jobdb"
	}
	var result string
	err := db.QueryRowContext(ctx, `WITH target AS MATERIALIZED (
    SELECT j.cancel_requested, j.lease_expires_at, j.consecutive_expirations, f.hidden_at
    FROM pgjobdb.jobs j JOIN pgjobdb.job_facts f USING (tenant_id, job_id)
    WHERE j.tenant_id=$1 AND j.job_id=$2 FOR UPDATE OF j
)
SELECT CASE
    WHEN cancel_requested OR hidden_at IS NOT NULL OR lease_expires_at>clock_timestamp() THEN 'conflict'
    WHEN consecutive_expirations=0 THEN 'cleared'
    WHEN pgjobdb.clear_crash_concern($1,$2,$3,$4) THEN 'cleared'
    ELSE 'conflict' END
FROM target
UNION ALL
SELECT CASE WHEN EXISTS(SELECT 1 FROM pgjobdb.job_facts WHERE tenant_id=$1 AND job_id=$2)
    THEN 'conflict' ELSE 'not_found' END WHERE NOT EXISTS(SELECT 1 FROM target)`,
		req.JobKey.TenantId, req.JobKey.JobId, worker, nilIfBlank(req.Reason)).Scan(&result)
	if err != nil {
		return err
	}
	switch result {
	case "cleared":
		return nil
	case "not_found":
		return jobdb.ErrJobNotFound
	default:
		return fmt.Errorf("%w: cannot clear crash concern for a live, final, cancelled, or hidden job", jobdb.ErrConflict)
	}
}

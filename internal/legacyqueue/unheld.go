package legacyqueue

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/lib/pq"
)

const (
	completeUnheldStmt   = `SELECT pgjobdb.complete_unheld_job($1, $2, $3, $4, $5)`
	rescheduleUnheldStmt = `
SELECT job_id, next_need, wait_for, available_at
FROM pgjobdb.reschedule_unheld_job($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
`
)

// CompleteUnheldJob finalizes a job without requiring a lease by locking it directly.
func CompleteUnheldJob(ctx context.Context, db DB, tenantID TenantID, jobID JobID, worker WorkerID) error {
	return CompleteUnheldJobWithStatus(ctx, db, tenantID, jobID, worker, CompletionStatusSucceeded, "")
}

// CompleteUnheldJobWithStatus finalizes a job without requiring a lease by locking it directly.
func CompleteUnheldJobWithStatus(ctx context.Context, db DB, tenantID TenantID, jobID JobID, worker WorkerID, status CompletionStatus, completionDetail string) error {
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

	completionStatus, completionArg := normalizeCompletion(status, completionDetail)
	row := db.QueryRowContext(
		ctx,
		completeUnheldStmt,
		string(tenantID),
		string(jobID),
		string(worker),
		string(completionStatus),
		completionArg,
	)
	var ok bool
	if err := row.Scan(&ok); err != nil {
		return annotateError(err)
	}
	if !ok {
		return wrap(ErrJobNotFound, fmt.Errorf("pgjobdb: complete_unheld_job returned false"))
	}
	return nil
}

// RescheduleUnheldJob updates dependencies/availability (and optional payload) for a ready job without re-leasing.
func RescheduleUnheldJob(ctx context.Context, db DB, tenantID TenantID, jobID JobID, worker WorkerID, deps JobDependencies, payload any) error {
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
	if err := deps.validate(); err != nil {
		return err
	}
	payloadArg, err := normalizePayloadOverride(payload)
	if err != nil {
		return err
	}
	altNeedArg, altAfterArg, altSet := deps.alternateArgsForReschedule()

	row := db.QueryRowContext(ctx, rescheduleUnheldStmt,
		string(tenantID),
		string(jobID),
		string(worker),
		string(deps.NextNeed),
		pq.Array(deps.waitForStrings()),
		deps.availableAtArg(),
		payloadArg,
		altNeedArg,
		altAfterArg,
		altSet,
	)

	var (
		id        string
		need      string
		waits     pq.StringArray
		available sql.NullTime
	)
	if err := row.Scan(&id, &need, (*pq.StringArray)(&waits), &available); err != nil {
		return annotateError(err)
	}
	return nil
}

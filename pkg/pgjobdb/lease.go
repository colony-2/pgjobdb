package pgjobdb

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"time"

	"github.com/lib/pq"
)

const (
	extendStmt     = `SELECT pgjobdb.extend_lease($1, $2, $3, $4, $5)`
	rescheduleStmt = `
SELECT job_id, next_need, wait_for, available_at
FROM pgjobdb.reschedule_job($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
`
	completeStmt = `SELECT pgjobdb.complete_job($1, $2, $3, $4, $5, $6)`
)

// WithKeepAlive spawns a goroutine that periodically extends the lease until released.
func (l *Lease) WithKeepAlive(db *sql.DB) *Lease {
	if db == nil || l == nil {
		return l
	}
	l.startKeepAlive(db)
	return l
}

// Extend refreshes the lease expiry in pgjobdb.
func (l *Lease) Extend(ctx context.Context, db DB, additional time.Duration) error {
	if err := l.validateActive(); err != nil {
		return err
	}
	if additional <= 0 {
		return fmt.Errorf("pgjobdb: additional duration must be positive")
	}
	seconds := l.secondsForExtension(additional)
	if err := l.extendInternal(ctx, db, seconds, l.worker); err != nil {
		return err
	}
	return nil
}

func (l *Lease) secondsForExtension(additional time.Duration) int {
	l.mu.RLock()
	expiry := l.leaseExpires
	l.mu.RUnlock()

	remaining := time.Until(expiry)
	if remaining < 0 {
		remaining = 0
	}
	addSeconds := int(math.Ceil(additional.Seconds()))
	if addSeconds < 1 {
		addSeconds = 1
	}
	total := int(math.Ceil(remaining.Seconds())) + addSeconds
	if total < 1 {
		total = addSeconds
	}
	return total
}

func (l *Lease) extendInternal(ctx context.Context, db DB, seconds int, worker WorkerID) error {
	if db == nil {
		return fmt.Errorf("pgjobdb: nil DB")
	}
	row := db.QueryRowContext(ctx, extendStmt, string(l.tenantID), string(l.jobID), l.leaseID, string(worker), seconds)
	var newExpiry time.Time
	if err := row.Scan(&newExpiry); err != nil {
		return annotateError(err)
	}
	l.updateExpiry(newExpiry)
	return nil
}

// Reschedule releases the lease, updates dependencies, and optionally replaces the payload.
func (l *Lease) Reschedule(ctx context.Context, db DB, deps JobDependencies, payload any) error {
	if err := l.validateActive(); err != nil {
		return err
	}
	if err := deps.validate(); err != nil {
		return err
	}
	payloadArg, err := normalizePayloadOverride(payload)
	if err != nil {
		return err
	}
	altNeedArg, altAfterArg, altSet := deps.alternateArgsForReschedule()
	row := db.QueryRowContext(ctx, rescheduleStmt,
		string(l.tenantID),
		string(l.jobID),
		l.leaseID,
		string(l.worker),
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
		available time.Time
	)
	if err := row.Scan(&id, &need, (*pq.StringArray)(&waits), &available); err != nil {
		return annotateError(err)
	}
	l.markReleased()
	return nil
}

// Complete archives the job and removes the lease.
func (l *Lease) Complete(ctx context.Context, db DB) error {
	return l.completeWithStatus(ctx, db, CompletionStatusSucceeded, "")
}

// CompleteWithStatus archives the job with an explicit completion status and optional failure detail.
func (l *Lease) CompleteWithStatus(ctx context.Context, db DB, status CompletionStatus, completionDetail string) error {
	return l.completeWithStatus(ctx, db, status, completionDetail)
}

func (l *Lease) completeWithStatus(ctx context.Context, db DB, status CompletionStatus, completionDetail string) error {
	if err := l.validateActive(); err != nil {
		return err
	}
	completionStatus, completionArg := normalizeCompletion(status, completionDetail)
	row := db.QueryRowContext(
		ctx,
		completeStmt,
		string(l.tenantID),
		string(l.jobID),
		l.leaseID,
		string(l.worker),
		string(completionStatus),
		completionArg,
	)
	var ok bool
	if err := row.Scan(&ok); err != nil {
		return annotateError(err)
	}
	if !ok {
		return wrap(ErrJobNotFound, fmt.Errorf("pgjobdb: complete returned false"))
	}
	l.markReleased()
	return nil
}

package legacyqueue

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"runtime"
	"strings"
	"sync"
	"time"
)

// TenantID uniquely identifies a tenant in pgjobdb.
type TenantID string

// JobID uniquely identifies a job in pgjobdb.
type JobID string

// Capability describes a worker capability.
type Capability string

// WorkerID identifies a worker process.
type WorkerID string

// DB captures the minimal subset used by pgjobdb helpers.
type DB interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

// JobDependencies define when/how a job becomes runnable.
type JobDependencies struct {
	NextNeed    Capability
	Alternate   *AlternateNext
	WaitFor     []JobID
	AvailableAt time.Time
}

// AlternateNext describes an optional fallback capability.
// When set, the job pivots to Need after the job has been READY for at least After.
// Use an empty Need with zero After to explicitly clear an existing alternate on reschedule.
type AlternateNext struct {
	Need  Capability
	After time.Duration
}

func (d JobDependencies) validate() error {
	if d.NextNeed == "" {
		return fmt.Errorf("next capability is required")
	}
	if d.Alternate != nil {
		if d.Alternate.After < 0 {
			return fmt.Errorf("alternate after must be non-negative")
		}
		if d.Alternate.Need == "" && d.Alternate.After > 0 {
			return fmt.Errorf("alternate capability is required when after is set")
		}
	}
	return nil
}

func (d JobDependencies) waitForStrings() []string {
	if len(d.WaitFor) == 0 {
		return nil
	}
	ids := make([]string, 0, len(d.WaitFor))
	for _, id := range d.WaitFor {
		if id == "" {
			continue
		}
		ids = append(ids, string(id))
	}
	return ids
}

func (d JobDependencies) availableAtArg() any {
	return optionalTime(d.AvailableAt)
}

// alternateArgsForSubmit returns args for submit_job; nil values mean "no alternate".
func (d JobDependencies) alternateArgsForSubmit() (any, any) {
	if d.Alternate == nil {
		return nil, nil
	}
	if d.Alternate.Need == "" && d.Alternate.After == 0 {
		return nil, nil
	}
	return string(d.Alternate.Need), durationToSecondsArg(d.Alternate.After)
}

// alternateArgsForReschedule controls whether to overwrite/clear alternate fields.
// set indicates p_set_alternate should be TRUE so the database applies the provided (possibly nil) values.
func (d JobDependencies) alternateArgsForReschedule() (need any, after any, set bool) {
	if d.Alternate == nil {
		return nil, nil, false
	}
	if d.Alternate.Need == "" && d.Alternate.After == 0 {
		return nil, nil, true // explicit clear
	}
	return string(d.Alternate.Need), durationToSecondsArg(d.Alternate.After), true
}

func durationToSecondsArg(d time.Duration) any {
	if d < 0 {
		return nil
	}
	secs := int(math.Ceil(d.Seconds()))
	if secs < 0 {
		secs = 0
	}
	return secs
}

func optionalTime(t time.Time) any {
	if t.IsZero() {
		return nil
	}
	return t
}

const (
	defaultLeaseSeconds   = 60
	maxBackoffInterval    = time.Minute
	initialBackoff        = time.Second
	keepAliveSafetyBuffer = 5 * time.Second
)

var (
	// ErrLeaseExpired indicates the lease can no longer be used safely.
	ErrLeaseExpired = errors.New("pgjobdb: lease expired")
	// ErrLeaseMismatch means the database rejected the lease_id/job_id pair.
	ErrLeaseMismatch = errors.New("pgjobdb: lease mismatch")
	// ErrJobNotFound indicates the job is missing from the database.
	ErrJobNotFound = errors.New("pgjobdb: job not found")
	// ErrDependencyViolation denotes dependency conflicts surfaced during submission.
	ErrDependencyViolation = errors.New("pgjobdb: dependency violation")
)

// Lease models a pgjobdb lease token.
type Lease struct {
	tenantID     TenantID
	jobID        JobID
	leaseID      string
	worker       WorkerID
	capability   Capability
	payload      json.RawMessage
	leaseExpires time.Time

	mu               sync.RWMutex
	released         bool
	keepAliveCancel  context.CancelFunc
	keepAliveDone    chan struct{}
	keepAliveDB      *sql.DB
	keepAliveStarted bool
}

// TenantID returns the tenant identifier associated with this lease.
func (l *Lease) TenantID() TenantID {
	if l == nil {
		return ""
	}
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.tenantID
}

// JobID returns the job identifier associated with this lease.
func (l *Lease) JobID() JobID {
	if l == nil {
		return ""
	}
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.jobID
}

// LeaseID exposes the pgjobdb lease identifier.
func (l *Lease) LeaseID() string {
	if l == nil {
		return ""
	}
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.leaseID
}

// Payload returns the immutable JSON payload associated with the job.
func (l *Lease) Payload() json.RawMessage {
	if l == nil {
		return nil
	}
	if l.payload == nil {
		return json.RawMessage(`{}`)
	}
	cpy := make(json.RawMessage, len(l.payload))
	copy(cpy, l.payload)
	return cpy
}

// LeaseExpiry reports the local notion of when the lease expires.
func (l *Lease) LeaseExpiry() time.Time {
	if l == nil {
		return time.Time{}
	}
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.leaseExpires
}

func (l *Lease) isReleased() bool {
	return l == nil || l.released
}

func (l *Lease) validateActive() error {
	if l == nil {
		return fmt.Errorf("nil lease")
	}
	l.mu.RLock()
	released := l.released
	expiry := l.leaseExpires
	l.mu.RUnlock()

	if released {
		return ErrLeaseExpired
	}
	if time.Now().After(expiry) {
		return ErrLeaseExpired
	}
	return nil
}

func (l *Lease) markReleased() {
	if l == nil {
		return
	}
	l.mu.Lock()
	l.released = true
	l.mu.Unlock()
	l.StopKeepAlive()
}

func (l *Lease) updateExpiry(newExpiry time.Time) {
	l.mu.Lock()
	l.leaseExpires = newExpiry
	l.mu.Unlock()
}

// NextNeed returns the capability the job is currently queued under.
func (l *Lease) NextNeed() Capability {
	if l == nil {
		return ""
	}
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.capability
}

func (l *Lease) startKeepAlive(db *sql.DB) {
	if l == nil || db == nil {
		return
	}
	l.mu.Lock()
	if l.keepAliveStarted {
		l.mu.Unlock()
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	l.keepAliveCancel = cancel
	l.keepAliveDone = make(chan struct{})
	l.keepAliveDB = db
	l.keepAliveStarted = true
	l.mu.Unlock()

	go l.keepAliveLoop(ctx)
	runtime.SetFinalizer(l, func(le *Lease) {
		le.StopKeepAlive()
	})
}

func (l *Lease) StopKeepAlive() {
	l.mu.Lock()
	cancel := l.keepAliveCancel
	done := l.keepAliveDone
	started := l.keepAliveStarted
	l.keepAliveCancel = nil
	l.keepAliveDone = nil
	l.keepAliveStarted = false
	l.mu.Unlock()

	if !started {
		return
	}

	if cancel != nil {
		cancel()
	}
	if done != nil {
		<-done
	}
	runtime.SetFinalizer(l, nil)
}

func (l *Lease) keepAliveLoop(ctx context.Context) {
	defer close(l.keepAliveDone)
	ticker := time.NewTimer(l.nextKeepAliveInterval())
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			l.mu.RLock()
			db := l.keepAliveDB
			worker := l.worker
			l.mu.RUnlock()

			if db == nil {
				return
			}

			extendCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
			seconds := l.secondsForExtension(time.Duration(defaultLeaseSeconds) * time.Second)
			err := l.extendInternal(extendCtx, db, seconds, worker)
			cancel()
			if err != nil {
				// stop extending if lease is gone.
				return
			}
		}
		ticker.Reset(l.nextKeepAliveInterval())
	}
}

func (l *Lease) nextKeepAliveInterval() time.Duration {
	l.mu.RLock()
	expiry := l.leaseExpires
	l.mu.RUnlock()

	remaining := time.Until(expiry)
	target := remaining/2 - keepAliveSafetyBuffer
	if target <= 0 {
		target = 5 * time.Second
	}
	return target
}

type sentinelError struct {
	marker error
	cause  error
}

func (e sentinelError) Error() string {
	if e.cause == nil {
		return e.marker.Error()
	}
	return fmt.Sprintf("%s: %v", e.marker, e.cause)
}

func (e sentinelError) Unwrap() error { return e.cause }

func (e sentinelError) Is(target error) bool {
	return target == e.marker || errors.Is(e.cause, target)
}

func wrap(marker, err error) error {
	if err == nil {
		return marker
	}
	if marker == nil {
		return err
	}
	return sentinelError{marker: marker, cause: err}
}

func annotateError(err error) error {
	if err == nil {
		return nil
	}
	msg := strings.ToLower(err.Error())
	switch {
	case strings.Contains(msg, "references unknown jobs"):
		return wrap(ErrDependencyViolation, err)
	case strings.Contains(msg, "not currently leased"), strings.Contains(msg, "actively leased"), strings.Contains(msg, "active lease not found"):
		return wrap(ErrLeaseMismatch, err)
	case strings.Contains(msg, "has expired"):
		return wrap(ErrLeaseExpired, err)
	case strings.Contains(msg, "not found"):
		return wrap(ErrJobNotFound, err)
	default:
		return err
	}
}

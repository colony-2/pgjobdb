package legacyqueue

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/lib/pq"
)

const getWorkStmt = `
SELECT tenant_id, job_id, lease_id, next_need, wait_for, payload, available_at, lease_expires_at
FROM pgjobdb.get_work($1, $2, $3, $4, $5, $6, $7)
`

const getJobLeaseStmt = `
SELECT tenant_id, job_id, lease_id, next_need, wait_for, payload, available_at, lease_expires_at
FROM pgjobdb.get_job_lease($1, $2, $3, $4, $5)
`

// GetWorkOptions specifies optional filters for leasing a single job.
type GetWorkOptions struct {
	TenantIDs      []TenantID
	LeaseSeconds   int
	MetadataEquals []MetadataPredicate
}

// GetJobLeaseOptions specifies optional controls for leasing a specific job.
type GetJobLeaseOptions struct {
	LeaseSeconds int
}

// GetWork attempts to fetch a single job lease matching the provided capabilities.
// tenantIDs filters which tenants to serve; nil means all tenants.
func GetWork(ctx context.Context, db DB, worker WorkerID, capabilities []Capability, tenantIDs []TenantID) (*Lease, error) {
	return GetWorkWithOptions(ctx, db, worker, capabilities, GetWorkOptions{TenantIDs: tenantIDs})
}

// GetWorkWithOptions attempts to fetch a single job lease matching the provided capabilities and optional metadata filters.
func GetWorkWithOptions(ctx context.Context, db DB, worker WorkerID, capabilities []Capability, opts GetWorkOptions) (*Lease, error) {
	caps, leaseSeconds, err := validateLeaseLookup(ctx, db, worker, capabilities, opts.LeaseSeconds)
	if err != nil {
		return nil, err
	}
	tenants := tenantIDsToStrings(opts.TenantIDs)
	filterPaths, filterValues, err := metadataPredicatesToStringFilters(opts.MetadataEquals)
	if err != nil {
		return nil, err
	}

	var pathArg any
	var valueArg any
	if len(filterPaths) > 0 {
		pathArg = pq.Array(filterPaths)
		valueArg = pq.Array(filterValues)
	}

	row := db.QueryRowContext(ctx, getWorkStmt, string(worker), pq.Array(caps), pq.Array(tenants), leaseSeconds, 1, pathArg, valueArg)
	return scanLeaseRow(row, worker)
}

// GetJobLease attempts to fetch a lease for a specific job id if it is presently leaseable.
func GetJobLease(ctx context.Context, db DB, tenantID TenantID, jobID JobID, worker WorkerID, capabilities []Capability) (*Lease, error) {
	return GetJobLeaseWithOptions(ctx, db, tenantID, jobID, worker, capabilities, GetJobLeaseOptions{})
}

// GetJobLeaseWithOptions attempts to fetch a lease for a specific job id with an optional lease duration override.
func GetJobLeaseWithOptions(ctx context.Context, db DB, tenantID TenantID, jobID JobID, worker WorkerID, capabilities []Capability, opts GetJobLeaseOptions) (*Lease, error) {
	if tenantID == "" {
		return nil, fmt.Errorf("pgjobdb: tenant id required")
	}
	if jobID == "" {
		return nil, fmt.Errorf("pgjobdb: job id required")
	}

	caps, leaseSeconds, err := validateLeaseLookup(ctx, db, worker, capabilities, opts.LeaseSeconds)
	if err != nil {
		return nil, err
	}

	row := db.QueryRowContext(
		ctx,
		getJobLeaseStmt,
		string(tenantID),
		string(jobID),
		string(worker),
		pq.Array(caps),
		leaseSeconds,
	)
	return scanLeaseRow(row, worker)
}

// AwaitWork polls pgjobdb.get_work with exponential backoff until a lease is found or the context ends.
// tenantIDs filters which tenants to serve; nil means all tenants.
func AwaitWork(ctx context.Context, db DB, worker WorkerID, caps []Capability, tenantIDs []TenantID) (*Lease, error) {
	return AwaitWorkWithOptions(ctx, db, worker, caps, GetWorkOptions{TenantIDs: tenantIDs})
}

// AwaitWorkWithOptions polls pgjobdb.get_work with exponential backoff until a lease is found or the context ends.
func AwaitWorkWithOptions(ctx context.Context, db DB, worker WorkerID, caps []Capability, opts GetWorkOptions) (*Lease, error) {
	if ctx == nil {
		return nil, fmt.Errorf("pgjobdb: nil context")
	}
	backoff := initialBackoff
	for {
		lease, err := GetWorkWithOptions(ctx, db, worker, caps, opts)
		if err != nil {
			return nil, err
		}
		if lease != nil {
			return lease, nil
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(backoff):
		}

		if backoff < maxBackoffInterval {
			backoff *= 2
			if backoff > maxBackoffInterval {
				backoff = maxBackoffInterval
			}
		}
	}
}

func validateLeaseLookup(ctx context.Context, db DB, worker WorkerID, capabilities []Capability, requestedLeaseSeconds int) ([]string, int, error) {
	if db == nil {
		return nil, 0, fmt.Errorf("pgjobdb: nil DB")
	}
	if ctx == nil {
		return nil, 0, fmt.Errorf("pgjobdb: nil context")
	}
	if worker == "" {
		return nil, 0, fmt.Errorf("pgjobdb: worker id required")
	}
	if len(capabilities) == 0 {
		return nil, 0, fmt.Errorf("pgjobdb: at least one capability is required")
	}

	leaseSeconds := requestedLeaseSeconds
	if leaseSeconds == 0 {
		leaseSeconds = defaultLeaseSeconds
	}
	if leaseSeconds <= 0 {
		return nil, 0, fmt.Errorf("pgjobdb: lease seconds must be positive")
	}

	return capabilitiesToStrings(capabilities), leaseSeconds, nil
}

func scanLeaseRow(row *sql.Row, worker WorkerID) (*Lease, error) {
	var (
		tenantID  string
		jobID     string
		leaseID   string
		need      string
		waits     pq.StringArray
		payload   json.RawMessage
		available time.Time
		expires   time.Time
	)

	if err := row.Scan(&tenantID, &jobID, &leaseID, &need, (*pq.StringArray)(&waits), &payload, &available, &expires); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, annotateError(err)
	}

	return &Lease{
		tenantID:     TenantID(tenantID),
		jobID:        JobID(jobID),
		leaseID:      leaseID,
		worker:       worker,
		capability:   Capability(need),
		payload:      payload,
		leaseExpires: expires,
	}, nil
}

func capabilitiesToStrings(caps []Capability) []string {
	out := make([]string, 0, len(caps))
	for _, c := range caps {
		if c == "" {
			continue
		}
		out = append(out, string(c))
	}
	return out
}

func metadataPredicatesToStringFilters(predicates []MetadataPredicate) ([]string, []string, error) {
	if len(predicates) == 0 {
		return nil, nil, nil
	}

	paths := make([]string, 0, len(predicates))
	values := make([]string, 0, len(predicates))
	for _, predicate := range predicates {
		if len(predicate.Path) == 0 {
			return nil, nil, wrap(ErrInvalidOptions, fmt.Errorf("metadata predicate path is required"))
		}

		pathLiteral, err := stringArrayLiteral(predicate.Path)
		if err != nil {
			return nil, nil, wrap(ErrInvalidOptions, fmt.Errorf("invalid metadata predicate path: %w", err))
		}

		stringValues := make([]string, 0, len(predicate.Values))
		for _, value := range predicate.Values {
			str, ok := value.(string)
			if !ok {
				return nil, nil, wrap(ErrInvalidOptions, fmt.Errorf("metadata predicate values must be strings"))
			}
			stringValues = append(stringValues, str)
		}
		if len(stringValues) == 0 {
			return nil, nil, wrap(ErrInvalidOptions, fmt.Errorf("metadata predicate values are required"))
		}

		valueLiteral, err := stringArrayLiteral(stringValues)
		if err != nil {
			return nil, nil, wrap(ErrInvalidOptions, fmt.Errorf("invalid metadata predicate values: %w", err))
		}

		paths = append(paths, pathLiteral)
		values = append(values, valueLiteral)
	}

	return paths, values, nil
}

func stringArrayLiteral(values []string) (string, error) {
	valuer := pq.Array(values)
	raw, err := valuer.Value()
	if err != nil {
		return "", err
	}
	literal, ok := raw.(string)
	if !ok {
		return "", fmt.Errorf("unexpected array literal type %T", raw)
	}
	return literal, nil
}

func tenantIDsToStrings(tenantIDs []TenantID) []string {
	if tenantIDs == nil {
		return nil
	}
	out := make([]string, 0, len(tenantIDs))
	for _, t := range tenantIDs {
		if t == "" {
			continue
		}
		out = append(out, string(t))
	}
	return out
}

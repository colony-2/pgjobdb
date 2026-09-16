package legacyqueue

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"
)

type stubDB struct{}

func (stubDB) QueryRowContext(context.Context, string, ...any) *sql.Row {
	return new(sql.Row)
}

func (stubDB) QueryContext(context.Context, string, ...any) (*sql.Rows, error) {
	return nil, nil
}

func TestSubmitJobValidationErrors(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	deps := JobDependencies{NextNeed: Capability("cap")}
	depsWithInvalidAlternate := JobDependencies{
		NextNeed:  Capability("cap"),
		Alternate: &AlternateNext{Need: "", After: time.Second},
	}
	depsWithNegativeAlternate := JobDependencies{
		NextNeed:  Capability("cap"),
		Alternate: &AlternateNext{Need: "alt", After: -time.Second},
	}
	cases := []struct {
		name     string
		ctx      context.Context
		db       DB
		tenantID TenantID
		jobID    JobID
		worker   WorkerID
		deps     JobDependencies
	}{
		{name: "nil db", ctx: ctx, db: nil, tenantID: "tenant", jobID: "job", worker: "w", deps: deps},
		{name: "nil ctx", ctx: nil, db: stubDB{}, tenantID: "tenant", jobID: "job", worker: "w", deps: deps},
		{name: "empty tenant", ctx: ctx, db: stubDB{}, tenantID: "", jobID: "job", worker: "w", deps: deps},
		{name: "empty job", ctx: ctx, db: stubDB{}, tenantID: "tenant", jobID: "", worker: "w", deps: deps},
		{name: "empty worker", ctx: ctx, db: stubDB{}, tenantID: "tenant", jobID: "job", worker: "", deps: deps},
		{name: "missing capability", ctx: ctx, db: stubDB{}, tenantID: "tenant", jobID: "job", worker: "w", deps: JobDependencies{}},
		{name: "alternate missing capability", ctx: ctx, db: stubDB{}, tenantID: "tenant", jobID: "job", worker: "w", deps: depsWithInvalidAlternate},
		{name: "alternate negative delay", ctx: ctx, db: stubDB{}, tenantID: "tenant", jobID: "job", worker: "w", deps: depsWithNegativeAlternate},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			if err := SubmitJob(tc.ctx, tc.db, tc.tenantID, tc.jobID, tc.deps, nil, nil, tc.worker, time.Time{}); err == nil {
				t.Fatalf("expected error for %s", tc.name)
			}
		})
	}

	t.Run("payload not object", func(t *testing.T) {
		if err := SubmitJob(ctx, stubDB{}, "tenant", "job", deps, []string{"not an object"}, nil, "w", time.Time{}); err == nil {
			t.Fatalf("expected payload validation error")
		}
	})
}

func TestGetWorkValidationErrors(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cases := []struct {
		name   string
		ctx    context.Context
		db     DB
		worker WorkerID
		caps   []Capability
	}{
		{name: "nil db", ctx: ctx, db: nil, worker: "w", caps: []Capability{"cap"}},
		{name: "nil ctx", ctx: nil, db: stubDB{}, worker: "w", caps: []Capability{"cap"}},
		{name: "empty worker", ctx: ctx, db: stubDB{}, worker: "", caps: []Capability{"cap"}},
		{name: "no caps", ctx: ctx, db: stubDB{}, worker: "w", caps: nil},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			if _, err := GetWork(tc.ctx, tc.db, tc.worker, tc.caps, nil); err == nil {
				t.Fatalf("expected error for %s", tc.name)
			}
		})
	}
}

func TestGetJobLeaseValidationErrors(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cases := []struct {
		name     string
		ctx      context.Context
		db       DB
		tenantID TenantID
		jobID    JobID
		worker   WorkerID
		caps     []Capability
	}{
		{name: "nil db", ctx: ctx, db: nil, tenantID: "tenant", jobID: "job", worker: "w", caps: []Capability{"cap"}},
		{name: "nil ctx", ctx: nil, db: stubDB{}, tenantID: "tenant", jobID: "job", worker: "w", caps: []Capability{"cap"}},
		{name: "empty tenant", ctx: ctx, db: stubDB{}, tenantID: "", jobID: "job", worker: "w", caps: []Capability{"cap"}},
		{name: "empty job", ctx: ctx, db: stubDB{}, tenantID: "tenant", jobID: "", worker: "w", caps: []Capability{"cap"}},
		{name: "empty worker", ctx: ctx, db: stubDB{}, tenantID: "tenant", jobID: "job", worker: "", caps: []Capability{"cap"}},
		{name: "no caps", ctx: ctx, db: stubDB{}, tenantID: "tenant", jobID: "job", worker: "w", caps: nil},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			if _, err := GetJobLease(tc.ctx, tc.db, tc.tenantID, tc.jobID, tc.worker, tc.caps); err == nil {
				t.Fatalf("expected error for %s", tc.name)
			}
		})
	}
}

func TestGetWorkWithOptionsValidationErrors(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	if _, err := GetWorkWithOptions(ctx, stubDB{}, "w", []Capability{"cap"}, GetWorkOptions{
		LeaseSeconds: -1,
	}); err == nil {
		t.Fatalf("expected lease seconds validation error")
	}

	if _, err := GetWorkWithOptions(ctx, stubDB{}, "w", []Capability{"cap"}, GetWorkOptions{
		MetadataEquals: []MetadataPredicate{
			{Path: nil, Values: []any{"x"}},
		},
	}); err == nil {
		t.Fatalf("expected metadata path validation error")
	}

	if _, err := GetWorkWithOptions(ctx, stubDB{}, "w", []Capability{"cap"}, GetWorkOptions{
		MetadataEquals: []MetadataPredicate{
			{Path: []string{"meta"}, Values: []any{1}},
		},
	}); err == nil {
		t.Fatalf("expected metadata string-only validation error")
	}

	if _, err := GetJobLeaseWithOptions(ctx, stubDB{}, "tenant", "job", "w", []Capability{"cap"}, GetJobLeaseOptions{
		LeaseSeconds: -1,
	}); err == nil {
		t.Fatalf("expected get job lease lease seconds validation error")
	}
}

func TestAwaitWorkValidation(t *testing.T) {
	t.Parallel()
	if _, err := AwaitWork(nil, stubDB{}, WorkerID("w"), []Capability{"cap"}, nil); err == nil {
		t.Fatalf("expected nil context error")
	}
	if _, err := AwaitWorkWithOptions(nil, stubDB{}, WorkerID("w"), []Capability{"cap"}, GetWorkOptions{}); err == nil {
		t.Fatalf("expected nil context error for AwaitWorkWithOptions")
	}
}

func TestMetadataPredicatesToStringFilters(t *testing.T) {
	t.Parallel()

	paths, values, err := metadataPredicatesToStringFilters([]MetadataPredicate{
		{Path: []string{"routing", "labels"}, Values: []any{"region:us", "region:ca"}},
		{Path: []string{"source"}, Values: []any{"api"}},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(paths) != 2 || paths[0] != `{"routing","labels"}` || paths[1] != `{"source"}` {
		t.Fatalf("unexpected path literals: %#v", paths)
	}
	if len(values) != 2 || values[0] != `{"region:us","region:ca"}` || values[1] != `{"api"}` {
		t.Fatalf("unexpected value literals: %#v", values)
	}
}

func TestLeaseExtendValidation(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	lease := &Lease{tenantID: "tenant", jobID: "job", leaseID: "lease", worker: "worker", leaseExpires: time.Now().Add(time.Minute)}
	if err := lease.Extend(ctx, nil, time.Second); err == nil {
		t.Fatalf("expected nil db error")
	}
	if err := lease.Extend(ctx, stubDB{}, 0); err == nil {
		t.Fatalf("expected duration error")
	}
}

func TestLeaseRescheduleValidation(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	lease := &Lease{tenantID: "tenant", jobID: "job", leaseID: "lease", worker: "worker", leaseExpires: time.Now().Add(time.Minute)}
	if err := lease.Reschedule(ctx, stubDB{}, JobDependencies{}, nil); err == nil {
		t.Fatalf("expected validation error")
	}
	if err := lease.Reschedule(ctx, stubDB{}, JobDependencies{NextNeed: "cap"}, []string{"not an object"}); err == nil {
		t.Fatalf("expected payload validation error")
	}
}

func TestCompleteUnheldValidation(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cases := []struct {
		name     string
		ctx      context.Context
		db       DB
		tenantID TenantID
		jobID    JobID
		worker   WorkerID
	}{
		{name: "nil db", ctx: ctx, db: nil, tenantID: "tenant", jobID: "job", worker: "worker"},
		{name: "nil ctx", ctx: nil, db: stubDB{}, tenantID: "tenant", jobID: "job", worker: "worker"},
		{name: "empty tenant", ctx: ctx, db: stubDB{}, tenantID: "", jobID: "job", worker: "worker"},
		{name: "empty job", ctx: ctx, db: stubDB{}, tenantID: "tenant", jobID: "", worker: "worker"},
		{name: "empty worker", ctx: ctx, db: stubDB{}, tenantID: "tenant", jobID: "job", worker: ""},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			if err := CompleteUnheldJob(tc.ctx, tc.db, tc.tenantID, tc.jobID, tc.worker); err == nil {
				t.Fatalf("expected error for %s", tc.name)
			}
		})
	}
}

func TestRescheduleUnheldValidation(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	deps := JobDependencies{NextNeed: Capability("cap")}
	cases := []struct {
		name     string
		ctx      context.Context
		db       DB
		tenantID TenantID
		jobID    JobID
		worker   WorkerID
		deps     JobDependencies
	}{
		{name: "nil db", ctx: ctx, db: nil, tenantID: "tenant", jobID: "job", worker: "worker", deps: deps},
		{name: "nil ctx", ctx: nil, db: stubDB{}, tenantID: "tenant", jobID: "job", worker: "worker", deps: deps},
		{name: "empty tenant", ctx: ctx, db: stubDB{}, tenantID: "", jobID: "job", worker: "worker", deps: deps},
		{name: "empty job", ctx: ctx, db: stubDB{}, tenantID: "tenant", jobID: "", worker: "worker", deps: deps},
		{name: "empty worker", ctx: ctx, db: stubDB{}, tenantID: "tenant", jobID: "job", worker: "", deps: deps},
		{name: "missing deps", ctx: ctx, db: stubDB{}, tenantID: "tenant", jobID: "job", worker: "worker", deps: JobDependencies{}},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			if err := RescheduleUnheldJob(tc.ctx, tc.db, tc.tenantID, tc.jobID, tc.worker, tc.deps, nil); err == nil {
				t.Fatalf("expected error for %s", tc.name)
			}
		})
	}

	t.Run("invalid payload", func(t *testing.T) {
		if err := RescheduleUnheldJob(ctx, stubDB{}, "tenant", "job", "worker", deps, []string{"not an object"}); err == nil {
			t.Fatalf("expected payload validation error")
		}
	})
}

func TestCancelJobValidation(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cases := []struct {
		name     string
		ctx      context.Context
		db       DB
		tenantID TenantID
		jobID    JobID
		worker   WorkerID
	}{
		{name: "nil db", ctx: ctx, db: nil, tenantID: "tenant", jobID: "job", worker: "worker"},
		{name: "nil ctx", ctx: nil, db: stubDB{}, tenantID: "tenant", jobID: "job", worker: "worker"},
		{name: "empty tenant", ctx: ctx, db: stubDB{}, tenantID: "", jobID: "job", worker: "worker"},
		{name: "empty job", ctx: ctx, db: stubDB{}, tenantID: "tenant", jobID: "", worker: "worker"},
		{name: "empty worker", ctx: ctx, db: stubDB{}, tenantID: "tenant", jobID: "job", worker: ""},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			if err := CancelJob(tc.ctx, tc.db, tc.tenantID, tc.jobID, tc.worker, "reason"); err == nil {
				t.Fatalf("expected error for %s", tc.name)
			}
		})
	}
}

func TestLeaseValidateActiveFailures(t *testing.T) {
	t.Parallel()
	if err := (*Lease)(nil).validateActive(); err == nil {
		t.Fatalf("expected error for nil lease")
	}
	lease := &Lease{leaseExpires: time.Now().Add(-time.Second)}
	if err := lease.validateActive(); !errors.Is(err, ErrLeaseExpired) {
		t.Fatalf("expected ErrLeaseExpired, got %v", err)
	}
}

func TestLeaseAccessorsNil(t *testing.T) {
	t.Parallel()
	var lease *Lease
	if lease.JobID() != "" || lease.LeaseID() != "" {
		t.Fatalf("expected empty values for nil lease")
	}
	if !lease.LeaseExpiry().IsZero() {
		t.Fatalf("expected zero time for nil lease")
	}
	if lease.NextNeed() != "" {
		t.Fatalf("expected empty capability for nil lease")
	}
}

func TestSecondsForExtension(t *testing.T) {
	t.Parallel()
	lease := &Lease{leaseExpires: time.Now().Add(-time.Second)}
	if got := lease.secondsForExtension(500 * time.Millisecond); got < 1 {
		t.Fatalf("seconds should round up, got %d", got)
	}
	lease2 := &Lease{leaseExpires: time.Now().Add(2 * time.Second)}
	if got := lease2.secondsForExtension(0); got != 3 {
		t.Fatalf("expected rounded seconds to include minimum additional second, got %d", got)
	}
}

func TestSentinelHelpers(t *testing.T) {
	t.Parallel()
	base := errors.New("base")
	marker := errors.New("marker")
	wrapped := wrap(marker, base)
	if !errors.Is(wrapped, marker) || !errors.Is(wrapped, base) {
		t.Fatalf("wrap should retain marker and base")
	}
	if wrap(marker, nil) != marker {
		t.Fatalf("wrap should return marker when err nil")
	}
	if wrap(nil, base) != base {
		t.Fatalf("wrap should return err when marker nil")
	}
	se := sentinelError{marker: marker}
	if se.Error() == "" {
		t.Fatalf("expected marker error text")
	}
	if se.Unwrap() != nil {
		t.Fatalf("expected nil cause")
	}
	if annotateError(nil) != nil {
		t.Fatalf("nil passthrough")
	}
	tests := []struct {
		msg  string
		want error
	}{
		{msg: "references unknown jobs", want: ErrDependencyViolation},
		{msg: "not currently leased", want: ErrLeaseMismatch},
		{msg: "has expired", want: ErrLeaseExpired},
		{msg: "object not found", want: ErrJobNotFound},
	}
	for _, tc := range tests {
		if err := annotateError(errors.New(tc.msg)); !errors.Is(err, tc.want) {
			t.Fatalf("expected %v for %q, got %v", tc.want, tc.msg, err)
		}
	}
}

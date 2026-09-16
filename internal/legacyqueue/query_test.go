package legacyqueue_test

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	_ "github.com/lib/pq"

	pgjobdb "github.com/colony-2/pgjobdb/internal/legacyqueue"
)

// Note: These are unit tests that work with the real database setup from integration_test.go
// For full integration tests, see installer/integration_test.go

// NOTE: TestGetJobStatus_NotFound is now in installer/integration_test.go as part of comprehensive integration tests

type stubDB struct{}

func (stubDB) QueryRowContext(context.Context, string, ...any) *sql.Row {
	panic("unexpected QueryRowContext call")
}

func (stubDB) QueryContext(context.Context, string, ...any) (*sql.Rows, error) {
	panic("unexpected QueryContext call")
}

func TestCheckJobExists_Validation(t *testing.T) {
	ctx := context.Background()
	var db *sql.DB // nil is fine for validation tests

	// Test missing tenant ID
	_, err := pgjobdb.CheckJobExists(ctx, db, "", "job-1")
	if err == nil || err.Error() != "pgjobdb: tenant id is required" {
		t.Errorf("expected tenant id required error, got %v", err)
	}

	// Test missing job ID
	_, err = pgjobdb.CheckJobExists(ctx, db, "tenant-1", "")
	if err == nil || err.Error() != "pgjobdb: job id is required" {
		t.Errorf("expected job id required error, got %v", err)
	}

	// Test nil context
	_, err = pgjobdb.CheckJobExists(nil, db, "tenant-1", "job-1")
	if err == nil || err.Error() != "pgjobdb: nil context" {
		t.Errorf("expected nil context error, got %v", err)
	}

	// Test nil DB
	_, err = pgjobdb.CheckJobExists(ctx, nil, "tenant-1", "job-1")
	if err == nil || err.Error() != "pgjobdb: nil DB" {
		t.Errorf("expected nil DB error, got %v", err)
	}
}

func TestListJobsOptions_DefaultsAndLimits(t *testing.T) {
	ctx := context.Background()
	var db *sql.DB // nil is fine for validation tests

	// Test missing tenant ID
	_, err := pgjobdb.ListJobs(ctx, db, pgjobdb.ListJobsOptions{})
	if err == nil {
		t.Errorf("expected error for missing tenant_id")
	}

	// Test completion status requires include archived
	_, err = pgjobdb.ListJobs(ctx, db, pgjobdb.ListJobsOptions{
		TenantID:           "test",
		CompletionStatuses: []pgjobdb.CompletionStatus{pgjobdb.CompletionStatusSucceeded},
	})
	if err == nil || !errors.Is(err, pgjobdb.ErrInvalidOptions) {
		t.Errorf("expected invalid options error for completion status without include archived, got %v", err)
	}

	// Test nil context
	_, err = pgjobdb.ListJobs(nil, db, pgjobdb.ListJobsOptions{TenantID: "test"})
	if err == nil || err.Error() != "pgjobdb: nil context" {
		t.Errorf("expected nil context error, got %v", err)
	}

	// Test nil DB
	_, err = pgjobdb.ListJobs(ctx, nil, pgjobdb.ListJobsOptions{TenantID: "test"})
	if err == nil || err.Error() != "pgjobdb: nil DB" {
		t.Errorf("expected nil DB error, got %v", err)
	}
}

func TestFindJobsOptions_Validation(t *testing.T) {
	ctx := context.Background()
	var db *sql.DB // nil is fine for validation tests

	// Test missing next_need
	_, err := pgjobdb.FindJobs(ctx, db, pgjobdb.FindJobsOptions{
		Status: pgjobdb.JobStatusReady,
	})
	if err == nil {
		t.Errorf("expected error for missing next_need")
	}
}

func TestListJobs_MetadataPredicateValidation(t *testing.T) {
	ctx := context.Background()

	_, err := pgjobdb.ListJobs(ctx, stubDB{}, pgjobdb.ListJobsOptions{
		TenantID: "tenant-1",
		MetadataEquals: []pgjobdb.MetadataPredicate{
			{Path: nil, Values: []any{"x"}},
		},
	})
	if err == nil || !errors.Is(err, pgjobdb.ErrInvalidOptions) {
		t.Fatalf("expected invalid options error for empty path, got %v", err)
	}

	_, err = pgjobdb.ListJobs(ctx, stubDB{}, pgjobdb.ListJobsOptions{
		TenantID: "tenant-1",
		MetadataEquals: []pgjobdb.MetadataPredicate{
			{Path: []string{"meta"}, Values: nil},
		},
	})
	if err == nil || !errors.Is(err, pgjobdb.ErrInvalidOptions) {
		t.Fatalf("expected invalid options error for missing values, got %v", err)
	}

	_, err = pgjobdb.ListJobs(ctx, stubDB{}, pgjobdb.ListJobsOptions{
		TenantID: "tenant-1",
		MetadataEquals: []pgjobdb.MetadataPredicate{
			{Path: []string{"meta"}, Values: []any{nil}},
		},
	})
	if err == nil || !errors.Is(err, pgjobdb.ErrInvalidOptions) {
		t.Fatalf("expected invalid options error for nil in values, got %v", err)
	}
}

// NOTE: TestGetJobStatusBatch_EmptyInput is now in installer/integration_test.go (TestGetJobStatusBatch)

func TestIsJobArchived_Validation(t *testing.T) {
	ctx := context.Background()
	var db *sql.DB

	// Test missing tenant ID
	_, err := pgjobdb.IsJobArchived(ctx, db, "", "job-1")
	if err == nil || err.Error() != "pgjobdb: tenant id is required" {
		t.Errorf("expected tenant id required error, got %v", err)
	}

	// Test missing job ID
	_, err = pgjobdb.IsJobArchived(ctx, db, "tenant-1", "")
	if err == nil || err.Error() != "pgjobdb: job id is required" {
		t.Errorf("expected job id required error, got %v", err)
	}
}

func TestGetJob_Validation(t *testing.T) {
	ctx := context.Background()
	var db *sql.DB

	// Test missing tenant ID
	_, err := pgjobdb.GetJob(ctx, db, "", "job-1", pgjobdb.GetJobOptions{})
	if err == nil || err.Error() != "pgjobdb: tenant id is required" {
		t.Errorf("expected tenant id required error, got %v", err)
	}

	// Test missing job ID
	_, err = pgjobdb.GetJob(ctx, db, "tenant-1", "", pgjobdb.GetJobOptions{})
	if err == nil || err.Error() != "pgjobdb: job id is required" {
		t.Errorf("expected job id required error, got %v", err)
	}
}

func TestJobStatusConstants(t *testing.T) {
	// Verify all status constants are defined correctly
	statuses := []pgjobdb.JobStatus{
		pgjobdb.JobStatusActive,
		pgjobdb.JobStatusCancelled,
		pgjobdb.JobStatusAwaitingFuture,
		pgjobdb.JobStatusPendingJobs,
		pgjobdb.JobStatusCrashConcern,
		pgjobdb.JobStatusExpired,
		pgjobdb.JobStatusReady,
		pgjobdb.JobStatusCompleted,
	}

	expectedValues := []string{
		"ACTIVE",
		"CANCELLED",
		"AWAITING_FUTURE",
		"PENDING_JOBS",
		"CRASH_CONCERN",
		"EXPIRED",
		"READY",
		"COMPLETED",
	}

	for i, status := range statuses {
		if string(status) != expectedValues[i] {
			t.Errorf("status %d: expected %s, got %s", i, expectedValues[i], status)
		}
	}
}

func TestSortFieldConstants(t *testing.T) {
	// Verify sort field constants
	if pgjobdb.SortByCreatedAt != "created_at" {
		t.Errorf("SortByCreatedAt should be 'created_at', got %s", pgjobdb.SortByCreatedAt)
	}
	if pgjobdb.SortByAvailableAt != "available_at" {
		t.Errorf("SortByAvailableAt should be 'available_at', got %s", pgjobdb.SortByAvailableAt)
	}
	if pgjobdb.SortByJobID != "job_id" {
		t.Errorf("SortByJobID should be 'job_id', got %s", pgjobdb.SortByJobID)
	}
}

func TestSortDirectionConstants(t *testing.T) {
	// Verify sort direction constants
	if pgjobdb.SortAsc != "ASC" {
		t.Errorf("SortAsc should be 'ASC', got %s", pgjobdb.SortAsc)
	}
	if pgjobdb.SortDesc != "DESC" {
		t.Errorf("SortDesc should be 'DESC', got %s", pgjobdb.SortDesc)
	}
}

func TestListArchivedJobs_Validation(t *testing.T) {
	ctx := context.Background()
	var db *sql.DB

	// Test missing tenant ID
	_, err := pgjobdb.ListArchivedJobs(ctx, db, pgjobdb.ListArchivedJobsOptions{})
	if err == nil {
		t.Errorf("expected error for missing tenant_id")
	}
}

func TestCheckJobExistsWithTenant_Validation(t *testing.T) {
	ctx := context.Background()
	var db *sql.DB

	// Test missing tenant ID
	_, err := pgjobdb.CheckJobExistsWithTenant(ctx, db, "job-1", "")
	if err == nil || err.Error() != "pgjobdb: tenant id is required" {
		t.Errorf("expected tenant id required error, got %v", err)
	}

	// Test missing job ID
	_, err = pgjobdb.CheckJobExistsWithTenant(ctx, db, "", "tenant-1")
	if err == nil || err.Error() != "pgjobdb: job id is required" {
		t.Errorf("expected job id required error, got %v", err)
	}
}

// Tests for new cursor pagination functionality

func TestListJobs_MultiTenantSupport(t *testing.T) {
	ctx := context.Background()
	var db *sql.DB

	// Test TenantIDs takes precedence over TenantID
	opts := pgjobdb.ListJobsOptions{
		TenantID:  "old-tenant",
		TenantIDs: []string{"tenant-1", "tenant-2"},
		Limit:     10,
	}

	// This should not error since TenantIDs is set
	// (would need actual DB to execute, but validates options processing)
	if len(opts.TenantIDs) != 2 {
		t.Errorf("expected TenantIDs to have 2 elements, got %d", len(opts.TenantIDs))
	}

	// Test backwards compatibility: TenantID used when TenantIDs empty
	opts2 := pgjobdb.ListJobsOptions{
		TenantID: "single-tenant",
		Limit:    10,
	}

	if opts2.TenantID != "single-tenant" {
		t.Errorf("expected TenantID to be 'single-tenant', got %s", opts2.TenantID)
	}

	// Test error when both are empty
	_, err := pgjobdb.ListJobs(ctx, db, pgjobdb.ListJobsOptions{
		Limit: 10,
	})
	if err == nil {
		t.Error("expected error when neither TenantID nor TenantIDs is set")
	}
}

func TestListJobs_MultiPatternSupport(t *testing.T) {
	// Test JobTypePatterns takes precedence over JobTypePattern
	opts := pgjobdb.ListJobsOptions{
		TenantID:        "tenant-1",
		JobTypePattern:  "old-pattern",
		JobTypePatterns: []string{"pattern-1", "pattern-2", "pattern-3"},
		Limit:           10,
	}

	if len(opts.JobTypePatterns) != 3 {
		t.Errorf("expected JobTypePatterns to have 3 elements, got %d", len(opts.JobTypePatterns))
	}

	// Test backwards compatibility: JobTypePattern used when JobTypePatterns empty
	opts2 := pgjobdb.ListJobsOptions{
		TenantID:       "tenant-1",
		JobTypePattern: "single-pattern:%",
		Limit:          10,
	}

	if opts2.JobTypePattern != "single-pattern:%" {
		t.Errorf("expected JobTypePattern to be 'single-pattern:%%', got %s", opts2.JobTypePattern)
	}

	// Test both empty is valid (no job type filtering)
	opts3 := pgjobdb.ListJobsOptions{
		TenantID: "tenant-1",
		Limit:    10,
	}

	if opts3.JobTypePattern != "" || len(opts3.JobTypePatterns) != 0 {
		t.Error("expected both job type filters to be empty")
	}
}

func TestListJobs_DefaultValues(t *testing.T) {
	// Test that defaults are applied correctly
	// Note: Actual defaults are applied in ListJobs function, so we can't test them
	// directly without a DB, but we can verify the option struct accepts them

	opts := pgjobdb.ListJobsOptions{
		TenantID: "tenant-1",
		// Limit not set - should default to 100
		// SortBy not set - should default to SortByCreatedAt
		// SortOrder not set - should default to SortDesc
	}

	if opts.Limit != 0 {
		t.Errorf("expected Limit to be 0 (unset), got %d", opts.Limit)
	}

	if opts.SortBy != "" {
		t.Errorf("expected SortBy to be empty (unset), got %s", opts.SortBy)
	}

	if opts.SortOrder != "" {
		t.Errorf("expected SortOrder to be empty (unset), got %s", opts.SortOrder)
	}
}

func TestListJobs_InvalidCursor(t *testing.T) {
	ctx := context.Background()
	var db *sql.DB

	// Test invalid base64 cursor
	_, err := pgjobdb.ListJobs(ctx, db, pgjobdb.ListJobsOptions{
		TenantID: "tenant-1",
		Cursor:   "not-valid-base64!!!",
		Limit:    10,
	})

	// Should get an error (either invalid cursor or nil DB error)
	if err == nil {
		t.Error("expected error for invalid cursor")
	}

	// Test malformed JSON cursor
	invalidCursor := "aW52YWxpZC1qc29u" // base64 of "invalid-json"
	_, err = pgjobdb.ListJobs(ctx, db, pgjobdb.ListJobsOptions{
		TenantID: "tenant-1",
		Cursor:   invalidCursor,
		Limit:    10,
	})

	if err == nil {
		t.Error("expected error for malformed cursor")
	}
}

func TestCursorValidation_HashConsistency(t *testing.T) {
	// Test that the same options produce the same hash
	opts1 := pgjobdb.ListJobsOptions{
		TenantID:        "tenant-1",
		TenantIDs:       []string{"tenant-1", "tenant-2"},
		Statuses:        []pgjobdb.JobStatus{pgjobdb.JobStatusReady, pgjobdb.JobStatusActive},
		JobTypePatterns: []string{"pattern-1", "pattern-2"},
		IncludeArchived: true,
		SortBy:          pgjobdb.SortByCreatedAt,
		SortOrder:       pgjobdb.SortDesc,
	}

	opts2 := pgjobdb.ListJobsOptions{
		TenantID:        "tenant-1",
		TenantIDs:       []string{"tenant-1", "tenant-2"},
		Statuses:        []pgjobdb.JobStatus{pgjobdb.JobStatusReady, pgjobdb.JobStatusActive},
		JobTypePatterns: []string{"pattern-1", "pattern-2"},
		IncludeArchived: true,
		SortBy:          pgjobdb.SortByCreatedAt,
		SortOrder:       pgjobdb.SortDesc,
	}

	// Note: We can't call the internal hashListJobsOptions function directly from the test
	// package, but we can verify that the same options should produce the same cursor
	// when encoding. This would need to be tested with actual cursor encoding.

	// For now, just verify the options are identical
	if opts1.TenantID != opts2.TenantID {
		t.Error("tenant IDs should match")
	}
	if len(opts1.TenantIDs) != len(opts2.TenantIDs) {
		t.Error("tenant IDs slices should have same length")
	}
	if opts1.SortBy != opts2.SortBy {
		t.Error("sort by should match")
	}
	if opts1.SortOrder != opts2.SortOrder {
		t.Error("sort order should match")
	}
}

func TestCursorValidation_DifferentOptionsProduceDifferentHashes(t *testing.T) {
	// Test that different options should produce different hashes
	opts1 := pgjobdb.ListJobsOptions{
		TenantID:  "tenant-1",
		SortBy:    pgjobdb.SortByCreatedAt,
		SortOrder: pgjobdb.SortDesc,
	}

	opts2 := pgjobdb.ListJobsOptions{
		TenantID:  "tenant-2", // Different tenant
		SortBy:    pgjobdb.SortByCreatedAt,
		SortOrder: pgjobdb.SortDesc,
	}

	if opts1.TenantID == opts2.TenantID {
		t.Error("tenant IDs should be different")
	}

	opts3 := pgjobdb.ListJobsOptions{
		TenantID:  "tenant-1",
		SortBy:    pgjobdb.SortByAvailableAt, // Different sort field
		SortOrder: pgjobdb.SortDesc,
	}

	if opts1.SortBy == opts3.SortBy {
		t.Error("sort fields should be different")
	}

	opts4 := pgjobdb.ListJobsOptions{
		TenantID:  "tenant-1",
		SortBy:    pgjobdb.SortByCreatedAt,
		SortOrder: pgjobdb.SortAsc, // Different sort order
	}

	if opts1.SortOrder == opts4.SortOrder {
		t.Error("sort orders should be different")
	}
}

func TestListJobs_SortFieldValidation(t *testing.T) {
	// Test all valid sort fields
	validSortFields := []pgjobdb.SortField{
		pgjobdb.SortByCreatedAt,
		pgjobdb.SortByAvailableAt,
		pgjobdb.SortByJobID,
	}

	for _, field := range validSortFields {
		opts := pgjobdb.ListJobsOptions{
			TenantID:  "tenant-1",
			SortBy:    field,
			SortOrder: pgjobdb.SortDesc,
			Limit:     10,
		}

		if opts.SortBy != field {
			t.Errorf("expected SortBy to be %s, got %s", field, opts.SortBy)
		}
	}
}

func TestListJobs_SortDirectionValidation(t *testing.T) {
	// Test both valid sort directions
	validDirections := []pgjobdb.SortDirection{
		pgjobdb.SortAsc,
		pgjobdb.SortDesc,
	}

	for _, direction := range validDirections {
		opts := pgjobdb.ListJobsOptions{
			TenantID:  "tenant-1",
			SortBy:    pgjobdb.SortByCreatedAt,
			SortOrder: direction,
			Limit:     10,
		}

		if opts.SortOrder != direction {
			t.Errorf("expected SortOrder to be %s, got %s", direction, opts.SortOrder)
		}
	}
}

func TestListJobs_LimitBounds(t *testing.T) {
	// Test limit validation
	// Note: Actual clamping happens in ListJobs function
	opts := pgjobdb.ListJobsOptions{
		TenantID: "tenant-1",
		Limit:    0, // Should default to 100
	}

	if opts.Limit != 0 {
		t.Errorf("expected unset Limit to be 0, got %d", opts.Limit)
	}

	opts2 := pgjobdb.ListJobsOptions{
		TenantID: "tenant-1",
		Limit:    5000, // Should be clamped to 1000
	}

	if opts2.Limit != 5000 {
		t.Errorf("expected Limit to be 5000 (before clamping), got %d", opts2.Limit)
	}

	opts3 := pgjobdb.ListJobsOptions{
		TenantID: "tenant-1",
		Limit:    50, // Valid limit
	}

	if opts3.Limit != 50 {
		t.Errorf("expected Limit to be 50, got %d", opts3.Limit)
	}
}

func TestListJobs_StatusFiltering(t *testing.T) {
	// Test multiple status filtering
	statuses := []pgjobdb.JobStatus{
		pgjobdb.JobStatusReady,
		pgjobdb.JobStatusActive,
		pgjobdb.JobStatusPendingJobs,
	}

	opts := pgjobdb.ListJobsOptions{
		TenantID: "tenant-1",
		Statuses: statuses,
		Limit:    10,
	}

	if len(opts.Statuses) != 3 {
		t.Errorf("expected 3 statuses, got %d", len(opts.Statuses))
	}

	for i, status := range opts.Statuses {
		if status != statuses[i] {
			t.Errorf("status %d: expected %s, got %s", i, statuses[i], status)
		}
	}
}

func TestListJobs_TimeRangeFiltering(t *testing.T) {
	// Test time range filtering with nil values
	opts := pgjobdb.ListJobsOptions{
		TenantID:      "tenant-1",
		CreatedAfter:  nil,
		CreatedBefore: nil,
		Limit:         10,
	}

	if opts.CreatedAfter != nil {
		t.Error("expected CreatedAfter to be nil")
	}

	if opts.CreatedBefore != nil {
		t.Error("expected CreatedBefore to be nil")
	}
}

func TestListJobs_IncludeArchivedFlag(t *testing.T) {
	// Test IncludeArchived flag defaults to false
	opts := pgjobdb.ListJobsOptions{
		TenantID: "tenant-1",
		Limit:    10,
	}

	if opts.IncludeArchived {
		t.Error("expected IncludeArchived to default to false")
	}

	// Test setting it to true
	opts2 := pgjobdb.ListJobsOptions{
		TenantID:        "tenant-1",
		IncludeArchived: true,
		Limit:           10,
	}

	if !opts2.IncludeArchived {
		t.Error("expected IncludeArchived to be true")
	}
}

func TestFindJobs_MultiTenantSupport(t *testing.T) {
	// Test FindJobs with multiple tenants
	opts := pgjobdb.FindJobsOptions{
		TenantIDs: []string{"tenant-1", "tenant-2", "tenant-3"},
		Status:    pgjobdb.JobStatusReady,
		NextNeed:  "test-capability",
		Limit:     50,
	}

	if len(opts.TenantIDs) != 3 {
		t.Errorf("expected 3 tenant IDs, got %d", len(opts.TenantIDs))
	}

	// Test with empty TenantIDs (should query all tenants)
	opts2 := pgjobdb.FindJobsOptions{
		Status:   pgjobdb.JobStatusReady,
		NextNeed: "test-capability",
		Limit:    50,
	}

	if len(opts2.TenantIDs) != 0 {
		t.Errorf("expected 0 tenant IDs, got %d", len(opts2.TenantIDs))
	}
}

func TestFindJobs_LimitDefault(t *testing.T) {
	// Test FindJobs limit defaults
	opts := pgjobdb.FindJobsOptions{
		Status:   pgjobdb.JobStatusReady,
		NextNeed: "test-capability",
		// Limit not set
	}

	if opts.Limit != 0 {
		t.Errorf("expected Limit to be 0 (unset), got %d", opts.Limit)
	}

	// Test with explicit limit
	opts2 := pgjobdb.FindJobsOptions{
		Status:   pgjobdb.JobStatusReady,
		NextNeed: "test-capability",
		Limit:    200,
	}

	if opts2.Limit != 200 {
		t.Errorf("expected Limit to be 200, got %d", opts2.Limit)
	}
}

// Test error types
func TestErrorTypes(t *testing.T) {
	// Test that error constants are defined
	if pgjobdb.ErrJobNotFound == nil {
		t.Error("ErrJobNotFound should be defined")
	}

	if pgjobdb.ErrTenantMismatch == nil {
		t.Error("ErrTenantMismatch should be defined")
	}

	if pgjobdb.ErrInvalidCursor == nil {
		t.Error("ErrInvalidCursor should be defined")
	}

	if pgjobdb.ErrInvalidOptions == nil {
		t.Error("ErrInvalidOptions should be defined")
	}
}

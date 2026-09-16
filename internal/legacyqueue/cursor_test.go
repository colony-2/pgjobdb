package legacyqueue

import (
	"encoding/base64"
	"encoding/json"
	"testing"
	"time"
)

// Tests for cursor encoding/decoding (internal functions)

func TestEncodeCursor_Success(t *testing.T) {
	now := time.Now()
	job := &JobListItem{
		JobID:       "test-job-123",
		CreatedAt:   now,
		AvailableAt: now.Add(-1 * time.Hour),
	}

	opts := ListJobsOptions{
		TenantID:  "tenant-1",
		SortBy:    SortByCreatedAt,
		SortOrder: SortDesc,
	}

	cursor, err := encodeCursor(job, opts)
	if err != nil {
		t.Fatalf("encodeCursor failed: %v", err)
	}

	if cursor == "" {
		t.Error("expected non-empty cursor")
	}

	// Verify it's valid base64
	decoded, err := base64.URLEncoding.DecodeString(cursor)
	if err != nil {
		t.Errorf("cursor is not valid base64: %v", err)
	}

	// Verify it's valid JSON
	var pc paginationCursor
	if err := json.Unmarshal(decoded, &pc); err != nil {
		t.Errorf("cursor is not valid JSON: %v", err)
	}

	// Verify cursor contents
	if pc.LastJobID != job.JobID {
		t.Errorf("expected LastJobID to be %s, got %s", job.JobID, pc.LastJobID)
	}

	if pc.SortBy != opts.SortBy {
		t.Errorf("expected SortBy to be %s, got %s", opts.SortBy, pc.SortBy)
	}

	if pc.SortOrder != opts.SortOrder {
		t.Errorf("expected SortOrder to be %s, got %s", opts.SortOrder, pc.SortOrder)
	}

	if pc.QueryHash == "" {
		t.Error("expected QueryHash to be non-empty")
	}
}

func TestEncodeCursor_NilJob(t *testing.T) {
	opts := ListJobsOptions{
		TenantID:  "tenant-1",
		SortBy:    SortByCreatedAt,
		SortOrder: SortDesc,
	}

	cursor, err := encodeCursor(nil, opts)
	if err != nil {
		t.Fatalf("encodeCursor with nil job should not error: %v", err)
	}

	if cursor != "" {
		t.Error("expected empty cursor for nil job")
	}
}

func TestEncodeCursor_DifferentSortFields(t *testing.T) {
	now := time.Now()
	job := &JobListItem{
		JobID:       "test-job",
		CreatedAt:   now,
		AvailableAt: now.Add(-1 * time.Hour),
	}

	testCases := []struct {
		name      string
		sortBy    SortField
		expectVal string
	}{
		{
			name:      "SortByCreatedAt",
			sortBy:    SortByCreatedAt,
			expectVal: job.CreatedAt.Format(time.RFC3339Nano),
		},
		{
			name:      "SortByAvailableAt",
			sortBy:    SortByAvailableAt,
			expectVal: job.AvailableAt.Format(time.RFC3339Nano),
		},
		{
			name:      "SortByJobID",
			sortBy:    SortByJobID,
			expectVal: job.JobID,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			opts := ListJobsOptions{
				TenantID:  "tenant-1",
				SortBy:    tc.sortBy,
				SortOrder: SortDesc,
			}

			cursor, err := encodeCursor(job, opts)
			if err != nil {
				t.Fatalf("encodeCursor failed: %v", err)
			}

			// Decode and verify
			decoded, _ := base64.URLEncoding.DecodeString(cursor)
			var pc paginationCursor
			json.Unmarshal(decoded, &pc)

			if pc.LastSortValue != tc.expectVal {
				t.Errorf("expected LastSortValue to be %s, got %s", tc.expectVal, pc.LastSortValue)
			}
		})
	}
}

func TestDecodeCursor_Success(t *testing.T) {
	// Create a valid cursor
	now := time.Now()
	job := &JobListItem{
		JobID:     "test-job",
		CreatedAt: now,
	}

	opts := ListJobsOptions{
		TenantID:  "tenant-1",
		SortBy:    SortByCreatedAt,
		SortOrder: SortDesc,
	}

	// Encode it
	cursorStr, err := encodeCursor(job, opts)
	if err != nil {
		t.Fatalf("encodeCursor failed: %v", err)
	}

	// Decode it
	cursor, err := decodeCursor(cursorStr, opts)
	if err != nil {
		t.Fatalf("decodeCursor failed: %v", err)
	}

	if cursor == nil {
		t.Fatal("expected non-nil cursor")
	}

	if cursor.LastJobID != job.JobID {
		t.Errorf("expected LastJobID to be %s, got %s", job.JobID, cursor.LastJobID)
	}
}

func TestDecodeCursor_EmptyString(t *testing.T) {
	opts := ListJobsOptions{
		TenantID:  "tenant-1",
		SortBy:    SortByCreatedAt,
		SortOrder: SortDesc,
	}

	cursor, err := decodeCursor("", opts)
	if err != nil {
		t.Fatalf("decodeCursor with empty string should not error: %v", err)
	}

	if cursor != nil {
		t.Error("expected nil cursor for empty string")
	}
}

func TestDecodeCursor_InvalidBase64(t *testing.T) {
	opts := ListJobsOptions{
		TenantID:  "tenant-1",
		SortBy:    SortByCreatedAt,
		SortOrder: SortDesc,
	}

	_, err := decodeCursor("not-valid-base64!!!", opts)
	if err == nil {
		t.Error("expected error for invalid base64")
	}

	if !isWrappedError(err, ErrInvalidCursor) {
		t.Errorf("expected ErrInvalidCursor, got %v", err)
	}
}

func TestDecodeCursor_InvalidJSON(t *testing.T) {
	opts := ListJobsOptions{
		TenantID:  "tenant-1",
		SortBy:    SortByCreatedAt,
		SortOrder: SortDesc,
	}

	// Create invalid JSON encoded as base64
	invalidJSON := base64.URLEncoding.EncodeToString([]byte("{invalid json"))

	_, err := decodeCursor(invalidJSON, opts)
	if err == nil {
		t.Error("expected error for invalid JSON")
	}

	if !isWrappedError(err, ErrInvalidCursor) {
		t.Errorf("expected ErrInvalidCursor, got %v", err)
	}
}

func TestDecodeCursor_QueryHashMismatch(t *testing.T) {
	// Create a cursor with one set of options
	now := time.Now()
	job := &JobListItem{
		JobID:     "test-job",
		CreatedAt: now,
	}

	opts1 := ListJobsOptions{
		TenantID:  "tenant-1",
		SortBy:    SortByCreatedAt,
		SortOrder: SortDesc,
	}

	cursorStr, err := encodeCursor(job, opts1)
	if err != nil {
		t.Fatalf("encodeCursor failed: %v", err)
	}

	// Try to decode with different options
	opts2 := ListJobsOptions{
		TenantID:  "tenant-2", // Different tenant
		SortBy:    SortByCreatedAt,
		SortOrder: SortDesc,
	}

	_, err = decodeCursor(cursorStr, opts2)
	if err == nil {
		t.Error("expected error for query hash mismatch")
	}

	if !isWrappedError(err, ErrInvalidCursor) {
		t.Errorf("expected ErrInvalidCursor, got %v", err)
	}
}

func TestDecodeCursor_SortParameterMismatch(t *testing.T) {
	// Create a cursor with one set of options
	now := time.Now()
	job := &JobListItem{
		JobID:     "test-job",
		CreatedAt: now,
	}

	opts1 := ListJobsOptions{
		TenantID:  "tenant-1",
		SortBy:    SortByCreatedAt,
		SortOrder: SortDesc,
	}

	cursorStr, err := encodeCursor(job, opts1)
	if err != nil {
		t.Fatalf("encodeCursor failed: %v", err)
	}

	// Try to decode with different sort order
	opts2 := ListJobsOptions{
		TenantID:  "tenant-1",
		SortBy:    SortByCreatedAt,
		SortOrder: SortAsc, // Different order
	}

	_, err = decodeCursor(cursorStr, opts2)
	if err == nil {
		t.Error("expected error for sort parameter mismatch")
	}

	if !isWrappedError(err, ErrInvalidCursor) {
		t.Errorf("expected ErrInvalidCursor, got %v", err)
	}
}

func TestHashListJobsOptions_Consistency(t *testing.T) {
	// Same options should produce same hash
	opts1 := ListJobsOptions{
		TenantID:        "tenant-1",
		TenantIDs:       []string{"t1", "t2"},
		Statuses:        []JobStatus{JobStatusReady, JobStatusActive},
		JobTypePatterns: []string{"p1", "p2"},
		IncludeArchived: true,
		SortBy:          SortByCreatedAt,
		SortOrder:       SortDesc,
	}

	opts2 := ListJobsOptions{
		TenantID:        "tenant-1",
		TenantIDs:       []string{"t1", "t2"},
		Statuses:        []JobStatus{JobStatusReady, JobStatusActive},
		JobTypePatterns: []string{"p1", "p2"},
		IncludeArchived: true,
		SortBy:          SortByCreatedAt,
		SortOrder:       SortDesc,
	}

	hash1 := hashListJobsOptions(opts1)
	hash2 := hashListJobsOptions(opts2)

	if hash1 != hash2 {
		t.Errorf("expected same hash for identical options, got %s and %s", hash1, hash2)
	}
}

func TestHashListJobsOptions_DifferentHashes(t *testing.T) {
	baseOpts := ListJobsOptions{
		TenantID:  "tenant-1",
		SortBy:    SortByCreatedAt,
		SortOrder: SortDesc,
	}

	testCases := []struct {
		name string
		opts ListJobsOptions
	}{
		{
			name: "different tenant",
			opts: ListJobsOptions{
				TenantID:  "tenant-2",
				SortBy:    SortByCreatedAt,
				SortOrder: SortDesc,
			},
		},
		{
			name: "different sort by",
			opts: ListJobsOptions{
				TenantID:  "tenant-1",
				SortBy:    SortByAvailableAt,
				SortOrder: SortDesc,
			},
		},
		{
			name: "different sort order",
			opts: ListJobsOptions{
				TenantID:  "tenant-1",
				SortBy:    SortByCreatedAt,
				SortOrder: SortAsc,
			},
		},
		{
			name: "additional status filter",
			opts: ListJobsOptions{
				TenantID:  "tenant-1",
				Statuses:  []JobStatus{JobStatusReady},
				SortBy:    SortByCreatedAt,
				SortOrder: SortDesc,
			},
		},
		{
			name: "different include archived",
			opts: ListJobsOptions{
				TenantID:        "tenant-1",
				IncludeArchived: true,
				SortBy:          SortByCreatedAt,
				SortOrder:       SortDesc,
			},
		},
	}

	baseHash := hashListJobsOptions(baseOpts)

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			hash := hashListJobsOptions(tc.opts)
			if hash == baseHash {
				t.Errorf("expected different hash for %s, got same hash", tc.name)
			}
		})
	}
}

func TestBuildCursorCondition_DESC(t *testing.T) {
	now := time.Now()
	cursor := &paginationCursor{
		LastSortValue: now.Format(time.RFC3339Nano),
		LastJobID:     "job-123",
		SortBy:        SortByCreatedAt,
		SortOrder:     SortDesc,
	}

	condition, args := buildCursorCondition(cursor, SortByCreatedAt, SortDesc, 1)

	if condition == "" {
		t.Error("expected non-empty condition")
	}

	if len(args) != 2 {
		t.Errorf("expected 2 args, got %d", len(args))
	}

	// Should use < operator for DESC
	if condition != "(created_at, job_id) < ($1, $2)" {
		t.Errorf("unexpected condition: %s", condition)
	}

	// First arg should be timestamp
	if _, ok := args[0].(time.Time); !ok {
		t.Errorf("first arg should be time.Time, got %T", args[0])
	}

	// Second arg should be job ID
	if args[1] != "job-123" {
		t.Errorf("expected second arg to be 'job-123', got %v", args[1])
	}
}

func TestBuildCursorCondition_ASC(t *testing.T) {
	now := time.Now()
	cursor := &paginationCursor{
		LastSortValue: now.Format(time.RFC3339Nano),
		LastJobID:     "job-456",
		SortBy:        SortByCreatedAt,
		SortOrder:     SortAsc,
	}

	condition, args := buildCursorCondition(cursor, SortByCreatedAt, SortAsc, 3)

	if condition == "" {
		t.Error("expected non-empty condition")
	}

	// Should use > operator for ASC
	if condition != "(created_at, job_id) > ($3, $4)" {
		t.Errorf("unexpected condition: %s", condition)
	}

	if args[1] != "job-456" {
		t.Errorf("expected second arg to be 'job-456', got %v", args[1])
	}
}

func TestBuildCursorCondition_JobID(t *testing.T) {
	cursor := &paginationCursor{
		LastSortValue: "job-xyz",
		LastJobID:     "job-xyz",
		SortBy:        SortByJobID,
		SortOrder:     SortDesc,
	}

	condition, args := buildCursorCondition(cursor, SortByJobID, SortDesc, 1)

	if condition == "" {
		t.Error("expected non-empty condition")
	}

	if len(args) != 2 {
		t.Errorf("expected 2 args, got %d", len(args))
	}

	// First arg should be string for job_id
	if _, ok := args[0].(string); !ok {
		t.Errorf("first arg should be string, got %T", args[0])
	}

	if args[0] != "job-xyz" {
		t.Errorf("expected first arg to be 'job-xyz', got %v", args[0])
	}
}

func TestBuildCursorCondition_NilCursor(t *testing.T) {
	condition, args := buildCursorCondition(nil, SortByCreatedAt, SortDesc, 1)

	if condition != "" {
		t.Errorf("expected empty condition for nil cursor, got %s", condition)
	}

	if len(args) != 0 {
		t.Errorf("expected 0 args for nil cursor, got %d", len(args))
	}
}

func TestBuildCursorCondition_InvalidTimestamp(t *testing.T) {
	cursor := &paginationCursor{
		LastSortValue: "not-a-timestamp",
		LastJobID:     "job-123",
		SortBy:        SortByCreatedAt,
		SortOrder:     SortDesc,
	}

	condition, args := buildCursorCondition(cursor, SortByCreatedAt, SortDesc, 1)

	// Should return empty condition for invalid timestamp
	if condition != "" {
		t.Errorf("expected empty condition for invalid timestamp, got %s", condition)
	}

	if len(args) != 0 {
		t.Errorf("expected 0 args for invalid timestamp, got %d", len(args))
	}
}

func TestJoinStrings(t *testing.T) {
	testCases := []struct {
		name     string
		strs     []string
		sep      string
		expected string
	}{
		{
			name:     "empty slice",
			strs:     []string{},
			sep:      ",",
			expected: "",
		},
		{
			name:     "single element",
			strs:     []string{"a"},
			sep:      ",",
			expected: "a",
		},
		{
			name:     "multiple elements",
			strs:     []string{"a", "b", "c"},
			sep:      ",",
			expected: "a,b,c",
		},
		{
			name:     "different separator",
			strs:     []string{"x", "y", "z"},
			sep:      " OR ",
			expected: "x OR y OR z",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := joinStrings(tc.strs, tc.sep)
			if result != tc.expected {
				t.Errorf("expected %q, got %q", tc.expected, result)
			}
		})
	}
}

// Helper function to check if an error wraps another error
func isWrappedError(err, target error) bool {
	if err == nil {
		return false
	}
	return err == target || err.Error() == target.Error() ||
		(len(err.Error()) > len(target.Error()) && err.Error()[:len(target.Error())] == target.Error())
}

package pgjobdb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/lib/pq"
)

var ErrJobDeleted = errors.New("pgjobdb: job was deleted")

// DeleteJobsRequest selects retained identities to hide, without purging history.
// Selectors intersect. All cannot be combined with selectors. Now defaults to UTC now.
type DeleteJobsRequest struct {
	TenantID           TenantID
	JobIDs             []JobID
	CreatedBefore      *time.Time
	MetadataPredicates []MetadataPredicate
	All                bool
	DryRun             bool
	Now                time.Time
}
type DeleteJobsResult struct{ MatchedCount, DeletedCount int64 }

// DeleteJobs selects and mutates the whole set in one database transaction.
func DeleteJobs(ctx context.Context, db DB, req DeleteJobsRequest) (DeleteJobsResult, error) {
	var out DeleteJobsResult
	if err := validateScheduleDB(ctx, db); err != nil {
		return out, err
	}
	if strings.TrimSpace(string(req.TenantID)) == "" {
		return out, fmt.Errorf("pgjobdb: tenant is required")
	}
	filtered := len(req.JobIDs) > 0 || req.CreatedBefore != nil || len(req.MetadataPredicates) > 0
	if req.All == filtered {
		return out, fmt.Errorf("pgjobdb: choose selectors or all, exclusively")
	}
	ids := make([]string, 0, len(req.JobIDs))
	for _, id := range req.JobIDs {
		if strings.TrimSpace(string(id)) == "" {
			return out, fmt.Errorf("pgjobdb: job ids must not be empty")
		}
		ids = append(ids, string(id))
	}
	if err := validateMetadataPredicates(req.MetadataPredicates); err != nil {
		return out, err
	}
	predicates := req.MetadataPredicates
	if predicates == nil {
		predicates = []MetadataPredicate{}
	}
	raw, err := json.Marshal(predicates)
	if err != nil {
		return out, err
	}
	var cutoff any
	if req.CreatedBefore != nil {
		if req.CreatedBefore.IsZero() {
			return out, fmt.Errorf("pgjobdb: cutoff must be nonzero")
		}
		// Stored timestamps have microsecond precision. Round an exclusive cutoff
		// upward so a boundary + 1ns still selects that boundary's stored row.
		at := req.CreatedBefore.Truncate(time.Microsecond)
		if at.Before(*req.CreatedBefore) {
			at = at.Add(time.Microsecond)
		}
		cutoff = at
	}
	if req.Now.IsZero() {
		req.Now = time.Now().UTC()
	}
	err = db.QueryRowContext(ctx, `SELECT matched_count,deleted_count FROM pgjobdb.delete_native_jobs($1,$2,$3,$4,$5,$6,$7)`,
		string(req.TenantID), pq.Array(ids), cutoff, string(raw), req.All, req.DryRun, req.Now).Scan(&out.MatchedCount, &out.DeletedCount)
	if err != nil {
		return DeleteJobsResult{}, err
	}
	return out, nil
}

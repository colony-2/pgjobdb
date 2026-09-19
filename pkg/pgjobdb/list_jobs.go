package pgjobdb

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"slices"
	"time"

	"github.com/lib/pq"
)

type jobCursor struct {
	Filter    string    `json:"f"`
	CreatedAt time.Time `json:"c"`
	TenantID  TenantID  `json:"t"`
	JobID     JobID     `json:"j"`
}

// ListJobs applies all native job filters before choosing a page.
func ListJobs(ctx context.Context, db DB, opts ListJobsOptions) (*ListJobsResult, error) {
	if err := validateScheduleDB(ctx, db); err != nil {
		return nil, err
	}
	if len(opts.TenantIDs) == 0 {
		return nil, fmt.Errorf("pgjobdb: tenant ids are required")
	}
	if opts.RootOnly && len(opts.ParentJobIDs) > 0 {
		return nil, fmt.Errorf("pgjobdb: root-only and parent filters cannot be combined")
	}
	if opts.PageSize == 0 {
		opts.PageSize = 100
	}
	if opts.PageSize < 1 || opts.PageSize > 200 {
		return nil, fmt.Errorf("pgjobdb: job page size must be between 1 and 200")
	}
	if opts.CreatedAfter != nil && opts.CreatedBefore != nil &&
		!opts.CreatedAfter.Before(*opts.CreatedBefore) {
		return nil, fmt.Errorf("pgjobdb: created-after must precede created-before")
	}
	tenantIDs := make([]string, 0, len(opts.TenantIDs))
	for _, id := range opts.TenantIDs {
		if id == "" {
			return nil, fmt.Errorf("pgjobdb: tenant id cannot be empty")
		}
		tenantIDs = append(tenantIDs, string(id))
	}
	statuses := make([]string, 0, len(opts.Statuses))
	for _, status := range opts.Statuses {
		switch status {
		case JobStatusReady, JobStatusExpired, JobStatusPendingJobs,
			JobStatusAwaitingFuture, JobStatusActive, JobStatusCrashConcern,
			JobStatusCancelled, JobStatusCompleted:
		default:
			return nil, fmt.Errorf("pgjobdb: invalid job status %q", status)
		}
		statuses = append(statuses, string(status))
	}
	stores := make([]string, 0, len(opts.Stores))
	for _, store := range opts.Stores {
		if store != JobStoreActive && store != JobStoreArchived {
			return nil, fmt.Errorf("pgjobdb: invalid job store %q", store)
		}
		stores = append(stores, string(store))
	}
	jobTypes := make([]string, 0, len(opts.JobTypes))
	for _, jobType := range opts.JobTypes {
		if !validTypeName(jobType) {
			return nil, fmt.Errorf("pgjobdb: job type filter cannot be empty")
		}
		jobTypes = append(jobTypes, string(jobType))
	}
	tasks := slices.Clone(opts.TaskSelectors)
	if tasks == nil {
		tasks = []TaskSelector{}
	}
	for _, task := range tasks {
		if !validTypeName(task.JobType) || !validTypeName(task.TaskType) {
			return nil, fmt.Errorf("pgjobdb: task selector needs job and task types")
		}
	}
	keys := slices.Clone(opts.JobKeys)
	if keys == nil {
		keys = []JobKey{}
	}
	for _, key := range keys {
		if key.TenantID == "" || key.JobID == "" {
			return nil, fmt.Errorf("pgjobdb: job key filter is incomplete")
		}
	}
	parentIDs := make([]string, 0, len(opts.ParentJobIDs))
	for _, id := range opts.ParentJobIDs {
		if id == "" {
			return nil, fmt.Errorf("pgjobdb: parent job id cannot be empty")
		}
		parentIDs = append(parentIDs, string(id))
	}
	predicates := slices.Clone(opts.MetadataPredicates)
	if predicates == nil {
		predicates = []MetadataPredicate{}
	}
	if err := validateMetadataPredicates(predicates); err != nil {
		return nil, err
	}
	slices.Sort(tenantIDs)
	slices.Sort(statuses)
	slices.Sort(stores)
	slices.Sort(jobTypes)
	slices.Sort(parentIDs)
	slices.SortFunc(tasks, func(a, b TaskSelector) int {
		if a.JobType < b.JobType {
			return -1
		}
		if a.JobType > b.JobType {
			return 1
		}
		if a.TaskType < b.TaskType {
			return -1
		}
		if a.TaskType > b.TaskType {
			return 1
		}
		return 0
	})
	slices.SortFunc(keys, func(a, b JobKey) int {
		if a.TenantID < b.TenantID {
			return -1
		}
		if a.TenantID > b.TenantID {
			return 1
		}
		if a.JobID < b.JobID {
			return -1
		}
		if a.JobID > b.JobID {
			return 1
		}
		return 0
	})
	taskJSON, _ := json.Marshal(tasks)
	keyJSON, _ := json.Marshal(keys)
	predicateJSON, err := json.Marshal(predicates)
	if err != nil {
		return nil, fmt.Errorf("pgjobdb: encode metadata predicates: %w", err)
	}
	filterJSON, err := json.Marshal([]any{
		tenantIDs, statuses, stores, jobTypes, tasks, keys, parentIDs,
		opts.RootOnly, predicates, opts.CreatedAfter, opts.CreatedBefore,
	})
	if err != nil {
		return nil, fmt.Errorf("pgjobdb: encode job filters: %w", err)
	}
	sum := sha256.Sum256(filterJSON)
	fingerprint := hex.EncodeToString(sum[:])
	var beforeCreated, beforeTenant, beforeJob any
	if opts.PageToken != "" {
		if len(opts.PageToken) > 4096 {
			return nil, fmt.Errorf("pgjobdb: invalid job page token")
		}
		data, err := base64.RawURLEncoding.DecodeString(opts.PageToken)
		if err != nil {
			return nil, fmt.Errorf("pgjobdb: invalid job page token")
		}
		var cursor jobCursor
		if json.Unmarshal(data, &cursor) != nil || cursor.Filter != fingerprint ||
			cursor.CreatedAt.IsZero() || cursor.TenantID == "" || cursor.JobID == "" {
			return nil, fmt.Errorf("pgjobdb: invalid job page token")
		}
		beforeCreated, beforeTenant, beforeJob = cursor.CreatedAt,
			string(cursor.TenantID), string(cursor.JobID)
	}
	rows, err := db.QueryContext(ctx, `SELECT * FROM pgjobdb.list_native_jobs(
		$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11,
		$12, $13, $14, $15)`,
		pq.Array(tenantIDs), pq.Array(nilIfEmpty(statuses)),
		pq.Array(nilIfEmpty(stores)), pq.Array(nilIfEmpty(jobTypes)),
		string(taskJSON), string(keyJSON), pq.Array(nilIfEmpty(parentIDs)),
		opts.RootOnly, string(predicateJSON), optionalTime(opts.CreatedAfter),
		optionalTime(opts.CreatedBefore), beforeCreated, beforeTenant, beforeJob,
		opts.PageSize+1)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := &ListJobsResult{Jobs: make([]JobDetail, 0, opts.PageSize)}
	for rows.Next() {
		var raw, payload []byte
		var revision int64
		var initialDigest string
		if err := rows.Scan(&raw, &payload, &revision, &initialDigest); err != nil {
			return nil, err
		}
		var item JobDetail
		if err := json.Unmarshal(raw, &item); err != nil {
			return nil, fmt.Errorf("pgjobdb: decode listed job: %w", err)
		}
		item.ClientPayload = append(json.RawMessage(nil), payload...)
		item.ClientPayloadRevision = revision
		item.InitialPayloadDigest = initialDigest
		result.Jobs = append(result.Jobs, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(result.Jobs) > opts.PageSize {
		result.Jobs = result.Jobs[:opts.PageSize]
		last := result.Jobs[len(result.Jobs)-1]
		data, err := json.Marshal(jobCursor{
			Filter: fingerprint, CreatedAt: last.CreatedAt,
			TenantID: last.TenantID, JobID: last.JobID,
		})
		if err != nil {
			return nil, err
		}
		result.NextPageToken = base64.RawURLEncoding.EncodeToString(data)
	}
	return result, nil
}

func validateMetadataPredicates(predicates []MetadataPredicate) error {
	for _, predicate := range predicates {
		if len(predicate.Path) == 0 || len(predicate.Values) == 0 {
			return fmt.Errorf("pgjobdb: metadata predicate requires path and values")
		}
		for _, element := range predicate.Path {
			if element == "" {
				return fmt.Errorf("pgjobdb: metadata path element cannot be empty")
			}
		}
		for _, value := range predicate.Values {
			if !json.Valid(value) {
				return fmt.Errorf("pgjobdb: metadata predicate value is invalid JSON")
			}
		}
	}
	return nil
}

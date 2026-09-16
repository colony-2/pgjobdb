package pgjobdb

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/lib/pq"
)

// SubmitJob stores immutable JobDB facts and the initial scheduler route in
// one database transaction. A matching active job ID is idempotent.
func SubmitJob(ctx context.Context, db DB, req SubmitJobRequest) (*SubmitJobResult, error) {
	if err := validateScheduleDB(ctx, db); err != nil {
		return nil, err
	}
	if req.TenantID == "" || req.JobID == "" || req.WorkerID == "" || req.JobType == "" {
		return nil, fmt.Errorf("pgjobdb: tenant, job, worker, and job type are required")
	}
	if req.RunPolicy.Retry.InitialIntervalMillis < 0 ||
		req.RunPolicy.Retry.MaximumIntervalMillis < 0 ||
		req.RunPolicy.Retry.MaximumAttempts < 0 ||
		req.RunPolicy.Retry.BackoffCoefficient < 0 {
		return nil, fmt.Errorf("pgjobdb: run policy retry values must be non-negative")
	}
	if req.RunPolicy.InvocationTimeoutMillis != nil && *req.RunPolicy.InvocationTimeoutMillis < 0 {
		return nil, fmt.Errorf("pgjobdb: invocation timeout must be non-negative")
	}
	if req.RunPolicy.TotalTimeoutMillis != nil && *req.RunPolicy.TotalTimeoutMillis < 0 {
		return nil, fmt.Errorf("pgjobdb: total timeout must be non-negative")
	}
	policy, err := json.Marshal(req.RunPolicy)
	if err != nil {
		return nil, fmt.Errorf("pgjobdb: encode run policy: %w", err)
	}
	appMetadata := req.AppMetadata
	if len(appMetadata) == 0 {
		appMetadata = json.RawMessage(`{}`)
	}
	leasePayload := req.LeasePayload
	if len(leasePayload) == 0 {
		leasePayload = json.RawMessage(`{}`)
	}
	if !isJSONObject(appMetadata) || !isJSONObject(leasePayload) {
		return nil, fmt.Errorf("pgjobdb: app metadata and lease payload must be JSON objects")
	}
	var schedule any
	if occurrence := req.Runtime.Schedule; occurrence != nil {
		if occurrence.ScheduleID == "" || occurrence.Generation < 1 ||
			occurrence.SpecHash == "" || occurrence.ScheduledAt.IsZero() || occurrence.RunID == "" {
			return nil, fmt.Errorf("pgjobdb: incomplete schedule occurrence")
		}
		if len(occurrence.FailureHistory) > 0 {
			var history []json.RawMessage
			if err := json.Unmarshal(occurrence.FailureHistory, &history); err != nil {
				return nil, fmt.Errorf("pgjobdb: failure history must be a JSON array: %w", err)
			}
		}
		encoded, err := json.Marshal(occurrence)
		if err != nil {
			return nil, fmt.Errorf("pgjobdb: encode schedule occurrence: %w", err)
		}
		schedule = string(encoded)
	}
	waitFor := make([]string, 0, len(req.WaitFor))
	for _, id := range req.WaitFor {
		if id == "" || id == req.JobID {
			return nil, fmt.Errorf("pgjobdb: wait for job id must be nonempty and distinct from job id")
		}
		waitFor = append(waitFor, string(id))
	}
	var result SubmitJobResult
	var returnedID string
	err = db.QueryRowContext(ctx, `SELECT job_id, created FROM pgjobdb.submit_native_job(
		$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)`,
		string(req.TenantID), string(req.JobID), string(req.WorkerID),
		string(req.JobType), string(policy), string(appMetadata),
		nilIfBlank(req.Runtime.SchemaHash), nilIfBlank(string(req.Runtime.ParentJobID)),
		schedule, pq.Array(waitFor), optionalTime(req.AvailableAt),
		optionalTime(req.ExpiresAt), string(leasePayload)).Scan(&returnedID, &result.Created)
	if err != nil {
		return nil, err
	}
	result.JobID = JobID(returnedID)
	return &result, nil
}

func nilIfBlank(value string) any {
	if value == "" {
		return nil
	}
	return value
}

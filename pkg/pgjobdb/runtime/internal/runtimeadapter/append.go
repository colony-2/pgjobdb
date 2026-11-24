package runtimeadapter

import (
	"context"
	"database/sql"
	"errors"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/cellstate"
	runtimecore "github.com/colony-2/jobdb/pkg/jobdb/runtime/core"
	"github.com/colony-2/pgjobdb/pkg/pgjobdb"
	"github.com/segmentio/ksuid"
)

func (s Scheduler) ResolveAppendCell(ctx context.Context, j jobdb.SubmitJob) (jobdb.ResolvedCell, error) {
	return pgjobdb.ResolveAppendCell(ctx, s.DB, j)
}
func (s Scheduler) GetAppendTail(ctx context.Context, c runtimecore.AppendChain) (runtimecore.StoredJob, bool, error) {
	var out runtimecore.StoredJob
	var expiry, archived sql.NullTime
	var worker, schema sql.NullString
	var crash bool
	err := s.DB.QueryRowContext(ctx, `WITH tail AS (
 SELECT * FROM pgjobdb.job_facts WHERE tenant_id=$1 AND cell_id=$2 AND job_type=$3 AND append_key=$4 ORDER BY append_gen DESC LIMIT 1
 ) SELECT f.job_id,f.append_gen,f.goal_id,f.cell_id,f.cell_name,f.cell_repo,f.job_type,f.schema_hash,
 COALESCE(a.archived_at,CASE WHEN j.job_id IS NULL THEN f.created_at END),COALESCE(j.cancel_requested,a.final_cancel_requested,false),
 j.lease_worker_id,NULLIF(j.lease_expires_at,'-infinity'::timestamptz),
 COALESCE(j.crash_budget_remaining=0 AND j.lease_expires_at<=pgjobdb.scheduler_now(),false)
 FROM tail f LEFT JOIN pgjobdb.jobs j USING(tenant_id,job_id) LEFT JOIN pgjobdb.jobs_archive a USING(tenant_id,job_id)`, c.TenantID, c.CellID, c.JobType, c.AppendKey).Scan(
		&out.JobKey.JobId, &out.AppendGeneration, &out.GoalID, &out.Cell.CellID, &out.Cell.Cell, &out.Cell.CellRepo, &out.JobType, &schema, &archived, &out.CancelRequested, &worker, &expiry, &crash)
	if errors.Is(err, sql.ErrNoRows) {
		return out, false, nil
	}
	if err != nil {
		return out, false, err
	}
	out.JobKey.TenantId = c.TenantID
	out.AppendKey = c.AppendKey
	out.SchemaHash = schema.String
	out.LeaseWorkerID = worker.String
	if expiry.Valid {
		out.LeaseExpiresAt = &expiry.Time
	}
	if archived.Valid {
		out.ArchivedAt = &archived.Time
	}
	if crash {
		out.Status = jobdb.JobStatusCrashConcern
	}
	return out, true, nil
}
func (s Scheduler) ClaimAppend(ctx context.Context, q runtimecore.ClaimAppendRequest) (runtimecore.LeaseIdentity, error) {
	if p, ok := cellstate.Lease(ctx); ok {
		if _, err := s.ValidateLease(ctx, runtimecore.LeaseIdentity{JobKey: p.JobKey, LeaseID: p.LeaseID, WorkerID: p.WorkerID}); err != nil {
			return runtimecore.LeaseIdentity{}, err
		}
	}
	if err := pgjobdb.AdmitProjectWrite(ctx, s.DB, q.JobKey.TenantId); err != nil {
		return runtimecore.LeaseIdentity{}, err
	}
	out := runtimecore.LeaseIdentity{JobKey: q.JobKey, LeaseID: ksuid.New().String(), WorkerID: q.WorkerID}
	var expires sql.NullTime
	err := s.DB.QueryRowContext(ctx, `SELECT pgjobdb.claim_append($1,$2,$3,$4,$5)`, q.JobKey.TenantId, q.JobKey.JobId, out.LeaseID, q.WorkerID, q.Duration.Seconds()).Scan(&expires)
	if err != nil {
		return out, err
	}
	if !expires.Valid {
		return out, jobdb.ErrConflict
	}
	out.ExpiresAt = expires.Time
	return out, nil
}
func (s Scheduler) ReleaseAppend(ctx context.Context, q runtimecore.LeaseIdentity) error {
	if err := pgjobdb.AdmitProjectWrite(ctx, s.DB, q.JobKey.TenantId); err != nil {
		return err
	}
	_, err := s.DB.ExecContext(ctx, `SELECT pgjobdb.release_append($1,$2,$3)`, q.JobKey.TenantId, q.JobKey.JobId, q.LeaseID)
	return err
}

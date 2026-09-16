package pgjobdb

import (
	"context"
	"fmt"
	"math"

	"github.com/lib/pq"
)

func (l *JobLease) Reschedule(ctx context.Context, db DB, req RescheduleRequest) error {
	return RescheduleJob(ctx, db, l.Identity(), req)
}

func RescheduleJob(ctx context.Context, db DB, identity LeaseIdentity,
	req RescheduleRequest) error {
	if err := validateLeaseIdentity(identity); err != nil {
		return err
	}
	return rescheduleJob(ctx, db, identity, req)
}

func RescheduleUnheldJob(ctx context.Context, db DB, tenant TenantID, job JobID,
	worker WorkerID, req RescheduleRequest) error {
	if tenant == "" || job == "" || worker == "" {
		return fmt.Errorf("pgjobdb: tenant, job, and worker ids are required")
	}
	return rescheduleJob(ctx, db, LeaseIdentity{
		TenantID: tenant, JobID: job, WorkerID: worker,
	}, req)
}

func rescheduleJob(ctx context.Context, db DB, identity LeaseIdentity,
	req RescheduleRequest) error {
	if err := validateScheduleDB(ctx, db); err != nil {
		return err
	}
	if req.RouteJobType == "" {
		return fmt.Errorf("pgjobdb: route job type is required")
	}
	var taskType, resumeType, inputOrdinal, outputOrdinal, inputHash any
	switch req.WorkKind {
	case WorkKindJob:
		if req.Task != nil {
			return fmt.Errorf("pgjobdb: job route cannot carry task coordinates")
		}
	case WorkKindTask:
		if req.Task == nil || req.Task.TaskType == "" ||
			req.Task.ResumeJobType == "" || req.Task.InputOrdinal < 0 ||
			req.Task.OutputOrdinal < 0 || req.Task.InputHash == "" {
			return fmt.Errorf("pgjobdb: task route requires complete coordinates")
		}
		taskType, resumeType = string(req.Task.TaskType), string(req.Task.ResumeJobType)
		inputOrdinal, outputOrdinal, inputHash = req.Task.InputOrdinal,
			req.Task.OutputOrdinal, req.Task.InputHash
	default:
		return fmt.Errorf("pgjobdb: work kind must be JOB or TASK")
	}
	waitFor := make([]string, 0, len(req.WaitFor))
	for _, id := range req.WaitFor {
		if id == "" || id == identity.JobID {
			return fmt.Errorf("pgjobdb: wait for job id must be nonempty and distinct from job id")
		}
		waitFor = append(waitFor, string(id))
	}
	var payload any
	if req.LeasePayload != nil {
		if !isJSONObject(req.LeasePayload) {
			return fmt.Errorf("pgjobdb: lease payload must be a JSON object")
		}
		payload = string(req.LeasePayload)
	}
	var alternateJob, alternateTask, alternateAfter any
	if req.Alternate != nil {
		if req.Alternate.JobType == "" {
			if req.Alternate.TaskType != "" || req.Alternate.After != 0 {
				return fmt.Errorf("pgjobdb: cleared alternate route cannot have task or delay")
			}
		} else {
			if req.Alternate.TaskType != "" && req.WorkKind != WorkKindTask {
				return fmt.Errorf("pgjobdb: alternate task route requires task coordinates")
			}
			if req.Alternate.After < 0 {
				return fmt.Errorf("pgjobdb: alternate delay must be non-negative")
			}
			seconds := math.Ceil(req.Alternate.After.Seconds())
			if seconds > math.MaxInt32 {
				return fmt.Errorf("pgjobdb: alternate delay exceeds database range")
			}
			alternateJob = string(req.Alternate.JobType)
			if req.Alternate.TaskType != "" {
				alternateTask = string(req.Alternate.TaskType)
			}
			alternateAfter = int(seconds)
		}
	}
	var updated bool
	if err := db.QueryRowContext(ctx, `SELECT pgjobdb.reschedule_native_job(
		$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12,
		$13, $14, $15, $16, $17, $18)`,
		string(identity.TenantID), string(identity.JobID),
		nilIfBlank(identity.LeaseID), string(identity.WorkerID),
		string(req.RouteJobType), string(req.WorkKind),
		taskType, resumeType, inputOrdinal, outputOrdinal, inputHash,
		pq.Array(waitFor), optionalTime(req.AvailableAt), payload,
		req.Alternate != nil, alternateJob, alternateTask, alternateAfter,
	).Scan(&updated); err != nil {
		return err
	}
	if !updated {
		return fmt.Errorf("pgjobdb: reschedule native job returned false")
	}
	return nil
}

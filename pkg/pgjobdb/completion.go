package pgjobdb

import (
	"context"
	"fmt"
)

func (l *JobLease) Identity() LeaseIdentity {
	if l == nil {
		return LeaseIdentity{}
	}
	return LeaseIdentity{
		TenantID: l.TenantID, JobID: l.JobID,
		LeaseID: l.LeaseID, WorkerID: l.WorkerID,
	}
}

func (l *JobLease) Complete(ctx context.Context, db DB, completion Completion) error {
	return CompleteJob(ctx, db, l.Identity(), completion)
}

func CompleteJob(ctx context.Context, db DB, identity LeaseIdentity, completion Completion) error {
	if err := validateScheduleDB(ctx, db); err != nil {
		return err
	}
	if identity.TenantID == "" || identity.JobID == "" ||
		identity.LeaseID == "" || identity.WorkerID == "" {
		return fmt.Errorf("pgjobdb: complete requires tenant, job, lease, and worker ids")
	}
	if err := validateCompletion(completion); err != nil {
		return err
	}
	mode, value, revision, err := clientUpdateArgs(completion.ClientPayloadUpdate, false)
	if err != nil {
		return err
	}
	var completed bool
	if err := db.QueryRowContext(ctx, `SELECT pgjobdb.complete_native_job(
		$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`,
		string(identity.TenantID), string(identity.JobID), identity.LeaseID,
		string(identity.WorkerID), string(completion.Status),
		nilIfBlank(completion.Detail), nilIfBlank(completion.ErrorKind),
		optionalBool(completion.Retryable), mode, value, revision).Scan(&completed); err != nil {
		return clientPayloadError(err)
	}
	if !completed {
		return fmt.Errorf("pgjobdb: complete native job returned false")
	}
	return nil
}

func CompleteUnheldJob(ctx context.Context, db DB, tenant TenantID, job JobID,
	worker WorkerID, completion Completion) error {
	if completion.ClientPayloadUpdate != nil {
		return fmt.Errorf("pgjobdb: payload updates require a held completion lease")
	}
	if err := validateScheduleDB(ctx, db); err != nil {
		return err
	}
	if tenant == "" || job == "" || worker == "" {
		return fmt.Errorf("pgjobdb: complete requires tenant, job, and worker ids")
	}
	if err := validateCompletion(completion); err != nil {
		return err
	}
	var completed bool
	if err := db.QueryRowContext(ctx, `SELECT pgjobdb.complete_native_unheld_job(
		$1, $2, $3, $4, $5, $6, $7)`,
		string(tenant), string(job), string(worker), string(completion.Status),
		nilIfBlank(completion.Detail), nilIfBlank(completion.ErrorKind),
		optionalBool(completion.Retryable)).Scan(&completed); err != nil {
		return err
	}
	if !completed {
		return fmt.Errorf("pgjobdb: complete native unheld job returned false")
	}
	return nil
}

func validateCompletion(completion Completion) error {
	switch completion.Status {
	case CompletionSuccess, CompletionFailedApp, CompletionFailedSystem,
		CompletionFailedTimeout, CompletionCancelled:
		return nil
	default:
		return fmt.Errorf("pgjobdb: invalid completion status %q", completion.Status)
	}
}

func optionalBool(value *bool) any {
	if value == nil {
		return nil
	}
	return *value
}

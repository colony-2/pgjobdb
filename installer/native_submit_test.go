package installer_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"testing"
	"time"

	"github.com/colony-2/pgjobdb"
)

func TestNativeSubmitJob(t *testing.T) {
	runDatabaseTest(t, func(ctx context.Context, db *sql.DB) {
		if _, err := db.ExecContext(ctx, `INSERT INTO pgjobdb.schedules
			(tenant_id, schedule_id, state, generation, spec_hash, trigger,
			target_job_type, target_snapshot, overlap_policy, failure_policy)
			VALUES ('tenant', 'daily', 'ACTIVE', 1, 'schedule-hash', '{}',
			'collect', '{}', 'ALLOW', '{}')`); err != nil {
			t.Fatalf("create schedule: %v", err)
		}
		scheduledAt := time.Now().UTC().Truncate(time.Microsecond)
		timeout := int64(30000)
		req := pgjobdb.SubmitJobRequest{
			TenantID: "tenant", JobID: "job-1", WorkerID: "submitter", JobType: "collect",
			RunPolicy:   pgjobdb.RunPolicy{InvocationTimeoutMillis: &timeout},
			AppMetadata: json.RawMessage(`{"source":"api"}`),
			Runtime: pgjobdb.RuntimeMetadata{
				SchemaHash: "schema-hash", ParentJobID: "parent-1",
				Schedule: &pgjobdb.ScheduleOccurrence{
					ScheduleID: "daily", Generation: 1, SpecHash: "schedule-hash",
					ScheduledAt: scheduledAt, RunID: "run-1", Reason: "automatic",
					FailureHistory: json.RawMessage(`{"bits":"101","windowSize":3}`),
				},
			},
			LeasePayload: json.RawMessage(`{"opaque":true}`),
		}
		result, err := pgjobdb.SubmitJob(ctx, db, req)
		if err != nil || !result.Created || result.JobID != "job-1" {
			t.Fatalf("submit = %+v, %v", result, err)
		}
		var jobType, route, workKind, schemaHash, parentID, scheduleID, runID string
		var policy, metadata, leasePayload, failureHistory string
		if err := db.QueryRowContext(ctx, `SELECT f.job_type, j.route_job_type,
			j.work_kind, f.schema_hash, f.parent_job_id, f.schedule_id,
			f.schedule_run_id, f.run_policy::text, f.app_metadata::text,
			f.schedule_failure_history::text,
			j.lease_payload::text
			FROM pgjobdb.job_facts f JOIN pgjobdb.jobs j USING (tenant_id, job_id)
			WHERE f.tenant_id = 'tenant' AND f.job_id = 'job-1'`).Scan(
			&jobType, &route, &workKind, &schemaHash, &parentID, &scheduleID,
			&runID, &policy, &metadata, &failureHistory, &leasePayload,
		); err != nil {
			t.Fatalf("read native job: %v", err)
		}
		if jobType != "collect" || route != "collect" || workKind != "JOB" ||
			schemaHash != "schema-hash" || parentID != "parent-1" ||
			scheduleID != "daily" || runID != "run-1" ||
			metadata != `{"source": "api"}` ||
			failureHistory != `{"bits": "101", "windowSize": 3}` ||
			leasePayload != `{"opaque": true}` {
			t.Fatalf("native job fields = %q %q %q %q %q %q %q %q %q",
				jobType, route, workKind, schemaHash, parentID, scheduleID,
				runID, metadata, leasePayload)
		}
		var gotPolicy struct {
			InvocationTimeoutMillis int64 `json:"invocation_timeout_millis"`
		}
		if err := json.Unmarshal([]byte(policy), &gotPolicy); err != nil ||
			gotPolicy.InvocationTimeoutMillis != timeout {
			t.Fatalf("run policy = %q, %v", policy, err)
		}
		result, err = pgjobdb.SubmitJob(ctx, db, req)
		if err != nil || result.Created {
			t.Fatalf("repeat submit = %+v, %v", result, err)
		}
		req.AppMetadata = json.RawMessage(`{"source":"other"}`)
		if _, err := pgjobdb.SubmitJob(ctx, db, req); err == nil {
			t.Fatal("expected conflicting immutable facts to fail")
		}

		req.JobID = "job-2"
		req.AppMetadata = json.RawMessage(`{"source":"api"}`)
		req.Runtime.Schedule = nil
		req.WaitFor = []pgjobdb.JobID{"job-1"}
		if result, err := pgjobdb.SubmitJob(ctx, db, req); err != nil || !result.Created {
			t.Fatalf("submit waiting job = %+v, %v", result, err)
		}
		var waitingFor string
		if err := db.QueryRowContext(ctx, `SELECT wait_for[1] FROM pgjobdb.jobs
			WHERE tenant_id = 'tenant' AND job_id = 'job-2'`).Scan(&waitingFor); err != nil {
			t.Fatal(err)
		}
		if waitingFor != "job-1" {
			t.Fatalf("wait for = %q", waitingFor)
		}

		req.JobID = "job-3"
		req.WaitFor = []pgjobdb.JobID{"missing"}
		if _, err := pgjobdb.SubmitJob(ctx, db, req); err == nil {
			t.Fatal("expected missing dependency to fail")
		}
		var factCount int
		if err := db.QueryRowContext(ctx, `SELECT count(*) FROM pgjobdb.job_facts
			WHERE tenant_id = 'tenant' AND job_id = 'job-3'`).Scan(&factCount); err != nil {
			t.Fatal(err)
		}
		if factCount != 0 {
			t.Fatalf("failed submit left %d fact rows", factCount)
		}
	})
}

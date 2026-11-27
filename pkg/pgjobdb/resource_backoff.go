package pgjobdb

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/pgjobdb/pkg/pgjobdb/internal/projectstorage"
)

// ResourceBackoffOptions supplies immutable chapter validation and internal
// transaction seams. Production uses database time and a fresh uniform sample.
type ResourceBackoffOptions struct {
	ValidateCheckpoint func(context.Context, jobdb.ResourceBackoffRequest) (time.Time, error)
	Sample             func() float64
	Fault              func(string) error
}

// VerifyResourceBackoff checks the installed storage contract, independently of
// Go interface presence. A damaged default rejects assignments separately so
// exact receipts remain recoverable without reading mutable configuration.
func VerifyResourceBackoff(ctx context.Context, db DB) error {
	return VerifyResourceBackoffDefinitions(ctx, db, false)
}

// VerifyResourceBackoffDefinitions also supports the unmodified historical
// function bodies during offline verification of a format-14 or -15 source.
func VerifyResourceBackoffDefinitions(ctx context.Context, db DB, historical bool) error {
	var valid bool
	err := db.QueryRowContext(ctx, resourceProtectionCatalog+` SELECT
 to_regclass('pgjobdb.resource_sequences') IS NOT NULL
 AND to_regclass('pgjobdb.resource_receipts') IS NOT NULL
 AND to_regclass('pgjobdb.resource_configuration') IS NOT NULL
 AND (SELECT count(*)=10 FROM information_schema.columns WHERE table_schema='pgjobdb' AND table_name IN ('jobs','jobs_archive') AND column_name IN ('crash_budget_remaining','resource_not_before_ns','resource_not_before','resource_deadline','resource_resume'))
 AND NOT EXISTS (SELECT 1 FROM resource_columns expected LEFT JOIN pg_attribute a
   ON a.attrelid=to_regclass('pgjobdb.'||expected.table_name) AND a.attname=expected.column_name
   AND a.attnum>0 AND NOT a.attisdropped
   WHERE a.attname IS NULL OR a.atttypid<>to_regtype(expected.type_name) OR a.attnotnull<>expected.required)
 AND NOT EXISTS (SELECT 1 FROM resource_keys expected WHERE NOT EXISTS (
   SELECT 1 FROM pg_constraint c JOIN pg_index i ON i.indexrelid=c.conindid
   WHERE c.conrelid=to_regclass('pgjobdb.'||expected.table_name) AND c.contype='p'
   AND c.convalidated AND NOT c.condeferrable AND NOT c.condeferred
   AND i.indisunique AND i.indisvalid AND i.indisready AND i.indislive
   AND i.indpred IS NULL AND i.indexprs IS NULL
   AND ARRAY(SELECT a.attname::text FROM unnest(c.conkey) WITH ORDINALITY k(num,ordinal)
     JOIN pg_attribute a ON a.attrelid=c.conrelid AND a.attnum=k.num ORDER BY k.ordinal)=expected.columns))
 AND NOT EXISTS (SELECT 1 FROM resource_foreign_keys expected WHERE NOT EXISTS (
   SELECT 1 FROM pg_constraint c WHERE c.conrelid=to_regclass('pgjobdb.'||expected.table_name)
   AND c.contype='f' AND c.confrelid=to_regclass('pgjobdb.job_facts') AND c.confdeltype='c'
   AND c.confupdtype='a' AND c.confmatchtype='s' AND c.convalidated AND NOT c.condeferrable AND NOT c.condeferred
   AND ARRAY(SELECT a.attname::text FROM unnest(c.conkey) WITH ORDINALITY k(num,ordinal)
     JOIN pg_attribute a ON a.attrelid=c.conrelid AND a.attnum=k.num ORDER BY k.ordinal)=ARRAY['tenant_id','job_id']
   AND ARRAY(SELECT a.attname::text FROM unnest(c.confkey) WITH ORDINALITY k(num,ordinal)
     JOIN pg_attribute a ON a.attrelid=c.confrelid AND a.attnum=k.num ORDER BY k.ordinal)=ARRAY['tenant_id','job_id']))
 AND NOT EXISTS (SELECT 1 FROM resource_checks expected WHERE NOT EXISTS (
   SELECT 1 FROM pg_constraint c WHERE c.conrelid=to_regclass('pgjobdb.'||expected.table_name)
   AND c.contype='c' AND c.convalidated AND NOT c.connoinherit
   AND pg_get_expr(c.conbin,c.conrelid)=expected.expression))
 AND EXISTS(SELECT 1 FROM pg_trigger WHERE tgrelid='pgjobdb.jobs_archive'::regclass
   AND tgname='archive_resource_state' AND tgenabled='O' AND NOT tgisinternal AND tgtype=7
   AND tgfoid=to_regprocedure('pgjobdb._archive_resource_state()') AND tgqual IS NULL AND tgnargs=0)`).Scan(&valid)
	if err != nil || !valid {
		return &jobdb.ResourceBackoffUnsupportedError{Feature: "installed PostgreSQL crash-budget resource contract"}
	}
	if err := verifyResourceFunctions(ctx, db, historical); err != nil {
		return err
	}
	return nil
}

// Compare the installed bodies and execution attributes to the embedded SQL,
// including every admission route. Presence alone cannot establish durability.
func verifyResourceFunctions(ctx context.Context, db DB, historical bool) error {
	var guarded bool
	if err := db.QueryRowContext(ctx, `SELECT to_regclass('pgjobdb.project_installation') IS NOT NULL`).Scan(&guarded); err != nil {
		return err
	}
	names := []string{"job_status_at", "effective_native_route", "get_native_work", "validate_native_lease", "claim_native_task", "claim_append", "reschedule_native_job", "clear_crash_concern", "crash_concern_threshold", "set_crash_concern_threshold", "_archive_resource_state"}
	expected := map[string][]string{}
	pattern := regexp.MustCompile(`(?s)CREATE OR REPLACE FUNCTION pgjobdb\.([a-z_]+)\([^$]*?AS \$\$(.*?)\$\$;`)
	program := baseSQL + "\n" + resourceSQL
	if historical {
		program += "\n" + HistoricalLeaseSQL
	}
	for _, match := range pattern.FindAllStringSubmatch(program, -1) {
		if slices.Contains(names, match[1]) {
			expected[match[1]] = match
		}
	}
	rows, err := db.QueryContext(ctx, `SELECT p.proname,p.prosrc,l.lanname,p.provolatile::text,p.prosecdef FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace JOIN pg_language l ON l.oid=p.prolang WHERE n.nspname='pgjobdb'`)
	if err != nil {
		return err
	}
	defer rows.Close()
	seen := map[string]bool{}
	for rows.Next() {
		var name, body, language, volatility string
		var definer bool
		if err = rows.Scan(&name, &body, &language, &volatility, &definer); err != nil {
			return err
		}
		if !slices.Contains(names, name) {
			continue
		}
		match, ok := expected[name]
		if !ok {
			return fmt.Errorf("missing embedded resource function %s", name)
		}
		header := strings.ToUpper(match[0][:strings.Index(match[0], "AS $$")])
		wantLanguage := "plpgsql"
		if strings.Contains(header, "LANGUAGE SQL") {
			wantLanguage = "sql"
		}
		wantVolatility := "v"
		if strings.Contains(header, "STABLE") {
			wantVolatility = "s"
		}
		wantBody := match[2]
		if guarded {
			wantBody, err = projectstorage.GuardedBody(name, wantBody)
			if err != nil {
				return err
			}
		}
		if seen[name] || body != wantBody || language != wantLanguage || volatility != wantVolatility || definer {
			return &jobdb.ResourceBackoffUnsupportedError{Feature: "altered PostgreSQL resource function " + name}
		}
		seen[name] = true
	}
	if err = rows.Err(); err != nil {
		return err
	}
	for _, name := range names {
		if !seen[name] {
			return &jobdb.ResourceBackoffUnsupportedError{Feature: "missing PostgreSQL resource function " + name}
		}
	}
	return nil
}

// ResourceIndexTime rounds toward the future without overflowing UnixNano at
// the maximum signed nanosecond wake. The receipt retains the exact instant.
func ResourceIndexTime(wake time.Time) time.Time {
	t := wake.UTC().Truncate(time.Microsecond)
	if t.Before(wake) {
		t = t.Add(time.Microsecond)
	}
	return t
}

func resourceReceipt(ctx context.Context, db DB, id LeaseIdentity, req jobdb.ResourceBackoffRequest, body []byte) (jobdb.ResourceBackoffReceipt, bool, error) {
	var saved, raw []byte
	var worker string
	err := db.QueryRowContext(ctx, `SELECT worker_id,request,receipt FROM pgjobdb.resource_receipts WHERE tenant_id=$1 AND job_id=$2 AND lease_id=$3 AND request_id=$4`, id.TenantID, id.JobID, id.LeaseID, req.RequestID).Scan(&worker, &saved, &raw)
	if errors.Is(err, sql.ErrNoRows) {
		return jobdb.ResourceBackoffReceipt{}, false, nil
	}
	if err != nil {
		return jobdb.ResourceBackoffReceipt{}, false, err
	}
	if worker != string(id.WorkerID) || !bytes.Equal(body, saved) {
		return jobdb.ResourceBackoffReceipt{}, true, jobdb.ErrConflict
	}
	var receipt jobdb.ResourceBackoffReceipt
	err = json.Unmarshal(raw, &receipt)
	return receipt, true, err
}

// ReplayResourceBackoffReceipt reads only an authenticated exact retained request.
func ReplayResourceBackoffReceipt(ctx context.Context, db DB, id LeaseIdentity, req jobdb.ResourceBackoffRequest) (jobdb.ResourceBackoffReceipt, error) {
	body, err := jobdb.CanonicalResourceBackoffRequest(req)
	if err != nil {
		return jobdb.ResourceBackoffReceipt{}, jobdb.ErrExecutionLeaseLost
	}
	receipt, found, err := resourceReceipt(ctx, db, id, req, body)
	if err != nil || !found {
		return jobdb.ResourceBackoffReceipt{}, jobdb.ErrExecutionLeaseLost
	}
	return receipt, nil
}

// RescheduleResourceBackoff commits selection, payload CAS, bound policy,
// countdown assignment and the immutable receipt with release of the lease.
func RescheduleResourceBackoff(ctx context.Context, db *sql.DB, id LeaseIdentity, req jobdb.ResourceBackoffRequest, options ResourceBackoffOptions) (receipt jobdb.ResourceBackoffReceipt, err error) {
	req, err = jobdb.CloneResourceBackoffRequest(req)
	if err != nil {
		return receipt, err
	}
	body, err := jobdb.CanonicalResourceBackoffRequest(req)
	if err != nil {
		return receipt, err
	}
	if err = validateLeaseIdentity(id); err != nil {
		return receipt, jobdb.ErrExecutionLeaseLost
	}
	// Never let new configuration, chapter, or lease reads block exact replay.
	if saved, found, e := resourceReceipt(ctx, db, id, req, body); e != nil || found {
		return saved, e
	}
	if err = AdmitProjectWrite(ctx, db, string(id.TenantID)); err != nil {
		return receipt, err
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return receipt, err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, `SELECT pgjobdb._goal_lock($1)`, id.TenantID); err != nil {
		return receipt, err
	}
	if saved, found, e := resourceReceipt(ctx, tx, id, req, body); e != nil || found {
		return saved, e
	}
	if err = VerifyResourceBackoff(ctx, tx); err != nil {
		return receipt, err
	}
	var allowance int64
	if err = tx.QueryRowContext(ctx, `SELECT crash_allowance FROM pgjobdb.resource_configuration WHERE singleton AND protocol_version=1 AND crash_allowance>0 FOR SHARE`).Scan(&allowance); err != nil {
		return receipt, fmt.Errorf("pgjobdb: read crash default: %w", err)
	}
	var route, kind string
	var task, resumeJob, hash sql.NullString
	var input, output sql.NullInt64
	var revision int64
	var now time.Time
	var live bool
	// The immutable chapter log can be read while this row lock remains held.
	err = tx.QueryRowContext(ctx, `SELECT j.route_job_type,j.work_kind,j.task_type,j.resume_job_type,j.task_input_ordinal,j.task_output_ordinal,j.task_input_hash,c.revision
 FROM pgjobdb.jobs j JOIN pgjobdb.job_client_state c USING(tenant_id,job_id)
 JOIN pgjobdb.job_facts f USING(tenant_id,job_id)
 WHERE j.tenant_id=$1 AND j.job_id=$2 AND j.lease_id=$3 AND j.lease_worker_id=$4
 AND NOT j.cancel_requested AND f.hidden_at IS NULL AND f.record_kind='EXECUTION'
 AND j.dependency_wait_id IS NULL AND cardinality(j.wait_for)=0 FOR UPDATE OF j,c`, id.TenantID, id.JobID, id.LeaseID, id.WorkerID).Scan(&route, &kind, &task, &resumeJob, &input, &output, &hash, &revision)
	if errors.Is(err, sql.ErrNoRows) {
		return receipt, jobdb.ErrExecutionLeaseLost
	}
	if err != nil {
		return receipt, err
	}
	if err = tx.QueryRowContext(ctx, `SELECT pgjobdb.scheduler_now(),lease_expires_at>pgjobdb.scheduler_now() FROM pgjobdb.jobs WHERE tenant_id=$1 AND job_id=$2`, id.TenantID, id.JobID).Scan(&now, &live); err != nil {
		return receipt, err
	}
	if !live {
		return receipt, jobdb.ErrExecutionLeaseLost
	}
	if req.Resume.Route.JobType != route || req.Resume.Route.TaskType != task.String || req.Resume.PayloadRevision != revision || (kind == "TASK") != (req.Resume.TaskWait != nil) {
		return receipt, jobdb.ErrConflict
	}
	if w := req.Resume.TaskWait; w != nil {
		if w.InputOrdinal != input.Int64 || w.InputHash != hash.String || w.ResumeJobType != resumeJob.String || w.OutputOrdinal != req.Resume.CheckpointOrdinal+1 {
			return receipt, jobdb.ErrConflict
		}
	}
	if options.ValidateCheckpoint == nil {
		return receipt, &jobdb.ResourceBackoffUnsupportedError{Feature: "immutable chapter validator"}
	}
	deadline, err := options.ValidateCheckpoint(ctx, req)
	if err != nil {
		return receipt, err
	}
	check := func(stage string) error {
		if options.Fault != nil {
			return options.Fault(stage)
		}
		return nil
	}
	if err = check("validated"); err != nil {
		return receipt, err
	}
	// Validation can block. Re-read the clock/lease after it and any lock wait.
	if err = tx.QueryRowContext(ctx, `SELECT pgjobdb.scheduler_now(),lease_expires_at>pgjobdb.scheduler_now() FROM pgjobdb.jobs WHERE tenant_id=$1 AND job_id=$2`, id.TenantID, id.JobID).Scan(&now, &live); err != nil {
		return receipt, err
	}
	if !live {
		return receipt, jobdb.ErrExecutionLeaseLost
	}
	policy, _ := json.Marshal(req.Policy)
	digest := sha256.Sum256([]byte(req.Invocation))
	var oldPolicy []byte
	var invocation string
	count := int64(0)
	err = tx.QueryRowContext(ctx, `SELECT invocation,count,policy,allowance FROM pgjobdb.resource_sequences WHERE tenant_id=$1 AND job_id=$2 AND invocation_digest=$3 AND attempt=$4 AND reason=$5 FOR UPDATE`, id.TenantID, id.JobID, digest[:], req.Attempt, req.Reason).Scan(&invocation, &count, &oldPolicy, &allowance)
	if err == nil {
		if invocation != req.Invocation || !bytes.Equal(policy, oldPolicy) {
			return receipt, jobdb.ErrConflict
		}
	} else if !errors.Is(err, sql.ErrNoRows) {
		return receipt, err
	} else if req.Policy.CrashAllowance != nil {
		allowance = *req.Policy.CrashAllowance
	}
	if count < math.MaxInt64 {
		count++
	}
	sample := rand.Float64()
	if options.Sample != nil {
		sample = options.Sample()
	}
	delay, err := jobdb.ResourceBackoffDelay(req.Policy, count, sample)
	if err != nil {
		return receipt, err
	}
	wake := jobdb.ResourceBackoffWake(now, delay)
	_, err = tx.ExecContext(ctx, `INSERT INTO pgjobdb.resource_sequences VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)
 ON CONFLICT(tenant_id,job_id,invocation_digest,attempt,reason) DO UPDATE SET count=excluded.count`, id.TenantID, id.JobID, digest[:], req.Invocation, req.Attempt, req.Reason, count, policy, allowance)
	if err != nil {
		return receipt, err
	}
	if err = check("sequence"); err != nil {
		return receipt, err
	}
	mode, value, expected, err := clientUpdateArgs(req.ClientPayloadUpdate, false)
	if err != nil {
		return receipt, err
	}
	if _, err = tx.ExecContext(ctx, `SELECT pgjobdb._update_client_payload($1,$2,$3,$4,$5)`, id.TenantID, id.JobID, mode, value, expected); err != nil {
		return receipt, clientPayloadError(err)
	}
	if err = tx.QueryRowContext(ctx, `SELECT revision FROM pgjobdb.job_client_state WHERE tenant_id=$1 AND job_id=$2`, id.TenantID, id.JobID).Scan(&revision); err != nil {
		return receipt, err
	}
	resume := req.Resume
	resume.PayloadRevision = revision
	encodedResume, _ := json.Marshal(resume)
	var taskOutput any
	if resume.TaskWait != nil {
		taskOutput = resume.TaskWait.OutputOrdinal
	}
	var sqlDeadline any
	if !deadline.IsZero() {
		sqlDeadline = ResourceIndexTime(deadline)
	}
	_, err = tx.ExecContext(ctx, `UPDATE pgjobdb.jobs SET crash_budget_remaining=$3,resource_not_before_ns=$4,resource_not_before=$5,resource_deadline=$6,resource_resume=$7,
 task_output_ordinal=COALESCE($8,task_output_ordinal),lease_id=NULL,lease_worker_id=NULL,lease_expires_at='-infinity',lease_route_job_type=NULL,lease_work_kind=NULL,lease_task_type=NULL,
 available_at=$5,alternate_job_type=NULL,alternate_task_type=NULL,alternate_at=NULL,alternate_after_seconds=NULL
 WHERE tenant_id=$1 AND job_id=$2`, id.TenantID, id.JobID, allowance, wake.UnixNano(), ResourceIndexTime(wake), sqlDeadline, string(encodedResume), taskOutput)
	if err != nil {
		return receipt, err
	}
	if err = check("job"); err != nil {
		return receipt, err
	}
	receipt = jobdb.ResourceBackoffReceipt{Version: 1, RequestID: req.RequestID, JobKey: jobdb.JobKey{TenantId: string(id.TenantID), JobId: string(id.JobID)}, LeaseID: id.LeaseID, WorkerID: string(id.WorkerID), Count: count, WakeAt: wake, Reason: req.Reason, Policy: req.Policy, ResolvedCrashAllowance: allowance, Resume: resume}
	raw, err := json.Marshal(receipt)
	if err != nil {
		return jobdb.ResourceBackoffReceipt{}, err
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO pgjobdb.resource_receipts VALUES($1,$2,$3,$4,$5,$6,$7)`, id.TenantID, id.JobID, id.LeaseID, req.RequestID, id.WorkerID, body, raw); err != nil {
		return jobdb.ResourceBackoffReceipt{}, err
	}
	if err = check("receipt"); err != nil {
		return jobdb.ResourceBackoffReceipt{}, err
	}
	if err = tx.Commit(); err != nil {
		return jobdb.ResourceBackoffReceipt{}, err
	}
	return receipt, nil
}

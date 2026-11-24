package integration_test

import (
	"context"
	"database/sql"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	"github.com/colony-2/jobdb/pkg/jobdb/runtimetest"
	"github.com/colony-2/pgjobdb/pkg/pgjobdb"
	"github.com/stretchr/testify/require"
)

func TestResourceBackoffPostgresBoundaries(t *testing.T) {
	newDSN := conformanceDSNFactory(t)
	for _, scenario := range []string{"P04_future_default_and_bound_policy", "A13_A14_A15_long_key_saturation_and_int64", "A03_retained_receipt_without_default", "A17_exact_wake_conservative_gate"} {
		t.Run(scenario, func(t *testing.T) {
			var db *sql.DB
			f := resourceFixture(t, newDSN(t), func(current *sql.DB) { db = current })
			ctx := celltest.Context(context.Background())
			at := time.Now().UTC().Truncate(time.Millisecond)
			f.SetTime(at)
			h, err := celltest.Submit(f.Runtime, ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "c2", Cell: "test", CellRepo: "example.test/test", JobID: "boundary", JobType: "work", Data: jobdb.NewTaskDataOrPanic("original")}})
			require.NoError(t, err)
			claim := func() jobdb.ExecutionLease {
				l, err := f.Runtime.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: h.JobKey, WorkerID: "worker", Routes: []jobdb.Route{{JobType: "work"}}, LeaseDuration: time.Minute})
				require.NoError(t, err)
				require.NotNil(t, l)
				return l
			}
			l := claim()
			p := jobdb.DefaultResourceBackoffPolicy()
			p.Jitter = 0
			invocation := "work/attempt:1"
			if strings.HasPrefix(scenario, "A13") {
				invocation = strings.Repeat("x", 16384)
				value := int64(math.MaxInt64)
				p.CrashAllowance = &value
			}
			if strings.HasPrefix(scenario, "A17") {
				p.Jitter = .2
				f.SetRandom(.123456789)
			}
			if strings.HasPrefix(scenario, "P04") {
				_, err = db.ExecContext(ctx, `SELECT pgjobdb.set_crash_concern_threshold(2)`)
				require.NoError(t, err)
				require.EqualValues(t, 5, *l.ExecutionState().CrashBudgetRemaining, "default changes preserve prior balances")
			}
			req, err := runtimetest.PrepareResourceBackoffCheckpoint(ctx, f.Runtime, l, invocation, 1, p)
			require.NoError(t, err)
			receipt, err := l.(jobdb.ResourceBackoffLease).RescheduleResourceBackoff(ctx, req)
			require.NoError(t, err)
			if strings.HasPrefix(scenario, "A03") {
				_, err = db.ExecContext(ctx, `DELETE FROM pgjobdb.resource_configuration`)
				require.NoError(t, err)
				replay, err := l.(jobdb.ResourceBackoffLease).RescheduleResourceBackoff(ctx, req)
				require.NoError(t, err)
				require.Equal(t, receipt, replay)
				recovered, err := f.Runtime.(jobdb.ResourceBackoffReceiptReader).ReplayResourceBackoffReceiptByID(ctx, h.JobKey, l.LeaseID(), "worker", req)
				require.NoError(t, err)
				require.Equal(t, receipt, recovered)
				return
			}
			if strings.HasPrefix(scenario, "A17") {
				require.NotZero(t, receipt.WakeAt.Nanosecond()%1000)
				var ns int64
				var indexed time.Time
				require.NoError(t, db.QueryRowContext(ctx, `SELECT resource_not_before_ns,resource_not_before FROM pgjobdb.jobs WHERE job_id='boundary'`).Scan(&ns, &indexed))
				require.Equal(t, receipt.WakeAt.UnixNano(), ns)
				require.Equal(t, pgjobdb.ResourceIndexTime(receipt.WakeAt), indexed.UTC())
				f.SetTime(receipt.WakeAt.Truncate(time.Microsecond))
				early, err := f.Runtime.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: h.JobKey, WorkerID: "early", Routes: []jobdb.Route{{JobType: "work"}}})
				require.NoError(t, err)
				require.Nil(t, early)
				f.SetTime(indexed)
				require.NotNil(t, claim())
				return
			}
			if strings.HasPrefix(scenario, "P04") {
				require.EqualValues(t, 2, receipt.ResolvedCrashAllowance)
				_, err = db.ExecContext(ctx, `SELECT pgjobdb.set_crash_concern_threshold(7)`)
				require.NoError(t, err)
			} else {
				require.EqualValues(t, math.MaxInt64, receipt.ResolvedCrashAllowance)
				_, err = db.ExecContext(ctx, `UPDATE pgjobdb.resource_sequences SET count=$1`, int64(math.MaxInt64))
				require.NoError(t, err)
			}
			f.SetTime(receipt.WakeAt)
			l = claim()
			req, err = runtimetest.PrepareResourceBackoffCheckpoint(ctx, f.Runtime, l, invocation, 1, p)
			require.NoError(t, err)
			again, err := l.(jobdb.ResourceBackoffLease).RescheduleResourceBackoff(ctx, req)
			require.NoError(t, err)
			require.Equal(t, receipt.ResolvedCrashAllowance, again.ResolvedCrashAllowance)
			if strings.HasPrefix(scenario, "A13") {
				require.EqualValues(t, math.MaxInt64, again.Count)
			}
			f.SetTime(again.WakeAt)
			l = claim()
			require.NoError(t, l.Reschedule(ctx, jobdb.RescheduleExecutionRequest{NextRoute: jobdb.Route{JobType: "work"}}))
			current, err := f.Runtime.GetJob(ctx, h.JobKey)
			require.NoError(t, err)
			expected := int64(5)
			if strings.HasPrefix(scenario, "P04") {
				expected = 7
			}
			require.Equal(t, expected, *current.ExecutionState.CrashBudgetRemaining)
		})
	}
}

func TestResourceBackoffInstalledProtections(t *testing.T) {
	newDSN := conformanceDSNFactory(t)
	var db *sql.DB
	resourceFixture(t, newDSN(t), func(current *sql.DB) { db = current })
	ctx := celltest.Context(context.Background())
	require.NoError(t, pgjobdb.VerifyResourceBackoff(ctx, db))
	for _, change := range []struct{ name, sql string }{
		{"receipt_uniqueness", `ALTER TABLE pgjobdb.resource_receipts DROP CONSTRAINT resource_receipts_pkey`},
		{"receipt_cleanup", `ALTER TABLE pgjobdb.resource_receipts DROP CONSTRAINT resource_receipts_tenant_id_job_id_fkey`},
		{"sequence_cleanup", `ALTER TABLE pgjobdb.resource_sequences DROP CONSTRAINT resource_sequences_tenant_id_job_id_fkey`},
		{"sequence_positive_count", `ALTER TABLE pgjobdb.resource_sequences DROP CONSTRAINT resource_sequences_count_check`},
		{"credit_guard", `ALTER TABLE pgjobdb.jobs DROP CONSTRAINT jobs_crash_budget_remaining_check`},
		{"receipt_exact_bytes", `ALTER TABLE pgjobdb.resource_receipts ALTER COLUMN receipt TYPE text USING encode(receipt,'hex')`},
		{"archive_trigger_binding", `DROP TRIGGER archive_resource_state ON pgjobdb.jobs_archive; CREATE TRIGGER archive_resource_state AFTER INSERT ON pgjobdb.jobs_archive FOR EACH ROW EXECUTE FUNCTION pgjobdb._archive_resource_state()`},
		{"gate_function", `CREATE OR REPLACE FUNCTION pgjobdb.effective_native_route(j pgjobdb.jobs, p_now TIMESTAMPTZ) RETURNS TABLE(effective_job_type TEXT,effective_work_kind TEXT,effective_task_type TEXT,alternate_due BOOLEAN) LANGUAGE sql STABLE AS $$ SELECT j.route_job_type,j.work_kind,j.task_type,false; $$`},
	} {
		t.Run(change.name, func(t *testing.T) {
			tx, err := db.BeginTx(ctx, nil)
			require.NoError(t, err)
			defer tx.Rollback()
			_, err = tx.ExecContext(ctx, change.sql)
			require.NoError(t, err)
			require.ErrorIs(t, pgjobdb.VerifyResourceBackoff(ctx, tx), jobdb.ErrResourceBackoffUnsupported)
		})
	}
	require.NoError(t, pgjobdb.VerifyResourceBackoff(ctx, db), "rejected mutations rolled back without repair")
}

func TestResourceBackoffDefaultAssignments(t *testing.T) {
	var db *sql.DB
	fixture := resourceFixture(t, conformanceDSNFactory(t)(t), func(current *sql.DB) { db = current })
	ctx, cancel := context.WithTimeout(celltest.Context(context.Background()), 15*time.Second)
	defer cancel()
	at := time.Now().UTC().Truncate(time.Millisecond)
	fixture.SetTime(at)
	setDefault := func(value int) {
		_, err := db.ExecContext(ctx, `SELECT pgjobdb.set_crash_concern_threshold($1)`, value)
		require.NoError(t, err)
	}
	submit := func(id string) (jobdb.JobHandle, error) {
		return celltest.Submit(fixture.Runtime, ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{
			TenantId: "c2", Cell: "test", CellRepo: "example.test/test", JobID: id, JobType: "work",
			Data: jobdb.NewTaskDataOrPanic("original"),
		}})
	}
	checkCredit := func(key jobdb.JobKey, expected int64) {
		view, err := fixture.Runtime.GetJob(ctx, key)
		require.NoError(t, err)
		require.NotNil(t, view.ExecutionState.CrashBudgetRemaining)
		require.Equal(t, expected, *view.ExecutionState.CrashBudgetRemaining)
	}
	setDefault(2)
	setter, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer setter.Rollback()
	_, err = setter.ExecContext(ctx, `SELECT pgjobdb.set_crash_concern_threshold(7)`)
	require.NoError(t, err)
	old, err := submit("before-commit")
	require.NoError(t, err)
	checkCredit(old.JobKey, 2)
	require.NoError(t, setter.Commit())
	fresh, err := submit("after-commit")
	require.NoError(t, err)
	checkCredit(fresh.JobKey, 7)
	checkCredit(old.JobKey, 2)
	setDefault(1)
	checkCredit(fresh.JobKey, 7)
	exhausted, err := submit("exhausted")
	require.NoError(t, err)
	claim := func() (jobdb.ExecutionLease, error) {
		return fixture.Runtime.GetJobLease(ctx, jobdb.GetJobLeaseRequest{
			JobKey: exhausted.JobKey, WorkerID: "worker", Routes: []jobdb.Route{{JobType: "work"}}, LeaseDuration: time.Second,
		})
	}
	for step := range 2 {
		fixture.SetTime(at.Add(time.Duration(step) * 2 * time.Second))
		lease, err := claim()
		require.NoError(t, err)
		require.NotNil(t, lease)
		require.EqualValues(t, 1-step, *lease.ExecutionState().CrashBudgetRemaining)
	}
	fixture.SetTime(at.Add(4 * time.Second))
	setDefault(7)
	lease, err := claim()
	require.NoError(t, err)
	require.Nil(t, lease)
	checkCredit(exhausted.JobKey, 0)
	require.NoError(t, pgjobdb.ClearCrashConcern(ctx, db, jobdb.ClearCrashConcernRequest{
		JobKey: exhausted.JobKey, WorkerID: "operator", Reason: "worker repaired",
	}))
	checkCredit(exhausted.JobKey, 7)
	_, err = db.ExecContext(ctx, `SELECT pgjobdb.set_crash_concern_threshold(0)`)
	require.ErrorContains(t, err, "threshold must be positive")
	_, err = db.ExecContext(ctx, `DELETE FROM pgjobdb.resource_configuration`)
	require.NoError(t, err)
	_, err = submit("invalid-config")
	require.Error(t, err)
	var inserted bool
	require.NoError(t, db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM pgjobdb.jobs WHERE job_id='invalid-config')`).Scan(&inserted))
	require.False(t, inserted)
}

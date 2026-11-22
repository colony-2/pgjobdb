package integration_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/runtimetest"
	"github.com/colony-2/pgjobdb/pkg/pgjobdb"
	"github.com/stretchr/testify/require"
)

// Run one mutation inside A's open transaction. Demonstrate that B is waiting
// on A's existing tenant advisory lock before A commits. No timing-based guess
// about which writer wins is used; each tested ordering names its first writer.
func orderedGoalWriters(t *testing.T, ctx context.Context, db *sql.DB, first, second func(pgjobdb.DB) error) error {
	t.Helper()
	a, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer a.Rollback()
	_, err = a.ExecContext(ctx, `SELECT pgjobdb._goal_lock('tenant')`)
	require.NoError(t, err)
	require.NoError(t, first(a))
	b, err := db.Conn(ctx)
	require.NoError(t, err)
	defer b.Close()
	var pid int
	require.NoError(t, b.QueryRowContext(ctx, `SELECT pg_backend_pid()`).Scan(&pid))
	result := make(chan error, 1)
	go func() { result <- second(b) }()
	require.Eventually(t, func() bool {
		var waiting bool
		err := db.QueryRowContext(ctx, `SELECT wait_event='advisory' FROM pg_stat_activity WHERE pid=$1`, pid).Scan(&waiting)
		return err == nil && waiting
	}, 5*time.Second, time.Millisecond)
	t.Logf("A holds tenant lock; B pid %d is waiting on advisory; committing A", pid)
	require.NoError(t, a.Commit())
	err = <-result
	t.Logf("B completed after A commit: %v", err)
	return err
}

func reopenedGoal(t *testing.T, ctx context.Context, db *sql.DB, key jobdb.GoalKey) jobdb.GoalInfo {
	t.Helper()
	var port string
	require.NoError(t, db.QueryRowContext(ctx, `SHOW port`).Scan(&port))
	fresh, err := sql.Open("postgres", fmt.Sprintf("host=localhost port=%s user=postgres password=postgres dbname=postgres sslmode=disable", port))
	require.NoError(t, err)
	defer fresh.Close()
	g, err := pgjobdb.GetGoal(ctx, fresh, key)
	require.NoError(t, err)
	return g
}

func TestRetainedGoalIndependentCommitOrders(t *testing.T) {
	runDatabaseTest(t, func(ctx context.Context, db *sql.DB) {
		ctx = runtimetest.PrimaryGoalContext(ctx)
		for _, reverse := range []bool{false, true} {
			t.Run(fmt.Sprintf("failure_success_reverse_%v", reverse), func(t *testing.T) {
				source := nativeGoalJob(t, ctx, db, fmt.Sprintf("fs-%v", reverse))
				lease := nativeGoalLease(t, ctx, db, string(source.JobID))
				key := nativeGoalKey(source)
				fail := func(q pgjobdb.DB) error {
					_, err := pgjobdb.FailGoal(ctx, q, jobdb.FailGoalRequest{GoalKey: key, Message: "first closure"})
					return err
				}
				success := func(q pgjobdb.DB) error {
					return lease.Complete(ctx, q, pgjobdb.Completion{Status: pgjobdb.CompletionSuccess})
				}
				if reverse {
					err := orderedGoalWriters(t, ctx, db, success, fail)
					require.ErrorIs(t, err, jobdb.ErrGoalConflict)
				} else {
					require.NoError(t, orderedGoalWriters(t, ctx, db, fail, success))
				}
				g := reopenedGoal(t, ctx, db, key)
				if reverse {
					require.Equal(t, jobdb.GoalSucceeded, g.State)
					require.Empty(t, g.FailureJobID)
				} else {
					require.Equal(t, jobdb.GoalFailed, g.State)
					require.NotEmpty(t, g.FailureJobID)
					require.Equal(t, []string{string(source.JobID)}, g.Failure.NonTerminalJobIDs)
				}
			})
			t.Run(fmt.Sprintf("failure_admission_reverse_%v", reverse), func(t *testing.T) {
				source := nativeGoalJob(t, ctx, db, fmt.Sprintf("fa-%v", reverse))
				retireNativeMember(t, ctx, db, string(source.JobID))
				key := nativeGoalKey(source)
				_, request := selectedNative(t, ctx, db, fmt.Sprintf("fa-member-%v", reverse), source.GoalID, nil)
				fail := func(q pgjobdb.DB) error {
					_, err := pgjobdb.FailGoal(ctx, q, jobdb.FailGoalRequest{GoalKey: key, Message: "stop"})
					return err
				}
				admit := func(q pgjobdb.DB) error { _, err := pgjobdb.SubmitJob(ctx, q, request); return err }
				if reverse {
					require.NoError(t, orderedGoalWriters(t, ctx, db, admit, fail))
				} else {
					err := orderedGoalWriters(t, ctx, db, fail, admit)
					require.ErrorIs(t, err, jobdb.ErrGoalConflict)
				}
				g := reopenedGoal(t, ctx, db, key)
				require.Equal(t, jobdb.GoalFailed, g.State)
				var active int
				require.NoError(t, db.QueryRowContext(ctx, `SELECT count(*) FROM pgjobdb.jobs WHERE tenant_id='tenant' AND goal_id=$1`, key.GoalID).Scan(&active))
				if reverse {
					require.Equal(t, 1, active)
					require.Contains(t, g.Failure.NonTerminalJobIDs, string(request.JobID))
				} else {
					require.Zero(t, active)
				}
			})
			t.Run(fmt.Sprintf("failure_wait_reverse_%v", reverse), func(t *testing.T) {
				source := nativeGoalJob(t, ctx, db, fmt.Sprintf("fw-%v", reverse))
				consumer := nativeGoalJob(t, ctx, db, fmt.Sprintf("fw-consumer-%v", reverse))
				lease := nativeGoalLease(t, ctx, db, string(consumer.JobID))
				key := nativeGoalKey(source)
				fail := func(q pgjobdb.DB) error {
					_, err := pgjobdb.FailGoal(ctx, q, jobdb.FailGoalRequest{GoalKey: key, Message: "stop waiter"})
					return err
				}
				wait := func(q pgjobdb.DB) error {
					return lease.Reschedule(ctx, q, pgjobdb.RescheduleRequest{RouteJobType: "work", WorkKind: pgjobdb.WorkKindJob, DependencyWait: nativeGoalWait("joined", key.GoalID)})
				}
				if reverse {
					require.NoError(t, orderedGoalWriters(t, ctx, db, wait, fail))
				} else {
					require.NoError(t, orderedGoalWriters(t, ctx, db, fail, wait))
				}
				g := reopenedGoal(t, ctx, db, key)
				w, err := pgjobdb.GetDependencyWait(ctx, db, jobdb.DependencyWaitKey{JobKey: jobdb.JobKey{TenantId: "tenant", JobId: string(consumer.JobID)}, WaitID: "joined"})
				require.NoError(t, err)
				require.Equal(t, jobdb.DependencyFailed, w.State)
				require.Equal(t, g.Failure, w.Failure)
				var targets int
				require.NoError(t, db.QueryRowContext(ctx, `SELECT count(*) FROM pgjobdb.dependency_targets WHERE job_id=$1`, consumer.JobID).Scan(&targets))
				require.Zero(t, targets)
			})
		}
		for _, reverse := range []bool{false, true} {
			t.Run(fmt.Sprintf("multi_target_reverse_%v", reverse), func(t *testing.T) {
				a := nativeGoalJob(t, ctx, db, fmt.Sprintf("multi-a-%v", reverse))
				b := nativeGoalJob(t, ctx, db, fmt.Sprintf("multi-b-%v", reverse))
				consumer := nativeGoalJob(t, ctx, db, fmt.Sprintf("multi-consumer-%v", reverse))
				la := nativeGoalLease(t, ctx, db, string(a.JobID))
				lb := nativeGoalLease(t, ctx, db, string(b.JobID))
				lc := nativeGoalLease(t, ctx, db, string(consumer.JobID))
				wait := nativeGoalWait("multi", a.GoalID)
				wait.Dependencies = append(wait.Dependencies, jobdb.Dependency{Target: jobdb.DependencyTarget{GoalID: b.GoalID}, Condition: jobdb.JobPrereqSuccess})
				require.NoError(t, lc.Reschedule(ctx, db, pgjobdb.RescheduleRequest{RouteJobType: "work", WorkKind: pgjobdb.WorkKindJob, DependencyWait: wait}))
				ca := func(q pgjobdb.DB) error {
					return la.Complete(ctx, q, pgjobdb.Completion{Status: pgjobdb.CompletionSuccess})
				}
				cb := func(q pgjobdb.DB) error {
					return lb.Complete(ctx, q, pgjobdb.Completion{Status: pgjobdb.CompletionSuccess})
				}
				if reverse {
					require.NoError(t, orderedGoalWriters(t, ctx, db, cb, ca))
				} else {
					require.NoError(t, orderedGoalWriters(t, ctx, db, ca, cb))
				}
				key := jobdb.DependencyWaitKey{JobKey: jobdb.JobKey{TenantId: "tenant", JobId: string(consumer.JobID)}, WaitID: "multi"}
				settled, err := pgjobdb.GetDependencyWait(ctx, db, key)
				require.NoError(t, err)
				require.Equal(t, jobdb.DependencySatisfied, settled.State)
				require.Equal(t, string(a.JobID), settled.Dependencies[0].SatisfiedByJobID)
				require.Equal(t, string(b.JobID), settled.Dependencies[1].SatisfiedByJobID)
				lease := nativeGoalLease(t, ctx, db, string(consumer.JobID))
				require.NoError(t, lease.Reschedule(ctx, db, pgjobdb.RescheduleRequest{RouteJobType: "work", WorkKind: pgjobdb.WorkKindJob, DependencyWait: nativeGoalWait("replacement", a.GoalID)}))
				frozen, err := pgjobdb.GetDependencyWait(ctx, db, key)
				require.NoError(t, err)
				require.Equal(t, settled, frozen)
				var targets int
				require.NoError(t, db.QueryRowContext(ctx, `SELECT count(*) FROM pgjobdb.dependency_targets WHERE job_id=$1`, consumer.JobID).Scan(&targets))
				require.Zero(t, targets)
			})
		}

		for _, same := range []bool{false, true} {
			for _, reverse := range []bool{false, true} {
				t.Run(fmt.Sprintf("failure_failure_equal_%v_reverse_%v", same, reverse), func(t *testing.T) {
					source := nativeGoalJob(t, ctx, db, fmt.Sprintf("ff-%v-%v", same, reverse))
					key := nativeGoalKey(source)
					message := "different"
					if same {
						message = "same"
					}
					var aResult, bResult jobdb.GoalInfo
					a := func(q pgjobdb.DB) error {
						var err error
						aResult, err = pgjobdb.FailGoal(ctx, q, jobdb.FailGoalRequest{GoalKey: key, Message: "same"})
						return err
					}
					b := func(q pgjobdb.DB) error {
						var err error
						bResult, err = pgjobdb.FailGoal(ctx, q, jobdb.FailGoalRequest{GoalKey: key, Message: message})
						return err
					}
					var err error
					var winner jobdb.GoalInfo
					if reverse {
						err = orderedGoalWriters(t, ctx, db, b, a)
						winner = bResult
					} else {
						err = orderedGoalWriters(t, ctx, db, a, b)
						winner = aResult
					}
					if same {
						require.NoError(t, err)
						require.Equal(t, aResult, bResult)
					} else {
						require.ErrorIs(t, err, jobdb.ErrGoalConflict)
					}
					require.Equal(t, winner, reopenedGoal(t, ctx, db, key))
					var count int
					require.NoError(t, db.QueryRowContext(ctx, `SELECT count(*) FROM pgjobdb.job_facts WHERE goal_id=$1 AND record_kind='GOAL_FAILURE'`, key.GoalID).Scan(&count))
					require.Equal(t, 1, count)
				})
			}
		}
	})
}

func TestRetainedGoalSimultaneousIdleAdmission(t *testing.T) {
	runDatabaseTest(t, func(ctx context.Context, db *sql.DB) {
		ctx = runtimetest.PrimaryGoalContext(ctx)
		source := nativeGoalJob(t, ctx, db, "idle-origin")
		retireNativeMember(t, ctx, db, "idle-origin")
		_, a := selectedNative(t, ctx, db, "idle-a", source.GoalID, nil)
		_, b := selectedNative(t, ctx, db, "idle-b", source.GoalID, nil)
		var n int
		require.NoError(t, db.QueryRowContext(ctx, `SELECT count(*) FROM pgjobdb.jobs WHERE goal_id=$1`, source.GoalID).Scan(&n))
		require.Zero(t, n, "preparation reserves no capacity")
		errs := raceGoalWrites(t, ctx, db, func(q pgjobdb.DB) error { _, e := pgjobdb.SubmitJob(ctx, q, a); return e }, func(q pgjobdb.DB) error { _, e := pgjobdb.SubmitJob(ctx, q, b); return e })
		winners := 0
		for _, err := range errs {
			if err == nil {
				winners++
			} else {
				require.ErrorIs(t, err, jobdb.ErrGoalConflict)
			}
		}
		require.Equal(t, 1, winners)
		require.NoError(t, db.QueryRowContext(ctx, `SELECT count(*) FROM pgjobdb.jobs WHERE goal_id=$1`, source.GoalID).Scan(&n))
		require.Equal(t, 1, n)
	})
}

func TestRetainedGoalPublicationRollbackAndReopen(t *testing.T) {
	runDatabaseTest(t, func(ctx context.Context, db *sql.DB) {
		source := nativeGoalJob(t, ctx, db, "fault-origin")
		key := nativeGoalKey(source)
		nativeGoalJob(t, ctx, db, "fault-waiter")
		lease := nativeGoalLease(t, ctx, db, "fault-waiter")
		require.NoError(t, lease.Reschedule(ctx, db, pgjobdb.RescheduleRequest{RouteJobType: "work", WorkKind: pgjobdb.WorkKindJob, DependencyWait: nativeGoalWait("wait", source.GoalID)}))
		_, err := db.ExecContext(ctx, `ALTER TABLE pgjobdb.jobs_archive ADD CONSTRAINT injected_terminal_failure CHECK(completion_status<>'failed') NOT VALID`)
		require.NoError(t, err)
		before := schedulerSnapshot(t, ctx, db)
		request := jobdb.FailGoalRequest{GoalKey: key, Message: " exact\n保存 "}
		_, err = pgjobdb.FailGoal(ctx, db, request)
		require.ErrorContains(t, err, "injected_terminal_failure")
		require.Equal(t, before, schedulerSnapshot(t, ctx, db))
		_, err = db.ExecContext(ctx, `ALTER TABLE pgjobdb.jobs_archive DROP CONSTRAINT injected_terminal_failure`)
		require.NoError(t, err)
		// Commit the result, discard its response, then reopen and retry that same
		// request. No public identity or first-closure snapshot is allocated twice.
		committed, err := pgjobdb.FailGoal(ctx, db, request)
		require.NoError(t, err)
		var raw string
		require.NoError(t, db.QueryRowContext(ctx, `SELECT terminal_failure::text FROM pgjobdb.job_facts WHERE job_id=$1`, committed.FailureJobID).Scan(&raw))
		require.True(t, json.Valid([]byte(raw)))
		replay, err := pgjobdb.FailGoal(ctx, db, request)
		require.NoError(t, err)
		require.Equal(t, committed, replay)
		require.Equal(t, committed, reopenedGoal(t, ctx, db, key))
		_, err = pgjobdb.DeleteJobs(ctx, db, pgjobdb.DeleteJobsRequest{TenantID: "tenant", JobIDs: []pgjobdb.JobID{pgjobdb.JobID(committed.FailureJobID), "fault-origin"}})
		require.NoError(t, err)
		replay, err = pgjobdb.FailGoal(ctx, db, request)
		require.NoError(t, err)
		require.Equal(t, committed, replay)
		request.Message = "changed"
		_, err = pgjobdb.FailGoal(ctx, db, request)
		require.ErrorIs(t, err, jobdb.ErrGoalConflict)
		var active, events int
		require.NoError(t, db.QueryRowContext(ctx, `SELECT (SELECT count(*) FROM pgjobdb.jobs WHERE job_id=$1),(SELECT count(*) FROM pgjobdb.lease_events WHERE job_id=$1)`, committed.FailureJobID).Scan(&active, &events))
		require.Zero(t, active)
		require.Zero(t, events)
	})
}

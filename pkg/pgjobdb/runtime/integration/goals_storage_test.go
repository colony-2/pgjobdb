package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/pgjobdb/pkg/pgjobdb"
	"github.com/stretchr/testify/require"
)

func nativeGoalJob(t *testing.T, ctx context.Context, db pgjobdb.DB, id string) *pgjobdb.JobDetail {
	t.Helper()
	_, err := pgjobdb.SubmitJob(ctx, db, pgjobdb.SubmitJobRequest{TenantID: "tenant", JobID: pgjobdb.JobID(id), WorkerID: "submit", JobType: "work"})
	require.NoError(t, err)
	j, err := pgjobdb.GetJob(ctx, db, "tenant", pgjobdb.JobID(id))
	require.NoError(t, err)
	require.NotEmpty(t, j.GoalID)
	return j
}
func nativeGoalLease(t *testing.T, ctx context.Context, db pgjobdb.DB, id string) *pgjobdb.JobLease {
	t.Helper()
	l, err := pgjobdb.GetJobLease(ctx, db, "tenant", pgjobdb.JobID(id), "worker", pgjobdb.WorkSelector{JobTypes: []pgjobdb.JobType{"work"}}, pgjobdb.GetJobLeaseOptions{LeaseDuration: time.Minute})
	require.NoError(t, err)
	require.NotNil(t, l)
	return l
}
func nativeGoalKey(j *pgjobdb.JobDetail) jobdb.GoalKey {
	return jobdb.GoalKey{TenantId: "tenant", GoalID: j.GoalID}
}
func nativeGoalWait(id, goal string) *jobdb.DependencyWait {
	return &jobdb.DependencyWait{WaitID: id, Dependencies: []jobdb.Dependency{{Target: jobdb.DependencyTarget{GoalID: goal}, Condition: jobdb.JobPrereqSuccess}}}
}

func TestPostgresGoalReservationAndRetention(t *testing.T) {
	runDatabaseTest(t, func(ctx context.Context, db *sql.DB) {
		req := pgjobdb.JobPreparation{Key: jobdb.JobKey{TenantId: "tenant", JobId: "reserved"}, Descriptor: "semantic-request"}
		prepared, err := pgjobdb.PrepareJob(ctx, db, req)
		require.NoError(t, err)
		require.False(t, prepared.Published)
		_, err = pgjobdb.GetGoal(ctx, db, jobdb.GoalKey{TenantId: "tenant", GoalID: prepared.GoalID})
		require.ErrorIs(t, err, jobdb.ErrGoalNotFound)
		_, err = pgjobdb.GetJob(ctx, db, "tenant", "reserved")
		require.ErrorIs(t, err, pgjobdb.ErrJobNotFound)
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		retry, err := pgjobdb.PrepareJob(ctx, conn, req)
		require.NoError(t, err)
		require.Equal(t, prepared, retry)
		req.Descriptor = "changed"
		_, err = pgjobdb.PrepareJob(ctx, conn, req)
		require.ErrorIs(t, err, jobdb.ErrConflict)
		req.Descriptor = "semantic-request"
		_, err = pgjobdb.SubmitJob(ctx, conn, pgjobdb.SubmitJobRequest{TenantID: "tenant", JobID: "reserved", GoalID: prepared.GoalID, JobType: "work", WorkerID: "publisher"})
		require.NoError(t, err)
		var active, facts string
		require.NoError(t, db.QueryRowContext(ctx, `SELECT j.goal_id,f.goal_id FROM pgjobdb.jobs j JOIN pgjobdb.job_facts f USING(tenant_id,job_id) WHERE j.job_id='reserved'`).Scan(&active, &facts))
		require.Equal(t, prepared.GoalID, active)
		require.Equal(t, active, facts)
		var count int
		require.NoError(t, db.QueryRowContext(ctx, `SELECT count(*) FROM pgjobdb.goal_candidates`).Scan(&count))
		require.Zero(t, count)
		lease := nativeGoalLease(t, ctx, conn, "reserved")
		before, err := pgjobdb.GetGoal(ctx, db, jobdb.GoalKey{TenantId: "tenant", GoalID: prepared.GoalID})
		require.NoError(t, err)
		require.NoError(t, lease.Complete(ctx, conn, pgjobdb.Completion{Status: pgjobdb.CompletionFailedApp}))
		after, err := pgjobdb.GetGoal(ctx, db, before.GoalKey)
		require.NoError(t, err)
		require.Equal(t, before, after, "failed candidates do not rewrite goals")
		_, err = pgjobdb.DeleteJobs(ctx, db, pgjobdb.DeleteJobsRequest{TenantID: "tenant", JobIDs: []pgjobdb.JobID{"reserved"}})
		require.NoError(t, err)
		failed, err := pgjobdb.FailGoal(ctx, db, jobdb.FailGoalRequest{GoalKey: before.GoalKey, Message: "retained failure"})
		require.NoError(t, err)
		require.Equal(t, []string{"reserved"}, failed.Failure.TerminalJobIDs)
		retry, err = pgjobdb.PrepareJob(ctx, conn, req)
		require.NoError(t, err)
		require.True(t, retry.Published)
		require.Equal(t, prepared.GoalID, retry.GoalID)
		require.NoError(t, db.QueryRowContext(ctx, `SELECT goal_id FROM pgjobdb.jobs_archive WHERE job_id='reserved'`).Scan(&active))
		require.Equal(t, prepared.GoalID, active)
	})
}

// Both sessions are demonstrably blocked on the database advisory lock before
// release, so these races exercise independent PostgreSQL transactions.
func raceGoalWrites(t *testing.T, ctx context.Context, db *sql.DB, a, b func(pgjobdb.DB) error) []error {
	t.Helper()
	gate, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer gate.Rollback()
	_, err = gate.ExecContext(ctx, `SELECT pgjobdb._goal_lock('tenant')`)
	require.NoError(t, err)
	c1, err := db.Conn(ctx)
	require.NoError(t, err)
	defer c1.Close()
	c2, err := db.Conn(ctx)
	require.NoError(t, err)
	defer c2.Close()
	var p1, p2 int
	require.NoError(t, c1.QueryRowContext(ctx, `SELECT pg_backend_pid()`).Scan(&p1))
	require.NoError(t, c2.QueryRowContext(ctx, `SELECT pg_backend_pid()`).Scan(&p2))
	out := make(chan error, 2)
	go func() { out <- a(c1) }()
	go func() { out <- b(c2) }()
	require.Eventually(t, func() bool {
		var n int
		err := db.QueryRowContext(ctx, `SELECT count(*) FROM pg_stat_activity WHERE pid IN ($1,$2) AND wait_event='advisory'`, p1, p2).Scan(&n)
		return err == nil && n == 2
	}, 5*time.Second, 10*time.Millisecond)
	require.NoError(t, gate.Commit())
	return []error{<-out, <-out}
}
func TestPostgresGoalIndependentWriters(t *testing.T) {
	runDatabaseTest(t, func(ctx context.Context, db *sql.DB) {
		t.Run("attach_and_complete", func(t *testing.T) {
			a := nativeGoalJob(t, ctx, db, "attach-a")
			b := nativeGoalJob(t, ctx, db, "attach-b")
			l := nativeGoalLease(t, ctx, db, "attach-b")
			errs := raceGoalWrites(t, ctx, db, func(q pgjobdb.DB) error {
				_, e := pgjobdb.AddGoalCandidate(ctx, q, jobdb.AddGoalCandidateRequest{GoalKey: nativeGoalKey(a), JobID: "attach-b"})
				return e
			}, func(q pgjobdb.DB) error {
				return l.Complete(ctx, q, pgjobdb.Completion{Status: pgjobdb.CompletionSuccess})
			})
			for _, err := range errs {
				require.NoError(t, err)
			}
			for _, j := range []*pgjobdb.JobDetail{a, b} {
				g, err := pgjobdb.GetGoal(ctx, db, nativeGoalKey(j))
				require.NoError(t, err)
				require.Equal(t, "attach-b", g.SuccessfulJobID)
			}
		})
		t.Run("attach_and_fail", func(t *testing.T) {
			a := nativeGoalJob(t, ctx, db, "fail-a")
			nativeGoalJob(t, ctx, db, "fail-b")
			errs := raceGoalWrites(t, ctx, db, func(q pgjobdb.DB) error {
				_, e := pgjobdb.AddGoalCandidate(ctx, q, jobdb.AddGoalCandidateRequest{GoalKey: nativeGoalKey(a), JobID: "fail-b"})
				return e
			}, func(q pgjobdb.DB) error {
				_, e := pgjobdb.FailGoal(ctx, q, jobdb.FailGoalRequest{GoalKey: nativeGoalKey(a), Message: "stop"})
				return e
			})
			for _, err := range errs {
				if err != nil {
					require.ErrorIs(t, err, jobdb.ErrGoalConflict)
				}
			}
			g, err := pgjobdb.GetGoal(ctx, db, nativeGoalKey(a))
			require.NoError(t, err)
			var n int
			require.NoError(t, db.QueryRowContext(ctx, `SELECT count(*) FROM pgjobdb.goal_candidates WHERE goal_id=$1`, a.GoalID).Scan(&n))
			require.Len(t, g.Failure.NonTerminalJobIDs, n+1)
		})
		t.Run("register_and_resolve", func(t *testing.T) {
			p := nativeGoalJob(t, ctx, db, "resolve-producer")
			nativeGoalJob(t, ctx, db, "resolve-consumer")
			lp := nativeGoalLease(t, ctx, db, "resolve-producer")
			lc := nativeGoalLease(t, ctx, db, "resolve-consumer")
			errs := raceGoalWrites(t, ctx, db, func(q pgjobdb.DB) error {
				return lc.Reschedule(ctx, q, pgjobdb.RescheduleRequest{RouteJobType: "work", WorkKind: pgjobdb.WorkKindJob, DependencyWait: nativeGoalWait("join", p.GoalID)})
			}, func(q pgjobdb.DB) error {
				return lp.Complete(ctx, q, pgjobdb.Completion{Status: pgjobdb.CompletionSuccess})
			})
			for _, err := range errs {
				require.NoError(t, err)
			}
			w, err := pgjobdb.GetDependencyWait(ctx, db, jobdb.DependencyWaitKey{JobKey: jobdb.JobKey{TenantId: "tenant", JobId: "resolve-consumer"}, WaitID: "join"})
			require.NoError(t, err)
			require.Equal(t, jobdb.DependencySatisfied, w.State)
			require.Equal(t, "resolve-producer", w.Dependencies[0].SatisfiedByJobID)
		})
		t.Run("graph_write_skew", func(t *testing.T) {
			a := nativeGoalJob(t, ctx, db, "cycle-a")
			b := nativeGoalJob(t, ctx, db, "cycle-b")
			la := nativeGoalLease(t, ctx, db, "cycle-a")
			lb := nativeGoalLease(t, ctx, db, "cycle-b")
			errs := raceGoalWrites(t, ctx, db, func(q pgjobdb.DB) error {
				return la.Reschedule(ctx, q, pgjobdb.RescheduleRequest{RouteJobType: "work", WorkKind: pgjobdb.WorkKindJob, DependencyWait: nativeGoalWait("join", b.GoalID)})
			}, func(q pgjobdb.DB) error {
				return lb.Reschedule(ctx, q, pgjobdb.RescheduleRequest{RouteJobType: "work", WorkKind: pgjobdb.WorkKindJob, DependencyWait: nativeGoalWait("join", a.GoalID)})
			})
			count := 0
			for _, err := range errs {
				if err != nil {
					require.ErrorIs(t, err, jobdb.ErrDependencyCycle)
					count++
				}
			}
			require.Equal(t, 1, count)
		})
		t.Run("publication_and_closure", func(t *testing.T) {
			a := nativeGoalJob(t, ctx, db, "publish-a")
			p, err := pgjobdb.PrepareJob(ctx, db, pgjobdb.JobPreparation{Key: jobdb.JobKey{TenantId: "tenant"}, RequestID: "restart-op", PriorJobID: "publish-a", ShareGoal: true, Descriptor: "same"})
			require.NoError(t, err)
			errs := raceGoalWrites(t, ctx, db, func(q pgjobdb.DB) error {
				_, e := pgjobdb.SubmitJob(ctx, q, pgjobdb.SubmitJobRequest{TenantID: "tenant", JobID: pgjobdb.JobID(p.Key.JobId), GoalID: p.GoalID, ShareGoal: true, WorkerID: "publish", JobType: "work"})
				return e
			}, func(q pgjobdb.DB) error {
				_, e := pgjobdb.FailGoal(ctx, q, jobdb.FailGoalRequest{GoalKey: nativeGoalKey(a), Message: "closed"})
				return e
			})
			for _, err := range errs {
				if err != nil {
					require.ErrorIs(t, err, jobdb.ErrGoalConflict)
				}
			}
			g, err := pgjobdb.GetGoal(ctx, db, nativeGoalKey(a))
			require.NoError(t, err)
			var n int
			require.NoError(t, db.QueryRowContext(ctx, `SELECT count(*) FROM pgjobdb.job_facts WHERE goal_id=$1`, a.GoalID).Scan(&n))
			require.Len(t, g.Failure.NonTerminalJobIDs, n)
		})
	})
}
func TestPostgresGoalDeadlineAndPagination(t *testing.T) {
	runDatabaseTest(t, func(ctx context.Context, db *sql.DB) {
		producer := nativeGoalJob(t, ctx, db, "producer")
		nativeGoalJob(t, ctx, db, "consumer")
		lease := nativeGoalLease(t, ctx, db, "consumer")
		deadline := time.Now().Add(120 * time.Millisecond)
		w := nativeGoalWait("timeout", producer.GoalID)
		w.Deadline = &deadline
		require.NoError(t, lease.Reschedule(ctx, db, pgjobdb.RescheduleRequest{RouteJobType: "resume", WorkKind: pgjobdb.WorkKindJob, DependencyWait: w}))
		key := jobdb.DependencyWaitKey{JobKey: jobdb.JobKey{TenantId: "tenant", JobId: "consumer"}, WaitID: w.WaitID}
		require.Eventually(t, func() bool {
			info, err := pgjobdb.GetDependencyWait(ctx, db, key)
			return err == nil && info.State == jobdb.DependencyTimedOut
		}, time.Second, 10*time.Millisecond)
		var state string
		require.NoError(t, db.QueryRowContext(ctx, `SELECT state FROM pgjobdb.dependency_waits WHERE job_id='consumer'`).Scan(&state))
		require.Equal(t, "PENDING", state, "inspection must not mutate")
		resumed, err := pgjobdb.GetWork(ctx, db, "poller", pgjobdb.WorkSelector{JobTypes: []pgjobdb.JobType{"resume"}}, pgjobdb.GetWorkOptions{TenantIDs: []pgjobdb.TenantID{"tenant"}})
		require.NoError(t, err)
		require.NotNil(t, resumed)
		require.Equal(t, pgjobdb.JobID("consumer"), resumed.JobID)
		require.NoError(t, db.QueryRowContext(ctx, `SELECT state FROM pgjobdb.dependency_waits WHERE job_id='consumer'`).Scan(&state))
		require.Equal(t, "TIMED_OUT", state)
		// Bindings stay fixed even if the producer succeeds after timeout.
		lp := nativeGoalLease(t, ctx, db, "producer")
		require.NoError(t, lp.Complete(ctx, db, pgjobdb.Completion{Status: pgjobdb.CompletionSuccess}))
		info, err := pgjobdb.GetDependencyWait(ctx, db, key)
		require.NoError(t, err)
		require.Equal(t, jobdb.DependencyTimedOut, info.State)
		group := nativeGoalJob(t, ctx, db, "group")
		for i := 0; i < 4; i++ {
			id := fmt.Sprintf("page-%d", i)
			nativeGoalJob(t, ctx, db, id)
			_, err := pgjobdb.AddGoalCandidate(ctx, db, jobdb.AddGoalCandidateRequest{GoalKey: nativeGoalKey(group), JobID: id})
			require.NoError(t, err)
		}
		opts := pgjobdb.ListJobsOptions{TenantIDs: []pgjobdb.TenantID{"tenant"}, GoalIDs: []string{group.GoalID}, PageSize: 2}
		seen := map[pgjobdb.JobID]bool{}
		for {
			page, err := pgjobdb.ListJobs(ctx, db, opts)
			require.NoError(t, err)
			for _, j := range page.Jobs {
				require.False(t, seen[j.JobID])
				seen[j.JobID] = true
			}
			if page.NextPageToken == "" {
				break
			}
			opts.PageToken = page.NextPageToken
		}
		require.Len(t, seen, 5)
		opts.GoalIDs = []string{producer.GoalID}
		_, err = pgjobdb.ListJobs(ctx, db, opts)
		require.Error(t, err, "page token binds goal filter")
	})
}

func TestPostgresGoalOrderedOutcomesAndDeletion(t *testing.T) {
	runDatabaseTest(t, func(ctx context.Context, db *sql.DB) {
		good := nativeGoalJob(t, ctx, db, "good")
		bad := nativeGoalJob(t, ctx, db, "bad")
		open := nativeGoalJob(t, ctx, db, "open")
		require.NoError(t, nativeGoalLease(t, ctx, db, "good").Complete(ctx, db, pgjobdb.Completion{Status: pgjobdb.CompletionSuccess}))
		failed, err := pgjobdb.FailGoal(ctx, db, jobdb.FailGoalRequest{GoalKey: nativeGoalKey(bad), Message: "blocked"})
		require.NoError(t, err)
		deps := []jobdb.Dependency{{Target: jobdb.DependencyTarget{JobID: "good"}, Condition: jobdb.JobPrereqSuccess}, {Target: jobdb.DependencyTarget{GoalID: bad.GoalID}, Condition: jobdb.JobPrereqSuccess}, {Target: jobdb.DependencyTarget{GoalID: open.GoalID}, Condition: jobdb.JobPrereqSuccess}}
		_, err = pgjobdb.SubmitJob(ctx, db, pgjobdb.SubmitJobRequest{TenantID: "tenant", JobID: "ordered", WorkerID: "submit", JobType: "work", Dependencies: deps})
		require.NoError(t, err)
		key := jobdb.DependencyWaitKey{JobKey: jobdb.JobKey{TenantId: "tenant", JobId: "ordered"}, WaitID: "submission"}
		w, err := pgjobdb.GetDependencyWait(ctx, db, key)
		require.NoError(t, err)
		require.Equal(t, jobdb.DependencyFailed, w.State)
		require.Equal(t, 1, *w.FailedDependencyIndex)
		require.Equal(t, *failed.Failure, *w.Failure)
		require.Equal(t, "good", w.Dependencies[0].SatisfiedByJobID)
		require.Equal(t, jobdb.DependencyCancelled, w.Dependencies[2].State)
		deps[0].Condition = jobdb.JobPrereqComplete
		_, err = pgjobdb.SubmitJob(ctx, db, pgjobdb.SubmitJobRequest{TenantID: "tenant", JobID: "ordered", WorkerID: "submit", JobType: "work", Dependencies: deps})
		require.ErrorIs(t, err, jobdb.ErrConflict)
		// Cancelling a pending consumer removes subscriptions, retaining its outcome.
		nativeGoalJob(t, ctx, db, "abandoned")
		l := nativeGoalLease(t, ctx, db, "abandoned")
		require.NoError(t, l.Reschedule(ctx, db, pgjobdb.RescheduleRequest{RouteJobType: "work", WorkKind: pgjobdb.WorkKindJob, DependencyWait: nativeGoalWait("join", open.GoalID)}))
		_, err = pgjobdb.DeleteJobs(ctx, db, pgjobdb.DeleteJobsRequest{TenantID: "tenant", JobIDs: []pgjobdb.JobID{"abandoned", "open"}})
		require.NoError(t, err)
		retained, err := pgjobdb.GetDependencyWait(ctx, db, jobdb.DependencyWaitKey{JobKey: jobdb.JobKey{TenantId: "tenant", JobId: "abandoned"}, WaitID: "join"})
		require.NoError(t, err)
		require.Equal(t, jobdb.DependencyCancelled, retained.State)
		g, err := pgjobdb.GetGoal(ctx, db, nativeGoalKey(open))
		require.NoError(t, err)
		require.Equal(t, jobdb.GoalOpen, g.State)
		var n int
		require.NoError(t, db.QueryRowContext(ctx, `SELECT count(*) FROM pgjobdb.dependency_targets WHERE job_id='abandoned'`).Scan(&n))
		require.Zero(t, n)
		own, err := pgjobdb.GetGoal(ctx, db, nativeGoalKey(good))
		require.NoError(t, err)
		require.Equal(t, jobdb.GoalSucceeded, own.State)
	})
}

func TestPostgresGoalLateSettlementHonorsDeadline(t *testing.T) {
	runDatabaseTest(t, func(ctx context.Context, db *sql.DB) {
		p := nativeGoalJob(t, ctx, db, "late-producer")
		nativeGoalJob(t, ctx, db, "late-consumer")
		producer := nativeGoalLease(t, ctx, db, "late-producer")
		consumer := nativeGoalLease(t, ctx, db, "late-consumer")
		deadline := time.Now().Add(100 * time.Millisecond)
		wait := nativeGoalWait("join", p.GoalID)
		wait.Deadline = &deadline
		req := pgjobdb.RescheduleRequest{RouteJobType: "work", WorkKind: pgjobdb.WorkKindJob, DependencyWait: wait}
		require.NoError(t, consumer.Reschedule(ctx, db, req))
		require.Error(t, consumer.Reschedule(ctx, db, req), "identical descriptor does not waive lease validation")
		require.Eventually(t, func() bool { return time.Now().After(deadline) }, time.Second, 10*time.Millisecond)
		require.NoError(t, producer.Complete(ctx, db, pgjobdb.Completion{Status: pgjobdb.CompletionSuccess}))
		var state string
		require.NoError(t, db.QueryRowContext(ctx, `SELECT state FROM pgjobdb.dependency_waits WHERE job_id='late-consumer'`).Scan(&state))
		require.Equal(t, "TIMED_OUT", state)
		nativeGoalLease(t, ctx, db, "late-consumer")
	})
}

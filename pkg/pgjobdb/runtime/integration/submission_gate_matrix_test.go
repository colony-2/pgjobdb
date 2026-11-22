package integration_test

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	wf "github.com/colony-2/jobdb/pkg/workflow"
	"github.com/colony-2/pgjobdb/pkg/pgjobdb"
	"github.com/stretchr/testify/require"
)

func (f *availabilityFixture) databaseTime() time.Time {
	var now time.Time
	require.NoError(f.t, f.db.QueryRowContext(f.ctx, `SELECT clock_timestamp()`).Scan(&now))
	return now.UTC()
}
func (f *availabilityFixture) waitUntil(at time.Time) {
	remaining := at.Sub(f.databaseTime())
	if remaining < 0 {
		remaining = 0
	}
	require.Eventually(f.t, func() bool { return !f.databaseTime().Before(at) }, remaining+8*time.Second, 10*time.Millisecond)
}
func (f *availabilityFixture) deny(key jobdb.JobKey) {
	t := f.t
	for range 3 {
		before := f.databaseTime()
		l, err := f.backend.GetJobLease(f.ctx, jobdb.GetJobLeaseRequest{JobKey: key, WorkerID: "early", Routes: []jobdb.Route{{JobType: "child"}}})
		require.NoError(t, err)
		require.Nil(t, l, "targeted acquisition at %s", before)
		leases, err := f.client.PollWork(f.ctx, jobdb.PollWorkRequest{TenantId: "tenant", WorkerID: "early", Routes: []jobdb.Route{{JobType: "child"}}, Limit: 1, MetadataEquals: []jobdb.MetadataPredicate{{Path: []string{"consumer"}, Values: []any{"consumer"}}}})
		require.NoError(t, err)
		require.Empty(t, leases, "poll at %s", before)
	}
}
func (f *availabilityFixture) complete(key jobdb.JobKey, success bool) {
	l, err := f.backend.GetJobLease(f.ctx, jobdb.GetJobLeaseRequest{JobKey: key, WorkerID: "producer", Routes: []jobdb.Route{{JobType: "child"}}})
	require.NoError(f.t, err)
	require.NotNil(f.t, l)
	chapter := jobdb.Chapter{Ordinal: 1, TaskType: "child", CreatedAt: time.Now().UTC(), Body: jobdb.JobAttemptOutcomeChapter{Outcome: jobdb.ApplicationOutputOutcome{Output: jobdb.ApplicationOutputBytes{Data: []byte(`null`)}}}}
	status := "success"
	if !success {
		status = "failed_app"
		chapter.Body = jobdb.JobAttemptOutcomeChapter{Outcome: jobdb.AppErrorOutcome{Error: jobdb.AppErrorPayload{Message: "expected prerequisite failure"}}}
	}
	require.NoError(f.t, l.Complete(f.ctx, jobdb.CompleteExecutionRequest{Status: status, Chapter: &chapter}))
}

func TestSubmissionAvailabilityGateMatrix(t *testing.T) {
	newDSN := conformanceDSNFactory(t)
	// Each case installs and reopens a real database. Bound fixture startup so
	// CPU and I/O contention cannot consume its observation window.
	fixtures := make(chan struct{}, 3)
	for _, route := range availabilityRoutes {
		for _, kind := range []string{"completion", "exact-success", "goal-success", "mixed"} {
			for _, timeFirst := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/%s/time-first=%v", route, kind, timeFirst), func(t *testing.T) {
					t.Parallel()
					fixtures <- struct{}{}
					defer func() { <-fixtures }()
					f := newAvailabilityFixture(t, newDSN(t))
					submit := f.submitter(route)
					kinds := []string{kind}
					if kind == "mixed" {
						kinds = []string{"completion", "exact-success", "goal-success"}
					}
					var producers []jobdb.JobHandle
					var deps []jobdb.Dependency
					for i, k := range kinds {
						p, err := f.backend.SubmitJob(f.ctx, availabilityRequest(fmt.Sprintf("producer-%d", i), nil))
						require.NoError(t, err)
						producers = append(producers, p)
						d := jobdb.Dependency{Target: jobdb.DependencyTarget{JobID: p.JobKey.JobId}, Condition: jobdb.JobPrereqSuccess}
						if k == "completion" {
							d.Condition = jobdb.JobPrereqComplete
						}
						if k == "goal-success" {
							d.Target = jobdb.DependencyTarget{GoalID: p.GoalID}
						}
						deps = append(deps, d)
					}
					var later jobdb.JobHandle
					if kind == "goal-success" || kind == "mixed" {
						p := producers[len(producers)-1]
						// A failed attempt leaves the goal open for a recovery producer.
						f.complete(p.JobKey, false)
						recoveryRequest := availabilityRequest("recovery-producer", nil)
						recoveryRequest.Job.GoalID = p.GoalID
						recovery, err := f.backend.SubmitJob(f.ctx, recoveryRequest)
						require.NoError(t, err)
						producers[len(producers)-1] = recovery
						later, err = f.backend.SubmitJob(f.ctx, availabilityRequest("independent-later", nil))
						require.NoError(t, err)
					}
					// Leave room for real installer verification on reopen while the
					// parallel matrix starts other databases. The assertions still
					// require observing both gates before this exact persisted time.
					original := f.databaseTime().Add(10 * time.Second).Truncate(time.Microsecond).Add(123 * time.Nanosecond)
					req := availabilityRequest("consumer", &original)
					req.Job.Dependencies = deps
					// Both consumers share a marker for bounded polling in this fixture.
					req.Job.Metadata = []byte(`{"consumer":"consumer"}`)
					h, err := submit(f.ctx, req)
					require.NoError(t, err)
					pollRequest := req
					pollRequest.Job.JobID = "consumer-poll"
					pollSubmit := submit
					if strings.HasPrefix(route, "selected-") {
						// The independently live polling consumer needs its own idle goal.
						origin, err := f.backend.SubmitJob(f.ctx, availabilityRequest("poll-origin", nil))
						require.NoError(t, err)
						f.complete(origin.JobKey, false)
						pollRequest.Job.GoalID = origin.GoalID
						pollSubmit = f.backend.SubmitJob
						if route == "selected-http" {
							pollSubmit = f.client.SubmitJob
						}
					}
					pollHandle, err := pollSubmit(f.ctx, pollRequest)
					require.NoError(t, err)
					effective := effectiveAvailability(original)
					f.assertReads(h.JobKey, effective)
					descriptor := ""
					if route != "native" {
						descriptor = f.descriptor(h.JobKey)
						require.Contains(t, descriptor, original.Format(time.RFC3339Nano))
					}
					f.deny(h.JobKey)
					if timeFirst {
						f.waitUntil(effective)
						f.deny(h.JobKey)
					} else {
						for i, p := range producers {
							f.complete(p.JobKey, true)
							if i < len(producers)-1 {
								f.deny(h.JobKey)
							}
						}
						require.True(t, f.databaseTime().Before(effective), "missed future observation")
						f.deny(h.JobKey)
					}
					f.reopen()
					f.assertReads(h.JobKey, effective)
					if !timeFirst {
						require.True(t, f.databaseTime().Before(effective), "missed persisted future observation")
					}
					f.deny(h.JobKey)
					if descriptor != "" {
						require.Equal(t, descriptor, f.descriptor(h.JobKey))
					}
					if timeFirst {
						for i, p := range producers {
							f.complete(p.JobKey, true)
							if i < len(producers)-1 {
								f.deny(h.JobKey)
							}
						}
					} else {
						f.waitUntil(effective)
					}
					// Winning recovery bindings survive reopen and independent completion.
					if kind == "goal-success" || kind == "mixed" {
						p := producers[len(producers)-1]
						goal, err := f.backend.GetGoal(f.ctx, jobdb.GoalKey{TenantId: "tenant", GoalID: p.GoalID})
						require.NoError(t, err)
						require.NotEmpty(t, goal.SuccessfulJobID)
						require.Equal(t, p.JobKey.JobId, goal.SuccessfulJobID)
						before := f.goalBinding(h.JobKey, p.JobKey.JobId)
						f.complete(later.JobKey, true)
						f.reopen()
						require.Equal(t, before, f.goalBinding(h.JobKey, p.JobKey.JobId))
						goal, err = f.backend.GetGoal(f.ctx, jobdb.GoalKey{TenantId: "tenant", GoalID: p.GoalID})
						require.NoError(t, err)
						require.Equal(t, p.JobKey.JobId, goal.SuccessfulJobID)
					}
					var consumer jobdb.ExecutionLease
					require.Eventually(t, func() bool {
						var err error
						consumer, err = f.client.GetJobLease(f.ctx, jobdb.GetJobLeaseRequest{JobKey: h.JobKey, WorkerID: "consumer", Routes: []jobdb.Route{{JobType: "child"}}})
						require.NoError(t, err)
						return consumer != nil
					}, 5*time.Second, 10*time.Millisecond)
					require.Equal(t, h, consumer.Job())
					polls, err := f.client.PollWork(f.ctx, jobdb.PollWorkRequest{TenantId: "tenant", WorkerID: "poll-consumer", Routes: []jobdb.Route{{JobType: "child"}}, Limit: 1, MetadataEquals: []jobdb.MetadataPredicate{{Path: []string{"consumer"}, Values: []any{"consumer"}}}})
					require.NoError(t, err)
					require.Len(t, polls, 1)
					require.Equal(t, pollHandle, polls[0].Job())
					require.False(t, f.databaseTime().Before(effective))
					duplicate, err := f.backend.GetJobLease(f.ctx, jobdb.GetJobLeaseRequest{JobKey: h.JobKey, WorkerID: "duplicate", Routes: []jobdb.Route{{JobType: "child"}}})
					require.NoError(t, err)
					require.Nil(t, duplicate)
					var saved time.Time
					require.NoError(t, f.db.QueryRowContext(f.ctx, `SELECT available_at FROM pgjobdb.jobs WHERE job_id=$1`, h.JobKey.JobId).Scan(&saved))
					require.True(t, effective.Equal(saved))
				})
			}
		}
	}
}

func TestSubmissionAvailabilityFailedDependencies(t *testing.T) {
	newDSN := conformanceDSNFactory(t)
	fixtures := make(chan struct{}, 3)
	for _, route := range availabilityRoutes {
		for _, kind := range []string{"completion", "exact-success", "goal-success"} {
			for _, timeFirst := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/%s/time-first=%v", route, kind, timeFirst), func(t *testing.T) {
					t.Parallel()
					fixtures <- struct{}{}
					defer func() { <-fixtures }()
					f := newAvailabilityFixture(t, newDSN(t))
					f.nativeHistory = route == "native"
					submit := f.submitter(route)
					p, err := f.backend.SubmitJob(f.ctx, availabilityRequest("producer", nil))
					require.NoError(t, err)
					d := jobdb.Dependency{Target: jobdb.DependencyTarget{JobID: p.JobKey.JobId}, Condition: jobdb.JobPrereqSuccess}
					if kind == "completion" {
						d.Condition = jobdb.JobPrereqComplete
					}
					if kind == "goal-success" {
						d.Target = jobdb.DependencyTarget{GoalID: p.GoalID}
					}
					at := f.databaseTime().Add(10 * time.Second).Truncate(time.Microsecond).Add(123 * time.Nanosecond)
					req := availabilityRequest("consumer", &at)
					req.Job.Dependencies = []jobdb.Dependency{d}
					req.Job.Metadata = []byte(`{"consumer":"consumer"}`)
					h, err := submit(f.ctx, req)
					require.NoError(t, err)
					if timeFirst {
						f.waitUntil(effectiveAvailability(at))
					}
					f.complete(p.JobKey, false)
					if kind == "goal-success" {
						// Failure of a candidate alone cannot close its open goal.
						f.deny(h.JobKey)
						_, err = f.backend.FailGoal(f.ctx, jobdb.FailGoalRequest{GoalKey: jobdb.GoalKey{TenantId: "tenant", GoalID: p.GoalID}, Message: "explicitly failed"})
						require.NoError(t, err)
					}
					f.reopen()
					if !timeFirst {
						require.True(t, f.databaseTime().Before(effectiveAvailability(at)), "missed failed-dependency future observation")
					}
					if kind == "completion" {
						if !timeFirst {
							f.deny(h.JobKey)
							f.waitUntil(effectiveAvailability(at))
						}
						l, err := f.backend.GetJobLease(f.ctx, jobdb.GetJobLeaseRequest{JobKey: h.JobKey, WorkerID: "consumer", Routes: []jobdb.Route{{JobType: "child"}}})
						require.NoError(t, err)
						require.NotNil(t, l)
					} else {
						// A failed wait admits an administrative lease so the workflow can
						// record failure. It must never call the application.
						if !timeFirst {
							f.deny(h.JobKey)
						}
						f.waitUntil(effectiveAvailability(at))
						calls := 0
						worker := wf.ExecutionWorker{JobType: "child", Execute: func(wf.ExecutionContext, jobdb.JobData) (jobdb.JobData, error) {
							calls++
							return jobdb.NewTaskDataOrPanic(nil), nil
						}}
						var rt jobdb.ConsumerRuntime = f.backend
						if route == "http-put" || route == "http-parent" || route == "imported-submit-only" {
							rt = f.client
						}
						outcome, err := runGoalWorkflow(t, rt, h.JobKey, worker)
						require.NoError(t, err)
						require.Equal(t, wf.JobRunFailed, outcome.Status)
						require.Zero(t, calls)
						row, err := pgjobdb.GetJob(f.ctx, f.db, "tenant", pgjobdb.JobID(h.JobKey.JobId))
						require.NoError(t, err)
						require.Equal(t, pgjobdb.JobStoreArchived, row.Store)
						require.NotNil(t, row.CompletionStatus)
						require.NotEqual(t, pgjobdb.CompletionSuccess, *row.CompletionStatus)
						f.deny(h.JobKey)
					}
				})
			}
		}
	}
}

func (f *availabilityFixture) goalBinding(key jobdb.JobKey, winner string) jobdb.DependencyWaitInfo {
	var waitID string
	require.NoError(f.t, f.db.QueryRowContext(f.ctx, `SELECT wait_id FROM pgjobdb.dependency_waits WHERE job_id=$1`, key.JobId).Scan(&waitID))
	wait, err := f.backend.GetDependencyWait(f.ctx, jobdb.DependencyWaitKey{JobKey: key, WaitID: waitID})
	require.NoError(f.t, err)
	found := false
	for _, d := range wait.Dependencies {
		if d.Target.GoalID != "" {
			require.Equal(f.t, winner, d.SatisfiedByJobID)
			found = true
		}
	}
	require.True(f.t, found)
	return wait
}

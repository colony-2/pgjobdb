package usageparity_test

import (
	"context"
	"testing"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/workflow"
	jobdbtest "github.com/colony-2/pgjobdb/pkg/pgjobdb/runtime/integration/internal/jobdbtest"
)

func TestDirectGetJobRunRetryRepresentationParity(t *testing.T) {
	for _, harness := range jobdbtest.BuiltInRuntimeHarnesses() {
		harness := harness
		if harness.Name != "direct" {
			continue
		}

		t.Run(harness.Name+"/job-retry", func(t *testing.T) {
			run := func(mode parityMode) normalizedJobRun {
				job := &retryJob{}
				ws := jobdbtest.MustWorkSet(t, job)
				return observeViaMode(t, harness, mode, []workflow.WorkSet{ws}, func(t *testing.T, ctx context.Context, subject scenarioSubject) normalizedJobRun {
					jobKey, err := subject.SubmitJob(ctx, jobdb.SubmitJob{
						TenantId: subject.built.WorkerTenantID,
						JobType:  job.Name(),
						JobID:    "job-retry-shape",
						Data:     jobdbtest.NumberTaskData(1),
						RunPolicy: jobdb.RunPolicy{
							Retry: jobdb.RetryPolicy{MaximumAttempts: 3, BackoffCoefficient: 1},
						},
					})
					if err != nil {
						t.Fatalf("start job retry via %s: %v", subject.mode, err)
					}
					subject.WaitForStatus(t, ctx, jobKey, jobdb.JobStatusCompleted)

					resp, err := subject.GetJobRun(ctx, jobdb.GetJobRunRequest{
						JobKey:         jobKey,
						IncludeOutputs: true,
					})
					if err != nil {
						t.Fatalf("get job retry run via %s: %v", subject.mode, err)
					}
					_, outputErr := resp.GetOutput(subject.Engine(), jobKey.TenantId)
					return normalizeJobRun(t, resp, outputErr)
				})
			}

			engineObs := run(engineMode)
			runtimeObs := run(runtimeMode)
			compareObservations(t, engineObs, runtimeObs)
		})

		t.Run(harness.Name+"/task-retry", func(t *testing.T) {
			run := func(mode parityMode) normalizedJobRun {
				job := &retryTaskJob{}
				task := &retryTask{}
				ws := jobdbtest.MustWorkSet(t, job, task)
				return observeViaMode(t, harness, mode, []workflow.WorkSet{ws}, func(t *testing.T, ctx context.Context, subject scenarioSubject) normalizedJobRun {
					jobKey, err := subject.SubmitJob(ctx, jobdb.SubmitJob{
						TenantId: subject.built.WorkerTenantID,
						JobType:  job.Name(),
						JobID:    "task-retry-shape",
						Data:     jobdbtest.NumberTaskData(1),
					})
					if err != nil {
						t.Fatalf("start task retry via %s: %v", subject.mode, err)
					}
					subject.WaitForStatus(t, ctx, jobKey, jobdb.JobStatusCompleted)

					resp, err := subject.GetJobRun(ctx, jobdb.GetJobRunRequest{
						JobKey:               jobKey,
						IncludeInputs:        true,
						IncludeOutputs:       true,
						IncludeAttemptInputs: true,
					})
					if err != nil {
						t.Fatalf("get task retry run via %s: %v", subject.mode, err)
					}
					_, outputErr := resp.GetOutput(subject.Engine(), jobKey.TenantId)
					return normalizeJobRun(t, resp, outputErr)
				})
			}

			engineObs := run(engineMode)
			runtimeObs := run(runtimeMode)
			compareObservations(t, engineObs, runtimeObs)
		})
	}
}

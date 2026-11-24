package integration_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	wf "github.com/colony-2/jobdb/pkg/workflow"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestResourceBackoffWorkflowPostgres(t *testing.T) {
	newDSN := conformanceDSNFactory(t)
	for _, test := range []struct {
		name                                    string
		external, reject, loseAck, cancelReplay bool
	}{
		{name: "acknowledged_local"},
		{name: "acknowledged_external", external: true},
		{name: "failed_before_commit_unselected", reject: true},
		{name: "lost_after_commit", loseAck: true},
		{name: "cancel_resumed_wait", cancelReplay: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			var db *sql.DB
			fixture := resourceFixture(t, newDSN(t), func(open *sql.DB) { db = open })
			ctx := celltest.Context(context.Background())
			at := time.Now().UTC().Truncate(time.Millisecond)
			fixture.SetTime(at)
			var runs, builds, priorRuns, primaryRuns, catches, taskEvents, failedEvents, outputReads atomic.Int32
			var cancelResumed func() error
			cancelResult := make(chan error, 1)
			call := wf.TaskCall{Site: "resource", TaskType: "primary", ContractVersion: 1, Command: json.RawMessage(`{}`)}
			call.Policy.Retry.MaximumAttempts = 3
			call.Catch = []wf.CatchRule{{ID: "ordinary-failure", Match: func(error) bool { catches.Add(1); return true }, Handle: func(wf.ExecutionContext, error) (wf.CatchResult, error) {
				return wf.CatchResult{Disposition: wf.CatchHandled}, nil
			}}}
			implementation := "primary"
			if test.external {
				implementation = "alternate"
				call.Alternate = &wf.TaskAlternateAfter{TaskType: implementation}
			}
			prior := postgresResourceTask{"prior", func(_ wf.TaskContext, in jobdb.TaskData) (jobdb.TaskData, error) {
				priorRuns.Add(1)
				return in, nil
			}}
			waiting := postgresResourceTask{implementation, func(taskCtx wf.TaskContext, in jobdb.TaskData) (jobdb.TaskData, error) {
				attemptRun := runs.Add(1)
				assert.Equal(t, 1, taskCtx.AttemptNumber)
				input, err := in.GetData()
				if err != nil {
					return nil, err
				}
				expected := 1
				if test.reject && attemptRun > 1 {
					expected = 2
				}
				assert.JSONEq(t, fmt.Sprintf(`"input-%d"`, expected), string(input))
				artifacts, err := in.GetArtifacts()
				if err != nil {
					return nil, err
				}
				if assert.Len(t, artifacts, 1) {
					data, err := artifacts[0].Bytes(ctx)
					assert.NoError(t, err)
					assert.Equal(t, fmt.Sprintf("artifact-%d", expected), string(data))
				}
				if attemptRun == 1 || test.cancelReplay {
					signal := wf.EAGAINWait(taskCtx, wf.WithInitialWait(30*time.Second), wf.WithBackoffJitter(0))
					if attemptRun == 2 {
						cancelResult <- cancelResumed()
					}
					return postgresResourceUnusedOutput{reads: &outputReads}, signal
				}
				return in, nil
			}}
			primary := postgresResourceTask{"primary", func(_ wf.TaskContext, in jobdb.TaskData) (jobdb.TaskData, error) {
				primaryRuns.Add(1)
				return in, nil
			}}
			worker := wf.ExecutionWorker{JobType: "resource-workflow", ObserveTaskResults: func(ctx context.Context) (context.Context, func(wf.TaskEndEvent)) {
				return ctx, func(event wf.TaskEndEvent) {
					taskEvents.Add(1)
					if event.Err != nil {
						failedEvents.Add(1)
					}
				}
			}, Execute: func(exec wf.ExecutionContext, in jobdb.JobData) (jobdb.JobData, error) {
				completed, err := exec.DoTask(wf.TaskCall{Site: "prior", TaskType: prior.Name(), ContractVersion: 1, Command: json.RawMessage(`{}`)}, func(context.Context) (jobdb.TaskData, error) { return in, nil })
				if err != nil {
					return nil, err
				}
				value, err := completed.GetData()
				if err != nil {
					return nil, err
				}
				assert.JSONEq(t, `"original job input"`, string(value))
				return exec.DoTask(call, func(context.Context) (jobdb.TaskData, error) {
					build := builds.Add(1)
					return jobdb.NewTaskData(fmt.Sprintf("input-%d", build), jobdb.NewArtifactFromBytes("input.txt", []byte(fmt.Sprintf("artifact-%d", build))))
				})
			}}
			handle, err := celltest.Submit(fixture.Runtime, ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "c2", Cell: "test", CellRepo: "example.test/test", JobID: "workflow-backoff", JobType: worker.Name(), Data: jobdb.NewTaskDataOrPanic("original job input")}})
			require.NoError(t, err)
			request := func(tasks ...wf.TaskWorker) wf.GetJobForRunRequest {
				return wf.GetJobForRunRequest{JobKey: handle.JobKey, JobWorker: worker, TaskWorkers: tasks, WorkerID: "engine", LeaseDuration: time.Minute}
			}
			run := func(tasks ...wf.TaskWorker) wf.JobRunOutcome {
				runnable, err := wf.GetJobForRun(ctx, fixture.Runtime, request(tasks...))
				require.NoError(t, err)
				out, err := runnable.Run(nil)
				require.NoError(t, err)
				return out
			}
			var ack postgresResourceAckLease
			if test.external {
				out := run(prior)
				require.Equal(t, wf.JobRunSuspended, out.Status)
				require.Zero(t, runs.Load())
			}
			if test.reject || test.loseAck {
				lease, err := fixture.Runtime.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: handle.JobKey, WorkerID: "engine", Routes: []jobdb.Route{{JobType: worker.Name()}}, LeaseDuration: time.Second})
				require.NoError(t, err)
				require.NotNil(t, lease)
				ack = postgresResourceAckLease{
					RenewableExecutionLease: lease.(jobdb.RenewableExecutionLease),
					request:                 new(jobdb.ResourceBackoffRequest), receipt: new(jobdb.ResourceBackoffReceipt), drop: test.loseAck,
				}
				if test.reject {
					fixture.Fault("receipt")
				}
				runnable, err := wf.GetJobForRunWithLease(ctx, fixture.Runtime, ack, request(prior, waiting))
				require.NoError(t, err)
				out, err := runnable.Run(nil)
				fixture.Fault("")
				require.True(t, jobdb.IsExecutionInfrastructureError(err), "%v", err)
				if test.reject {
					require.ErrorContains(t, err, "injected resource fault at receipt")
				} else {
					require.ErrorContains(t, err, "resource acknowledgment lost")
				}
				require.NotEqual(t, wf.JobRunSuspended, out.Status)
			} else {
				out := run(prior, waiting)
				require.Equal(t, wf.JobRunSuspended, out.Status, "%v", out.JobError)
			}
			require.EqualValues(t, 1, runs.Load())
			require.Zero(t, outputReads.Load())
			require.Positive(t, taskEvents.Load(), "the completed sibling reports a result")
			require.Zero(t, failedEvents.Load())
			require.Zero(t, catches.Load())
			chapters, err := fixture.Runtime.ListChapters(ctx, jobdb.ListChaptersRequest{JobKey: handle.JobKey})
			require.NoError(t, err)
			var checkpoint *jobdb.ResourceResume
			var resultIDs []string
			for _, chapter := range chapters {
				body, ok := chapter.Body.(jobdb.ExecutionChapter)
				if !ok {
					continue
				}
				record, err := jobdb.DecodeExecutionRecord(body.Record)
				require.NoError(t, err)
				require.Empty(t, record.ErrorKind)
				require.False(t, record.Retry)
				require.NotContains(t, []string{"catch", "handled", "end"}, record.Kind)
				if record.Kind == "result" {
					resultIDs = append(resultIDs, record.ID)
				}
				if record.Kind == "resource_checkpoint" {
					require.Nil(t, checkpoint)
					require.Equal(t, implementation, record.Resource.ExecutionTaskType)
					require.Equal(t, 1, record.Resource.Attempt)
					checkpoint = &jobdb.ResourceResume{CheckpointOrdinal: chapter.Ordinal, CheckpointHash: jobdb.ResourceCheckpointHash(body.Record), Invocation: record.ID, Attempt: record.Resource.Attempt}
				}
			}
			require.NotNil(t, checkpoint)
			require.Len(t, resultIDs, 1)
			require.NotContains(t, resultIDs, checkpoint.Invocation)
			info, err := fixture.Runtime.GetJob(ctx, handle.JobKey)
			require.NoError(t, err)
			if test.reject {
				require.Nil(t, info.ExecutionState.ResourceResume)
				fixture.SetTime(at.Add(2 * time.Second))
			} else {
				selected := info.ExecutionState.ResourceResume
				require.NotNil(t, selected)
				require.Equal(t, checkpoint.CheckpointOrdinal, selected.CheckpointOrdinal)
				require.Equal(t, checkpoint.CheckpointHash, selected.CheckpointHash)
				require.Equal(t, checkpoint.Invocation, selected.Invocation)
				require.Equal(t, checkpoint.Attempt, selected.Attempt)
				require.Equal(t, test.external, selected.TaskWait != nil)
				var wake int64
				require.NoError(t, db.QueryRowContext(ctx, `SELECT resource_not_before_ns FROM pgjobdb.jobs WHERE tenant_id=$1 AND job_id=$2`, handle.JobKey.TenantId, handle.JobKey.JobId).Scan(&wake))
				fixture.SetTime(time.Unix(0, wake))
			}
			fixture.Runtime = fixture.Reopen()
			if test.reject || test.loseAck {
				require.Equal(t, checkpoint.CheckpointHash, ack.request.Resume.CheckpointHash)
				replay, err := fixture.Runtime.(jobdb.ResourceBackoffReceiptReader).ReplayResourceBackoffReceiptByID(ctx, handle.JobKey, ack.LeaseID(), "engine", *ack.request)
				if test.reject {
					require.Error(t, err)
					require.Zero(t, replay)
				} else {
					require.NoError(t, err)
					require.Equal(t, *ack.receipt, replay)
				}
			}
			tasks := []wf.TaskWorker{prior, waiting}
			if test.external {
				tasks = append(tasks, primary)
			}
			if test.cancelReplay {
				resourceState := func() (int64, int64, []byte) {
					var receipts, waits int64
					var receipt []byte
					require.NoError(t, db.QueryRowContext(ctx, `SELECT count(*), min(encode(receipt,'hex')) FROM pgjobdb.resource_receipts WHERE tenant_id=$1 AND job_id=$2`, handle.JobKey.TenantId, handle.JobKey.JobId).Scan(&receipts, &receipt))
					require.NoError(t, db.QueryRowContext(ctx, `SELECT COALESCE(sum(count),0) FROM pgjobdb.resource_sequences WHERE tenant_id=$1 AND job_id=$2`, handle.JobKey.TenantId, handle.JobKey.JobId).Scan(&waits))
					return receipts, waits, receipt
				}
				receipts, waits, receipt := resourceState()
				require.EqualValues(t, 1, receipts)
				require.EqualValues(t, 1, waits)
				cancelResumed = func() error {
					return fixture.Runtime.CancelJob(ctx, jobdb.CancelJobRequest{JobKey: handle.JobKey, WorkerID: "operator", Reason: "cancel resumed execution"})
				}
				runnable, err := wf.GetJobForRun(ctx, fixture.Runtime, request(tasks...))
				require.NoError(t, err)
				out, runErr := runnable.Run(nil)
				t.Logf("resumed execution after operator cancellation: status=%v, error=%v", out.Status, runErr)
				require.NotEqual(t, wf.JobRunSuspended, out.Status, "%v", runErr)
				require.Len(t, cancelResult, 1, "resumed task reached the second wait")
				require.NoError(t, <-cancelResult)
				info, err := fixture.Runtime.GetJob(ctx, handle.JobKey)
				require.NoError(t, err)
				require.Equal(t, jobdb.JobStatusCancelled, info.Status)
				require.Equal(t, "cancel resumed execution", info.CompletionDetail)
				receiptsAfter, waitsAfter, receiptAfter := resourceState()
				require.Equal(t, receipts, receiptsAfter)
				require.Equal(t, waits, waitsAfter)
				require.Equal(t, receipt, receiptAfter)
				require.EqualValues(t, 2, runs.Load())
				require.EqualValues(t, 1, priorRuns.Load())
				require.EqualValues(t, 1, builds.Load())
				require.Zero(t, catches.Load())
				require.Zero(t, outputReads.Load())
				return
			}
			out := run(tasks...)
			require.Equal(t, wf.JobRunCompleted, out.Status, "%v", out.JobError)
			require.EqualValues(t, 2, runs.Load())
			require.EqualValues(t, 1, priorRuns.Load())
			require.Zero(t, primaryRuns.Load())
			require.Zero(t, failedEvents.Load())
			require.Zero(t, catches.Load())
			expectedBuilds := 1
			if test.reject {
				expectedBuilds = 2
			}
			require.EqualValues(t, expectedBuilds, builds.Load())
		})
	}
}

type postgresResourceTask struct {
	name string
	run  func(wf.TaskContext, jobdb.TaskData) (jobdb.TaskData, error)
}

func (task postgresResourceTask) Name() string { return task.name }
func (task postgresResourceTask) Run(ctx wf.TaskContext, in jobdb.TaskData) (jobdb.TaskData, error) {
	return task.run(ctx, in)
}

type postgresResourceUnusedOutput struct {
	jobdb.TaskData
	reads *atomic.Int32
}

func (data postgresResourceUnusedOutput) GetData() (json.RawMessage, error) {
	data.reads.Add(1)
	return nil, errors.New("EAGAIN output must not be read")
}

func (data postgresResourceUnusedOutput) GetArtifacts() ([]jobdb.Artifact, error) {
	data.reads.Add(1)
	return nil, errors.New("EAGAIN output artifacts must not be read")
}

type postgresResourceAckLease struct {
	jobdb.RenewableExecutionLease
	request *jobdb.ResourceBackoffRequest
	receipt *jobdb.ResourceBackoffReceipt
	drop    bool
}

func (lease postgresResourceAckLease) Renew(ctx context.Context) (jobdb.RenewableExecutionLease, error) {
	next, err := lease.RenewableExecutionLease.Renew(ctx)
	if err != nil {
		return nil, err
	}
	lease.RenewableExecutionLease = next
	return lease, nil
}

func (lease postgresResourceAckLease) RescheduleResourceBackoff(ctx context.Context, request jobdb.ResourceBackoffRequest) (jobdb.ResourceBackoffReceipt, error) {
	*lease.request = request
	receipt, err := lease.RenewableExecutionLease.(jobdb.ResourceBackoffLease).RescheduleResourceBackoff(ctx, request)
	if err == nil && lease.drop {
		*lease.receipt = receipt
		return jobdb.ResourceBackoffReceipt{}, errors.New("resource acknowledgment lost")
	}
	return receipt, err
}

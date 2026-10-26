package workflow_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/workflow"
)

const (
	prereqSuccessJobName   = "prereq_success_job"
	prereqFailJobName      = "prereq_fail_job"
	prereqDependentJobName = "prereq_dependent_job"
	prereqRestartJobName   = "prereq_restart_job"
	prereqRestartTaskName  = "prereq_restart_task"
)

type prereqSuccessWorker struct{}

func (prereqSuccessWorker) Name() string { return prereqSuccessJobName }
func (prereqSuccessWorker) Run(ctx workflow.ExecutionContext, input jobdb.JobData) (jobdb.JobData, error) {
	return input, nil
}

type prereqFailWorker struct{}

func (prereqFailWorker) Name() string { return prereqFailJobName }
func (prereqFailWorker) Run(ctx workflow.ExecutionContext, input jobdb.JobData) (jobdb.JobData, error) {
	return nil, jobdb.AppError{Payload: jobdb.AppErrorPayload{Message: "prereq failed"}}
}

type prereqDependentWorker struct{}

func (prereqDependentWorker) Name() string { return prereqDependentJobName }
func (prereqDependentWorker) Run(ctx workflow.ExecutionContext, input jobdb.JobData) (jobdb.JobData, error) {
	return input, nil
}

type prereqRestartJobWorker struct{}

func (prereqRestartJobWorker) Name() string { return prereqRestartJobName }
func (prereqRestartJobWorker) Run(ctx workflow.ExecutionContext, input jobdb.JobData) (jobdb.JobData, error) {
	return ctx.DoTask(workflow.TaskCall{Command: []byte("{}"), Site: "task", TaskType: prereqRestartTaskName, ContractVersion: 1, Policy: jobdb.RunPolicy{}}, func(context.Context) (jobdb.TaskData, error) { return input, nil })
}

type prereqRestartTaskWorker struct{}

func (prereqRestartTaskWorker) Name() string { return prereqRestartTaskName }
func (prereqRestartTaskWorker) Run(ctx workflow.TaskContext, input jobdb.TaskData) (jobdb.TaskData, error) {
	return input, nil
}

func TestPrerequisitesSuccessAndComplete(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	postgresDSN, stopPG := startEmbeddedPostgres(t)
	defer stopPG()
	if err := installPgjobdb(ctx, postgresDSN); err != nil {
		t.Fatalf("failed to install pgjobdb: %v", err)
	}

	blobStoreURI, blobs := startChapterBlobStore(t)
	defer blobs.Shutdown()

	tenantID := "prereq-tenant"
	engine := buildDirectEngine(t, postgresDSN, blobStoreURI, func(b *workflow.EngineBuilder) {
		b.WithWorkerTenantId(tenantID).
			PlusWorkers(prereqSuccessWorker{}).
			PlusWorkers(prereqFailWorker{}).
			PlusWorkers(prereqDependentWorker{}).
			PlusWorkers(prereqRestartJobWorker{}, prereqRestartTaskWorker{})
	})

	go engine.Run(ctx)

	successJobID := "prereq-success"
	successKeyHandle, err := engine.SubmitJob(ctx, jobdb.SubmitJob{
		TenantId: tenantID,
		JobType:  prereqSuccessJobName,
		JobID:    successJobID,
		Data:     jobdb.NewTaskDataOrPanic(map[string]interface{}{"n": 1}),
	})
	successKey := successKeyHandle.JobKey
	if err != nil {
		t.Fatalf("start success prereq: %v", err)
	}

	failJobID := "prereq-fail"
	failKeyHandle, err := engine.SubmitJob(ctx, jobdb.SubmitJob{
		TenantId: tenantID,
		JobType:  prereqFailJobName,
		JobID:    failJobID,
		Data:     jobdb.NewTaskDataOrPanic(map[string]interface{}{"n": 2}),
	})
	failKey := failKeyHandle.JobKey
	if err != nil {
		t.Fatalf("start failed prereq: %v", err)
	}

	successDependentHandle, err := engine.SubmitJob(ctx, jobdb.SubmitJob{
		TenantId: tenantID,
		JobType:  prereqDependentJobName,
		JobID:    "dependent-success",
		Data:     jobdb.NewTaskDataOrPanic(map[string]interface{}{"n": 3}),
		Prerequisites: []jobdb.JobPrerequisite{
			{JobID: successJobID, Condition: jobdb.JobPrereqSuccess},
		},
	})
	successDependent := successDependentHandle.JobKey
	if err != nil {
		t.Fatalf("start dependent success: %v", err)
	}

	failedDependentHandle, err := engine.SubmitJob(ctx, jobdb.SubmitJob{
		TenantId: tenantID,
		JobType:  prereqDependentJobName,
		JobID:    "dependent-failed",
		Data:     jobdb.NewTaskDataOrPanic(map[string]interface{}{"n": 4}),
		Prerequisites: []jobdb.JobPrerequisite{
			{JobID: failJobID, Condition: jobdb.JobPrereqSuccess},
		},
	})
	failedDependent := failedDependentHandle.JobKey
	if err != nil {
		t.Fatalf("start dependent failed: %v", err)
	}

	completeDependentHandle, err := engine.SubmitJob(ctx, jobdb.SubmitJob{
		TenantId: tenantID,
		JobType:  prereqDependentJobName,
		JobID:    "dependent-complete",
		Data:     jobdb.NewTaskDataOrPanic(map[string]interface{}{"n": 5}),
		Prerequisites: []jobdb.JobPrerequisite{
			{JobID: failJobID, Condition: jobdb.JobPrereqComplete},
		},
	})
	completeDependent := completeDependentHandle.JobKey
	if err != nil {
		t.Fatalf("start dependent complete: %v", err)
	}

	for _, key := range []jobdb.JobKey{successKey, failKey, successDependent, failedDependent, completeDependent} {
		if err := workflow.WaitForJobToComplete(ctx, 30*time.Second, key, engine); err != nil {
			t.Fatalf("wait for job %s: %v", key.String(), err)
		}
	}

	if _, err := jobResultForTest(engine, ctx, successDependent); err != nil {
		t.Fatalf("expected success dependent to succeed, got %v", err)
	}

	if _, err := jobResultForTest(engine, ctx, completeDependent); err != nil {
		t.Fatalf("expected complete dependent to succeed, got %v", err)
	}

	if _, err := jobResultForTest(engine, ctx, failedDependent); err == nil {
		t.Fatalf("expected failed dependent to error")
	} else if !strings.Contains(err.Error(), "prerequisite job") {
		t.Fatalf("expected prereq error, got %v", err)
	}
}

func TestRestartPrerequisitesCheckedAtExecutionBoundary(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	postgresDSN, stopPG := startEmbeddedPostgres(t)
	defer stopPG()
	if err := installPgjobdb(ctx, postgresDSN); err != nil {
		t.Fatalf("failed to install pgjobdb: %v", err)
	}

	blobStoreURI, blobs := startChapterBlobStore(t)
	defer blobs.Shutdown()

	tenantID := "restart-prereq-tenant"
	engine := buildDirectEngine(t, postgresDSN, blobStoreURI, func(b *workflow.EngineBuilder) {
		b.WithWorkerTenantId(tenantID).
			PlusWorkers(prereqSuccessWorker{}).
			PlusWorkers(prereqFailWorker{}).
			PlusWorkers(prereqDependentWorker{}).
			PlusWorkers(prereqRestartJobWorker{}, prereqRestartTaskWorker{})
	})

	go engine.Run(ctx)

	baseInput := map[string]interface{}{"n": 1}
	baseKeyHandle, err := engine.SubmitJob(ctx, jobdb.SubmitJob{
		TenantId: tenantID,
		JobType:  prereqRestartJobName,
		JobID:    "restart-base",
		Data:     jobdb.NewTaskDataOrPanic(baseInput),
	})
	baseKey := baseKeyHandle.JobKey
	if err != nil {
		t.Fatalf("start base job: %v", err)
	}
	if err := workflow.WaitForJobToComplete(ctx, 30*time.Second, baseKey, engine); err != nil {
		t.Fatalf("wait base job: %v", err)
	}

	failKeyHandle, err := engine.SubmitJob(ctx, jobdb.SubmitJob{
		TenantId: tenantID,
		JobType:  prereqFailJobName,
		JobID:    "restart-prereq-fail",
		Data:     jobdb.NewTaskDataOrPanic(map[string]interface{}{"n": 2}),
	})
	failKey := failKeyHandle.JobKey
	if err != nil {
		t.Fatalf("start fail prereq: %v", err)
	}
	if err := workflow.WaitForJobToComplete(ctx, 30*time.Second, failKey, engine); err != nil {
		t.Fatalf("wait fail prereq: %v", err)
	}

	history, err := engine.GetJobRun(ctx, jobdb.GetJobRunRequest{JobKey: baseKey})
	if err != nil {
		t.Fatal(err)
	}
	target := ""
	for _, entry := range history.Execution {
		if entry.Record.Kind == "task" {
			target = entry.Record.ID
			break
		}
	}
	if target == "" {
		t.Fatal("missing task restart boundary")
	}
	restartKeyHandle, err := engine.SubmitRestartJob(ctx, jobdb.SubmitRestartJobRequest{Job: jobdb.SubmitRestartJob{
		PriorJobKey:      baseKey,
		ExecutionRestart: &jobdb.ExecutionRestart{ScopeID: target},
		JobID:            "restart-with-prereqs",
		Prerequisites: []jobdb.JobPrerequisite{
			{JobID: failKey.JobId, Condition: jobdb.JobPrereqSuccess},
		},
	}})
	restartKey := restartKeyHandle.JobKey
	if err != nil {
		t.Fatalf("restart job: %v", err)
	}
	if err := workflow.WaitForJobToComplete(ctx, 30*time.Second, restartKey, engine); err != nil {
		t.Fatalf("wait restart job: %v", err)
	}
	if _, err := jobResultForTest(engine, ctx, restartKey); err == nil {
		t.Fatalf("expected restart to fail due to prereqs")
	} else if !strings.Contains(err.Error(), "prerequisite job") {
		t.Fatalf("expected prereq error, got %v", err)
	}
}

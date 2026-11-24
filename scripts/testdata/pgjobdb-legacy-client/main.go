package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/remote"
)

func main() {
	ctx, cancel := context.WithTimeout(celltest.Context(context.Background()), 20*time.Second)
	defer cancel()
	client, err := remote.New(os.Getenv("PGJOBDB_TEST_SERVER"), nil)
	check(err)
	route := jobdb.Route{JobType: "legacy-work"}
	handle, err := celltest.Submit(client, ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{
		TenantId: "c2", Cell: "test", CellRepo: "example.test/test", JobID: "legacy-job",
		JobType: route.JobType, Data: jobdb.NewTaskDataOrPanic("legacy-input"),
	}})
	check(err)
	fmt.Println("submitted job")
	info, err := client.GetJob(ctx, handle.JobKey)
	check(err)
	equal(jobdb.JobStatusReady, info.Status)
	page, err := client.ListJobs(ctx, jobdb.ListJobsRequest{TenantIds: []string{"c2"}})
	check(err)
	equal(1, len(page.Jobs))
	fmt.Println("read and listed job")
	request := jobdb.GetJobLeaseRequest{JobKey: handle.JobKey, WorkerID: "legacy-worker", Routes: []jobdb.Route{route}, LeaseDuration: time.Minute}
	lease, err := client.GetJobLease(ctx, request)
	check(err)
	expect(lease != nil, "no lease returned")
	equal(handle.JobKey, lease.Job().JobKey)
	owner, err := lease.(jobdb.RenewableExecutionLease).Renew(ctx)
	check(err)
	equal(lease.LeaseID(), owner.LeaseID())
	fmt.Println("claimed and renewed signed owner lease")
	delegated, err := client.CreateDelegation(ctx, owner, jobdb.CreateDelegationRequest{})
	check(err)
	delegated, err = delegated.Renew(ctx)
	check(err)
	equal(owner.LeaseID(), delegated.LeaseID())
	fmt.Println("renewed signed delegated lease with no optional permissions")
	check(owner.Reschedule(ctx, jobdb.RescheduleExecutionRequest{NextRoute: route}))
	lease, err = client.GetJobLease(ctx, request)
	check(err)
	expect(lease != nil, "no lease returned")
	expect(owner.LeaseID() != lease.LeaseID(), "reschedule did not release lease")
	fmt.Println("ordinarily rescheduled and reacquired job")
	chapter := jobdb.Chapter{Ordinal: 1, TaskType: "job", CreatedAt: time.Now().UTC(), Body: jobdb.JobAttemptOutcomeChapter{Outcome: jobdb.ApplicationOutputOutcome{Output: jobdb.ApplicationOutputBytes{Data: []byte(`"done"`)}}}}
	check(lease.Complete(ctx, jobdb.CompleteExecutionRequest{Status: "succeeded", Chapter: &chapter, Detail: "legacy completed"}))
	info, err = client.GetJob(ctx, handle.JobKey)
	check(err)
	equal(jobdb.JobStatusCompleted, info.Status)
	equal("legacy completed", info.CompletionDetail)
	fmt.Println("completed and read final job")
}

func check(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func expect(condition bool, message string) {
	if !condition {
		log.Fatal(message)
	}
}

func equal[Value comparable](expected, actual Value) {
	if expected != actual {
		log.Fatalf("expected %v, got %v", expected, actual)
	}
}

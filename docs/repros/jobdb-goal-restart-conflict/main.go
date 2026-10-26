// Verifies the JobDB HTTP restart conflict fix without PostgreSQL.
package main

import (
	"context"
	"errors"
	"fmt"
	"net/http/httptest"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/remote"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/toy"
)

func main() {
	ctx := context.Background()
	local := toy.New()
	server := httptest.NewServer(remote.NewServer(local))
	defer server.Close()
	client, err := remote.New(server.URL, server.Client())
	check(err)
	h, err := client.SubmitJob(ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", JobID: "source", JobType: "work", Data: jobdb.NewTaskDataOrPanic(nil)}})
	check(err)
	lease, err := client.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: h.JobKey, WorkerID: "worker", Routes: []jobdb.Route{{JobType: "work"}}, LeaseDuration: time.Minute})
	check(err)
	chapter := jobdb.Chapter{Ordinal: 1, TaskType: "work", CreatedAt: time.Now().UTC(), Body: jobdb.JobAttemptOutcomeChapter{Outcome: jobdb.AppErrorOutcome{Error: jobdb.AppErrorPayload{Message: "try another approach"}}}}
	check(lease.Complete(ctx, jobdb.CompleteExecutionRequest{Status: "failed_app", Chapter: &chapter}))
	request := jobdb.SubmitRestartJobRequest{RequestID: "operation", Job: jobdb.SubmitRestartJob{PriorJobKey: h.JobKey, ShareGoal: true}}
	_, err = client.SubmitRestartJob(ctx, request)
	check(err)
	request.Job.ShareGoal = false
	_, directErr := local.SubmitRestartJob(ctx, request)
	_, httpErr := client.SubmitRestartJob(ctx, request)
	fmt.Printf("direct: mismatch=%v conflict=%v error=%v\n", errors.Is(directErr, jobdb.ErrExistingJobMismatch), errors.Is(directErr, jobdb.ErrConflict), directErr)
	fmt.Printf("http: mismatch=%v conflict=%v error=%v\n", errors.Is(httpErr, jobdb.ErrExistingJobMismatch), errors.Is(httpErr, jobdb.ErrConflict), httpErr)
	for _, err := range []error{directErr, httpErr} {
		if !errors.Is(err, jobdb.ErrExistingJobMismatch) || !errors.Is(err, jobdb.ErrConflict) {
			panic("restart conflict lost its typed error")
		}
	}
}
func check(err error) {
	if err != nil {
		panic(err)
	}
}

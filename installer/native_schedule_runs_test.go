package installer_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"testing"
	"time"

	"github.com/colony-2/pgjobdb"
)

func TestNativeScheduleRunsFilterBeforePagination(t *testing.T) {
	runDatabaseTest(t, func(ctx context.Context, db *sql.DB) {
		for _, scheduleID := range []string{"daily", "other"} {
			if _, err := pgjobdb.UpsertSchedule(ctx, db, pgjobdb.UpsertScheduleRequest{
				TenantID: "tenant", ScheduleID: scheduleID,
				State: pgjobdb.ScheduleStateActive, SpecHash: "hash",
				Trigger: json.RawMessage(`{}`), TargetJobType: "collect",
				TargetSnapshot: json.RawMessage(`{}`), OverlapPolicy: "ALLOW",
				FailurePolicy: json.RawMessage(`{}`),
			}); err != nil {
				t.Fatalf("create schedule %s: %v", scheduleID, err)
			}
		}
		first := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
		second := first.Add(time.Hour)
		third := second.Add(time.Hour)
		fourth := third.Add(time.Hour)
		for _, item := range []struct {
			jobID      pgjobdb.JobID
			scheduleID string
			when       time.Time
		}{
			{"run-1", "daily", first},
			{"run-2", "daily", second},
			{"run-3", "daily", third},
			{"other-run", "other", fourth},
		} {
			if _, err := pgjobdb.SubmitJob(ctx, db, pgjobdb.SubmitJobRequest{
				TenantID: "tenant", JobID: item.jobID, WorkerID: "scheduler",
				JobType: "collect", Runtime: pgjobdb.RuntimeMetadata{
					Schedule: &pgjobdb.ScheduleOccurrence{
						ScheduleID: item.scheduleID, Generation: 1, SpecHash: "hash",
						ScheduledAt: item.when, RunID: string(item.jobID),
					},
				},
			}); err != nil {
				t.Fatalf("submit run %s: %v", item.jobID, err)
			}
		}
		if err := pgjobdb.CompleteUnheldJob(ctx, db, "tenant", "run-2", "worker",
			pgjobdb.Completion{Status: pgjobdb.CompletionSuccess}); err != nil {
			t.Fatalf("complete run-2: %v", err)
		}
		opts := pgjobdb.ListScheduleRunsOptions{
			TenantID: "tenant", ScheduleID: "daily", PageSize: 1,
		}
		var listed []pgjobdb.JobID
		for {
			page, err := pgjobdb.ListScheduleRuns(ctx, db, opts)
			if err != nil || len(page.Runs) != 1 {
				t.Fatalf("schedule page = %+v, %v", page, err)
			}
			listed = append(listed, page.Runs[0].JobID)
			if page.NextPageToken == "" {
				break
			}
			opts.PageToken = page.NextPageToken
		}
		if len(listed) != 3 || listed[0] != "run-3" ||
			listed[1] != "run-2" || listed[2] != "run-1" {
			t.Fatalf("daily run pages = %v", listed)
		}
		opts.PageToken = ""
		opts.Statuses = []pgjobdb.JobStatus{pgjobdb.JobStatusCompleted}
		completed, err := pgjobdb.ListScheduleRuns(ctx, db, opts)
		if err != nil || len(completed.Runs) != 1 ||
			completed.Runs[0].JobID != "run-2" ||
			completed.Runs[0].Store != pgjobdb.JobStoreArchived ||
			completed.NextPageToken != "" {
			t.Fatalf("completed runs = %+v, %v", completed, err)
		}
		opts.Statuses = nil
		opts.ScheduledAfter, opts.ScheduledBefore = &second, &third
		opts.PageSize = 10
		window, err := pgjobdb.ListScheduleRuns(ctx, db, opts)
		if err != nil || len(window.Runs) != 2 ||
			window.Runs[0].JobID != "run-3" || window.Runs[1].JobID != "run-2" {
			t.Fatalf("bounded runs = %+v, %v", window, err)
		}
	})
}

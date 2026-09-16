package installer_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/colony-2/pgjobdb/pkg/pgjobdb"
)

func TestScheduleBindings(t *testing.T) {
	runDatabaseTest(t, func(ctx context.Context, db *sql.DB) {
		next := time.Now().UTC().Add(time.Hour)
		nextJob := pgjobdb.JobID("run-1")
		req := pgjobdb.UpsertScheduleRequest{
			TenantID: "tenant", ScheduleID: "daily",
			State: pgjobdb.ScheduleStateActive, SpecHash: "hash",
			Trigger:       json.RawMessage(`{"kind":"interval"}`),
			TargetJobType: "collect", TargetSnapshot: json.RawMessage(`{"input":1}`),
			OverlapPolicy: "ALLOW", FailurePolicy: json.RawMessage(`{}`),
			NextFireAt: &next, NextJobID: &nextJob,
		}
		daily, err := pgjobdb.UpsertSchedule(ctx, db, req)
		if err != nil {
			t.Fatalf("upsert daily: %v", err)
		}
		if daily.Generation != 1 || daily.TargetJobType != "collect" ||
			daily.NextJobID == nil || *daily.NextJobID != nextJob {
			t.Fatalf("daily = %+v", daily)
		}
		got, err := pgjobdb.GetSchedule(ctx, db, "tenant", "daily")
		if err != nil || string(got.TargetSnapshot) != `{"input": 1}` {
			t.Fatalf("get daily = %+v, %v", got, err)
		}
		if _, err := pgjobdb.GetSchedule(ctx, db, "tenant", "missing"); !errors.Is(err, pgjobdb.ErrScheduleNotFound) {
			t.Fatalf("missing schedule error = %v", err)
		}

		req.ScheduleID = "weekly"
		req.NextJobID = nil
		req.NextFireAt = nil
		if _, err := pgjobdb.UpsertSchedule(ctx, db, req); err != nil {
			t.Fatalf("upsert weekly: %v", err)
		}
		page, err := pgjobdb.ListSchedules(ctx, db, pgjobdb.ListSchedulesOptions{
			TenantID: "tenant", States: []pgjobdb.ScheduleState{pgjobdb.ScheduleStateActive},
			PageSize: 1,
		})
		if err != nil || len(page.Schedules) != 1 || page.Schedules[0].ScheduleID != "weekly" ||
			page.NextPageToken == "" {
			t.Fatalf("first page = %+v, %v", page, err)
		}
		second, err := pgjobdb.ListSchedules(ctx, db, pgjobdb.ListSchedulesOptions{
			TenantID: "tenant", States: []pgjobdb.ScheduleState{pgjobdb.ScheduleStateActive},
			PageSize: 1, PageToken: page.NextPageToken,
		})
		if err != nil || len(second.Schedules) != 1 ||
			second.Schedules[0].ScheduleID != "daily" || second.NextPageToken != "" {
			t.Fatalf("second page = %+v, %v", second, err)
		}
		if _, err := pgjobdb.ListSchedules(ctx, db, pgjobdb.ListSchedulesOptions{
			TenantID: "tenant", States: []pgjobdb.ScheduleState{pgjobdb.ScheduleStatePaused},
			PageSize: 1, PageToken: page.NextPageToken,
		}); err == nil {
			t.Fatal("expected cursor filter mismatch")
		}
		filtered, err := pgjobdb.ListSchedules(ctx, db, pgjobdb.ListSchedulesOptions{
			TenantID: "tenant", ScheduleIDs: []string{"daily"}, PageSize: 1,
		})
		if err != nil || len(filtered.Schedules) != 1 || filtered.Schedules[0].ScheduleID != "daily" {
			t.Fatalf("filtered schedules = %+v, %v", filtered, err)
		}

		generation := int64(1)
		mutation := pgjobdb.ScheduleMutationRequest{
			TenantID: "tenant", ScheduleID: "daily", ExpectedGeneration: &generation,
		}
		paused, err := pgjobdb.PauseSchedule(ctx, db, mutation)
		if err != nil || paused.State != pgjobdb.ScheduleStatePaused || paused.Generation != 2 {
			t.Fatalf("pause = %+v, %v", paused, err)
		}
		if _, err := pgjobdb.PauseSchedule(ctx, db, mutation); err == nil {
			t.Fatal("expected stale generation to fail")
		}
		generation = 2
		mutation.NextFireAt, mutation.NextJobID = &next, &nextJob
		resumed, err := pgjobdb.ResumeSchedule(ctx, db, mutation)
		if err != nil || resumed.State != pgjobdb.ScheduleStateActive || resumed.Generation != 3 {
			t.Fatalf("resume = %+v, %v", resumed, err)
		}
		generation = 3
		mutation.NextFireAt, mutation.NextJobID = nil, nil
		archived, err := pgjobdb.ArchiveSchedule(ctx, db, mutation)
		if err != nil || archived.State != pgjobdb.ScheduleStateArchived || archived.Generation != 4 {
			t.Fatalf("archive = %+v, %v", archived, err)
		}
	})
}

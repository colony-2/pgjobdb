package installer_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/colony-2/pgjobdb"
)

func TestNativeGetJobKeepsArchivedFields(t *testing.T) {
	runDatabaseTest(t, func(ctx context.Context, db *sql.DB) {
		future := time.Now().UTC().Add(time.Hour)
		if _, err := pgjobdb.SubmitJob(ctx, db, pgjobdb.SubmitJobRequest{
			TenantID: "tenant", JobID: "future", WorkerID: "submitter",
			JobType: "collect", AvailableAt: &future,
			AppMetadata: json.RawMessage(`{"source":"api"}`),
		}); err != nil {
			t.Fatalf("submit future: %v", err)
		}
		active, err := pgjobdb.GetJob(ctx, db, "tenant", "future")
		if err != nil || active.Status != pgjobdb.JobStatusAwaitingFuture ||
			active.Store != pgjobdb.JobStoreActive || active.ExpiresAt != nil ||
			active.RouteJobType != "collect" || active.WorkKind != pgjobdb.WorkKindJob ||
			string(active.AppMetadata) != `{"source": "api"}` {
			t.Fatalf("active detail = %+v, %v", active, err)
		}
		if _, err := pgjobdb.GetJob(ctx, db, "tenant", "missing"); !errors.Is(err, pgjobdb.ErrJobNotFound) {
			t.Fatalf("missing job error = %v", err)
		}

		if _, err := pgjobdb.SubmitJob(ctx, db, pgjobdb.SubmitJobRequest{
			TenantID: "tenant", JobID: "task", WorkerID: "submitter",
			JobType: "collect",
		}); err != nil {
			t.Fatalf("submit task: %v", err)
		}
		task := &pgjobdb.TaskWork{
			TaskType: "download", ResumeJobType: "collect",
			InputOrdinal: 7, OutputOrdinal: 8, InputHash: "sha256:task",
		}
		if err := pgjobdb.RescheduleUnheldJob(ctx, db, "tenant", "task", "worker",
			pgjobdb.RescheduleRequest{
				RouteJobType: "collect", WorkKind: pgjobdb.WorkKindTask,
				Task: task, LeasePayload: json.RawMessage(`{"opaque":true}`),
				Alternate: &pgjobdb.AlternateRoute{
					JobType: "fallback", TaskType: "download", After: time.Minute,
				},
			}); err != nil {
			t.Fatalf("route task: %v", err)
		}
		current, err := pgjobdb.GetJob(ctx, db, "tenant", "task")
		if err != nil || current.WorkKind != pgjobdb.WorkKindTask ||
			current.TaskInputOrdinal == nil || *current.TaskInputOrdinal != 7 {
			t.Fatalf("current task detail = %+v, %v", current, err)
		}
		if err := pgjobdb.CompleteUnheldJob(ctx, db, "tenant", "task", "worker",
			pgjobdb.Completion{Status: pgjobdb.CompletionFailedApp,
				Detail: "download failed", ErrorKind: "DownloadError"}); err != nil {
			t.Fatalf("complete task: %v", err)
		}
		archived, err := pgjobdb.GetJob(ctx, db, "tenant", "task")
		if err != nil || archived.Store != pgjobdb.JobStoreArchived ||
			archived.Status != pgjobdb.JobStatusCompleted || archived.ArchivedAt == nil ||
			archived.WorkKind != pgjobdb.WorkKindTask ||
			archived.TaskInputOrdinal == nil || *archived.TaskInputOrdinal != 7 ||
			archived.TaskOutputOrdinal == nil || *archived.TaskOutputOrdinal != 8 ||
			archived.TaskInputHash != "sha256:task" ||
			archived.AlternateJobType != "fallback" ||
			archived.AlternateTaskType != "download" ||
			archived.AlternateAfterSeconds == nil ||
			*archived.AlternateAfterSeconds != 60 ||
			string(archived.LeasePayload) != `{"opaque": true}` ||
			archived.CompletionStatus == nil ||
			*archived.CompletionStatus != pgjobdb.CompletionFailedApp ||
			archived.CompletionErrorKind == nil ||
			*archived.CompletionErrorKind != "DownloadError" {
			t.Fatalf("archived detail = %+v, %v", archived, err)
		}
		status, err := pgjobdb.GetJobStatus(ctx, db, "tenant", "task")
		if err != nil || status.Status != pgjobdb.JobStatusCompleted ||
			status.Store != pgjobdb.JobStoreArchived || status.ArchivedAt == nil {
			t.Fatalf("archived status = %+v, %v", status, err)
		}
	})
}

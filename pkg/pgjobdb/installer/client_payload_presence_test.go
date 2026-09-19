package installer_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"testing"

	"github.com/colony-2/pgjobdb/pkg/pgjobdb"
)

func TestNativeClientPayloadPresenceSurvivesRoutesAndArchive(t *testing.T) {
	runDatabaseTest(t, func(ctx context.Context, db *sql.DB) {
		for _, tc := range []struct {
			id      pgjobdb.JobID
			payload json.RawMessage
			visible bool
		}{
			{id: "generated", visible: false},
			{id: "explicit-empty", payload: json.RawMessage(`{}`), visible: true},
		} {
			_, err := pgjobdb.SubmitJob(ctx, db, pgjobdb.SubmitJobRequest{
				TenantID: "tenant", JobID: tc.id, WorkerID: "submitter",
				JobType: "collect", ClientPayloadUpdate: &pgjobdb.ClientPayloadUpdate{Mode: "reset", Value: tc.payload},
			})
			if err != nil {
				t.Fatalf("submit %s: %v", tc.id, err)
			}
			detail, err := pgjobdb.GetJob(ctx, db, "tenant", tc.id)
			if err != nil || (detail.ClientPayload != nil) != tc.visible {
				t.Fatalf("submitted %s visibility = %+v, %v", tc.id, detail, err)
			}
		}

		selector := pgjobdb.WorkSelector{JobTypes: []pgjobdb.JobType{"collect"}}
		lease, err := pgjobdb.GetJobLease(ctx, db, "tenant", "explicit-empty",
			"worker", selector, pgjobdb.GetJobLeaseOptions{})
		if err != nil || lease == nil || lease.ClientPayload == nil ||
			string(lease.ClientPayload) != `{}` {
			t.Fatalf("explicit empty lease = %+v, %v", lease, err)
		}
		if err := lease.Reschedule(ctx, db, pgjobdb.RescheduleRequest{
			RouteJobType: "collect", WorkKind: pgjobdb.WorkKindJob,
			ClientPayloadUpdate: &pgjobdb.ClientPayloadUpdate{Mode: "reset", ExpectedRevision: ptrRevision(1)},
		}); err != nil {
			t.Fatalf("clear payload: %v", err)
		}
		lease, err = pgjobdb.GetJobLease(ctx, db, "tenant", "explicit-empty",
			"worker", selector, pgjobdb.GetJobLeaseOptions{})
		if err != nil || lease == nil || (lease.ClientPayload != nil) {
			t.Fatalf("cleared lease = %+v, %v", lease, err)
		}
		if err := lease.Reschedule(ctx, db, pgjobdb.RescheduleRequest{
			RouteJobType: "collect", WorkKind: pgjobdb.WorkKindJob,
			ClientPayloadUpdate: &pgjobdb.ClientPayloadUpdate{Mode: "reset", ExpectedRevision: ptrRevision(2), Value: json.RawMessage(`{}`)},
		}); err != nil {
			t.Fatalf("restore explicit empty payload: %v", err)
		}
		if err := pgjobdb.CompleteUnheldJob(ctx, db, "tenant", "explicit-empty",
			"worker", pgjobdb.Completion{Status: pgjobdb.CompletionSuccess}); err != nil {
			t.Fatalf("complete: %v", err)
		}
		archived, err := pgjobdb.GetJob(ctx, db, "tenant", "explicit-empty")
		if err != nil || archived.Store != pgjobdb.JobStoreArchived ||
			archived.ClientPayload == nil || string(archived.ClientPayload) != `{}` {
			t.Fatalf("archived visibility = %+v, %v", archived, err)
		}

		task := pgjobdb.TaskWork{TaskType: "download", ResumeJobType: "collect",
			InputOrdinal: 1, OutputOrdinal: 2, InputHash: "sha256:input"}
		owner, err := pgjobdb.GetJobLease(ctx, db, "tenant", "generated", "worker", pgjobdb.WorkSelector{JobTypes: []pgjobdb.JobType{"collect"}}, pgjobdb.GetJobLeaseOptions{})
		if err != nil || owner == nil {
			t.Fatalf("lease: %v", err)
		}
		if err := owner.Reschedule(ctx, db,
			pgjobdb.RescheduleRequest{RouteJobType: "collect", WorkKind: pgjobdb.WorkKindTask,
				Task: &task, ClientPayloadUpdate: &pgjobdb.ClientPayloadUpdate{Mode: "reset", ExpectedRevision: ptrRevision(0), Value: json.RawMessage(`{}`)}}); err != nil {
			t.Fatalf("route task with explicit empty payload: %v", err)
		}
		if err := pgjobdb.CompleteTaskWork(ctx, db, pgjobdb.CompleteTaskWorkRequest{
			TenantID: "tenant", JobID: "generated", WorkerID: "external",
			JobType: "collect", Task: task, ClientPayloadUpdate: &pgjobdb.ClientPayloadUpdate{Mode: "reset", ExpectedRevision: ptrRevision(1)},
		}); err != nil {
			t.Fatalf("complete task and clear payload: %v", err)
		}
		resumed, err := pgjobdb.GetJob(ctx, db, "tenant", "generated")
		if err != nil || resumed.WorkKind != pgjobdb.WorkKindJob ||
			(resumed.ClientPayload != nil) || resumed.ClientPayloadRevision != 2 {
			t.Fatalf("resumed generated view = %+v, %v", resumed, err)
		}
	})
}

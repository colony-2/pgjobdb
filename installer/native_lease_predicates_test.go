package installer_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"testing"

	"github.com/colony-2/pgjobdb"
)

func TestNativeLeaseMetadataPredicates(t *testing.T) {
	runDatabaseTest(t, func(ctx context.Context, db *sql.DB) {
		for _, item := range []struct {
			id       pgjobdb.JobID
			metadata string
		}{
			{"api", `{"source":"api","team":"alpha"}`},
			{"batch", `{"source":"batch","team":"alpha"}`},
			{"other", `{"source":"other","team":"alpha"}`},
			{"beta", `{"source":"api","team":"beta"}`},
		} {
			if _, err := pgjobdb.SubmitJob(ctx, db, pgjobdb.SubmitJobRequest{
				TenantID: "tenant", JobID: item.id, WorkerID: "submitter",
				JobType: "collect", AppMetadata: json.RawMessage(item.metadata),
			}); err != nil {
				t.Fatalf("submit %s: %v", item.id, err)
			}
		}
		selector := pgjobdb.WorkSelector{JobTypes: []pgjobdb.JobType{"collect"}}
		predicates := []pgjobdb.MetadataPredicate{{
			Path: []string{"source"}, Values: []json.RawMessage{
				json.RawMessage(`"api"`), json.RawMessage(`"batch"`),
			},
		}}
		if wrong, err := pgjobdb.GetJobLease(ctx, db, "tenant", "other", "worker",
			selector, pgjobdb.GetJobLeaseOptions{
				MetadataPredicates: predicates,
			}); err != nil || wrong != nil {
			t.Fatalf("targeted predicate mismatch = %+v, %v", wrong, err)
		}
		opts := pgjobdb.GetWorkOptions{
			AppMetadataContains: json.RawMessage(`{"team":"alpha"}`),
			MetadataPredicates:  predicates,
		}
		for _, want := range []pgjobdb.JobID{"api", "batch"} {
			lease, err := pgjobdb.GetWork(ctx, db, "worker", selector, opts)
			if err != nil || lease == nil || lease.JobID != want {
				t.Fatalf("metadata lease = %+v, %v; want %s", lease, err, want)
			}
		}
		if lease, err := pgjobdb.GetWork(ctx, db, "worker", selector, opts); err != nil || lease != nil {
			t.Fatalf("expected no more matching work: %+v, %v", lease, err)
		}
	})
}

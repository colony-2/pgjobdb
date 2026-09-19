package installer_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/colony-2/jobdb/pkg/jobdb/clientpayload"
	"github.com/colony-2/pgjobdb/pkg/pgjobdb"
)

func TestNativeClientJSONMatchesGo(t *testing.T) {
	runDatabaseTest(t, func(ctx context.Context, db *sql.DB) {
		for _, raw := range []string{`null`, `{}`, `[true,null,"<>&\u2028",9007199254740993]`, `{"b":1.200e3,"a":"\ud83d\ude00"}`, `1e999999999999999999999`, `1e+000000009`, `-0.000e200`, `{"large":1e999999,"small":1e-999999}`} {
			want, err := clientpayload.Digest(json.RawMessage(raw))
			if err != nil {
				t.Fatal(err)
			}
			var got string
			if err := db.QueryRowContext(ctx, `SELECT encode(sha256(convert_to(pgjobdb._canonical_client_json($1::json),'UTF8')),'hex')`, raw).Scan(&got); err != nil {
				t.Fatalf("%s: %v", raw, err)
			}
			if got != want {
				t.Fatalf("different JSON identity for %s: %s != %s", raw, got, want)
			}
		}
		for _, raw := range []string{`{"a":1,"\u0061":2}`, `"\ud800"`, `"\u0000"`, strings.Repeat("[", 129) + "0" + strings.Repeat("]", 129), `"` + strings.Repeat("a", 65536) + `"`} {
			if _, err := db.ExecContext(ctx, `SELECT pgjobdb._canonical_client_json($1::json)`, raw); err == nil {
				t.Fatalf("SQL accepted invalid payload %s", raw[:min(len(raw), 30)])
			}
		}
		large := json.RawMessage(`{"text":"` + strings.Repeat("<", 12000) + `"}`)
		rev := int64(1)
		if _, _, err := clientpayload.Apply(large, rev, &clientpayload.Update{Mode: "patch", Value: json.RawMessage(`{}`), ExpectedRevision: &rev}); !errors.Is(err, clientpayload.ErrTooLarge) {
			t.Fatalf("Go result limit: %v", err)
		}
		if _, err := db.ExecContext(ctx, `SELECT pgjobdb._client_update_value($1::json,'patch','{}'::json)`, string(large)); err == nil {
			t.Fatal("SQL accepted oversized escaped patch result")
		}
		_, err := pgjobdb.SubmitJob(ctx, db, pgjobdb.SubmitJobRequest{TenantID: "tenant", JobID: "precise", JobType: "collect", WorkerID: "submitter", ClientPayloadUpdate: &pgjobdb.ClientPayloadUpdate{Mode: "reset", Value: json.RawMessage(`{"huge":1e999999,"n":9007199254740993}`)}})
		if err != nil {
			t.Fatal(err)
		}
		lease, err := pgjobdb.GetJobLease(ctx, db, "tenant", "precise", "worker", pgjobdb.WorkSelector{JobTypes: []pgjobdb.JobType{"collect"}}, pgjobdb.GetJobLeaseOptions{})
		if err != nil || lease == nil {
			t.Fatalf("lease: %v", err)
		}
		patch := &pgjobdb.ClientPayloadUpdate{Mode: "patch", Value: json.RawMessage(`{"x":null,"a":[1]}`), ExpectedRevision: ptrRevision(0)}
		if err := lease.Reschedule(ctx, db, pgjobdb.RescheduleRequest{RouteJobType: "collect", WorkKind: pgjobdb.WorkKindJob, ClientPayloadUpdate: patch}); !errors.Is(err, clientpayload.ErrConflict) {
			t.Fatalf("CAS: %v", err)
		}
		patch.ExpectedRevision = ptrRevision(1)
		if err := lease.Reschedule(ctx, db, pgjobdb.RescheduleRequest{RouteJobType: "collect", WorkKind: pgjobdb.WorkKindJob, ClientPayloadUpdate: patch}); err != nil {
			t.Fatal(err)
		}
		detail, err := pgjobdb.GetJob(ctx, db, "tenant", "precise")
		if err != nil {
			t.Fatal(err)
		}
		want, _, _ := clientpayload.Apply(json.RawMessage(`{"huge":1e999999,"n":9007199254740993}`), 1, patch)
		a, _ := clientpayload.Digest(want)
		b, _ := clientpayload.Digest(detail.ClientPayload)
		if a != b || detail.ClientPayloadRevision != 2 {
			t.Fatalf("lossy SQL patch: %s/%d", detail.ClientPayload, detail.ClientPayloadRevision)
		}
	})
}

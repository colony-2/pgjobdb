package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/remote"
	pgruntime "github.com/colony-2/pgjobdb/pkg/pgjobdb/runtime"
	"github.com/colony-2/pgjobdb/pkg/pgjobdb/runtime/integration/internal/jobdbtest"
	"github.com/stretchr/testify/require"
)

func TestConsumerConstructorCapabilities(t *testing.T) {
	base := jobdbtest.NewNamespacedRuntime(nil, "")
	require.Panics(t, func() { remote.NewConsumerServer(base) })
	handler, err := remote.NewConsumerServerWithOptions(base, remote.ServerOptions{LeaseTokenSigningKey: []byte("constructor-key-at-least-32-bytes-long")})
	require.Nil(t, handler)
	require.ErrorContains(t, err, "configuration administration")
	handler, err = remote.NewConsumerServerWithOptions(base, remote.ServerOptions{LeaseTokenSigningKey: []byte("short")})
	require.Nil(t, handler)
	require.Error(t, err)
}

// Snapshot every scheduler table, with ordered full row values. This includes
// preparations/receipts, facts, memberships, waits and active/archived jobs.
func schedulerSnapshot(t *testing.T, ctx context.Context, db *sql.DB) map[string]string {
	t.Helper()
	rows, err := db.QueryContext(ctx, `SELECT tablename FROM pg_tables WHERE schemaname='pgjobdb' ORDER BY tablename`)
	require.NoError(t, err)
	var tables []string
	for rows.Next() {
		var name string
		require.NoError(t, rows.Scan(&name))
		tables = append(tables, name)
	}
	require.NoError(t, rows.Err())
	require.NoError(t, rows.Close())
	out := map[string]string{}
	for _, table := range tables {
		var value string
		require.NoError(t, db.QueryRowContext(ctx, fmt.Sprintf(`SELECT coalesce(jsonb_agg(value ORDER BY value::text),'[]'::jsonb)::text FROM (SELECT to_jsonb(t) AS value FROM pgjobdb.%s t) rows`, table)).Scan(&value))
		out[table] = value
	}
	return out
}

func TestConsumerHTTPRetiresAttachment(t *testing.T) {
	withBareDatabase(t, func(ctx context.Context, db *sql.DB) {
		rt, err := pgruntime.NewSQLDB(ctx, db, pgruntime.Config{BlobStoreURI: "blobfs://" + t.TempDir()})
		require.NoError(t, err)
		defer rt.Close(ctx)
		probe := &nativeMutationProbe{Runtime: rt}
		a := goalSubmit(t, probe, "a")
		b := goalSubmit(t, probe, "b")
		_ = goalSubmit(t, probe, "consumer", jobdb.Dependency{Target: jobdb.DependencyTarget{GoalID: a.GoalID}, Condition: jobdb.JobPrereqSuccess})
		for _, signed := range []bool{false, true} {
			t.Run(map[bool]string{false: "default", true: "signed"}[signed], func(t *testing.T) {
				view := jobdbtest.NewNamespacedBackend(probe, "")
				_, mutator := reflect.TypeOf(view).MethodByName("AddGoalCandidate")
				require.False(t, mutator)
				var handler http.Handler = remote.NewConsumerServer(view)
				if signed {
					handler, err = remote.NewConsumerServerWithOptions(view, remote.ServerOptions{LeaseTokenSigningKey: []byte("http-retirement-key-at-least-32-bytes")})
					require.NoError(t, err)
				}
				before := schedulerSnapshot(t, ctx, db)
				probe.mutations.Store(0)
				probe.attachments.Store(0)
				request := httptest.NewRequest(http.MethodPut, "/v1/tenants/tenant/goals/"+a.GoalID+"/candidates/"+b.JobKey.JobId, nil)
				response := httptest.NewRecorder()
				celltest.Handler(handler).ServeHTTP(response, request)
				require.Equal(t, http.StatusNotFound, response.Code)
				require.Zero(t, probe.attachments.Load())
				require.Zero(t, probe.mutations.Load())
				require.Equal(t, before, schedulerSnapshot(t, ctx, db))
			})
		}
	})
}

func TestConsumerSignedLeaseAuthority(t *testing.T) {
	factory := conformanceRuntimeFactory(t)
	rt := factory(t)
	ctx := celltest.Context(context.Background())
	h := goalSubmit(t, rt, "signed-owner")
	key := []byte("signed-authority-key-with-at-least-32-bytes")
	options := remote.ServerOptions{LeaseTokenSigningKey: key, LeaseTokenAudience: "authority-one"}
	handler, err := remote.NewConsumerServerWithOptions(jobdbtest.NewNamespacedBackend(rt, ""), options)
	require.NoError(t, err)
	server := httptest.NewServer(celltest.Handler(handler))
	defer server.Close()
	client, err := remote.New(server.URL, server.Client())
	require.NoError(t, err)
	lease, err := client.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: h.JobKey, WorkerID: "worker", Routes: []jobdb.Route{{JobType: "work"}}})
	require.NoError(t, err)
	require.NotNil(t, lease)
	token := lease.(interface{ LeaseToken() string }).LeaseToken()
	variants := []struct {
		name    string
		options remote.ServerOptions
		token   string
	}{
		{"tampered", options, token + "tampered"},
		{"wrong-key", remote.ServerOptions{LeaseTokenSigningKey: []byte("another-signing-key-with-at-least-32-bytes"), LeaseTokenAudience: "authority-one"}, token},
		{"wrong-audience", remote.ServerOptions{LeaseTokenSigningKey: key, LeaseTokenAudience: "authority-two"}, token},
	}
	for _, variant := range variants {
		t.Run(variant.name, func(t *testing.T) {
			handler, err := remote.NewConsumerServerWithOptions(jobdbtest.NewNamespacedBackend(rt, ""), variant.options)
			require.NoError(t, err)
			request := httptest.NewRequest(http.MethodPost, "/v1/tenants/tenant/jobs/"+h.JobKey.JobId+"/leases/"+lease.LeaseID()+"/keepalive", nil)
			request.Header.Set("X-JobDB-Lease-Token", variant.token)
			before, err := rt.GetJob(ctx, h.JobKey)
			require.NoError(t, err)
			response := httptest.NewRecorder()
			celltest.Handler(handler).ServeHTTP(response, request)
			require.Equal(t, http.StatusConflict, response.Code)
			after, err := rt.GetJob(ctx, h.JobKey)
			require.NoError(t, err)
			require.Equal(t, before, after)
		})
	}
}

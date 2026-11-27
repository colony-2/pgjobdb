package integration_test

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/remote"
	"github.com/colony-2/pgjobdb/pkg/pgjobdb"
	pgruntime "github.com/colony-2/pgjobdb/pkg/pgjobdb/runtime"
	"github.com/lib/pq"
	"github.com/stretchr/testify/require"
)

type leaseListTransport func(*http.Request) (*http.Response, error)

func (transport leaseListTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	return transport(request)
}

func TestLastLeaseUpdatedAtHTTP(t *testing.T) {
	dsn := conformanceDSNFactory(t)(t)
	connector, err := pq.NewConnector(dsn)
	require.NoError(t, err)
	counted := &countingListConnector{Connector: connector}
	db := sql.OpenDB(counted)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	ctx := celltest.Context(context.Background())
	backend, err := pgruntime.NewSQLDB(ctx, db, pgruntime.Config{BlobStoreURI: "blobfs://" + t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, backend.Close(ctx)) })
	nativeGoalJob(t, ctx, db, "seed-cell")
	for _, id := range []pgjobdb.JobID{"active", "archived", "unknown"} {
		_, err := pgjobdb.SubmitJob(ctx, db, pgjobdb.SubmitJobRequest{Cell: "test", CellRepo: "example.test/test", TenantID: "c2", JobID: id, WorkerID: "submitter", JobType: "activity"})
		require.NoError(t, err)
	}
	stamps := make(map[string]time.Time)
	for _, id := range []pgjobdb.JobID{"active", "archived"} {
		lease, err := pgjobdb.GetJobLease(ctx, db, "c2", id, "worker", pgjobdb.WorkSelector{JobTypes: []pgjobdb.JobType{"activity"}}, pgjobdb.GetJobLeaseOptions{LeaseDuration: time.Minute})
		require.NoError(t, err)
		require.NotNil(t, lease)
		var acquired time.Time
		require.NoError(t, db.QueryRowContext(ctx, `SELECT occurred_at FROM pgjobdb.lease_events WHERE tenant_id='c2' AND job_id=$1 AND event_type='lease_acquired'`, id).Scan(&acquired))
		stamps[string(id)] = acquired.UTC()
		if id == "archived" {
			require.NoError(t, pgjobdb.CompleteJob(ctx, db, lease.Identity(), pgjobdb.Completion{Status: pgjobdb.CompletionSuccess}))
		}
	}
	server := httptest.NewServer(celltest.Handler(remote.NewConsumerServer(backend)))
	defer server.Close()
	for _, test := range []struct {
		name, scope string
		constructor func(string, *http.Client, ...remote.Option) (*remote.Runtime, error)
	}{{"tenant", "tenants", remote.New}, {"project", "projects", remote.NewProjectRuntime}} {
		t.Run(test.name, func(t *testing.T) {
			var raw []byte
			var path string
			client, err := test.constructor(server.URL, &http.Client{Transport: leaseListTransport(func(request *http.Request) (*http.Response, error) {
				path = request.URL.Path
				response, err := server.Client().Transport.RoundTrip(request)
				if err != nil {
					return nil, err
				}
				defer response.Body.Close()
				raw, err = io.ReadAll(response.Body)
				response.Body = io.NopCloser(bytes.NewReader(raw))
				return response, err
			})})
			require.NoError(t, err)
			counted.queries.Store(0)
			page, err := client.ListJobs(ctx, jobdb.ListJobsRequest{TenantIds: []string{"c2"}, JobTypes: []string{"activity"}})
			pageWire := raw
			require.NoError(t, err)
			require.Contains(t, path, "/v1/"+test.scope+"/c2/jobs")
			if test.name == "tenant" {
				require.EqualValues(t, 2, counted.queries.Load())
			} else {
				queries := counted.queries.Load()
				counted.queries.Store(0)
				_, err = client.ListJobs(ctx, jobdb.ListJobsRequest{TenantIds: []string{"c2"}, JobTypes: []string{"activity"}, PageSize: 1})
				require.NoError(t, err)
				require.Equal(t, queries, counted.queries.Load(), "project admission does not add per-job queries")
			}
			require.Len(t, page.Jobs, 3)
			var wire struct {
				Jobs []map[string]json.RawMessage `json:"jobs"`
			}
			require.NoError(t, json.Unmarshal(pageWire, &wire))
			require.Len(t, wire.Jobs, 3)
			for i, row := range page.Jobs {
				stamp, present := wire.Jobs[i]["lastLeaseUpdatedAt"]
				if row.JobKey.JobId == "unknown" {
					require.False(t, present)
					require.Nil(t, row.LastLeaseUpdatedAt)
					require.Nil(t, row.LeaseExpiresAt)
					continue
				}
				require.True(t, present)
				at := stamps[row.JobKey.JobId]
				var wireTime time.Time
				require.NoError(t, json.Unmarshal(stamp, &wireTime))
				require.Equal(t, at, wireTime)
				require.Equal(t, &at, row.LastLeaseUpdatedAt)
				require.NotNil(t, row.LeaseExpiresAt)
				require.Equal(t, at.Add(time.Minute), *row.LeaseExpiresAt)
			}
		})
	}
}

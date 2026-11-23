package integration_test

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/remote"
	"github.com/colony-2/pgjobdb/internal/projectfixture"
	pgruntime "github.com/colony-2/pgjobdb/pkg/pgjobdb/runtime"
	"github.com/stretchr/testify/require"
)

func TestProjectHTTPAdministrationAndReceiptReplay(t *testing.T) {
	withPreparedDatabase(t, func(ctx context.Context, db *sql.DB) {
		cfg := pgruntime.Config{BlobStoreURI: "blobfs://" + t.TempDir()}
		rt, err := pgruntime.NewSQLDB(ctx, db, cfg)
		require.NoError(t, err)
		defer rt.Close(ctx)
		for _, signed := range []bool{false, true} {
			opts := remote.ServerOptions{}
			if signed {
				opts.LeaseTokenSigningKey = []byte("project-test-signing-key-at-least-32-bytes")
			}
			var handler http.Handler = remote.NewConsumerServer(rt)
			if signed {
				handler, err = remote.NewConsumerServerWithOptions(rt, opts)
				require.NoError(t, err)
			}
			server := httptest.NewServer(celltest.Handler(handler))
			client, err := remote.NewProjectRuntime(server.URL, server.Client())
			require.NoError(t, err)
			_, err = client.CreateProject(ctx, jobdb.CreateProjectRequest{Name: "denied", RequestID: "denied"})
			require.ErrorIs(t, err, jobdb.ErrProjectAccessDenied)
			server.Close()
			trusted := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				handler.ServeHTTP(w, r.WithContext(projectfixture.Context(celltest.Context(r.Context()))))
			}))
			defer trusted.Close()
			client, err = remote.NewProjectRuntime(trusted.URL, trusted.Client())
			require.NoError(t, err)
			name := map[bool]string{false: "http-plain", true: "http-signed"}[signed]
			request := jobdb.CreateProjectRequest{Name: name, RequestID: name, Description: "opaque\x00description", Metadata: json.RawMessage(`{"huge":90071992547409931234,"value":"\u0000"}`)}
			first, err := client.CreateProject(ctx, request)
			require.NoError(t, err)
			again, err := client.CreateProject(ctx, request)
			require.NoError(t, err)
			require.Equal(t, first.Body, again.Body)
			require.Equal(t, first.Status, again.Status)
			require.Equal(t, first.ETag, again.ETag)
			require.Equal(t, first.Location, again.Location)
			for _, body := range []string{`{"name":"bad","name":"duplicate","requestId":"invalid"}`, `{"id":"provided","name":"bad","requestId":"invalid"}`, `{"name":"bad","requestId":"invalid","extra":true}`} {
				response, e := trusted.Client().Post(trusted.URL+"/v1/projects", "application/json", bytes.NewBufferString(body))
				require.NoError(t, e)
				io.Copy(io.Discard, response.Body)
				response.Body.Close()
				require.Equal(t, 400, response.StatusCode)
			}
			// Legacy and explicitly selected project transport address the same immutable ID.
			legacy, e := remote.New(trusted.URL, trusted.Client())
			require.NoError(t, e)
			_, err = legacy.CreateCell(ctx, jobdb.CreateCellRequest{TenantID: first.Project.ID, Name: "work", Repo: "example.test/work"})
			require.NoError(t, err)
			handle, err := client.SubmitJob(ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: first.Project.ID, Cell: "work", JobID: name, JobType: "work", Data: jobdb.NewTaskDataOrPanic(map[string]any{"tenant_id": "opaque"}), Metadata: json.RawMessage(`{"tenant_id":"opaque","n":90071992547409931234}`)}})
			require.NoError(t, err)
			got, err := legacy.GetJob(ctx, handle.JobKey)
			require.NoError(t, err)
			require.Equal(t, handle.GoalID, got.GoalID)
		}
	})
}

// Exercise lease operations through the production composition, without a
// namespace/conformance decorator, so transport failures retain their source.
func TestProjectHTTPLeaseRenewal(t *testing.T) {
	withPreparedDatabase(t, func(ctx context.Context, db *sql.DB) {
		rt, err := pgruntime.NewSQLDB(ctx, db, pgruntime.Config{BlobStoreURI: "blobfs://" + t.TempDir()})
		require.NoError(t, err)
		defer rt.Close(ctx)
		p := newProject(t, ctx, db, "http-lease")
		_, err = rt.CreateCell(ctx, jobdb.CreateCellRequest{TenantID: p.Project.ID, Name: "work", Repo: "example.test/work"})
		require.NoError(t, err)
		for _, signed := range []bool{false, true} {
			for _, project := range []bool{false, true} {
				name := fmt.Sprintf("signed-%t-project-%t", signed, project)
				t.Run(name, func(t *testing.T) {
					var handler http.Handler = remote.NewConsumerServer(rt)
					if signed {
						handler, err = remote.NewConsumerServerWithOptions(rt, remote.ServerOptions{LeaseTokenSigningKey: []byte("project-lease-signing-key-at-least-32-bytes")})
						require.NoError(t, err)
					}
					server := httptest.NewServer(celltest.Handler(handler))
					defer server.Close()
					var client jobdb.ConsumerRuntime
					if project {
						client, err = remote.NewProjectRuntime(server.URL, server.Client())
					} else {
						client, err = remote.New(server.URL, server.Client())
					}
					require.NoError(t, err)
					h, err := client.SubmitJob(ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: p.Project.ID, JobID: name, Cell: "work", JobType: "work", Data: jobdb.NewTaskDataOrPanic(1)}})
					require.NoError(t, err)
					lease, err := client.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: h.JobKey, WorkerID: "worker", Routes: []jobdb.Route{{JobType: "work"}}, LeaseDuration: time.Minute})
					require.NoError(t, err)
					require.NotNil(t, lease)
					_, err = lease.(jobdb.RenewableExecutionLease).Renew(ctx)
					if err != nil {
						var diagnostic *remote.LeaseTransportError
						if errors.As(err, &diagnostic) {
							t.Logf("renewal diagnostic: %s", diagnostic.SafeMessage())
						}
					}
					require.NoError(t, err)
				})
			}
		}
	})
}

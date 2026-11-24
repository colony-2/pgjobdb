package integration_test

import (
	"bytes"
	"context"
	"debug/buildinfo"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	remote "github.com/colony-2/jobdb/pkg/jobdb/runtime/remote"
	"github.com/colony-2/jobdb/pkg/jobdb/runtimetest"
	"github.com/stretchr/testify/require"
)

func TestResourceBackoffHTTPPostgres(t *testing.T) {
	newDSN := conformanceDSNFactory(t)
	t.Run("H08_pre_eagain_client", func(t *testing.T) {
		binary := os.Getenv("PGJOBDB_TEST_LEGACY_CLIENT")
		if binary == "" {
			binary = filepath.Join(t.TempDir(), "legacy-client")
			ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
			defer cancel()
			build := exec.CommandContext(ctx, "go", "build", "-mod=readonly", "-o", binary, ".")
			build.Dir = filepath.Join("..", "..", "..", "..", "scripts", "testdata", "pgjobdb-legacy-client")
			build.Env = append(os.Environ(), "GOWORK=off", "GOFLAGS=", "GOTOOLCHAIN=local")
			output, err := build.CombinedOutput()
			require.NoError(t, err, "build pinned pre-EAGAIN client: %s", output)
			t.Log("built isolated pre-EAGAIN client")
		}
		info, err := buildinfo.ReadFile(binary)
		require.NoError(t, err)
		var jobdbVersion string
		for _, dep := range info.Deps {
			require.Nil(t, dep.Replace)
			if dep.Path == "github.com/colony-2/jobdb" {
				jobdbVersion = dep.Version
			}
		}
		require.Equal(t, "v0.0.94", jobdbVersion)
		fixture := resourceFixture(t, newDSN(t))
		handler, err := remote.NewConsumerServerWithOptions(fixture.Runtime, remote.ServerOptions{
			LeaseTokenSigningKey: bytes.Repeat([]byte{47}, 32), LeaseTokenAudience: "pg-resource-legacy-client",
		})
		require.NoError(t, err)
		server := httptest.NewServer(celltest.Handler(handler))
		t.Cleanup(server.Close)
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ctx, binary)
		cmd.Env = append(os.Environ(), "PGJOBDB_TEST_SERVER="+server.URL)
		output, err := cmd.CombinedOutput()
		t.Logf("JobDB %s:\n%s", jobdbVersion, output)
		require.NoError(t, err)
	})
	for _, mode := range []string{"tenant", "project", "delegated"} {
		t.Run(mode, func(t *testing.T) {
			f := resourceFixture(t, newDSN(t))
			ctx := celltest.Context(context.Background())
			f.SetTime(time.Now().UTC().Truncate(time.Millisecond))
			options := remote.ServerOptions{LeaseTokenSigningKey: bytes.Repeat([]byte{47}, 32), LeaseTokenAudience: "pg-resource"}
			var handler http.Handler
			setHandler := func(rt jobdb.ConsumerRuntime) {
				h, err := remote.NewConsumerServerWithOptions(rt, options)
				require.NoError(t, err)
				handler = celltest.Handler(h)
			}
			setHandler(f.Runtime)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { handler.ServeHTTP(w, r) }))
			defer server.Close()
			client, err := remote.New(server.URL, server.Client())
			require.NoError(t, err)
			prefix := "/v1/tenants/c2/jobs/over-http/leases/"
			if mode == "project" {
				client, err = remote.NewProjectRuntime(server.URL, server.Client())
				require.NoError(t, err)
				prefix = strings.Replace(prefix, "/tenants/", "/projects/", 1)
			}
			h, err := celltest.Submit(client, ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "c2", Cell: "test", CellRepo: "example.test/test", JobID: "over-http", JobType: "work", Data: jobdb.NewTaskDataOrPanic("input")}})
			require.NoError(t, err)
			owner, err := client.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: h.JobKey, WorkerID: "owner", Routes: []jobdb.Route{{JobType: "work"}}, LeaseDuration: 2 * time.Second})
			require.NoError(t, err)
			require.NotNil(t, owner)
			require.Equal(t, 1, owner.ExecutionState().ResourceBackoff.DurabilityVersion)
			policy := jobdb.DefaultResourceBackoffPolicy()
			allowance := int64(math.MaxInt64)
			policy.CrashAllowance = &allowance
			req, err := runtimetest.PrepareResourceBackoffCheckpoint(ctx, f.Runtime, owner, "http/attempt:1", 1, policy)
			require.NoError(t, err)
			lease := owner
			if mode == "delegated" {
				delegated, err := client.CreateDelegation(ctx, owner, jobdb.CreateDelegationRequest{Permissions: jobdb.DelegationPermissions{ResourceBackoff: true}})
				require.NoError(t, err)
				require.NotNil(t, delegated.ExecutionState().CrashBudgetRemaining)
				require.Nil(t, delegated.ExecutionState().ResourceResume)
				require.ErrorIs(t, delegated.Reschedule(ctx, jobdb.RescheduleExecutionRequest{NextRoute: jobdb.Route{JobType: "work"}}), jobdb.ErrLeasePermissionDenied)
				lease = delegated
			}
			token := lease.(interface{ LeaseToken() string }).LeaseToken()
			path := prefix + owner.LeaseID() + "/resource-backoff"
			raw, err := json.Marshal(req)
			require.NoError(t, err)
			send := func(path, token string, body []byte) (int, []byte) {
				r, err := http.NewRequestWithContext(context.Background(), http.MethodPost, server.URL+path, bytes.NewReader(body))
				require.NoError(t, err)
				r.Header.Set("Content-Type", "application/json")
				r.Header.Set("X-JobDB-Lease-Token", token)
				response, err := server.Client().Do(r)
				require.NoError(t, err)
				defer response.Body.Close()
				data, err := io.ReadAll(response.Body)
				require.NoError(t, err)
				return response.StatusCode, data
			}
			t.Run("H02_strict_request_before_mutation", func(t *testing.T) {
				for _, body := range [][]byte{append([]byte(`{"unknown":true,`), raw[1:]...), append([]byte(`{"version":1,`), raw[1:]...), []byte(`null`)} {
					status, _ := send(path, token, body)
					require.NotEqual(t, 200, status)
				}
				still, err := f.Runtime.GetJob(ctx, h.JobKey)
				require.NoError(t, err)
				require.Equal(t, jobdb.JobStatusActive, still.Status)
			})
			receipt, err := lease.(jobdb.ResourceBackoffLease).RescheduleResourceBackoff(ctx, req)
			require.NoError(t, err)
			require.EqualValues(t, math.MaxInt64, receipt.ResolvedCrashAllowance)
			if mode != "project" {
				current, err := client.GetJob(ctx, h.JobKey)
				require.NoError(t, err)
				require.EqualValues(t, math.MaxInt64, *current.ExecutionState.CrashBudgetRemaining)
			}
			t.Run("H04_binding", func(t *testing.T) {
				for _, target := range []string{strings.Replace(path, "/c2/", "/another/", 1), strings.Replace(path, "/over-http/", "/other/", 1)} {
					status, _ := send(target, token, raw)
					require.NotEqual(t, 200, status)
				}
				status, _ := send(path, token+"forged", raw)
				require.NotEqual(t, 200, status)
				changed := req
				changed.RequestID = "not-the-retained-receipt"
				body, err := json.Marshal(changed)
				require.NoError(t, err)
				status, _ = send(path, token, body)
				require.NotEqual(t, 200, status)
			})
			// Wait for real credential expiry, then reconnect actual storage and replace
			// the server while retaining verification authority and the original token.
			time.Sleep(2100 * time.Millisecond)
			f.Runtime = f.Reopen()
			setHandler(f.Runtime)
			t.Run("H03_H07_expired_exact_receipt_after_reopen", func(t *testing.T) {
				status, body := send(path, token, raw)
				require.Equal(t, 200, status, string(body))
				if mode != "project" {
					var replay jobdb.ResourceBackoffReceipt
					require.NoError(t, json.Unmarshal(body, &replay))
					require.Equal(t, receipt, replay)
				} else {
					require.Contains(t, string(body), fmt.Sprint(math.MaxInt64))
				}
				retired := options
				retired.LeaseTokenAudience = "other-audience"
				h, err := remote.NewConsumerServerWithOptions(f.Runtime, retired)
				require.NoError(t, err)
				handler = celltest.Handler(h)
				status, _ = send(path, token, raw)
				require.NotEqual(t, 200, status)
			})
		})
	}
}

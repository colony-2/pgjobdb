package integration_test

import (
	"context"
	"net/http/httptest"
	"testing"

	remoteruntime "github.com/colony-2/jobdb/pkg/jobdb/runtime/remote"
	"github.com/colony-2/jobdb/pkg/jobdb/runtimetest"
	pgjobdbruntime "github.com/colony-2/pgjobdb/pkg/pgjobdb/runtime"
)

func TestPostgresRuntimeConformance(t *testing.T) {
	capabilities := runtimetest.Capabilities{
		Leases: true, Schedules: true, SchemaRegistry: true, RuntimeStorage: true,
	}
	runtimetest.RunWorkflowRuntimeConformance(t,
		runtimetest.Harness{
			Name:         "postgres",
			Capabilities: capabilities,
			New: func(tb testing.TB) runtimetest.Fixture {
				embedded := startConformanceRuntime(tb)
				return runtimetest.Fixture{
					Runtime: embedded.Runtime, SchemaRegistry: embedded.Runtime,
					Cleanup: func(context.Context) error { embedded.Shutdown(); return nil },
				}
			},
		},
		runtimetest.Harness{
			Name:         "remote-postgres",
			Capabilities: capabilities,
			New: func(tb testing.TB) runtimetest.Fixture {
				embedded := startConformanceRuntime(tb)
				server := httptest.NewServer(remoteruntime.NewServer(embedded.Runtime))
				remote, err := remoteruntime.New(server.URL, server.Client())
				if err != nil {
					server.Close()
					embedded.Shutdown()
					tb.Fatalf("start remote Postgres runtime: %v", err)
				}
				return runtimetest.Fixture{
					Runtime: remote, SchemaRegistry: remote,
					Cleanup: func(context.Context) error {
						server.Close()
						embedded.Shutdown()
						return nil
					},
				}
			},
		},
	)
}

func startConformanceRuntime(tb testing.TB) *pgjobdbruntime.EmbeddedRuntime {
	tb.Helper()
	embedded, err := pgjobdbruntime.StartEmbeddedRuntime(context.Background())
	if err != nil {
		tb.Fatalf("start embedded Postgres runtime: %v", err)
	}
	return embedded
}

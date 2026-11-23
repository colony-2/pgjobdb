package integration_test

import (
	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/remote"
	"github.com/colony-2/jobdb/pkg/jobdb/runtimetest"
	"github.com/colony-2/pgjobdb/pkg/pgjobdb/runtime/integration/internal/jobdbtest"
	"github.com/stretchr/testify/require"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestRetainedGoalPublicConformance(t *testing.T) {
	factory := conformanceRuntimeFactory(t)
	for _, transport := range []string{"direct", "default-http", "signed-http"} {
		t.Run(transport, func(t *testing.T) {
			rt := factory(t)
			view := jobdbtest.NewProjectBackend(rt, "")
			var r jobdb.WorkflowRuntime = view
			if transport != "direct" {
				var handler http.Handler = remote.NewConsumerServer(rt)
				if transport == "signed-http" {
					var err error
					handler, err = remote.NewConsumerServerWithOptions(rt, remote.ServerOptions{LeaseTokenSigningKey: []byte("retained-goal-conformance-32-byte-secret"), LeaseTokenAudience: "retained-goals"})
					require.NoError(t, err)
				}
				server := httptest.NewServer(selectionTestHandler(handler))
				t.Cleanup(server.Close)
				client, err := remote.New(server.URL, server.Client())
				require.NoError(t, err)
				r = jobdbtest.NewProjectClient(client, rt, "")
			}
			t.Run("admission", func(t *testing.T) { runtimetest.RunGoalAdmissionConformance(t, r) })
			t.Run("failure-records", func(t *testing.T) { runtimetest.RunTerminalFailureConformance(t, r) })
		})
	}
}

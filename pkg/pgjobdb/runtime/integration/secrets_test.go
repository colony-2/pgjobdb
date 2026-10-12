package integration_test

import (
	"context"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/stretchr/testify/require"
)

func TestPostgresSecretsWithoutProvider(t *testing.T) {
	newRuntime := conformanceRuntimeFactory(t)
	for _, transport := range []string{"embedded", "http"} {
		t.Run(transport, func(t *testing.T) {
			ctx := context.Background()
			rt := configurationTransport(t, newRuntime(t), transport)
			job, err := rt.SubmitJob(ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", JobType: "job", Data: jobdb.NewTaskDataOrPanic(1)}})
			require.NoError(t, err)
			lease, err := rt.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: job.JobKey, WorkerID: "worker", Routes: []jobdb.Route{{JobType: "job"}}, LeaseDuration: time.Minute})
			require.NoError(t, err)
			require.NotNil(t, lease)
			result, err := lease.ResolveSecrets(ctx, []string{"example"})
			require.ErrorIs(t, err, jobdb.ErrNoSecretProvider)
			require.Empty(t, result.Secrets)
			_, err = rt.ResolveSecrets(ctx, jobdb.ResolveSecretsRequest{JobKey: job.JobKey, LeaseID: lease.LeaseID(), Names: []string{"example"}})
			require.ErrorIs(t, err, jobdb.ErrExecutionLeaseLost)
			require.NoError(t, rt.CancelJob(ctx, jobdb.CancelJobRequest{JobKey: job.JobKey, WorkerID: "admin"}))
			_, err = lease.ResolveSecrets(ctx, []string{"example"})
			require.ErrorIs(t, err, jobdb.ErrExecutionLeaseLost)
		})
	}
}

package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	"github.com/colony-2/jobdb/pkg/jobdb/runtimetest"
	embeddedpostgres "github.com/fergusstrange/embedded-postgres"
	"github.com/stretchr/testify/require"
)

func TestResourceBackoffPostgresProcessRestart(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := uint32(listener.Addr().(*net.TCPAddr).Port)
	require.NoError(t, listener.Close())
	root := t.TempDir()
	pg := embeddedpostgres.NewDatabase(embeddedpostgres.DefaultConfig().Port(port).RuntimePath(filepath.Join(root, "runtime")).DataPath(filepath.Join(root, "data")))
	require.NoError(t, pg.Start())
	t.Cleanup(func() { require.NoError(t, pg.Stop()) })
	dsn := fmt.Sprintf("postgres://postgres:postgres@localhost:%d/postgres?sslmode=disable", port)
	var db *sql.DB
	f := resourceFixture(t, dsn, func(current *sql.DB) { db = current })
	ctx := celltest.Context(context.Background())
	at := time.Now().UTC().Truncate(time.Millisecond)
	f.SetTime(at)
	h, err := celltest.Submit(f.Runtime, ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "c2", Cell: "test", CellRepo: "example.test/test", JobID: "restart", JobType: "work", Data: jobdb.NewTaskDataOrPanic("original")}})
	require.NoError(t, err)
	claim := func() jobdb.ExecutionLease {
		l, err := f.Runtime.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: h.JobKey, WorkerID: "worker", Routes: []jobdb.Route{{JobType: "work"}}, LeaseDuration: time.Minute})
		require.NoError(t, err)
		require.NotNil(t, l)
		return l
	}
	lease := claim()
	policy := jobdb.DefaultResourceBackoffPolicy()
	allowance := int64(100)
	policy.CrashAllowance = &allowance
	req, err := runtimetest.PrepareResourceBackoffCheckpoint(ctx, f.Runtime, lease, "restart/attempt:1", 1, policy)
	require.NoError(t, err)
	receipt, err := lease.(jobdb.ResourceBackoffLease).RescheduleResourceBackoff(ctx, req)
	require.NoError(t, err)
	before, err := f.Runtime.GetJob(ctx, h.JobKey)
	require.NoError(t, err)
	require.NoError(t, pg.Stop())
	require.NoError(t, pg.Start())
	f.Runtime = f.Reopen()
	after, err := f.Runtime.GetJob(ctx, h.JobKey)
	require.NoError(t, err)
	require.Equal(t, before, after)
	replay, err := f.Runtime.(jobdb.ResourceBackoffTransport).RescheduleResourceBackoffByID(ctx, h.JobKey, lease.LeaseID(), "worker", req)
	require.NoError(t, err)
	require.Equal(t, receipt, replay)
	var count, credit int64
	require.NoError(t, db.QueryRowContext(ctx, `SELECT count FROM pgjobdb.resource_sequences WHERE job_id='restart'`).Scan(&count))
	require.EqualValues(t, 1, count)
	f.SetTime(receipt.WakeAt)
	require.EqualValues(t, 100, *claim().ExecutionState().CrashBudgetRemaining)
	f.SetTime(receipt.WakeAt.Add(time.Minute))
	require.EqualValues(t, 99, *claim().ExecutionState().CrashBudgetRemaining)
	require.NoError(t, db.QueryRowContext(ctx, `SELECT crash_budget_remaining FROM pgjobdb.jobs WHERE job_id='restart'`).Scan(&credit))
	require.EqualValues(t, 99, credit)
}

package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	"github.com/colony-2/pgjobdb/pkg/pgjobdb"
	pgruntime "github.com/colony-2/pgjobdb/pkg/pgjobdb/runtime"
	"github.com/lib/pq"
	"github.com/stretchr/testify/require"
)

func TestLeaseEventRuntimeDriverQueryCounts(t *testing.T) {
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
	ids := []string{}
	goals := []string{}
	jobs, workers, leases := []string{}, []string{}, []string{}
	// 100 cells and 240 jobs, with multiple renewals for each lease. Every call
	// goes through the real scheduler; chapter/payload data is irrelevant to reads.
	for i := 0; i < 100; i++ {
		c, err := backend.CreateCell(ctx, jobdb.CreateCellRequest{TenantID: "tenant", Name: fmt.Sprintf("cell%d", i), Repo: fmt.Sprintf("example.test/cell%d", i)})
		require.NoError(t, err)
		ids = append(ids, c.ID)
	}
	for i := 0; i < 240; i++ {
		c := i % 100
		_, err := pgjobdb.SubmitJob(ctx, db, pgjobdb.SubmitJobRequest{Cell: fmt.Sprintf("cell%d", c), CellRepo: fmt.Sprintf("example.test/cell%d", c), TenantID: "tenant", JobID: pgjobdb.JobID(fmt.Sprintf("job%d", i)), WorkerID: "submitter", JobType: "collect"})
		require.NoError(t, err)
		if i < 100 {
			var goal string
			require.NoError(t, db.QueryRowContext(ctx, `SELECT goal_id FROM pgjobdb.job_facts WHERE tenant_id='tenant' AND job_id=$1`, fmt.Sprintf("job%d", i)).Scan(&goal))
			goals = append(goals, goal)
		}
		worker := fmt.Sprintf("worker%d", c)
		lease, err := pgjobdb.GetJobLease(ctx, db, "tenant", pgjobdb.JobID(fmt.Sprintf("job%d", i)), pgjobdb.WorkerID(worker), pgjobdb.WorkSelector{JobTypes: []pgjobdb.JobType{"collect"}}, pgjobdb.GetJobLeaseOptions{})
		require.NoError(t, err)
		require.NotNil(t, lease, "targeted fixture claim job%d", i)
		if i < 100 {
			jobs = append(jobs, fmt.Sprintf("job%d", i))
			workers = append(workers, worker)
			leases = append(leases, lease.LeaseID)
		}
		for n := 0; n < 2; n++ {
			_, err = pgjobdb.KeepAliveLease(ctx, db, lease.Identity(), time.Minute)
			require.NoError(t, err)
		}
	}

	// Plans exercise the same SQL used by runtime reads, including the deduplicated
	// additional-goal relation and tenant-qualified joins.
	recorder := &listQueryRecorder{DB: db}
	for _, n := range []int{1, 100} {
		_, err = pgjobdb.ListLeaseEvents(ctx, recorder, jobdb.ListLeaseEventsRequest{TenantID: "tenant", PageSize: 100, CellIDs: ids[:n], GoalIDs: goals[:n]})
		require.NoError(t, err)
		recordLeaseEventPlan(t, db, recorder, fmt.Sprintf("lease-events-goals-%d", n))
	}

	// A second runtime/connection accepts a cursor signed by the first instance.
	cursorReq := jobdb.ListLeaseEventsRequest{TenantID: "tenant", PageSize: 1}
	first, err := backend.ListLeaseEvents(ctx, cursorReq)
	require.NoError(t, err)
	require.NotEmpty(t, first.NextPageToken)
	peer, err := pgruntime.OpenDSN(ctx, dsn, pgruntime.Config{BlobStoreURI: "blobfs://" + t.TempDir()})
	require.NoError(t, err)
	cursorReq.PageToken = first.NextPageToken
	next, err := peer.ListLeaseEvents(ctx, cursorReq)
	require.NoError(t, err)
	require.Len(t, next.Events, 1)
	require.NotEqual(t, first.Events[0].EventID, next.Events[0].EventID)
	require.NoError(t, peer.Close(ctx))
	forbidListDiagnostics(t, db)
	for _, transport := range []string{"embedded", "http"} {
		t.Run(transport, func(t *testing.T) {
			rt := configurationTransport(t, backend, transport)
			for _, size := range []int{1, 100, 200} {
				for _, n := range []int{1, 100} {
					t.Run(fmt.Sprintf("size%d-cardinality%d", size, n), func(t *testing.T) {
						req := jobdb.ListLeaseEventsRequest{TenantID: "tenant", PageSize: size, CellIDs: ids[:n], GoalIDs: goals[:n], JobIDs: jobs[:n], WorkerIDs: workers[:n], LeaseIDs: leases[:n]}
						counted.queries.Store(0)
						page, err := rt.ListLeaseEvents(ctx, req)
						require.NoError(t, err)
						require.EqualValues(t, 2, counted.queries.Load())
						require.Len(t, page.Events, min(size, 3*n))
						t.Logf("driver reads=%d pageSize=%d cell/goal/job/worker/leaseFilters=%d returned=%d", counted.queries.Load(), size, n, len(page.Events))
						if page.NextPageToken != "" {
							req.PageToken = page.NextPageToken
							counted.queries.Store(0)
							_, err = rt.ListLeaseEvents(ctx, req)
							require.NoError(t, err)
							require.EqualValues(t, 2, counted.queries.Load())
						}
					})
				}
			}
		})
	}
}

func TestLeaseEventRenewableAndKeepAlive(t *testing.T) {
	factory := conformanceRuntimeFactory(t)
	rt := factory(t)
	ctx := celltest.Context(context.Background())
	result, err := celltest.Submit(rt, ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", JobID: "job", Cell: "test", CellRepo: "example.test/test", JobType: "collect", Data: jobdb.NewTaskDataOrPanic(1)}})
	require.NoError(t, err)
	lease, err := rt.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: result.JobKey, WorkerID: "worker", Routes: []jobdb.Route{{JobType: "collect"}}, LeaseDuration: 2 * time.Second})
	require.NoError(t, err)
	require.NotNil(t, lease)
	renewable, ok := lease.(jobdb.RenewableExecutionLease)
	require.True(t, ok)
	lease, err = renewable.Renew(ctx)
	require.NoError(t, err)
	require.NotNil(t, lease)
	page, err := rt.ListLeaseEvents(ctx, jobdb.ListLeaseEventsRequest{TenantID: "tenant"})
	require.NoError(t, err)
	require.Len(t, page.Events, 2)
	require.NoError(t, lease.KeepAlive(ctx))
	defer lease.StopKeepAlive()
	// KeepAlive starts the existing timed loop; it is not a synchronous renewal.
	require.Eventually(t, func() bool {
		page, e := rt.ListLeaseEvents(ctx, jobdb.ListLeaseEventsRequest{TenantID: "tenant"})
		return e == nil && len(page.Events) >= 3
	}, 3*time.Second, 20*time.Millisecond)
}

func recordLeaseEventPlan(t *testing.T, db *sql.DB, d *listQueryRecorder, name string) {
	t.Helper()
	require.Contains(t, d.query, "SELECT DISTINCT tenant_id,job_id")
	require.Contains(t, d.query, "g.tenant_id=e.tenant_id AND g.job_id=e.job_id")
	require.NotContains(t, d.query, "OFFSET")
	rows, err := db.Query("EXPLAIN (ANALYZE, BUFFERS, VERBOSE, FORMAT JSON) "+d.query, d.args...)
	require.NoError(t, err)
	require.True(t, rows.Next())
	var raw []byte
	require.NoError(t, rows.Scan(&raw))
	require.NoError(t, rows.Err())
	rows.Close()
	textRows, err := db.Query("EXPLAIN (ANALYZE, BUFFERS) "+d.query, d.args...)
	require.NoError(t, err)
	var plan strings.Builder
	for textRows.Next() {
		var line string
		require.NoError(t, textRows.Scan(&line))
		plan.WriteString(line + "\n")
	}
	require.NoError(t, textRows.Err())
	textRows.Close()
	t.Log(plan.String())
	if dir := os.Getenv("PGJOBDB_EXPLAIN_DIR"); dir != "" {
		require.NoError(t, os.MkdirAll(dir, 0755))
		require.NoError(t, os.WriteFile(filepath.Join(dir, name+".json"), raw, 0644))
		require.NoError(t, os.WriteFile(filepath.Join(dir, name+".txt"), []byte(plan.String()), 0644))
	}
}

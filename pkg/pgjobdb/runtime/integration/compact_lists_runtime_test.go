package integration_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/pgjobdb/pkg/pgjobdb"
	pgruntime "github.com/colony-2/pgjobdb/pkg/pgjobdb/runtime"
	"github.com/lib/pq"
	"github.com/stretchr/testify/require"
)

// Count at the driver boundary, including any queries made by the scheduler,
// chapter store, or core runtime rather than just the compact SQL helper.
type countingListConnector struct {
	driver.Connector
	queries atomic.Int64
}

func (c *countingListConnector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := c.Connector.Connect(ctx)
	if err != nil {
		return nil, err
	}
	return &countingListConn{Conn: conn, queries: &c.queries}, nil
}

type countingListConn struct {
	driver.Conn
	queries *atomic.Int64
}

func (c *countingListConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	c.queries.Add(1)
	return c.Conn.(driver.ExecerContext).ExecContext(ctx, query, args)
}
func (c *countingListConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	c.queries.Add(1)
	return c.Conn.(driver.QueryerContext).QueryContext(ctx, query, args)
}

func TestCompactListRuntimeQueryCounts(t *testing.T) {
	dsn := conformanceDSNFactory(t)(t)
	connector, err := pq.NewConnector(dsn)
	require.NoError(t, err)
	counted := &countingListConnector{Connector: connector}
	db := sql.OpenDB(counted)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	ctx := context.Background()
	backend, err := pgruntime.NewSQLDB(ctx, db, pgruntime.Config{BlobStoreURI: "blobfs://" + t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, backend.Close(ctx)) })
	producer := nativeGoalJob(t, ctx, db, "producer")
	secondary := nativeGoalJob(t, ctx, db, "secondary")
	metadata := json.RawMessage(`{"large":"` + strings.Repeat("m", 4096) + `"}`)
	// Deliberately no chapter/artifact records: listing must work without reading
	// either store. Include both pending waits and archived outcomes on each page.
	for i := 0; i < 240; i++ {
		id := pgjobdb.JobID(fmt.Sprintf("consumer-%03d", i))
		req := pgjobdb.SubmitJobRequest{TenantID: "tenant", JobID: id, WorkerID: "test", JobType: "consumer", AppMetadata: metadata, ClientPayloadUpdate: &pgjobdb.ClientPayloadUpdate{Mode: "reset", Value: json.RawMessage(`{"secretDetail":true}`)}}
		if i%3 != 0 {
			req.Dependencies = nativeGoalWait("", producer.GoalID).Dependencies
		}
		_, err := pgjobdb.SubmitJob(ctx, db, req)
		require.NoError(t, err)
		if i%3 == 0 {
			require.NoError(t, pgjobdb.CompleteUnheldJob(ctx, db, "tenant", id, "test", pgjobdb.Completion{Status: pgjobdb.CompletionFailedApp, Detail: "full completion detail"}))
		}
		if i%20 == 0 {
			_, err = pgjobdb.AddGoalCandidate(ctx, db, jobdb.AddGoalCandidateRequest{GoalKey: nativeGoalKey(secondary), JobID: string(id)})
			require.NoError(t, err)
		}
	}
	d := &listQueryRecorder{DB: db}
	_, err = pgjobdb.ListJobSummaries(ctx, d, pgjobdb.ListJobsOptions{TenantIDs: []pgjobdb.TenantID{"tenant"}, JobTypes: []pgjobdb.JobType{"consumer"}, PageSize: 100})
	require.NoError(t, err)
	recordCompactListPlan(t, db, d, "mixed-active-archive")
	forbidListDiagnostics(t, db)
	for _, transport := range []string{"embedded", "http"} {
		t.Run(transport, func(t *testing.T) {
			rt := configurationTransport(t, backend, transport)
			for _, size := range []int{1, 100, 200} {
				t.Run(fmt.Sprint(size), func(t *testing.T) {
					req := jobdb.ListJobsRequest{TenantIds: []string{"tenant"}, JobTypes: []string{"consumer"}, PageSize: size}
					total, pending, archived := 0, 0, 0
					for {
						counted.queries.Store(0)
						page, err := rt.ListJobs(ctx, req)
						require.NoError(t, err)
						require.EqualValues(t, 1, counted.queries.Load(), "listing query count must not grow with page size")
						for _, row := range page.Jobs {
							total++
							require.JSONEq(t, string(metadata), string(row.Metadata))
							require.Nil(t, row.BlockingWait)
							require.Empty(t, row.OtherGoalIDs)
							require.Empty(t, row.WaitFor)
							require.Empty(t, row.CompletionDetail)
							require.Empty(t, row.ClientPayload)
							require.Empty(t, row.ExecutionState)
							require.EqualValues(t, 1, row.ClientPayloadRevision)
							if row.ArchivedAt != nil {
								archived++
								require.Nil(t, row.BlockingWaitSummary)
								require.Equal(t, "failed_app", row.CompletionStatus)
							} else {
								pending++
								require.Equal(t, &jobdb.DependencyWaitSummary{WaitID: "submission"}, row.BlockingWaitSummary)
							}
						}
						if page.NextPageToken == "" {
							break
						}
						req.PageToken = page.NextPageToken
					}
					require.Equal(t, 240, total)
					require.Equal(t, 160, pending)
					require.Equal(t, 80, archived)
				})
			}
			counted.queries.Store(0)
			page, err := rt.ListJobs(ctx, jobdb.ListJobsRequest{TenantIds: []string{"tenant"}, GoalIDs: []string{secondary.GoalID}, JobTypes: []string{"consumer"}})
			require.NoError(t, err)
			require.Len(t, page.Jobs, 12)
			require.EqualValues(t, 1, counted.queries.Load())
		})
	}
}

package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	"github.com/colony-2/jobdb/pkg/jobdb/runtimetest"
	pgruntime "github.com/colony-2/pgjobdb/pkg/pgjobdb/runtime"
	"github.com/stretchr/testify/require"
)

// Hold the first real initial-chapter INSERT, after Create has begun. The
// equivalent writer must wait for complete history rather than treating the
// visible story identity as a complete initial chapter. Two independent runtime
// connections exercise the same window as the public selection conformance.
func TestRetainedGoalConcurrentInitialHistory(t *testing.T) {
	runDatabaseTest(t, func(ctx context.Context, db *sql.DB) {
		ctx, cancel := context.WithTimeout(runtimetest.PrimaryGoalContext(ctx), 20*time.Second)
		defer cancel()
		var port string
		require.NoError(t, db.QueryRowContext(ctx, `SHOW port`).Scan(&port))
		open := func() (*pgruntime.Runtime, int) {
			connection, err := sql.Open("postgres", fmt.Sprintf("host=localhost port=%s user=postgres password=postgres dbname=postgres sslmode=disable", port))
			require.NoError(t, err)
			t.Cleanup(func() { _ = connection.Close() })
			connection.SetMaxOpenConns(1)
			runtime, err := pgruntime.NewSQLDB(ctx, connection, pgruntime.Config{BlobStoreURI: "blobfs://" + t.TempDir()})
			require.NoError(t, err)
			t.Cleanup(func() { _ = runtime.Close(context.Background()) })
			var pid int
			require.NoError(t, connection.QueryRowContext(ctx, `SELECT pg_backend_pid()`).Scan(&pid))
			return runtime, pid
		}
		a, aPID := open()
		b, bPID := open()
		source, err := celltest.Submit(a, ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", JobID: "origin", Cell: "test", CellRepo: "example.test/test", JobType: "work", Data: jobdb.NewTaskDataOrPanic(nil)}})
		require.NoError(t, err)
		require.NoError(t, a.CancelJob(ctx, jobdb.CancelJobRequest{JobKey: source.JobKey}))
		_, err = db.ExecContext(ctx, `CREATE FUNCTION pgjobdb.test_initial_chapter_barrier() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN
		 IF NEW.story_id='staged' AND NEW.ordinal=0 THEN PERFORM pg_advisory_xact_lock(68317491); END IF;
		 RETURN NEW;
		END $$;
		CREATE TRIGGER test_initial_chapter_barrier BEFORE INSERT ON jobdb_chapter_chapters
		FOR EACH ROW EXECUTE FUNCTION pgjobdb.test_initial_chapter_barrier()`)
		require.NoError(t, err)
		barrier, err := db.BeginTx(ctx, nil)
		require.NoError(t, err)
		defer barrier.Rollback()
		_, err = barrier.ExecContext(ctx, `SELECT pg_advisory_xact_lock(68317491)`)
		require.NoError(t, err)
		request := jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", JobID: "staged", GoalID: source.GoalID, Cell: source.Cell, CellRepo: source.CellRepo, JobType: "work", Data: jobdb.NewTaskDataOrPanic("same input")}}
		type outcome struct {
			handle jobdb.JobHandle
			err    error
		}
		submit := func(r *pgruntime.Runtime, result chan<- outcome) {
			h, e := r.SubmitJob(ctx, request)
			result <- outcome{h, e}
		}
		first, second := make(chan outcome, 1), make(chan outcome, 1)
		go submit(a, first)
		waiting := func(pid int) bool {
			var locked bool
			e := db.QueryRowContext(ctx, `SELECT COALESCE(wait_event_type='Lock',false) FROM pg_stat_activity WHERE pid=$1`, pid).Scan(&locked)
			return e == nil && locked
		}
		require.Eventually(t, func() bool { return waiting(aPID) }, 5*time.Second, time.Millisecond)
		t.Logf("first runtime pid %d blocked inside initial chapter INSERT", aPID)
		go submit(b, second)
		var early *outcome
		// The old store commits the story before its initial chapter and the
		// second submission returns ErrChapterNotFound. Atomic creation instead
		// blocks the second connection until the first publication can finish.
		require.Eventually(t, func() bool {
			select {
			case got := <-second:
				early = &got
				return true
			default:
				return waiting(bPID)
			}
		}, 5*time.Second, time.Millisecond)
		t.Logf("second runtime pid %d: early result=%v, waiting=%v", bPID, early, waiting(bPID))
		require.NoError(t, barrier.Commit())
		x := <-first
		var y outcome
		if early != nil {
			y = *early
		} else {
			y = <-second
		}
		t.Logf("after barrier release: first=%v, equivalent=%v", x.err, y.err)
		require.NoError(t, x.err, "first submission")
		require.NoError(t, y.err, "equivalent submission must await complete initial history")
		require.Equal(t, x.handle, y.handle)
		chapters, err := a.ListChapters(ctx, jobdb.ListChaptersRequest{JobKey: x.handle.JobKey})
		require.NoError(t, err)
		require.Len(t, chapters, 1)
		listing, err := a.ListJobs(ctx, jobdb.ListJobsRequest{TenantIds: []string{"tenant"}, GoalIDs: []string{source.GoalID}})
		require.NoError(t, err)
		require.Len(t, listing.Jobs, 2, "origin plus one accepted selected identity")
	})
}

package integration_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/remote"
	"github.com/colony-2/jobdb/pkg/jobdb/runtimetest"
	"github.com/colony-2/jobdb/pkg/workflow"
	"github.com/colony-2/pgjobdb/pkg/pgjobdb"
	pgruntime "github.com/colony-2/pgjobdb/pkg/pgjobdb/runtime"
	"github.com/stretchr/testify/require"
)

func TestRetainedGoalHiddenAndCancellationAdmission(t *testing.T) {
	runDatabaseTest(t, func(ctx context.Context, db *sql.DB) {
		ctx = runtimetest.PrimaryGoalContext(ctx)
		for _, state := range []string{"hidden", "cancellation-requested"} {
			t.Run(state, func(t *testing.T) {
				source := nativeGoalJob(t, ctx, db, state)
				if state == "hidden" {
					// Retained hidden facts may be inherited or hidden without an
					// execution archive. Visibility cannot release capacity.
					_, err := db.ExecContext(ctx, `UPDATE pgjobdb.job_facts SET hidden_at=clock_timestamp() WHERE job_id=$1`, state)
					require.NoError(t, err)
				} else {
					// Request cancellation while an actual lease keeps execution nonterminal.
					nativeGoalLease(t, ctx, db, state)
					_, err := db.ExecContext(ctx, `UPDATE pgjobdb.jobs SET cancel_requested=TRUE,cancel_requested_by='cancel',cancel_requested_at=clock_timestamp() WHERE job_id=$1`, state)
					require.NoError(t, err)
				}
				var nonterminal bool
				require.NoError(t, db.QueryRowContext(ctx, `SELECT NOT EXISTS(SELECT 1 FROM pgjobdb.jobs_archive WHERE job_id=$1)`, state).Scan(&nonterminal))
				require.True(t, nonterminal)
				_, request := selectedNative(t, ctx, db, state+"-recovery", source.GoalID, nil)
				_, err := pgjobdb.SubmitJob(ctx, db, request)
				require.ErrorIs(t, err, jobdb.ErrGoalConflict)
				assertNoSelectedScheduler(t, ctx, db, state+"-recovery")
			})
		}
	})
}

func TestRetainedGoalFailureCurrentAuthority(t *testing.T) {
	runDatabaseTest(t, func(ctx context.Context, db *sql.DB) {
		source := nativeGoalJob(t, ctx, db, "authority-origin")
		req := jobdb.FailGoalRequest{GoalKey: nativeGoalKey(source), Message: "exact reason"}
		denied := jobdb.WithCellAuthority(ctx, jobdb.CellAuthority{Principal: "reader", TenantID: "c2", Permissions: map[string]bool{"goals.get": true}})
		for _, replay := range []bool{false, true} {
			if replay {
				_, err := pgjobdb.FailGoal(ctx, db, req)
				require.NoError(t, err)
			}
			before := schedulerSnapshot(t, ctx, db)
			_, err := pgjobdb.FailGoal(denied, db, req)
			require.ErrorIs(t, err, jobdb.ErrCellForbidden)
			require.Equal(t, before, schedulerSnapshot(t, ctx, db))
		}
	})
}

type losingGoalTransport func(*http.Request) (*http.Response, error)

func (f losingGoalTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestRetainedGoalLostHTTPFailureResponse(t *testing.T) {
	runDatabaseTest(t, func(ctx context.Context, db *sql.DB) {
		for _, signed := range []bool{false, true} {
			t.Run(map[bool]string{false: "default", true: "signed"}[signed], func(t *testing.T) {
				cfg := pgruntime.Config{BlobStoreURI: "blobfs://" + t.TempDir()}
				backend, err := pgruntime.NewSQLDB(ctx, db, cfg)
				require.NoError(t, err)
				defer backend.Close(ctx)
				source := goalSubmit(t, backend, map[bool]string{false: "lost-default", true: "lost-signed"}[signed])
				serve := func(rt *pgruntime.Runtime) *httptest.Server {
					var handler http.Handler = remote.NewConsumerServer(rt)
					if signed {
						handler, err = remote.NewConsumerServerWithOptions(rt, remote.ServerOptions{LeaseTokenSigningKey: []byte("lost-response-signing-key-at-least-32-bytes")})
						require.NoError(t, err)
					}
					return httptest.NewServer(selectionTestHandler(handler))
				}
				server := serve(backend)
				defer server.Close()
				var dropped atomic.Bool
				lost := errors.New("injected lost committed HTTP response")
				transport := server.Client().Transport
				client, err := remote.New(server.URL, &http.Client{Transport: losingGoalTransport(func(req *http.Request) (*http.Response, error) {
					response, err := transport.RoundTrip(req)
					if err == nil && req.Method == http.MethodPost && strings.HasSuffix(req.URL.Path, "/fail") && dropped.CompareAndSwap(false, true) {
						_, err = io.Copy(io.Discard, response.Body)
						response.Body.Close()
						if err != nil {
							return nil, err
						}
						return nil, lost
					}
					return response, err
				})})
				require.NoError(t, err)
				request := jobdb.FailGoalRequest{GoalKey: goalKey(source), Message: " exact\n保存 lost "}
				_, err = client.FailGoal(ctx, request)
				require.ErrorIs(t, err, lost)
				require.True(t, dropped.Load())
				saved, err := backend.GetGoal(ctx, request.GoalKey)
				require.NoError(t, err)
				_, err = backend.DeleteJobs(ctx, jobdb.DeleteJobsRequest{TenantID: "c2", JobIDs: []string{saved.FailureJobID}})
				require.NoError(t, err)
				server.Close()
				require.NoError(t, backend.Close(ctx))
				backend, err = pgruntime.NewSQLDB(ctx, db, cfg)
				require.NoError(t, err)
				defer backend.Close(ctx)
				server = serve(backend)
				defer server.Close()
				client, err = remote.New(server.URL, server.Client())
				require.NoError(t, err)
				replay, err := client.FailGoal(ctx, request)
				require.NoError(t, err)
				require.Equal(t, saved, replay)
				jobs, err := client.ListJobs(ctx, jobdb.ListJobsRequest{TenantIds: []string{"c2"}, GoalIDs: []string{source.GoalID}, IncludeHidden: true})
				require.NoError(t, err)
				require.Len(t, jobs.Jobs, 2)
				t.Logf("%v HTTP response lost after commit, hidden record %s retained after runtime reopen", signed, replay.FailureJobID)
			})
		}
	})
}

func TestRetainedGoalDiscoveryAndRetirement(t *testing.T) {
	runDatabaseTest(t, func(ctx context.Context, db *sql.DB) {
		source := nativeGoalJob(t, ctx, db, "discovery-origin")
		var cell jobdb.Cell
		cells, err := pgjobdb.ListCells(ctx, db, jobdb.ListCellsRequest{TenantID: "c2", Name: "test"})
		cell = cells.Cells[0]
		require.NoError(t, err)
		retire := func() error {
			_, err := pgjobdb.RetireCell(ctx, db, jobdb.RetireCellRequest{CellKey: jobdb.CellKey{TenantID: "c2", CellID: cell.ID}, IfMatch: `"` + cell.Revision + `"`})
			return err
		}
		_, err = db.ExecContext(ctx, `UPDATE pgjobdb.job_facts SET hidden_at=clock_timestamp() WHERE job_id='discovery-origin'`)
		require.NoError(t, err)
		require.Error(t, retire(), "hidden nonterminal blocks retirement")
		retireNativeMember(t, ctx, db, "discovery-origin")
		require.Error(t, retire(), "open zero-active recovery gap blocks retirement")
		for i := range 200 {
			nativeGoalJob(t, ctx, db, fmt.Sprintf("discovery-%03d", i))
		}
		d := &listQueryRecorder{DB: db}
		page, err := pgjobdb.ListGoals(ctx, d, jobdb.ListGoalsRequest{TenantId: "c2", States: []jobdb.GoalState{jobdb.GoalOpen}, Candidate: &jobdb.GoalCandidateFilter{JobIDs: []string{"discovery-origin"}}, PageSize: 1})
		require.NoError(t, err)
		require.Len(t, page.Goals, 1)
		require.Equal(t, source.GoalID, page.Goals[0].GoalKey.GoalID)
		require.Zero(t, page.Goals[0].NonTerminalCandidateCount)
		require.Empty(t, page.NextPageToken)
		require.NotContains(t, d.query, "chapters")
		require.NotContains(t, d.query, "goal_candidates")
		require.Contains(t, d.query, "EXISTS")
		rows, err := db.QueryContext(ctx, "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) "+d.query, d.args...)
		require.NoError(t, err)
		require.True(t, rows.Next())
		var plan string
		require.NoError(t, rows.Scan(&plan))
		require.NoError(t, rows.Close())
		require.Contains(t, plan, "Index")
		t.Logf("retained goal primary-filter plan: %s", plan)
		// Other open goals still belong to this cell. Close every one before the
		// final retirement, preserving all hidden memberships.
		request := jobdb.ListGoalsRequest{TenantId: "c2", PageSize: 100}
		for {
			goals, err := pgjobdb.ListGoals(ctx, db, request)
			require.NoError(t, err)
			for _, g := range goals.Goals {
				_, err = pgjobdb.FailGoal(ctx, db, jobdb.FailGoalRequest{GoalKey: g.GoalKey, Message: "retirement test"})
				require.NoError(t, err)
			}
			if goals.NextPageToken == "" {
				break
			}
			request.PageToken = goals.NextPageToken
		}
		for i := range 200 {
			retireNativeMember(t, ctx, db, fmt.Sprintf("discovery-%03d", i))
		}
		p, err := pgjobdb.PrepareJob(ctx, db, pgjobdb.JobPreparation{Key: jobdb.JobKey{TenantId: "c2", JobId: "unpublished"}, Cell: "test", CellRepo: "example.test/test", Descriptor: "not published"})
		require.NoError(t, err)
		require.Error(t, retire(), "preparation blocks retirement")
		require.NoError(t, pgjobdb.DiscardJobPreparation(ctx, db, p.Key))
		require.NoError(t, retire())
	})
}

type replayHistoryGuard struct {
	jobdb.ConsumerRuntime
	calls *atomic.Int32
}

func (r replayHistoryGuard) GetChapter(context.Context, jobdb.ChapterRef) (jobdb.Chapter, error) {
	r.calls.Add(1)
	return jobdb.Chapter{}, errors.New("protected replay reached history")
}

type replayCallbackGuard struct{ calls *atomic.Int32 }

func (r replayCallbackGuard) Name() string { return jobdb.GoalFailureJobType }
func (r replayCallbackGuard) Run(workflow.ExecutionContext, jobdb.JobData) (jobdb.JobData, error) {
	r.calls.Add(1)
	return nil, errors.New("protected replay reached worker")
}

func TestRetainedGoalProtectedHistoryGuards(t *testing.T) {
	runDatabaseTest(t, func(ctx context.Context, db *sql.DB) {
		source := nativeGoalJob(t, ctx, db, "missing-ordinary-history")
		retireNativeMember(t, ctx, db, string(source.JobID))
		runtime, err := pgruntime.NewSQLDB(ctx, db, pgruntime.Config{BlobStoreURI: "blobfs://" + t.TempDir()})
		require.NoError(t, err)
		defer runtime.Close(ctx)
		_, err = runtime.GetJob(ctx, jobdb.JobKey{TenantId: "c2", JobId: string(source.JobID)})
		require.Error(t, err, "ordinary absent history remains an error")
		_, err = runtime.ListChapters(ctx, jobdb.ListChaptersRequest{JobKey: jobdb.JobKey{TenantId: "c2", JobId: string(source.JobID)}})
		require.Error(t, err)
		failed, err := runtime.FailGoal(ctx, jobdb.FailGoalRequest{GoalKey: nativeGoalKey(source), Message: "protected"})
		require.NoError(t, err)
		key := jobdb.JobKey{TenantId: "c2", JobId: failed.FailureJobID}
		calls := &atomic.Int32{}
		engine, err := workflow.NewEngineBuilder().WithRuntime(replayHistoryGuard{ConsumerRuntime: runtime, calls: calls}).BuildEngine()
		require.NoError(t, err)
		_, err = engine.ReplayJobRun(ctx, workflow.ReplayRunRequest{JobKey: key, JobWorker: replayCallbackGuard{calls: calls}})
		require.ErrorIs(t, err, jobdb.ErrJobRecordOperationUnsupported)
		require.Zero(t, calls.Load())
		before := schedulerSnapshot(t, ctx, db)
		_, err = runtime.SubmitRestartJob(ctx, jobdb.SubmitRestartJobRequest{Job: jobdb.SubmitRestartJob{PriorJobKey: key, Cell: "test", CellRepo: "example.test/test", JobID: "forbidden-restart"}})
		require.ErrorIs(t, err, jobdb.ErrJobRecordOperationUnsupported)
		require.Equal(t, before, schedulerSnapshot(t, ctx, db))
	})
}

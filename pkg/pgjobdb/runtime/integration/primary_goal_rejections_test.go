package integration_test

import (
	"context"
	"database/sql"
	"testing"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/runtimetest"
	"github.com/colony-2/pgjobdb/pkg/pgjobdb"
	"github.com/stretchr/testify/require"
)

func TestSelectedNativeEligibilityWithoutSource(t *testing.T) {
	runDatabaseTest(t, func(ctx context.Context, db *sql.DB) {
		ctx = runtimetest.PrimaryGoalContext(ctx)
		_, err := pgjobdb.CreateCell(ctx, db, jobdb.CreateCellRequest{TenantID: "c2", Name: "test", Repo: "example.test/test"})
		require.NoError(t, err)
		_, err = pgjobdb.CreateCell(ctx, db, jobdb.CreateCellRequest{TenantID: "c2", Name: "other", Repo: "example.test/other"})
		require.NoError(t, err)
		retired, err := pgjobdb.CreateCell(ctx, db, jobdb.CreateCellRequest{TenantID: "c2", Name: "retired", Repo: "example.test/retired"})
		require.NoError(t, err)
		_, err = pgjobdb.RetireCell(ctx, db, jobdb.RetireCellRequest{CellKey: jobdb.CellKey{TenantID: "c2", CellID: retired.ID}, IfMatch: `"` + retired.Revision + `"`})
		require.NoError(t, err)
		for _, fixture := range []struct{ id, cell, repo string }{{"open", "test", "example.test/test"}, {"foreign", "other", "example.test/other"}, {"drift", "test", "example.test/test"}, {"closed", "test", "example.test/test"}} {
			_, err = pgjobdb.SubmitJob(ctx, db, pgjobdb.SubmitJobRequest{TenantID: "c2", JobID: pgjobdb.JobID("origin-" + fixture.id), GoalID: fixture.id, Cell: fixture.cell, CellRepo: fixture.repo, WorkerID: "test", JobType: "work"})
			require.NoError(t, err)
			retireNativeMember(t, ctx, db, "origin-"+fixture.id)
		}
		// Registry repository drift leaves the captured origin owner unchanged.
		_, err = db.ExecContext(ctx, `ALTER TABLE pgjobdb.job_facts DISABLE TRIGGER immutable_retained_goal; UPDATE pgjobdb.job_facts SET cell_repo='example.test/old' WHERE goal_id='drift'; ALTER TABLE pgjobdb.job_facts ENABLE TRIGGER immutable_retained_goal`)
		require.NoError(t, err)
		_, err = pgjobdb.FailGoal(ctx, db, jobdb.FailGoalRequest{GoalKey: jobdb.GoalKey{TenantId: "c2", GoalID: "closed"}, Message: "closed"})
		require.NoError(t, err)
		cases := []struct {
			id, goal, cell, repo string
			want                 error
		}{
			{"missing", "missing", "test", "example.test/test", jobdb.ErrGoalNotFound},
			{"closed", "closed", "test", "example.test/test", jobdb.ErrGoalConflict},
			{"foreign", "foreign", "test", "example.test/test", jobdb.ErrCellConflict},
			{"drift", "drift", "test", "example.test/test", jobdb.ErrCellRepoMismatch},
			{"retired", "open", "retired", "example.test/retired", jobdb.ErrCellRetired},
		}
		for _, c := range cases {
			p, err := pgjobdb.PrepareJob(ctx, db, pgjobdb.JobPreparation{Key: jobdb.JobKey{TenantId: "c2", JobId: c.id}, SelectedGoalID: c.goal, Cell: c.cell, CellRepo: c.repo, Descriptor: "submit " + c.id})
			require.ErrorIs(t, err, c.want, c.id)
			require.Nil(t, p.Receipt)
			assertNoSelectedScheduler(t, ctx, db, c.id)
		}
		_, publication := selectedNative(t, ctx, db, "no-source", "open", nil)
		_, err = pgjobdb.SubmitJob(ctx, db, publication)
		require.NoError(t, err)
		var jobs, goals int
		require.NoError(t, db.QueryRowContext(ctx, `SELECT (SELECT count(*) FROM pgjobdb.job_facts),(SELECT count(*) FROM pgjobdb.retained_goal_projection)`).Scan(&jobs, &goals))
		require.Equal(t, 6, jobs)
		require.Equal(t, 4, goals)
	})
}

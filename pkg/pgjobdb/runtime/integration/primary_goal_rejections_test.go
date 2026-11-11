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
		cell, err := pgjobdb.CreateCell(ctx, db, jobdb.CreateCellRequest{TenantID: "tenant", Name: "test", Repo: "example.test/test"})
		require.NoError(t, err)
		other, err := pgjobdb.CreateCell(ctx, db, jobdb.CreateCellRequest{TenantID: "tenant", Name: "other", Repo: "example.test/other"})
		require.NoError(t, err)
		retired, err := pgjobdb.CreateCell(ctx, db, jobdb.CreateCellRequest{TenantID: "tenant", Name: "retired", Repo: "example.test/retired"})
		require.NoError(t, err)
		_, err = pgjobdb.RetireCell(ctx, db, jobdb.RetireCellRequest{CellKey: jobdb.CellKey{TenantID: "tenant", CellID: retired.ID}, IfMatch: `"` + retired.Revision + `"`})
		require.NoError(t, err)
		_, err = db.ExecContext(ctx, `INSERT INTO pgjobdb.goals(tenant_id,goal_id,cell_id,cell_repo) VALUES ('tenant','open',$1,'example.test/test'),('tenant','foreign',$2,'example.test/other'),('tenant','drift',$1,'example.test/old'),('tenant','closed',$1,'example.test/test')`, cell.ID, other.ID)
		require.NoError(t, err)
		_, err = pgjobdb.FailGoal(ctx, db, jobdb.FailGoalRequest{GoalKey: jobdb.GoalKey{TenantId: "tenant", GoalID: "closed"}, Message: "closed"})
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
			p, err := pgjobdb.PrepareJob(ctx, db, pgjobdb.JobPreparation{Key: jobdb.JobKey{TenantId: "tenant", JobId: c.id}, SelectedGoalID: c.goal, Cell: c.cell, CellRepo: c.repo, Descriptor: "submit " + c.id})
			require.ErrorIs(t, err, c.want, c.id)
			require.Nil(t, p.Receipt)
			assertNoSelectedScheduler(t, ctx, db, c.id)
		}
		_, publication := selectedNative(t, ctx, db, "no-source", "open", nil)
		_, err = pgjobdb.SubmitJob(ctx, db, publication)
		require.NoError(t, err)
		var jobs, goals, candidates int
		require.NoError(t, db.QueryRowContext(ctx, `SELECT (SELECT count(*) FROM pgjobdb.job_facts),(SELECT count(*) FROM pgjobdb.goals),(SELECT count(*) FROM pgjobdb.goal_candidates)`).Scan(&jobs, &goals, &candidates))
		require.Equal(t, 1, jobs)
		require.Equal(t, 4, goals)
		require.Zero(t, candidates)
	})
}

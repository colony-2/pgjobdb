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

func selectedNative(t *testing.T, ctx context.Context, db pgjobdb.DB, id, goal string, deps []jobdb.Dependency) (pgjobdb.JobPreparation, pgjobdb.SubmitJobRequest) {
	t.Helper()
	prep := pgjobdb.JobPreparation{Key: jobdb.JobKey{TenantId: "tenant", JobId: id}, Cell: "test", CellRepo: "example.test/test", SelectedGoalID: goal, Descriptor: "submit " + id, Dependencies: deps}
	p, err := pgjobdb.PrepareJob(ctx, db, prep)
	require.NoError(t, err)
	require.False(t, p.Published)
	return prep, pgjobdb.SubmitJobRequest{TenantID: "tenant", JobID: pgjobdb.JobID(id), Cell: "test", CellRepo: "example.test/test", Target: p.Target, GoalID: goal, SelectedGoalID: goal, WorkerID: "submit", JobType: "work", Dependencies: deps}
}

func assertNoSelectedScheduler(t *testing.T, ctx context.Context, db pgjobdb.DB, id string) {
	t.Helper()
	for _, table := range []string{"jobs", "jobs_archive", "job_facts", "goal_candidates", "dependency_waits", "dependency_targets"} {
		var n int
		require.NoError(t, db.QueryRowContext(ctx, `SELECT count(*) FROM pgjobdb.`+table+` WHERE tenant_id='tenant' AND job_id=$1`, id).Scan(&n))
		require.Zero(t, n, table)
	}
	var n int
	require.NoError(t, db.QueryRowContext(ctx, `SELECT count(*) FROM pgjobdb.job_preparations WHERE tenant_id='tenant' AND job_id=$1 AND (published OR receipt IS NOT NULL)`, id).Scan(&n))
	require.Zero(t, n)
}

func TestSelectedNativeTransactions(t *testing.T) {
	runDatabaseTest(t, func(ctx context.Context, db *sql.DB) {
		ctx = runtimetest.PrimaryGoalContext(ctx)
		source := nativeGoalJob(t, ctx, db, "source")
		dependency := nativeGoalJob(t, ctx, db, "dependency")
		deps := []jobdb.Dependency{{Target: jobdb.DependencyTarget{JobID: string(dependency.JobID)}, Condition: jobdb.JobPrereqSuccess}}
		for _, savepoint := range []bool{false, true} {
			name := "rollback"
			if savepoint {
				name = "savepoint"
			}
			t.Run(name, func(t *testing.T) {
				prep, request := selectedNative(t, ctx, db, name, source.GoalID, deps)
				tx, err := db.BeginTx(ctx, nil)
				require.NoError(t, err)
				defer tx.Rollback()
				if savepoint {
					_, err = tx.ExecContext(ctx, "SAVEPOINT caller")
					require.NoError(t, err)
				}
				_, err = pgjobdb.SubmitJob(ctx, tx, request)
				require.NoError(t, err)
				if savepoint {
					_, err = tx.ExecContext(ctx, "ROLLBACK TO SAVEPOINT caller")
					require.NoError(t, err)
					require.NoError(t, tx.Commit())
				} else {
					require.NoError(t, tx.Rollback())
				}
				assertNoSelectedScheduler(t, ctx, db, name)
				p, err := pgjobdb.PrepareJob(ctx, db, prep)
				require.NoError(t, err)
				require.False(t, p.Published)
				_, err = pgjobdb.SubmitJob(ctx, db, request)
				require.NoError(t, err)
				p, err = pgjobdb.PrepareJob(ctx, db, prep)
				require.NoError(t, err)
				require.NotNil(t, p.Receipt)
			})
		}
		// Publication errors roll back their own savepoint, leaving the caller's
		// transaction usable and removing partial facts and dependencies.
		prep, request := selectedNative(t, ctx, db, "cycle", source.GoalID, []jobdb.Dependency{{Target: jobdb.DependencyTarget{GoalID: source.GoalID}, Condition: jobdb.JobPrereqSuccess}})
		tx, err := db.BeginTx(ctx, nil)
		require.NoError(t, err)
		defer tx.Rollback()
		_, err = pgjobdb.SubmitJob(ctx, tx, request)
		require.ErrorIs(t, err, jobdb.ErrDependencyCycle)
		require.NoError(t, tx.Commit())
		assertNoSelectedScheduler(t, ctx, db, prep.Key.JobId)
	})
}

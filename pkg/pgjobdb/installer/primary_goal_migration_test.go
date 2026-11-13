package installer_test

import (
	"context"
	"database/sql"
	_ "embed"
	"errors"
	"testing"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/pgjobdb/pkg/pgjobdb"
	pginstaller "github.com/colony-2/pgjobdb/pkg/pgjobdb/installer"
	"github.com/stretchr/testify/require"
)

//go:embed testdata/format10.sql
var format10 string

func TestPriorPrimaryGoalPreparationsRejectedUnchanged(t *testing.T) {
	withBareDatabase(t, func(ctx context.Context, db *sql.DB) {
		_, err := db.ExecContext(ctx, format10)
		require.NoError(t, err)
		historicalEventJob(t, ctx, db, "source")
		requests := []pgjobdb.JobPreparation{
			{Key: jobdb.JobKey{TenantId: "tenant", JobId: "ordinary"}, Cell: "test", CellRepo: "example.test/test", Descriptor: "ordinary descriptor"},
			{Key: jobdb.JobKey{TenantId: "tenant", JobId: "restart"}, Cell: "test", CellRepo: "example.test/test", Descriptor: "restart descriptor", PriorJobID: "source", ShareGoal: true},
		}
		for _, req := range requests {
			_, err := db.ExecContext(ctx, `SELECT pgjobdb.prepare_job($1,$2,NULL,$3,$4,$5,'[]', $6,$7,c.cell_id,c.name,c.repo,'') FROM pgjobdb.cells c WHERE tenant_id=$1 AND name=$6`, req.Key.TenantId, req.Key.JobId, nullablePrior(req.PriorJobID), req.ShareGoal, req.Descriptor, req.Cell, req.CellRepo)
			require.NoError(t, err)
		}
		// Include a published reconciliation identity as well as unpublished work.
		_, err = db.ExecContext(ctx, `UPDATE pgjobdb.job_preparations SET published=TRUE WHERE job_id='ordinary'`)
		require.NoError(t, err)
		var before string
		require.NoError(t, db.QueryRowContext(ctx, `SELECT jsonb_agg(to_jsonb(p) ORDER BY job_id)::text FROM pgjobdb.job_preparations p`).Scan(&before))
		inst := pginstaller.Installer{DB: db}
		require.Error(t, inst.Verify(ctx))
		require.ErrorContains(t, inst.Apply(ctx), "unsupported schema format 10")
		var after string
		require.NoError(t, db.QueryRowContext(ctx, `SELECT jsonb_agg(to_jsonb(p) ORDER BY job_id)::text FROM pgjobdb.job_preparations p`).Scan(&after))
		require.JSONEq(t, before, after)
		var version int
		require.NoError(t, db.QueryRowContext(ctx, `SELECT format_version FROM pgjobdb.installation`).Scan(&version))
		require.Equal(t, 10, version)

	})
}

func nullablePrior(s string) any {
	if s == "" {
		return nil
	}
	return s
}

// Historical fixtures use their own SQL entrypoint, rather than current Go
// submission code which deliberately requires the current selection schema.
func historicalEventJob(t *testing.T, ctx context.Context, db *sql.DB, id string) {
	t.Helper()
	_, err := pgjobdb.CreateCell(ctx, db, jobdb.CreateCellRequest{TenantID: "tenant", Name: "test", Repo: "example.test/test"})
	if !errors.Is(err, jobdb.ErrCellConflict) {
		require.NoError(t, err)
	}
	_, err = db.ExecContext(ctx, `SELECT pgjobdb.submit_native_job('tenant',$1,'submitter','collect','{}','{}',NULL,NULL,NULL,ARRAY[]::TEXT[],NULL,NULL,NULL,NULL,NULL,FALSE,'[]',jsonb_build_object('cellId',cell_id,'cell',name,'cellRepo',repo)) FROM pgjobdb.cells WHERE tenant_id='tenant' AND name='test'`, id)
	require.NoError(t, err)
}

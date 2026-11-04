package integration_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/pgjobdb/pkg/pgjobdb"
	"github.com/stretchr/testify/require"
)

func TestCellStorageColumns(t *testing.T) {
	runDatabaseTest(t, func(ctx context.Context, db *sql.DB) {
		req := jobdb.CreateCellRequest{TenantID: "storage", Name: "root", Repo: "example.test/root", RequestID: "create", Metadata: map[string]json.RawMessage{"large": json.RawMessage(`1e999999`)}}
		c, err := pgjobdb.CreateCell(ctx, db, req)
		require.NoError(t, err)
		p, err := pgjobdb.PrepareJob(ctx, db, pgjobdb.JobPreparation{Key: jobdb.JobKey{TenantId: "storage", JobId: "job"}, Cell: "root", Descriptor: "job"})
		require.NoError(t, err)
		var id, name, repo string
		require.NoError(t, db.QueryRowContext(ctx, `SELECT cell_id,cell_name,cell_repo FROM pgjobdb.job_preparations WHERE tenant_id='storage'`).Scan(&id, &name, &repo))
		require.Equal(t, c.ID, id)
		require.Equal(t, c.Name, name)
		require.Equal(t, c.Repo, repo)
		target, err := json.Marshal(c.Resolved())
		require.NoError(t, err)
		_, err = pgjobdb.UpsertSchedule(ctx, db, pgjobdb.UpsertScheduleRequest{TenantID: "storage", ScheduleID: "schedule", State: pgjobdb.ScheduleStateActive, SpecHash: "hash", Trigger: json.RawMessage(`{}`), TargetJobType: "recipe", TargetSnapshot: target, OverlapPolicy: "ALLOW", FailurePolicy: json.RawMessage(`{}`)})
		require.NoError(t, err)
		var duplicates bool
		require.NoError(t, db.QueryRowContext(ctx, `SELECT cell_id,cell_name,cell_repo,target_snapshot ?| ARRAY['cell','cellId','cellRepo'] FROM pgjobdb.schedules WHERE tenant_id='storage'`).Scan(&id, &name, &repo, &duplicates))
		require.False(t, duplicates)
		require.Equal(t, c.ID, id)
		require.Equal(t, c.Name, name)
		require.Equal(t, c.Repo, repo)
		for table, old := range map[string][]string{"cells": {"payload", "is_root"}, "cell_catalog": {"metadata"}, "job_preparations": {"cell_target"}} {
			for _, column := range old {
				var count int
				require.NoError(t, db.QueryRowContext(ctx, `SELECT count(*) FROM information_schema.columns WHERE table_schema='pgjobdb' AND table_name=$1 AND column_name=$2`, table, column).Scan(&count))
				require.Zero(t, count)
			}
		}
		var generated string
		require.NoError(t, db.QueryRowContext(ctx, `SELECT is_generated FROM information_schema.columns WHERE table_schema='pgjobdb' AND table_name='schedules' AND column_name='cell_id'`).Scan(&generated))
		require.Equal(t, "NEVER", generated)
		renamed := "renamed"
		newRepo := "example.test/renamed"
		updated, err := pgjobdb.UpdateCell(ctx, db, jobdb.UpdateCellRequest{CellKey: jobdb.CellKey{TenantID: c.TenantID, CellID: c.ID}, IfMatch: fmt.Sprintf("%q", c.Revision), Name: &renamed, Repo: &newRepo})
		require.NoError(t, err)
		got, err := pgjobdb.GetCell(ctx, db, jobdb.CellKey{TenantID: c.TenantID, CellID: c.ID})
		require.NoError(t, err)
		require.Equal(t, updated, got)
		replay, err := pgjobdb.CreateCell(ctx, db, req)
		require.NoError(t, err)
		require.Equal(t, c, replay)
		require.NoError(t, pgjobdb.DiscardJobPreparation(ctx, db, p.Key))
	})
}

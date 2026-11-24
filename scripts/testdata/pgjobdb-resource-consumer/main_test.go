package resourceconsumer_test

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	"github.com/colony-2/jobdb/pkg/jobdb/runtimetest"
	"github.com/colony-2/pgjobdb/pkg/pgjobdb/installer"
	"github.com/colony-2/pgjobdb/pkg/pgjobdb/projects"
	pgruntime "github.com/colony-2/pgjobdb/pkg/pgjobdb/runtime"
	embeddedpostgres "github.com/fergusstrange/embedded-postgres"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var participants = strings.Repeat("1", 64)

type database struct {
	server *embeddedpostgres.EmbeddedPostgres
	port   uint32
}

func startDatabase(t *testing.T) database {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := uint32(listener.Addr().(*net.TCPAddr).Port)
	require.NoError(t, listener.Close())
	dir := t.TempDir()
	server := embeddedpostgres.NewDatabase(embeddedpostgres.DefaultConfig().Port(port).
		RuntimePath(filepath.Join(dir, "runtime")).DataPath(filepath.Join(dir, "data")))
	require.NoError(t, server.Start())
	t.Cleanup(func() { require.NoError(t, server.Stop()) })
	return database{server: server, port: port}
}

func (db database) dsn(name string) string {
	return fmt.Sprintf("postgres://postgres:postgres@127.0.0.1:%d/%s?sslmode=disable", db.port, name)
}

func openSQL(t *testing.T, dsn string) *sql.DB {
	t.Helper()
	db, err := sql.Open("postgres", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	return db
}

func projectAdmin() context.Context {
	return jobdb.WithProjectAdministration(celltest.Context(context.Background()), func(context.Context) error { return nil })
}

func TestExternalResourceBackoffRestart(t *testing.T) {
	db := startDatabase(t)
	ctx := projectAdmin()
	dsn := db.dsn("postgres")
	setup := openSQL(t, dsn)
	_, err := projects.Initialize(ctx, setup, participants, io.Discard)
	require.NoError(t, err)
	control := projects.NewControl(setup, nil)
	created, err := control.CreateProject(ctx, jobdb.CreateProjectRequest{RequestID: "external", Name: "external"})
	require.NoError(t, err)
	require.NoError(t, setup.Close())
	cfg := pgruntime.Config{BlobStoreURI: "blobfs://" + t.TempDir()}
	open := func() *pgruntime.Runtime {
		rt, err := pgruntime.OpenDSN(ctx, dsn, cfg)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, rt.Close(ctx)) })
		return rt
	}
	rt := open()
	handle, err := celltest.Submit(rt, ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{
		TenantId: created.Project.ID, Cell: "external", CellRepo: "example.test/external",
		JobID: "restart", JobType: "work", Data: jobdb.NewTaskDataOrPanic("initial"),
	}})
	require.NoError(t, err)
	claim := func() (jobdb.ExecutionLease, error) {
		return rt.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: handle.JobKey, WorkerID: "worker",
			Routes: []jobdb.Route{{JobType: "work"}}, LeaseDuration: time.Minute})
	}
	lease, err := claim()
	require.NoError(t, err)
	require.NotNil(t, lease)
	require.Equal(t, 1, lease.ExecutionState().ResourceBackoff.DurabilityVersion)
	policy := jobdb.DefaultResourceBackoffPolicy()
	policy.Initial, policy.Jitter = 250*time.Millisecond, 0
	allowance := int64(100)
	policy.CrashAllowance = &allowance
	request, err := runtimetest.PrepareResourceBackoffCheckpoint(ctx, rt, lease, "work/attempt:1", 1, policy)
	require.NoError(t, err)
	backoff, ok := lease.(jobdb.ResourceBackoffLease)
	require.True(t, ok)
	receipt, err := backoff.RescheduleResourceBackoff(ctx, request)
	require.NoError(t, err)
	require.EqualValues(t, 1, receipt.Count)
	require.Equal(t, allowance, receipt.ResolvedCrashAllowance)
	before, err := rt.GetJob(ctx, handle.JobKey)
	require.NoError(t, err)
	require.NoError(t, rt.Close(ctx))
	require.NoError(t, db.server.Stop())
	require.NoError(t, db.server.Start())
	rt = open()
	after, err := rt.GetJob(ctx, handle.JobKey)
	require.NoError(t, err)
	require.Equal(t, before.ExecutionState, after.ExecutionState)
	require.Equal(t, before.GoalID, after.GoalID)
	require.Equal(t, before.NextRoute, after.NextRoute)
	replay, err := rt.RescheduleResourceBackoffByID(ctx, handle.JobKey, lease.LeaseID(), "worker", request)
	require.NoError(t, err)
	require.Equal(t, receipt, replay)
	var resumed jobdb.ExecutionLease
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		var claimErr error
		resumed, claimErr = claim()
		if assert.NoError(collect, claimErr) {
			assert.NotNil(collect, resumed)
		}
	}, 5*time.Second, 20*time.Millisecond)
	require.NotNil(t, resumed.ExecutionState().CrashBudgetRemaining)
	require.Equal(t, allowance, *resumed.ExecutionState().CrashBudgetRemaining)
	require.NotNil(t, resumed.ExecutionState().ResourceResume)
	require.Equal(t, receipt.Resume, *resumed.ExecutionState().ResourceResume)
}

func TestExternalHistoricalConversion(t *testing.T) {
	fixtureDir := os.Getenv("PGJOBDB_HISTORY_FIXTURES")
	require.NotEmpty(t, fixtureDir)
	postgres := startDatabase(t)
	ctx := projectAdmin()
	admin := openSQL(t, postgres.dsn("postgres"))
	for format := 9; format <= 13; format++ {
		t.Run(fmt.Sprint(format), func(t *testing.T) {
			name := fmt.Sprintf("history%d", format)
			_, err := admin.ExecContext(ctx, "CREATE DATABASE "+name)
			require.NoError(t, err)
			dsn := postgres.dsn(name)
			db := openSQL(t, dsn)
			fixture, err := os.ReadFile(filepath.Join(fixtureDir, fmt.Sprintf("format%d.sql", format)))
			require.NoError(t, err)
			_, err = db.ExecContext(ctx, string(fixture))
			require.NoError(t, err)
			_, err = db.ExecContext(ctx, `SELECT pgjobdb.set_crash_concern_threshold(9)`)
			require.NoError(t, err)
			install := installer.Installer{DB: db}
			require.ErrorContains(t, install.Apply(ctx), "explicit offline migration")
			var staged bytes.Buffer
			confirmation, err := install.MigrateWithReport(ctx, &staged)
			require.NoError(t, err)
			require.True(t, confirmation.Committed)
			require.Equal(t, format, confirmation.SourceFormat)
			require.Equal(t, 14, confirmation.TargetFormat)
			var report installer.ConversionReport
			require.NoError(t, json.Unmarshal(staged.Bytes(), &report))
			path := []int{format, 14}
			if format < 13 {
				path = []int{format, 13, 14}
			}
			require.Equal(t, path, report.PhasePath)
			require.EqualValues(t, 9, report.OriginalCrashDefault)
			require.NoError(t, install.Verify(ctx))
			require.NoError(t, projects.PrepareShared(ctx, db))
			inventory, err := projects.Inspect(ctx, db, participants)
			if format <= 11 {
				require.ErrorContains(t, err, "unknown participant table: pgjobdb.schedules")
				return
			}
			require.NoError(t, err)
			_, err = projects.Install(ctx, db, inventory, nil, io.Discard)
			require.NoError(t, err)
			require.NoError(t, db.Close())
			rt, err := pgruntime.OpenDSN(ctx, dsn, pgruntime.Config{BlobStoreURI: "blobfs://" + t.TempDir()})
			require.NoError(t, err)
			require.NoError(t, rt.Close(ctx))
		})
	}
}

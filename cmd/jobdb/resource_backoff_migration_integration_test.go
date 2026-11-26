//go:build integration

package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"testing"

	"github.com/colony-2/pgjobdb/pkg/pgjobdb/installer"
	embeddedpostgres "github.com/fergusstrange/embedded-postgres"
	"github.com/stretchr/testify/require"
)

func TestPostgresResourceBackoffMigrationCommands(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := uint32(listener.Addr().(*net.TCPAddr).Port)
	require.NoError(t, listener.Close())
	root := t.TempDir()
	pg := embeddedpostgres.NewDatabase(embeddedpostgres.DefaultConfig().Port(port).RuntimePath(filepath.Join(root, "runtime")).DataPath(filepath.Join(root, "data")))
	require.NoError(t, pg.Start())
	t.Cleanup(func() { require.NoError(t, pg.Stop()) })
	dsn := fmt.Sprintf("postgres://postgres:postgres@localhost:%d/postgres?sslmode=disable", port)
	db, err := sql.Open("postgres", dsn)
	require.NoError(t, err)
	defer db.Close()
	t.Run("inspect-empty", func(t *testing.T) {
		output, err := executeRoot(t, "direct", "check-migrate", "--postgres-dsn", dsn)
		require.NoError(t, err)
		var report installer.ConversionReport
		require.NoError(t, json.Unmarshal([]byte(output), &report))
		require.Equal(t, 0, report.SourceFormat)
		require.Equal(t, 15, report.TargetFormat)
		require.True(t, report.Advisory)
		var exists bool
		require.NoError(t, db.QueryRow(`SELECT to_regnamespace('pgjobdb') IS NOT NULL`).Scan(&exists))
		require.False(t, exists)
	})
	reportPath := filepath.Join(root, "staged.json")
	t.Run("migrate-and-confirm", func(t *testing.T) {
		output, err := executeRoot(t, "direct", "migrate", "--postgres-dsn", dsn, "--report-file", reportPath)
		require.NoError(t, err)
		require.NotContains(t, output, "postgres:postgres")
		var result struct {
			Confirmation installer.CommitConfirmation `json:"confirmation"`
		}
		require.NoError(t, json.Unmarshal([]byte(output), &result))
		require.True(t, result.Confirmation.Committed)
		require.NotEmpty(t, result.Confirmation.ReportSHA256)
		require.NoError(t, (installer.Installer{DB: db}).Verify(context.Background()))
		var exists bool
		require.NoError(t, db.QueryRow(`SELECT to_regclass('jobdb_chapter_stories') IS NOT NULL OR to_regclass('jobdb_project_catalog') IS NOT NULL`).Scan(&exists))
		require.False(t, exists, "migration must not start runtime participants")
	})
	t.Run("preserve-report", func(t *testing.T) {
		before, err := os.ReadFile(reportPath)
		require.NoError(t, err)
		_, err = executeRoot(t, "direct", "migrate", "--postgres-dsn", dsn, "--report-file", reportPath)
		require.ErrorContains(t, err, "never overwritten")
		after, err := os.ReadFile(reportPath)
		require.NoError(t, err)
		require.Equal(t, before, after)
	})
	t.Run("unwritable-report", func(t *testing.T) {
		_, err := executeRoot(t, "direct", "migrate", "--postgres-dsn", dsn, "--report-file", filepath.Join(root, "missing", "report.json"))
		require.Error(t, err)
		require.NoError(t, (installer.Installer{DB: db}).Verify(context.Background()))
	})
}

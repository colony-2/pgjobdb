package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestResourceBackoffMigrationFlags(t *testing.T) {
	t.Setenv(postgresDSNEnvVar, "postgres://user:secret@invalid/ignored")
	for _, name := range []string{"check-migrate", "migrate"} {
		_, err := executeRoot(t, "direct", name)
		require.ErrorContains(t, err, "--postgres-dsn is required explicitly")
	}
	_, err := executeRoot(t, "direct", "migrate", "--postgres-dsn", "postgres://invalid/ignored")
	require.ErrorContains(t, err, "--report-file is required")
	for _, name := range []string{"check-migrate", "migrate"} {
		output, err := executeRoot(t, "direct", name, "--help")
		require.NoError(t, err)
		require.Contains(t, output, "--postgres-dsn")
	}
}

func TestResourceBackoffSyncedReport(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cutover.json")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	require.NoError(t, err)
	writer := syncedReport{f}
	n, err := writer.Write([]byte("staged\n"))
	require.NoError(t, err)
	require.Equal(t, 7, n)
	require.NoError(t, f.Close())
	_, err = writer.Write([]byte("cannot sync closed file"))
	require.Error(t, err)
	_, err = os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	require.Error(t, err)
	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, "staged\n", string(raw))
}

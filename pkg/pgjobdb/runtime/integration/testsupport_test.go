package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/colony-2/pgjobdb/internal/projectfixture"
	"math/rand"
	"path/filepath"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb/celltest"

	"github.com/colony-2/jobdb/pkg/jobdb/configuration"
	embeddedpostgres "github.com/fergusstrange/embedded-postgres"
	_ "github.com/lib/pq"
)

func runDatabaseTest(t *testing.T, fn func(context.Context, *sql.DB)) {
	t.Helper()
	withPreparedDatabase(t, fn)
}

func withPreparedDatabase(t *testing.T, fn func(context.Context, *sql.DB)) {
	t.Helper()
	port := uint32(6000 + rand.Intn(1000))
	tempDir := t.TempDir()
	runtimeDir := filepath.Join(tempDir, "runtime")
	dataDir := filepath.Join(runtimeDir, "data")
	cfg := embeddedpostgres.DefaultConfig().
		Port(port).
		RuntimePath(runtimeDir).
		DataPath(dataDir)
	pg := embeddedpostgres.NewDatabase(cfg)
	if err := pg.Start(); err != nil {
		t.Fatalf("start embedded postgres: %v", err)
	}
	t.Cleanup(func() { _ = pg.Stop() })

	dsn := fmt.Sprintf("host=localhost port=%d user=postgres password=postgres dbname=postgres sslmode=disable", port)
	sqlDB, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() { _ = sqlDB.Close() })
	ctx, cancel := context.WithTimeout(celltest.Context(context.Background()), time.Minute)
	t.Cleanup(cancel)
	if err := projectfixture.Prepare(ctx, sqlDB); err != nil {
		t.Fatal(err)
	}
	fn(ctx, sqlDB)
}

func newConfigurationStore(t *testing.T, ctx context.Context, db *sql.DB) configuration.Store {
	t.Helper()
	store, err := configuration.NewSQLStore(ctx, db, configuration.Postgres, projectfixture.Control(db))
	if err != nil {
		t.Fatal(err)
	}
	return store
}

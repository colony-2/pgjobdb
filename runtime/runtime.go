// Package runtime composes pgjobdb's scheduler with JobDB's public runtime
// core. Applications import this package when they choose the Postgres backend.
package runtime

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"sync"

	"github.com/colony-2/jobdb/pkg/jobdb"
	chapterpostgres "github.com/colony-2/jobdb/pkg/jobdb/chapterstore/postgres"
	runtimecore "github.com/colony-2/jobdb/pkg/jobdb/runtime/core"
	schemapostgres "github.com/colony-2/jobdb/pkg/jobdb/schemastore/postgres"
	"github.com/colony-2/pgjobdb/installer"
	"github.com/colony-2/pgjobdb/internal/runtimeadapter"
	_ "github.com/lib/pq"
)

// Config selects Postgres and artifact storage for the runtime.
type Config struct {
	BlobStoreURI           string
	MaxInlineArtifactBytes int64
	Logger                 *slog.Logger
}

// Runtime is JobDB's workflow facade backed by pgjobdb's typed scheduler.
// The caller owns the database passed to New; OpenDSN owns its connection.
type Runtime struct {
	*runtimecore.Runtime
	*runtimecore.SchemaRegistry
	chapters *chapterpostgres.Store
	db       *sql.DB
	ownsDB   bool
	close    sync.Once
	closeErr error
}

var _ jobdb.WorkflowRuntime = (*Runtime)(nil)
var _ jobdb.JobSchemaRegistry = (*Runtime)(nil)

// New initializes a clean Postgres database or verifies an existing pgjobdb
// installation, then composes the public JobDB runtime core.
func New(ctx context.Context, db *sql.DB, cfg Config) (*Runtime, error) {
	if ctx == nil {
		return nil, fmt.Errorf("pgjobdb runtime: context is nil")
	}
	if db == nil {
		return nil, fmt.Errorf("pgjobdb runtime: database is required")
	}
	if cfg.BlobStoreURI == "" {
		return nil, fmt.Errorf("pgjobdb runtime: blob store URI is required")
	}
	install := installer.Installer{DB: db}
	if err := install.Apply(ctx); err != nil {
		return nil, fmt.Errorf("pgjobdb runtime: install scheduler: %w", err)
	}
	if err := install.Verify(ctx); err != nil {
		return nil, fmt.Errorf("pgjobdb runtime: verify scheduler: %w", err)
	}
	chapters, err := chapterpostgres.NewSQLDB(ctx, db, chapterpostgres.Config{
		BlobStoreURI:           cfg.BlobStoreURI,
		MaxInlineArtifactBytes: cfg.MaxInlineArtifactBytes,
		Logger:                 cfg.Logger,
	})
	if err != nil {
		return nil, err
	}
	schemas, err := schemapostgres.NewSQLDB(ctx, db)
	if err != nil {
		_ = chapters.Close(ctx)
		return nil, err
	}
	core, err := runtimecore.NewRuntime(runtimecore.Config{
		Scheduler: runtimeadapter.Scheduler{DB: db}, Chapters: chapters, Schemas: schemas,
	})
	if err != nil {
		_ = chapters.Close(ctx)
		return nil, err
	}
	registry, err := runtimecore.NewSchemaRegistry(runtimecore.SchemaRegistryConfig{Store: schemas})
	if err != nil {
		_ = chapters.Close(ctx)
		return nil, err
	}
	return &Runtime{Runtime: core, SchemaRegistry: registry, chapters: chapters, db: db}, nil
}

// OpenDSN opens and owns a Postgres connection.
func OpenDSN(ctx context.Context, dsn string, cfg Config) (*Runtime, error) {
	if ctx == nil {
		return nil, fmt.Errorf("pgjobdb runtime: context is nil")
	}
	if dsn == "" {
		return nil, fmt.Errorf("pgjobdb runtime: Postgres DSN is required")
	}
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, err
	}
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	runtime, err := New(ctx, db, cfg)
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	runtime.ownsDB = true
	return runtime, nil
}

// Close releases artifact storage and a connection opened by OpenDSN.
func (r *Runtime) Close(ctx context.Context) error {
	if r == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	r.close.Do(func() {
		var errs []error
		if r.chapters != nil {
			errs = append(errs, r.chapters.Close(ctx))
		}
		if r.ownsDB && r.db != nil {
			errs = append(errs, r.db.Close())
		}
		r.closeErr = errors.Join(errs...)
	})
	return r.closeErr
}

package runtime

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/colony-2/pgjobdb/pkg/pgjobdb/projects"
	"io"
	"os"
	"path/filepath"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/pgjobdb/pkg/pgjobdb/runtime/internal/testsupport"
)

type EmbeddedRuntime struct {
	Runtime *Runtime
	stopPG  func()
	blobDir string
}

func (e *EmbeddedRuntime) Shutdown() {
	if e == nil {
		return
	}
	if e.Runtime != nil {
		_ = e.Runtime.Close(context.Background())
	}
	e.stopPG()
	if e.blobDir != "" {
		_ = os.RemoveAll(e.blobDir)
	}
}

func StartEmbeddedRuntime(ctx context.Context, preparation ...func(context.Context, *sql.DB) error) (*EmbeddedRuntime, error) {
	if ctx != nil && ctx.Err() != nil {
		return nil, ctx.Err()
	}
	dsn, stopPG, err := testsupport.StartEmbeddedPostgres()
	if err != nil {
		return nil, err
	}
	cleanup := func() {
		stopPG()
	}
	blobDir, err := os.MkdirTemp("", "jobdb-direct-blobs-*")
	if err != nil {
		cleanup()
		return nil, err
	}

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		cleanup()
		return nil, err
	}
	if len(preparation) > 1 {
		db.Close()
		cleanup()
		return nil, fmt.Errorf("one explicit fixture preparation is supported")
	}
	if len(preparation) == 1 {
		err = preparation[0](ctx, db)
	} else {
		operator := jobdb.WithProjectAdministration(ctx, func(context.Context) error { return nil })
		_, err = projects.Initialize(operator, db, "1111111111111111111111111111111111111111111111111111111111111111", io.Discard)
	}
	db.Close()
	if err != nil {
		os.RemoveAll(blobDir)
		cleanup()
		return nil, err
	}
	rt, err := NewFromConfig(Config{
		PostgresDSN:  dsn,
		BlobStoreURI: fmt.Sprintf("blobfs://%s", filepath.ToSlash(blobDir)),
	})
	if err != nil {
		_ = os.RemoveAll(blobDir)
		cleanup()
		return nil, err
	}

	return &EmbeddedRuntime{
		Runtime: rt,
		stopPG:  cleanup,
		blobDir: blobDir,
	}, nil
}

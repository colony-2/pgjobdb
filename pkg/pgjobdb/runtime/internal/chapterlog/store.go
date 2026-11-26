// Package chapterlog makes initial history publication atomic while delegating
// chapter encoding, artifact storage, and all other operations to JobDB.
package chapterlog

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	chapterpostgres "github.com/colony-2/jobdb/pkg/jobdb/chapterstore/postgres"
	runtimecore "github.com/colony-2/jobdb/pkg/jobdb/runtime/core"
)

type Store struct {
	*chapterpostgres.Store
	bridge *sql.DB
	route  *connector
}

func New(ctx context.Context, db *sql.DB, cfg chapterpostgres.Config) (*Store, error) {
	route := &connector{db: db}
	bridge := sql.OpenDB(route)
	bridge.SetMaxIdleConns(0)
	store, err := chapterpostgres.NewSQLDB(ctx, bridge, cfg)
	if err != nil {
		_ = bridge.Close()
		return nil, err
	}
	return &Store{Store: store, bridge: bridge, route: route}, nil
}

func (store *Store) Close(ctx context.Context) error {
	return errors.Join(store.Store.Close(ctx), store.bridge.Close())
}

// Create commits the story, chapter zero and its artifact rows together. The
// transaction starts at the first SQL call, after JobDB's project admission,
// which has to commit independently (including with a single-connection pool).
func (store *Store) Create(ctx context.Context, key runtimecore.ChapterLogKey, initial runtimecore.EncodedChapter) error {
	operation := &creation{route: store.route}
	defer func() {
		if operation.tx != nil {
			_ = operation.tx.Rollback()
		}
	}()
	if err := store.Store.Create(context.WithValue(ctx, creationKey{}, operation), key, initial); err != nil {
		return err
	}
	if operation.tx == nil || operation.rolledBack.Load() {
		return fmt.Errorf("postgres chapter log: initial history transaction did not complete")
	}
	if err := operation.tx.Commit(); err != nil {
		return fmt.Errorf("postgres chapter log: commit initial history: %w", err)
	}
	return nil
}

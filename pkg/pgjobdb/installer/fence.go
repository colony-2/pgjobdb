package installer

import (
	"context"
	"database/sql"
	"fmt"
	"slices"

	"github.com/lib/pq"
)

type participant struct {
	schema, name, kind string
	oid                int64
}

func participants(ctx context.Context, tx *sql.Tx) ([]participant, error) {
	rows, err := tx.QueryContext(ctx, `SELECT n.nspname,c.relname,c.relkind,c.oid
 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
 WHERE (n.nspname='pgjobdb' AND c.relkind IN ('r','p','f')) OR
 (n.nspname='public' AND c.relname IN ('jobdb_project_catalog','jobdb_configuration','jobdb_schemas','jobdb_chapter_stories','jobdb_chapter_chapters','jobdb_chapter_artifacts'))
 ORDER BY n.nspname COLLATE "C", c.relname COLLATE "C"`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []participant
	for rows.Next() {
		var table participant
		if err := rows.Scan(&table.schema, &table.name, &table.kind, &table.oid); err != nil {
			return nil, err
		}
		result = append(result, table)
	}
	return result, rows.Err()
}

func fenceInstallation(ctx context.Context, tx *sql.Tx) error {
	before, err := participants(ctx, tx)
	if err != nil {
		return err
	}
	for _, table := range before {
		name := pq.QuoteIdentifier(table.schema) + "." + pq.QuoteIdentifier(table.name)
		if table.kind != "r" {
			return fmt.Errorf("pgjobdb: unsupported installation participant %s", name)
		}
		if _, err := tx.ExecContext(ctx, "LOCK TABLE ONLY "+name+" IN ACCESS EXCLUSIVE MODE NOWAIT"); err != nil {
			if table.schema == "public" {
				return fmt.Errorf("pgjobdb: exclude project participant writers on %s: %w", name, err)
			}
			return fmt.Errorf("pgjobdb: exclude conversion writers on %s: %w", name, err)
		}
	}
	after, err := participants(ctx, tx)
	if err != nil {
		return err
	}
	if !slices.Equal(before, after) {
		return fmt.Errorf("pgjobdb: installation participants changed before writer exclusion")
	}
	return nil
}

func executeProgram(ctx context.Context, tx *sql.Tx, program string) error {
	var searchPath string
	if err := tx.QueryRowContext(ctx, `SHOW search_path`).Scan(&searchPath); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, program); err != nil {
		return err
	}
	_, err := tx.ExecContext(ctx, `SELECT set_config('search_path',$1,true)`, searchPath)
	return err
}

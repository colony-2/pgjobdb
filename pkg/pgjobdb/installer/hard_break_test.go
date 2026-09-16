package installer_test

import (
	"context"
	"database/sql"
	"strings"
	"testing"

	pgjobdbinstaller "github.com/colony-2/pgjobdb/pkg/pgjobdb/installer"
)

func TestInstallerRejectsLegacyState(t *testing.T) {
	t.Run("pgwf schema", func(t *testing.T) {
		withBareDatabase(t, func(ctx context.Context, db *sql.DB) {
			if _, err := db.ExecContext(ctx, `CREATE SCHEMA pgwf`); err != nil {
				t.Fatal(err)
			}
			err := (pgjobdbinstaller.Installer{DB: db}).Apply(ctx)
			if err == nil || !strings.Contains(err.Error(), "legacy pgwf schema") {
				t.Fatalf("expected legacy schema rejection, got %v", err)
			}
		})
	})
	t.Run("existing chapter rows", func(t *testing.T) {
		withBareDatabase(t, func(ctx context.Context, db *sql.DB) {
			if _, err := db.ExecContext(ctx, `CREATE TABLE jobdb_chapter_stories (id INTEGER)`); err != nil {
				t.Fatal(err)
			}
			if _, err := db.ExecContext(ctx, `INSERT INTO jobdb_chapter_stories VALUES (1)`); err != nil {
				t.Fatal(err)
			}
			err := (pgjobdbinstaller.Installer{DB: db}).Apply(ctx)
			if err == nil || !strings.Contains(err.Error(), "existing table public.jobdb_chapter_stories") {
				t.Fatalf("expected chapter state rejection, got %v", err)
			}
		})
	})
	t.Run("unmarked schema", func(t *testing.T) {
		withBareDatabase(t, func(ctx context.Context, db *sql.DB) {
			if _, err := db.ExecContext(ctx, `CREATE SCHEMA pgjobdb`); err != nil {
				t.Fatal(err)
			}
			err := (pgjobdbinstaller.Installer{DB: db}).Apply(ctx)
			if err == nil || !strings.Contains(err.Error(), "no installation marker") {
				t.Fatalf("expected unmarked schema rejection, got %v", err)
			}
		})
	})
	t.Run("empty chapter table", func(t *testing.T) {
		withBareDatabase(t, func(ctx context.Context, db *sql.DB) {
			if _, err := db.ExecContext(ctx, `CREATE TABLE jobdb_chapter_stories (id INTEGER)`); err != nil {
				t.Fatal(err)
			}
			err := (pgjobdbinstaller.Installer{DB: db}).Apply(ctx)
			if err == nil || !strings.Contains(err.Error(), "existing table public.jobdb_chapter_stories") {
				t.Fatalf("expected empty chapter table rejection, got %v", err)
			}
		})
	})
	t.Run("unrelated table", func(t *testing.T) {
		withBareDatabase(t, func(ctx context.Context, db *sql.DB) {
			if _, err := db.ExecContext(ctx, `CREATE TABLE unrelated (id INTEGER)`); err != nil {
				t.Fatal(err)
			}
			err := (pgjobdbinstaller.Installer{DB: db}).Apply(ctx)
			if err == nil || !strings.Contains(err.Error(), "existing table public.unrelated") {
				t.Fatalf("expected nonempty database rejection, got %v", err)
			}
		})
	})
}

func TestInstallerRejectsInvalidSchemaName(t *testing.T) {
	inst := pgjobdbinstaller.Installer{DB: new(sql.DB), Schema: `bad;DROP TABLE jobs`}
	if err := inst.Apply(context.Background()); err == nil ||
		!strings.Contains(err.Error(), "invalid schema name") {
		t.Fatalf("expected invalid schema name, got %v", err)
	}
}

func TestInstallerRejectsAlternateSchema(t *testing.T) {
	inst := pgjobdbinstaller.Installer{DB: new(sql.DB), Schema: "alternate"}
	if err := inst.Apply(context.Background()); err == nil ||
		!strings.Contains(err.Error(), "unsupported schema") {
		t.Fatalf("expected unsupported schema error, got %v", err)
	}
}

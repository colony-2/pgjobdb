package installer_test

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"

	"github.com/colony-2/pgjobdb/pkg/pgjobdb/installer"
	"github.com/stretchr/testify/require"
)

func TestStatusViewDriftRejected(t *testing.T) {
	for _, format := range []int{9, 13, 14, 15, 16} {
		t.Run(fmt.Sprint(format), func(t *testing.T) {
			withBareDatabaseDSN(t, func(ctx context.Context, db *sql.DB, dsn string) {
				install := installer.Installer{DB: db}
				switch format {
				case 9, 13:
					program := format9
					if format == 13 {
						program = format13
					}
					_, err := db.ExecContext(ctx, program)
					require.NoError(t, err)
				case 14, 15:
					archiveReleaseFixture(t, ctx, dsn, "plain", format)
				case 16:
					require.NoError(t, install.Apply(ctx))
					require.NoError(t, install.Verify(ctx))
				}
				_, err := install.Preflight(ctx)
				require.NoError(t, err)

				const viewSQL = `SELECT pg_get_viewdef('pgjobdb.jobs_with_status'::regclass)`
				const createView = `CREATE OR REPLACE VIEW pgjobdb.jobs_with_status AS `
				var original string
				require.NoError(t, db.QueryRowContext(ctx, viewSQL).Scan(&original))
				changed := strings.TrimSuffix(strings.TrimSpace(original), ";") + " WHERE FALSE"
				if format == 16 {
					require.Contains(t, original, "j.last_lease_updated_at")
					changed = strings.Replace(original, "j.last_lease_updated_at", "NULL::timestamptz AS last_lease_updated_at", 1)
				}
				_, err = db.ExecContext(ctx, createView+changed)
				require.NoError(t, err)
				var damaged, after string
				require.NoError(t, db.QueryRowContext(ctx, viewSQL).Scan(&damaged))
				require.NotEqual(t, original, damaged)
				before := catalogSnapshot(t, ctx, db)

				const diagnostic = "altered status view pgjobdb.jobs_with_status"
				_, err = install.Preflight(ctx)
				require.ErrorContains(t, err, diagnostic)
				var confirmation installer.CommitConfirmation
				if format == 16 {
					require.ErrorContains(t, install.Verify(ctx), diagnostic)
					confirmation, err = install.ApplyWithReport(ctx, nil)
				} else {
					confirmation, err = install.MigrateWithReport(ctx, nil)
				}
				require.ErrorContains(t, err, diagnostic)
				require.False(t, confirmation.Committed)
				require.Equal(t, before, catalogSnapshot(t, ctx, db))
				require.NoError(t, db.QueryRowContext(ctx, viewSQL).Scan(&after))
				require.Equal(t, damaged, after)

				_, err = db.ExecContext(ctx, createView+original)
				require.NoError(t, err)
				_, err = install.Preflight(ctx)
				require.NoError(t, err)
			})
		})
	}
}

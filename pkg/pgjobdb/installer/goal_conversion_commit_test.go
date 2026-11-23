package installer_test

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/colony-2/pgjobdb/pkg/pgjobdb/installer"
	pgruntime "github.com/colony-2/pgjobdb/pkg/pgjobdb/runtime"
	"github.com/lib/pq"
	"github.com/stretchr/testify/require"
)

// The real PostgreSQL COMMIT happens; only its acknowledgement is lost. Neither
// the installer nor this driver retries the uncertain transaction.
type lostCommitConnector struct {
	driver.Connector
	lose atomic.Bool
}

func (c *lostCommitConnector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := c.Connector.Connect(ctx)
	if err != nil {
		return nil, err
	}
	return &lostCommitConn{Conn: conn, owner: c}, nil
}

type lostCommitConn struct {
	driver.Conn
	owner *lostCommitConnector
}

func (c *lostCommitConn) BeginTx(ctx context.Context, o driver.TxOptions) (driver.Tx, error) {
	tx, err := c.Conn.(driver.ConnBeginTx).BeginTx(ctx, o)
	if err != nil {
		return nil, err
	}
	return lostCommitTx{Tx: tx, owner: c.owner}, nil
}
func (c *lostCommitConn) ExecContext(ctx context.Context, q string, a []driver.NamedValue) (driver.Result, error) {
	return c.Conn.(driver.ExecerContext).ExecContext(ctx, q, a)
}
func (c *lostCommitConn) QueryContext(ctx context.Context, q string, a []driver.NamedValue) (driver.Rows, error) {
	return c.Conn.(driver.QueryerContext).QueryContext(ctx, q, a)
}

type lostCommitTx struct {
	driver.Tx
	owner *lostCommitConnector
}

func (t lostCommitTx) Commit() error {
	if err := t.Tx.Commit(); err != nil {
		return err
	}
	if t.owner.lose.Swap(false) {
		return errors.New("injected lost COMMIT acknowledgement")
	}
	return nil
}

func TestGoalConversionLostCommitAcknowledgement(t *testing.T) {
	withBareDatabase(t, func(ctx context.Context, db *sql.DB) {
		_, err := db.ExecContext(ctx, format12BeforeAvailabilityRepair)
		require.NoError(t, err)
		historicalBaseline(t, ctx, db, 12)
		var port string
		require.NoError(t, db.QueryRowContext(ctx, `SHOW port`).Scan(&port))
		base, err := pq.NewConnector(fmt.Sprintf("host=localhost port=%s user=postgres password=postgres dbname=postgres sslmode=disable", port))
		require.NoError(t, err)
		connector := &lostCommitConnector{Connector: base}
		connector.lose.Store(true)
		uncertain := sql.OpenDB(connector)
		defer uncertain.Close()
		var staged bytes.Buffer
		confirmation, err := (installer.Installer{DB: uncertain}).ApplyWithReport(ctx, &staged)
		require.ErrorContains(t, err, "commit not confirmed")
		require.False(t, confirmation.Committed)
		require.NotEmpty(t, confirmation.ReportSHA256)
		var report installer.ConversionReport
		require.NoError(t, json.Unmarshal(staged.Bytes(), &report))
		require.Len(t, report.MigrationFailures, 1)
		// Reconcile the staged identity on an independently opened connection before
		// permitting a reapply. A successful process/old preflight cannot do this.
		fresh := sql.OpenDB(base)
		defer fresh.Close()
		install := installer.Installer{DB: fresh}
		require.NoError(t, install.Verify(ctx))
		var proposed struct {
			JobID string `json:"jobId"`
		}
		require.NoError(t, json.Unmarshal(report.MigrationFailures[0], &proposed))
		var id string
		require.NoError(t, fresh.QueryRowContext(ctx, `SELECT job_id FROM pgjobdb.job_facts WHERE record_kind='MIGRATED_GOAL_FAILURE'`).Scan(&id))
		require.Equal(t, proposed.JobID, id)
		require.NoError(t, install.Apply(ctx))
		var count int
		require.NoError(t, fresh.QueryRowContext(ctx, `SELECT count(*) FROM pgjobdb.job_facts WHERE record_kind='MIGRATED_GOAL_FAILURE' AND job_id=$1`, id).Scan(&count))
		require.Equal(t, 1, count)
		t.Logf("lost acknowledgement remained unconfirmed; fresh Verify and retained migration identity %s reconciled the committed staged report", id)
	})
}

func TestGoalConversionStartupReport(t *testing.T) {
	for _, exportFailure := range []bool{true, false} {
		t.Run(fmt.Sprint(exportFailure), func(t *testing.T) {
			withBareDatabase(t, func(ctx context.Context, db *sql.DB) {
				_, err := db.ExecContext(ctx, format11)
				require.NoError(t, err)
				historicalBaseline(t, ctx, db, 11)
				before, catalog := stableHistoricalRows(t, ctx, db), catalogSnapshot(t, ctx, db)
				var report bytes.Buffer
				cfg := pgruntime.Config{BlobStoreURI: "blobfs://" + t.TempDir(), ConversionReportWriter: &report}
				if exportFailure {
					cfg.ConversionReportWriter = failingReportWriter{}
				}
				runtime, err := pgruntime.NewSQLDB(ctx, db, cfg)
				require.Error(t, err)
				require.Nil(t, runtime)
				_, err = (installer.Installer{DB: db}).ApplyWithReport(ctx, cfg.ConversionReportWriter)
				if exportFailure {
					require.ErrorContains(t, err, "export staged conversion report")
					require.Nil(t, runtime)
					require.JSONEq(t, before, stableHistoricalRows(t, ctx, db))
					require.JSONEq(t, catalog, catalogSnapshot(t, ctx, db))
					return
				}
				require.NoError(t, err)
				prepareHistoricalProjectFixture(t, ctx, db)
				var staged installer.ConversionReport
				require.NoError(t, json.Unmarshal(report.Bytes(), &staged))
				require.Equal(t, 11, staged.SourceFormat)
				require.Len(t, staged.MigrationFailures, 1)
				require.NoError(t, (installer.Installer{DB: db}).Verify(ctx))
				require.JSONEq(t, before, stableHistoricalRows(t, ctx, db))
				cfg.ConversionReportWriter = &bytes.Buffer{}
				reopened, err := pgruntime.NewSQLDB(ctx, db, cfg)
				require.NoError(t, err)
				require.NoError(t, reopened.Close(ctx))
			})
		})
	}
}

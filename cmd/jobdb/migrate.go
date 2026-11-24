package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/colony-2/pgjobdb/pkg/pgjobdb/installer"
	"github.com/spf13/cobra"
)

// syncedReport makes durable evidence a prerequisite of the database commit.
// O_EXCL at creation preserves prior cutover reports, including uncertain ones.
type syncedReport struct{ *os.File }

func (w syncedReport) Write(data []byte) (int, error) {
	n, err := w.File.Write(data)
	if err == nil && n != len(data) {
		err = io.ErrShortWrite
	}
	if err == nil {
		err = w.File.Sync()
	}
	return n, err
}

func newMigrationCmd(migrate bool) *cobra.Command {
	var dsn, reportPath string
	var timeout time.Duration
	name, description := "check-migrate", "Inspect an explicit database for offline migration to format 14"
	if migrate {
		name, description = "migrate", "Convert an explicit offline database to format 14 with a durable staged report"
	}
	cmd := &cobra.Command{Use: name, Short: description, Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, _ []string) error {
		if strings.TrimSpace(dsn) == "" {
			return fmt.Errorf("--postgres-dsn is required explicitly")
		}
		if migrate && strings.TrimSpace(reportPath) == "" {
			return fmt.Errorf("--report-file is required")
		}
		ctx, cancel := context.WithTimeout(cmd.Context(), timeout)
		defer cancel()
		db, err := sql.Open("postgres", dsn)
		if err != nil {
			return fmt.Errorf("open migration database: invalid database configuration")
		}
		defer db.Close()
		if err = db.PingContext(ctx); err != nil {
			return fmt.Errorf("connect to migration database failed")
		}
		install := installer.Installer{DB: db}
		if !migrate {
			report, err := install.CheckMigration(ctx)
			if err != nil {
				return fmt.Errorf("inspect migration: %s", strings.ReplaceAll(err.Error(), dsn, "[database]"))
			}
			return json.NewEncoder(cmd.OutOrStdout()).Encode(report)
		}
		report, err := os.OpenFile(reportPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
		if err != nil {
			return fmt.Errorf("create staged report (existing evidence is never overwritten): %w", err)
		}
		defer report.Close()
		confirmation, err := install.MigrateWithReport(ctx, syncedReport{report})
		// A staged report alone never establishes commit. Emit confirmation even
		// on uncertain acknowledgment so operators can reconcile before retrying.
		outputErr := json.NewEncoder(cmd.OutOrStdout()).Encode(struct {
			StagedReport string                       `json:"stagedReport"`
			Confirmation installer.CommitConfirmation `json:"confirmation"`
		}{reportPath, confirmation})
		if err != nil {
			return fmt.Errorf("migration not confirmed; retain staged evidence and reconcile on a fresh connection before retry: %s", strings.ReplaceAll(err.Error(), dsn, "[database]"))
		}
		return outputErr
	}}
	cmd.Flags().StringVar(&dsn, "postgres-dsn", "", "explicit PostgreSQL database to inspect or migrate")
	cmd.Flags().DurationVar(&timeout, "timeout", 10*time.Minute, "maximum offline migration time")
	if migrate {
		cmd.Flags().StringVar(&reportPath, "report-file", "", "new staged evidence file; written and synced before commit")
	}
	return cmd
}

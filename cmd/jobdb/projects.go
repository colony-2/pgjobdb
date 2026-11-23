package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/projectcatalog"
	"github.com/colony-2/pgjobdb/pkg/pgjobdb/projects"
	"github.com/segmentio/ksuid"
	"github.com/spf13/cobra"
)

// Local maintenance exercises explicit operator DSN authority. It does not grant
// administration to HTTP callers or enable project deletion on the server.
func newPostgresProjectsCmd() *cobra.Command {
	root := &cobra.Command{Use: "postgres", Short: "Maintain a fenced PostgreSQL project installation", Long: "Fence every database writer, independent store and provider first. Review their inventory, identities, backups and keys separately. These commands cannot establish external coverage or a fleet fence."}
	var dsn string
	root.PersistentFlags().StringVar(&dsn, "postgres-dsn", "", "operator PostgreSQL DSN (or JOBDB_POSTGRES_DSN)")
	open := func(cmd *cobra.Command, fn func(context.Context, *sql.DB) error) error {
		value, err := resolveRequiredString(dsn, postgresDSNEnvVar, "postgres DSN")
		if err != nil {
			return err
		}
		db, err := sql.Open("postgres", value)
		if err != nil {
			return fmt.Errorf("open PostgreSQL maintenance connection failed")
		}
		defer db.Close()
		ctx := jobdb.WithProjectAdministration(cmd.Context(), func(context.Context) error { return nil })
		return fn(ctx, db)
	}
	emit := func(cmd *cobra.Command, v any) error { return json.NewEncoder(cmd.OutOrStdout()).Encode(v) }
	installation := &cobra.Command{Use: "installation", Short: "Explicitly initialize, inspect or adopt PostgreSQL storage"}
	var participants string
	var externalIDs []string
	inspect := &cobra.Command{Use: "inspect", Args: cobra.NoArgs, Short: "Capture read-only current-format inventory", RunE: func(cmd *cobra.Command, _ []string) error {
		return open(cmd, func(ctx context.Context, db *sql.DB) error {
			in, err := projects.Inspect(ctx, db, participants, externalIDs...)
			if err != nil {
				return err
			}
			return emit(cmd, in)
		})
	}}
	inspect.Flags().StringVar(&participants, "participants-sha256", "", "SHA-256 of reviewed external participant inventory")
	inspect.Flags().StringSliceVar(&externalIDs, "external-id", nil, "explicit external-only immutable identity (repeatable)")
	var freshParticipants string
	initialize := &cobra.Command{Use: "initialize", Args: cobra.NoArgs, Short: "Prepare empty storage with zero projects", RunE: func(cmd *cobra.Command, _ []string) error {
		return open(cmd, func(ctx context.Context, db *sql.DB) error {
			receipt, err := projects.Initialize(ctx, db, freshParticipants, cmd.ErrOrStderr())
			if err != nil {
				return err
			}
			return emit(cmd, map[string]any{"committed": true, "receipt": receipt})
		})
	}}
	initialize.Flags().StringVar(&freshParticipants, "participants-sha256", "", "SHA-256 of reviewed external participant inventory")
	prepare := &cobra.Command{Use: "prepare-shared", Args: cobra.NoArgs, Short: "Explicitly prepare a wholly absent shared-store set", RunE: func(cmd *cobra.Command, _ []string) error { return open(cmd, projects.PrepareShared) }}
	var inventoryFile string
	var mappings []string
	install := &cobra.Command{Use: "install", Args: cobra.NoArgs, Short: "Recheck inventory and atomically adopt approved identities", RunE: func(cmd *cobra.Command, _ []string) error {
		if inventoryFile == "" {
			return fmt.Errorf("--inventory is required")
		}
		raw, err := os.ReadFile(inventoryFile)
		if err != nil {
			return err
		}
		var in projects.Inventory
		if err = decodeInventory(raw, &in); err != nil {
			return err
		}
		seeds, err := decodeSeeds(mappings)
		if err != nil {
			return err
		}
		return open(cmd, func(ctx context.Context, db *sql.DB) error {
			receipt, err := projects.Install(ctx, db, in, seeds, cmd.ErrOrStderr())
			if err != nil {
				return err
			}
			return emit(cmd, map[string]any{"committed": true, "receipt": receipt})
		})
	}}
	install.Flags().StringVar(&inventoryFile, "inventory", "", "saved, reviewed inventory JSON")
	install.Flags().StringArrayVar(&mappings, "seed", nil, "approved ID=NAME mapping; c2 and 0 may use their literal ID (repeatable)")
	installation.AddCommand(initialize, inspect, install, prepare)
	var name, requestID, description, metadata string
	create := &cobra.Command{Use: "create", Args: cobra.NoArgs, Short: "Create a project using explicit local operator authority", RunE: func(cmd *cobra.Command, _ []string) error {
		if requestID == "" {
			requestID = ksuid.New().String()
			if _, err := fmt.Fprintf(cmd.ErrOrStderr(), "Request ID: %s (reuse after an uncertain result)\n", requestID); err != nil {
				return err
			}
		}
		// Strict public decoding rejects malformed fields before opening a connection.
		raw, err := json.Marshal(map[string]any{"name": name, "requestId": requestID, "description": description, "metadata": json.RawMessage(metadata)})
		if err != nil {
			return jobdb.ErrInvalidProject
		}
		req, err := projectcatalog.DecodeCreate(bytes.NewReader(raw))
		if err != nil {
			return err
		}
		return open(cmd, func(ctx context.Context, db *sql.DB) error {
			if err := projects.Verify(ctx, db); err != nil {
				return err
			}
			receipt, err := projects.NewControl(db, nil).CreateProject(ctx, req)
			if err != nil {
				return err
			}
			return emit(cmd, receipt)
		})
	}}
	create.Flags().StringVar(&name, "name", "", "unique discovery name")
	create.Flags().StringVar(&requestID, "request-id", "", "retained creation request ID; save and reuse after uncertainty")
	create.Flags().StringVar(&description, "description", "", "project description")
	create.Flags().StringVar(&metadata, "metadata", "{}", "project metadata object")
	root.AddCommand(installation, create)
	return root
}
func decodeInventory(raw []byte, in *projects.Inventory) error {
	// Reject duplicate and case-aliased keys before typed decoding. The envelope
	// contains only digests and identity lists; historical bytes are never decoded.
	keys := map[string]bool{}
	scan := json.NewDecoder(bytes.NewReader(raw))
	token, err := scan.Token()
	if err != nil || token != json.Delim('{') {
		return fmt.Errorf("inventory must be one JSON object")
	}
	for scan.More() {
		key, err := scan.Token()
		if err != nil {
			return err
		}
		name, ok := key.(string)
		if !ok || keys[strings.ToLower(name)] {
			return fmt.Errorf("duplicate inventory field")
		}
		keys[strings.ToLower(name)] = true
		switch name {
		case "protocol", "participantsSHA256", "resourcesSHA256", "structureSHA256", "tenantIDs", "externalIDs":
		default:
			return fmt.Errorf("unknown inventory field %q", name)
		}
		var value json.RawMessage
		if err = scan.Decode(&value); err != nil {
			return err
		}
	}
	if _, err = scan.Token(); err != nil {
		return err
	}
	if len(keys) != 6 {
		return fmt.Errorf("inventory requires all six fields")
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if err := d.Decode(in); err != nil {
		return err
	}
	var extra any
	if err := d.Decode(&extra); err != io.EOF {
		return fmt.Errorf("inventory must be one JSON object")
	}
	return nil
}
func decodeSeeds(values []string) ([]projectcatalog.Seed, error) {
	seeds := []projectcatalog.Seed{}
	seen := map[string]bool{}
	for _, v := range values {
		id, name, ok := strings.Cut(v, "=")
		if !ok {
			if v != "c2" && v != "0" {
				return nil, fmt.Errorf("seed needs ID=NAME")
			}
			id = v
			name = v
		}
		if id == "" || name == "" || seen[id] {
			return nil, fmt.Errorf("invalid or duplicate seed mapping")
		}
		seen[id] = true
		seeds = append(seeds, projectcatalog.Seed{ID: id, Name: name})
	}
	return seeds, nil
}

package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb/cli"
	remoteruntime "github.com/colony-2/jobdb/pkg/jobdb/runtime/remote"
	pgjobdbruntime "github.com/colony-2/pgjobdb/pkg/pgjobdb/runtime"
	"github.com/spf13/cobra"

	_ "github.com/colony-2/jobdb/pkg/jobdb/blobstore/gocdk"
)

const (
	postgresDSNEnvVar      = "JOBDB_POSTGRES_DSN"
	sqliteDSNEnvVar        = "JOBDB_SQLITE_DSN"
	listenAddrEnvVar       = "JOBDB_LISTEN"
	defaultSetupTimeout    = 45 * time.Second
	defaultShutdownTimeout = 10 * time.Second
)

type directServerConfig struct {
	ListenAddr             string
	PostgresDSN            string
	BlobStoreURI           string
	MaxInlineArtifactBytes int64
	LeaseTokenSigningKey   []byte
}

var runDirectServerFunc = runDirectServer

func newRootCmd() *cobra.Command {
	root := cli.NewRootCmd()
	root.AddCommand(newDirectCmd(), newServeCmd())
	return root
}

func newDirectCmd() *cobra.Command {
	var postgresDSN string
	cmd := &cobra.Command{
		Use:   "direct",
		Short: "Run a direct runtime with Postgres-backed records",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			dsn, err := resolveRequiredString(postgresDSN, postgresDSNEnvVar, "postgres DSN")
			if err != nil {
				return err
			}
			listenAddr, err := cmd.Root().PersistentFlags().GetString("listen")
			if err != nil {
				return err
			}
			blobStoreURI, err := cmd.Root().PersistentFlags().GetString("blob-store-uri")
			if err != nil {
				return err
			}
			return runDirectServerFunc(cmd.Context(), directServerConfig{
				ListenAddr:   listenAddr,
				PostgresDSN:  dsn,
				BlobStoreURI: blobStoreURI,
			})
		},
	}
	cmd.Flags().StringVar(&postgresDSN, "postgres-dsn", "", "postgres DSN for pgjobdb state (overrides "+postgresDSNEnvVar+")")
	return cmd
}

func runDirectServer(ctx context.Context, cfg directServerConfig) error {
	setupCtx, cancel := context.WithTimeout(ctx, defaultSetupTimeout)
	defer cancel()
	runtime, err := pgjobdbruntime.OpenDSN(setupCtx, cfg.PostgresDSN, pgjobdbruntime.Config{
		BlobStoreURI:           cfg.BlobStoreURI,
		MaxInlineArtifactBytes: cfg.MaxInlineArtifactBytes,
	})
	if err != nil {
		return fmt.Errorf("build direct runtime: %w", err)
	}

	var handler http.Handler
	if len(cfg.LeaseTokenSigningKey) == 0 {
		handler = remoteruntime.NewServer(runtime)
	} else {
		handler, err = remoteruntime.NewServerWithOptions(runtime, remoteruntime.ServerOptions{
			LeaseTokenSigningKey: cfg.LeaseTokenSigningKey,
		})
		if err != nil {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), defaultShutdownTimeout)
			defer cleanupCancel()
			return errors.Join(fmt.Errorf("build remote server: %w", err), runtime.Close(cleanupCtx))
		}
	}

	log.Printf("using direct Postgres runtime")
	return cli.ServeHTTP(ctx, cfg.ListenAddr, handler, runtime.Close)
}

func resolveRequiredString(flagValue, envVar, fieldName string) (string, error) {
	if flagValue != "" {
		return flagValue, nil
	}
	if envValue := os.Getenv(envVar); envValue != "" {
		return envValue, nil
	}
	return "", fmt.Errorf("%s is required via --postgres-dsn or %s", fieldName, envVar)
}

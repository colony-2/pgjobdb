package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/pgjobdb/internal/archivefixture"
	"github.com/colony-2/pgjobdb/pkg/pgjobdb/projects"
	pg "github.com/colony-2/pgjobdb/pkg/pgjobdb/runtime"
	"github.com/colony-2/pgjobdb/pkg/pgjobdb/runtime/internal/testsupport"
)

func serve(ctx context.Context, infoPath string) error {
	if infoPath == "" {
		return fmt.Errorf("-info must name a new fixture descriptor file")
	}
	if _, err := os.Stat(infoPath); !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("fixture descriptor must not already exist: %s", infoPath)
	}
	setup, cancelSetup := context.WithTimeout(ctx, time.Minute)
	defer cancelSetup()
	dsn, stop, err := testsupport.StartEmbeddedPostgres()
	if err != nil {
		return err
	}
	defer stop()
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return err
	}
	defer db.Close()
	blobs, err := os.MkdirTemp("", "cellarchive-blobs-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(blobs)
	admin := jobdb.WithProjectAdministration(setup, func(context.Context) error { return nil })
	if _, err := projects.Initialize(admin, db, "1111111111111111111111111111111111111111111111111111111111111111", io.Discard); err != nil {
		return err
	}
	runtime, err := pg.NewSQLDB(setup, db, pg.Config{BlobStoreURI: "blobfs://" + blobs})
	if err != nil {
		return err
	}
	defer runtime.Close(context.Background())
	project, err := runtime.CreateProject(admin, jobdb.CreateProjectRequest{Name: "archive-fixture", RequestID: "archive-fixture"})
	if err != nil {
		return err
	}
	other, err := runtime.CreateProject(admin, jobdb.CreateProjectRequest{Name: "other-fixture", RequestID: "other-fixture"})
	if err != nil {
		return err
	}
	fixture, err := archivefixture.Seed(setup, runtime, project.Project.ID, other.Project.ID)
	if err != nil {
		return err
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return err
	}
	defer listener.Close()
	mux := http.NewServeMux()
	mux.HandleFunc("GET /readyz", func(writer http.ResponseWriter, req *http.Request) {
		if err := db.PingContext(req.Context()); err != nil {
			http.Error(writer, "database unavailable", http.StatusServiceUnavailable)
			return
		}
		writer.WriteHeader(http.StatusNoContent)
	})
	mux.Handle("/", fixture.Handler(runtime))
	server := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	info := struct {
		archivefixture.Cells
		BaseURL string `json:"baseURL"`
		DSN     string `json:"dsn"`
	}{fixture, "http://" + listener.Addr().String(), dsn}
	staged, err := os.CreateTemp(filepath.Dir(infoPath), ".cellarchive-fixture-")
	if err != nil {
		return err
	}
	defer os.Remove(staged.Name())
	err = json.NewEncoder(staged).Encode(info)
	if err = errors.Join(err, staged.Close()); err != nil {
		return err
	}
	if err := os.Link(staged.Name(), infoPath); err != nil {
		return err
	}
	defer os.Remove(infoPath)
	served := make(chan error, 1)
	go func() { served <- server.Serve(listener) }()
	select {
	case err := <-served:
		return err
	case <-ctx.Done():
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return server.Shutdown(shutdown)
	}
}

func main() {
	infoPath := flag.String("info", "", "write the disposable fixture connection and seed descriptor here")
	flag.Parse()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := serve(ctx, *infoPath); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

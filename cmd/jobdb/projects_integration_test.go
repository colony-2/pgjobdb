//go:build integration

package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/remote"
	"github.com/colony-2/pgjobdb/internal/projectfixture"
	"github.com/colony-2/pgjobdb/pkg/pgjobdb/projects"
	embeddedpostgres "github.com/fergusstrange/embedded-postgres"
	"github.com/stretchr/testify/require"
)

func TestPostgresProjectCommands(t *testing.T) {
	binary := os.Getenv("PGJOBDB_TEST_CLI")
	require.NotEmpty(t, binary, "verification must supply the built production CLI")
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := l.Addr().(*net.TCPAddr).Port
	l.Close()
	dir := t.TempDir()
	pg := embeddedpostgres.NewDatabase(embeddedpostgres.DefaultConfig().Port(uint32(port)).RuntimePath(filepath.Join(dir, "runtime")).DataPath(filepath.Join(dir, "data")))
	require.NoError(t, pg.Start())
	defer pg.Stop()
	dsn := fmt.Sprintf("postgres://postgres:postgres@localhost:%d/postgres?sslmode=disable", port)
	ctx := context.Background()
	run := func(args ...string) (string, string, error) {
		cmd := exec.CommandContext(ctx, binary, args...)
		var stdout, stderr bytes.Buffer
		cmd.Stdout = &stdout
		cmd.Stderr = &stderr
		err := cmd.Run()
		require.NotContains(t, stdout.String(), dsn)
		require.NotContains(t, stderr.String(), dsn)
		return stdout.String(), stderr.String(), err
	}
	out, _, err := run("project", "postgres", "installation", "initialize", "--postgres-dsn", dsn, "--participants-sha256", projectfixture.Participants)
	require.NoError(t, err, out)
	require.Contains(t, out, `"committed":true`)
	metadata := `{"value":"\u0000"}`
	out, stderr, err := run("project", "postgres", "create", "--postgres-dsn", dsn, "--name", "cli-project", "--metadata", metadata)
	require.NoError(t, err, stderr)
	fields := strings.Fields(stderr)
	require.GreaterOrEqual(t, len(fields), 3)
	requestID := fields[2]
	var first jobdb.ProjectCreation
	require.NoError(t, json.Unmarshal([]byte(out), &first))
	require.Equal(t, metadata, string(first.Project.Metadata))
	retry, _, err := run("project", "postgres", "create", "--postgres-dsn", dsn, "--name", "cli-project", "--request-id", requestID, "--metadata", metadata)
	require.NoError(t, err)
	var again jobdb.ProjectCreation
	require.NoError(t, json.Unmarshal([]byte(retry), &again))
	require.Equal(t, first, again)
	// Both actual server commands open the explicitly prepared storage. Standalone
	// middleware grants cell operations, while project administration stays denied.
	for _, mode := range []string{"direct", "serve"} {
		l, e := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, e)
		addr := l.Addr().String()
		l.Close()
		serverCtx, cancel := context.WithCancel(ctx)
		cmd := exec.CommandContext(serverCtx, binary, mode, "--postgres-dsn", dsn, "--listen", addr, "--blob-store-uri", "blobfs://"+t.TempDir())
		if mode == "serve" {
			cmd = exec.CommandContext(serverCtx, binary, "serve")
			cmd.Env = append(os.Environ(), "JOBDB_POSTGRES_DSN="+dsn, "JOBDB_LISTEN="+addr, "JOBDB_BLOB_STORE_URI=s3://fixture?region=us-east-1", "AWS_ACCESS_KEY_ID=fixture", "AWS_SECRET_ACCESS_KEY=fixture", "AWS_EC2_METADATA_DISABLED=true", "JOBDB_LEASE_TOKEN_SIGNING_KEY="+base64.StdEncoding.EncodeToString([]byte("cli-test-signing-key-at-least-32-bytes")))
		}
		var log bytes.Buffer
		cmd.Stdout = &log
		cmd.Stderr = &log
		require.NoError(t, cmd.Start())
		t.Cleanup(func() { cancel(); cmd.Wait() })
		endpoint := "http://" + addr
		client, e := remote.New(endpoint, &http.Client{Timeout: time.Second})
		require.NoError(t, e)
		deadline := time.Now().Add(20 * time.Second)
		for {
			response, e := http.Get(endpoint + "/v1/capabilities/job-goals")
			if e == nil {
				response.Body.Close()
				break
			}
			if time.Now().After(deadline) {
				cancel()
				cmd.Wait()
				t.Fatalf("CLI server startup: %s", log.String())
			}
			time.Sleep(30 * time.Millisecond)
		}
		admin := celltest.Context(ctx)
		if mode == "direct" {
			_, e = client.CreateCell(admin, jobdb.CreateCellRequest{TenantID: first.Project.ID, Name: "work", Repo: "example.test/work"})
			require.NoError(t, e)
		}
		_, e = client.SubmitJob(admin, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: first.Project.ID, Cell: "work", JobID: mode, JobType: "work", Data: jobdb.NewTaskDataOrPanic(1)}})
		require.NoError(t, e)
		projectClient, e := remote.NewProjectRuntime(endpoint, &http.Client{Timeout: time.Second})
		require.NoError(t, e)
		_, e = projectClient.CreateProject(ctx, jobdb.CreateProjectRequest{Name: "denied", RequestID: mode})
		require.ErrorIs(t, e, jobdb.ErrProjectAccessDenied)
		cancel()
		cmd.Wait()
	}
	// Historical operations use a different explicitly prepared scheduler database.
	db, e := sql.Open("postgres", dsn)
	require.NoError(t, e)
	defer db.Close()
	_, e = projects.Inspect(ctx, db, projectfixture.Participants)
	require.NoError(t, e)
}

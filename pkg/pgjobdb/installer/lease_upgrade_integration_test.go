//go:build integration

package installer_test

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"debug/buildinfo"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/remote"
	"github.com/colony-2/pgjobdb/internal/projectfixture"
	"github.com/colony-2/pgjobdb/pkg/pgjobdb"
	"github.com/colony-2/pgjobdb/pkg/pgjobdb/installer"
	"github.com/colony-2/pgjobdb/pkg/pgjobdb/internal/projectstorage"
	"github.com/colony-2/pgjobdb/pkg/pgjobdb/projects"
	pgruntime "github.com/colony-2/pgjobdb/pkg/pgjobdb/runtime"
	"github.com/lib/pq"
	"github.com/stretchr/testify/require"
)

func format13Producer(t *testing.T) string {
	t.Helper()
	binary := os.Getenv("PGJOBDB_TEST_FORMAT13")
	if binary == "" {
		binary = filepath.Join(t.TempDir(), "format13")
		build := exec.Command("go", "build", "-mod=readonly", "-o", binary, ".")
		build.Dir = "../../../scripts/testdata/pgjobdb-format13"
		build.Env = append(os.Environ(), "GOWORK=off", "GOFLAGS=")
		output, err := build.CombinedOutput()
		require.NoError(t, err, "%s", output)
	}
	info, err := buildinfo.ReadFile(binary)
	require.NoError(t, err)
	var postgres, job string
	for _, module := range info.Deps {
		require.Nil(t, module.Replace)
		if module.Path == "github.com/colony-2/pgjobdb" {
			postgres = module.Version
		}
		if module.Path == "github.com/colony-2/jobdb" {
			job = module.Version
		}
	}
	require.Equal(t, "v0.0.29", postgres)
	require.Equal(t, "v0.0.99-0.20261122051819-86f7ba9bc009", job)
	return binary
}

func TestLastLeaseUpdatedAtUpgrade(t *testing.T) {
	producer := format13Producer(t)
	for _, mode := range []string{"no-project", "projects"} {
		t.Run(mode, func(t *testing.T) {
			withBareDatabase(t, func(ctx context.Context, db *sql.DB) {
				ctx = projectfixture.Context(ctx)
				var port string
				require.NoError(t, db.QueryRowContext(ctx, `SHOW port`).Scan(&port))
				dsn := fmt.Sprintf("host=localhost port=%s user=postgres password=postgres dbname=postgres sslmode=disable", port)
				blobs := "blobfs://" + t.TempDir()
				command := func(action string) *exec.Cmd {
					cmd := exec.CommandContext(ctx, producer, action)
					cmd.Env = append(os.Environ(), "PGJOBDB_TEST_DSN="+dsn, "PGJOBDB_TEST_BLOBS="+blobs)
					return cmd
				}
				fixtureMode := "install"
				if mode == "no-project" {
					fixtureMode = "install-no-project"
				}
				output, err := command(fixtureMode).CombinedOutput()
				require.NoError(t, err, "%s", output)
				var cursors struct {
					CatalogCursor, JobCursor string
					Project                  jobdb.ProjectCreation
				}
				if mode == "projects" {
					require.NoError(t, json.Unmarshal(output, &cursors))
				}
				install := installer.Installer{DB: db}
				report, err := install.Preflight(ctx)
				require.NoError(t, err)
				require.True(t, report.Advisory)
				require.Equal(t, []int{13, 14, 15, 16}, report.PhasePath)
				require.ErrorContains(t, install.Verify(ctx), "explicit offline migration")
				_, err = install.ApplyWithReport(ctx, nil)
				require.ErrorContains(t, err, "explicit offline migration")

				var receipt, catalog, admission []byte
				receiptSQL := `SELECT payload FROM pgjobdb.project_installation WHERE singleton=1`
				catalogSQL := `SELECT payload,admission FROM public.jobdb_project_catalog WHERE singleton=1`
				read := func(query string, args ...any) {
					t.Helper()
					require.NoError(t, db.QueryRowContext(ctx, query).Scan(args...))
				}
				var sourceBindings, sourceData string
				bindingSQL := `SELECT string_agg(n.nspname||'.'||c.relname||'='||c.oid::text,',' ORDER BY n.nspname,c.relname) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE c.relkind IN ('r','v','S') AND (n.nspname='pgjobdb' OR n.nspname='public' AND c.relname LIKE 'jobdb_%') AND c.relname NOT IN ('resource_configuration','resource_sequences','resource_receipts','project_scheduler_upgrade')`
				dataSQL := `SELECT jsonb_build_array((SELECT jsonb_agg(to_jsonb(j) ORDER BY job_id) FROM pgjobdb.jobs j),(SELECT jsonb_agg(to_jsonb(a) ORDER BY job_id) FROM pgjobdb.jobs_archive a),(SELECT jsonb_agg(to_jsonb(e) ORDER BY event_id) FROM pgjobdb.lease_events e))::text`
				var history string
				historySQL := `SELECT jsonb_agg(to_jsonb(e) ORDER BY event_id)::text FROM pgjobdb.lease_events e`
				read(historySQL, &history)
				if mode == "no-project" {
					require.NoError(t, projects.PrepareShared(ctx, db))
				} else {
					read(receiptSQL, &receipt)
					read(catalogSQL, &catalog, &admission)
				}
				read(bindingSQL, &sourceBindings)
				read(dataSQL, &sourceData)

				assertSource := func() {
					t.Helper()
					var format int
					var certificate bool
					read(`SELECT format_version,to_regclass('pgjobdb.project_scheduler_upgrade') IS NOT NULL FROM pgjobdb.installation`, &format, &certificate)
					require.Equal(t, 13, format)
					require.False(t, certificate)
					var bindings, data string
					read(bindingSQL, &bindings)
					read(dataSQL, &data)
					require.Equal(t, sourceBindings, bindings)
					require.Equal(t, sourceData, data)
					if mode == "projects" {
						var got []byte
						read(receiptSQL, &got)
						require.Equal(t, receipt, got)
						require.NoError(t, projectstorage.Verify(ctx, db))
					}
				}
				tables := []string{"pgjobdb.installation", "pgjobdb.jobs", "public.jobdb_configuration", "public.jobdb_schemas", "public.jobdb_chapter_stories", "public.jobdb_chapter_chapters", "public.jobdb_chapter_artifacts"}
				if mode == "projects" {
					tables = append(tables, "pgjobdb.project_installation", "public.jobdb_project_catalog")
				}
				for _, table := range tables {
					t.Run(table, func(t *testing.T) {
						blocker, err := db.BeginTx(ctx, nil)
						require.NoError(t, err)
						defer blocker.Rollback()
						_, err = blocker.ExecContext(ctx, `SELECT pgjobdb._goal_lock('c2'); LOCK TABLE `+table+` IN ACCESS EXCLUSIVE MODE`)
						require.NoError(t, err)
						deadline, cancel := context.WithTimeout(ctx, 2*time.Second)
						defer cancel()
						_, err = install.MigrateWithReport(deadline, nil)
						var conflict *pq.Error
						require.ErrorAs(t, err, &conflict)
						require.Equal(t, pq.ErrorCode("55P03"), conflict.Code)
						require.NoError(t, blocker.Rollback())
						assertSource()
					})
				}
				_, err = install.MigrateWithReport(ctx, failingReportWriter{})
				require.ErrorContains(t, err, "export staged conversion report")
				db.SetMaxIdleConns(0)
				assertSource()
				if mode == "projects" {
					output, err = command("probe").CombinedOutput()
					require.NoError(t, err, "%s", output)
					read(catalogSQL, &catalog, &admission)
					assertNewClientAgainstOldServer(t, ctx, command("serve"))
				}
				var leaseID string
				var keptExpiry time.Time
				read(`SELECT lease_id,lease_expires_at FROM pgjobdb.jobs WHERE job_id='held'`, &leaseID, &keptExpiry)
				var conf installer.CommitConfirmation
				if mode == "projects" {
					base, err := pq.NewConnector(dsn)
					require.NoError(t, err)
					connector := &lostCommitConnector{Connector: base}
					connector.lose.Store(true)
					uncertain := sql.OpenDB(connector)
					defer uncertain.Close()
					var staged bytes.Buffer
					conf, err = (installer.Installer{DB: uncertain}).MigrateWithReport(ctx, &staged)
					require.ErrorContains(t, err, "conversion commit not confirmed")
					require.False(t, conf.Committed)
					require.NotEmpty(t, staged.Bytes())
					require.NoError(t, projects.Verify(ctx, db), "reconcile on a new connection before replay")
				} else {
					conf, err = install.MigrateWithReport(ctx, nil)
					require.NoError(t, err)
					require.True(t, conf.Committed)
				}
				var bindings, events string
				read(bindingSQL, &bindings)
				read(historySQL, &events)
				require.Equal(t, sourceBindings, bindings)
				require.Equal(t, history, events)
				listOptions := pgjobdb.ListJobsOptions{TenantIDs: []pgjobdb.TenantID{"c2"}}
				page, err := pgjobdb.ListJobSummaries(ctx, db, listOptions)
				require.NoError(t, err)
				require.Len(t, page.Jobs, 3)
				for _, row := range page.Jobs {
					require.Nil(t, row.LastLeaseUpdatedAt)
					if row.JobID == "held" {
						require.Equal(t, keptExpiry.UTC(), *row.LeaseExpiresAt)
					}
				}
				var expiry time.Time
				require.NoError(t, db.QueryRowContext(ctx, `SELECT pgjobdb.renew_native_lease('c2','held',$1,'historical-worker',60)`, leaseID).Scan(&expiry))
				var eventTime time.Time
				read(`SELECT occurred_at FROM pgjobdb.lease_events WHERE job_id='held' AND event_type='lease_renewed'`, &eventTime)
				page, err = pgjobdb.ListJobSummaries(ctx, db, listOptions)
				require.NoError(t, err)
				for _, row := range page.Jobs {
					if row.JobID == "held" {
						require.Equal(t, eventTime.UTC(), *row.LastLeaseUpdatedAt)
						require.Equal(t, expiry.UTC(), *row.LeaseExpiresAt)
					}
				}
				if mode == "no-project" {
					return
				}

				var gotReceipt, gotCatalog, gotAdmission []byte
				read(receiptSQL, &gotReceipt)
				read(catalogSQL, &gotCatalog, &gotAdmission)
				require.Equal(t, receipt, gotReceipt)
				require.Equal(t, catalog, gotCatalog)
				require.Equal(t, admission, gotAdmission)
				var certificate string
				const certificateSQL = `SELECT to_jsonb(u)::text FROM pgjobdb.project_scheduler_upgrade u WHERE singleton=1`
				read(certificateSQL, &certificate)
				var retry bytes.Buffer
				conf, err = install.ApplyWithReport(ctx, &retry)
				require.NoError(t, err)
				require.True(t, conf.Committed)
				require.Contains(t, retry.String(), `"noOp": true`)
				var after string
				read(certificateSQL, &after)
				require.Equal(t, certificate, after)
				require.NoError(t, projects.Verify(ctx, db))
				runtime, err := pgruntime.OpenDSN(ctx, dsn, pgruntime.Config{BlobStoreURI: blobs})
				require.NoError(t, err)
				defer runtime.Close(ctx)
				catalogPage, err := runtime.ListProjects(ctx, jobdb.ListProjectsRequest{PageSize: 1, PageToken: cursors.CatalogCursor})
				require.NoError(t, err)
				require.Len(t, catalogPage.Projects, 1)
				jobs, err := runtime.ListJobs(ctx, jobdb.ListJobsRequest{TenantIds: []string{"c2"}, PageSize: 1, PageToken: cursors.JobCursor})
				require.NoError(t, err)
				require.Len(t, jobs.Jobs, 1)
				replay, err := runtime.CreateProject(ctx, jobdb.CreateProjectRequest{Name: "historical", RequestID: "historical-receipt"})
				require.NoError(t, err)
				require.Equal(t, cursors.Project, replay)
				_, err = runtime.CreateProject(ctx, jobdb.CreateProjectRequest{Name: "after", RequestID: "after"})
				require.NoError(t, err)
				server := httptest.NewServer(celltest.Handler(remote.NewConsumerServer(runtime)))
				defer server.Close()
				client := command("client")
				client.Env = append(client.Env, "PGJOBDB_TEST_SERVER="+server.URL)
				output, err = client.CombinedOutput()
				require.NoError(t, err, "%s", output)

				for _, denied := range []string{`UPDATE pgjobdb.jobs SET tenant_id='missing' WHERE job_id='held'`, `TRUNCATE public.jobdb_schemas`, `SELECT pgjobdb.release_append('missing','noop','none')`} {
					_, err := db.ExecContext(ctx, denied)
					require.Error(t, err, denied)
				}
				for _, changed := range []struct{ name, sql, restore string }{
					{"missing-certificate", `ALTER TABLE pgjobdb.project_scheduler_upgrade RENAME TO hidden_certificate`, `ALTER TABLE pgjobdb.hidden_certificate RENAME TO project_scheduler_upgrade`},
					{"wrong-receipt", `UPDATE pgjobdb.project_scheduler_upgrade SET source_receipt_sha256='wrong'`, ""},
					{"wrong-source", `UPDATE pgjobdb.project_scheduler_upgrade SET source_structure_sha256='wrong'`, ""},
					{"wrong-source-format", `UPDATE pgjobdb.project_scheduler_upgrade SET source_format=14`, ""},
					{"wrong-target", `UPDATE pgjobdb.project_scheduler_upgrade SET target_structure_sha256='wrong'`, ""},
					{"disabled-guard", `ALTER TABLE pgjobdb.jobs DISABLE TRIGGER project_admission_row`, `ALTER TABLE pgjobdb.jobs ENABLE TRIGGER project_admission_row`},
					{"fixed-clock", `CREATE OR REPLACE FUNCTION pgjobdb.scheduler_now() RETURNS timestamptz LANGUAGE sql VOLATILE AS $$ SELECT '2026-01-01T00:00:00Z'::timestamptz; $$`, `CREATE OR REPLACE FUNCTION pgjobdb.scheduler_now() RETURNS timestamptz LANGUAGE sql VOLATILE AS $$ SELECT clock_timestamp(); $$`},
				} {
					t.Run(changed.name, func(t *testing.T) {
						_, err := db.ExecContext(ctx, changed.sql)
						require.NoError(t, err)
						defer func() {
							if changed.restore != "" {
								_, err = db.ExecContext(ctx, changed.restore)
							} else {
								_, err = db.ExecContext(ctx, `UPDATE pgjobdb.project_scheduler_upgrade u SET source_format=original.source_format,source_receipt_sha256=original.source_receipt_sha256,source_structure_sha256=original.source_structure_sha256,target_structure_sha256=original.target_structure_sha256 FROM json_populate_record(NULL::pgjobdb.project_scheduler_upgrade,$1) original WHERE u.singleton=1`, certificate)
							}
							require.NoError(t, err)
							require.NoError(t, projects.Verify(ctx, db))
						}()
						_, err = pgruntime.OpenDSN(ctx, dsn, pgruntime.Config{BlobStoreURI: blobs})
						require.Error(t, err)
						control := projects.NewControl(db, nil)
						_, err = control.ListProjects(ctx, jobdb.ListProjectsRequest{})
						require.Error(t, err)
						_, err = control.CreateProject(ctx, jobdb.CreateProjectRequest{Name: "bad-attempt", RequestID: "bad-attempt"})
						require.Error(t, err)
					})
				}
			})
		})
	}
}

func assertNewClientAgainstOldServer(t *testing.T, ctx context.Context, cmd *exec.Cmd) {
	t.Helper()
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	stdout, err := cmd.StdoutPipe()
	require.NoError(t, err)
	stop, err := cmd.StdinPipe()
	require.NoError(t, err)
	require.NoError(t, cmd.Start())
	defer func() { _ = stop.Close(); require.NoError(t, cmd.Wait(), "%s", stderr.String()) }()
	var address string
	require.NoError(t, json.NewDecoder(bufio.NewReader(stdout)).Decode(&address))
	for _, constructor := range []func(string, *http.Client, ...remote.Option) (*remote.Runtime, error){remote.New, remote.NewProjectRuntime} {
		client, err := constructor(address, nil)
		require.NoError(t, err)
		page, err := client.ListJobs(ctx, jobdb.ListJobsRequest{TenantIds: []string{"c2"}})
		require.NoError(t, err)
		require.Len(t, page.Jobs, 3)
		for _, row := range page.Jobs {
			require.Nil(t, row.LastLeaseUpdatedAt)
		}
	}
}

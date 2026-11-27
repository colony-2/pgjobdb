package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	"github.com/colony-2/jobdb/pkg/jobdb/projectcatalog"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/remote"
	"github.com/colony-2/pgjobdb/pkg/pgjobdb/installer"
	"github.com/colony-2/pgjobdb/pkg/pgjobdb/projects"
	pgruntime "github.com/colony-2/pgjobdb/pkg/pgjobdb/runtime"
	_ "github.com/lib/pq"
)

func main() {
	ctx := jobdb.WithProjectAdministration(celltest.Context(context.Background()), func(context.Context) error { return nil })
	mode := os.Args[1]
	if mode == "client" {
		for _, constructor := range []func(string, *http.Client, ...remote.Option) (*remote.Runtime, error){remote.New, remote.NewProjectRuntime} {
			client, err := constructor(os.Getenv("PGJOBDB_TEST_SERVER"), nil)
			check(err)
			page, err := client.ListJobs(ctx, jobdb.ListJobsRequest{TenantIds: []string{"c2"}})
			check(err)
			if len(page.Jobs) != 3 {
				log.Fatalf("historical client listed %d jobs, expected 3", len(page.Jobs))
			}
		}
		return
	}
	db, err := sql.Open("postgres", os.Getenv("PGJOBDB_TEST_DSN"))
	check(err)
	defer db.Close()
	if mode == "install" || mode == "install-no-project" {
		check((installer.Installer{DB: db}).Apply(ctx))
		_, err = db.ExecContext(ctx, `INSERT INTO pgjobdb.cells(tenant_id,cell_id,name,repo,state,description,metadata,revision,created_at_ns,updated_at_ns) VALUES('c2','historical-cell','test','example.test/test','ACTIVE','','{}','historical-revision',0,0); INSERT INTO pgjobdb.cell_names VALUES('c2','test','historical-cell')`)
		check(err)
		for _, id := range []string{"archive", "held", "unknown"} {
			_, err = db.ExecContext(ctx, `SELECT pgjobdb.submit_native_job('c2',$1,'submitter','historical','{}','{}',NULL,NULL,ARRAY[]::TEXT[],NULL,NULL,NULL,NULL,NULL,FALSE,'[]',jsonb_build_object('cellId',cell_id,'cell',name,'cellRepo',repo)) FROM pgjobdb.cells WHERE tenant_id='c2' AND name='test'`, id)
			check(err)
			if id == "unknown" {
				continue
			}
			var leaseID string
			check(db.QueryRowContext(ctx, `SELECT lease_id FROM pgjobdb.get_native_work('historical-worker',ARRAY['c2'],ARRAY['historical'],'[]','{}',3600,'c2',$1,'[]',ARRAY['historical-cell'])`, id).Scan(&leaseID))
			if id == "archive" {
				_, err = db.ExecContext(ctx, `SELECT pgjobdb.complete_native_job('c2',$1,$2,'historical-worker','success',NULL,NULL,FALSE,NULL,NULL,NULL)`, id, leaseID)
				check(err)
			}
		}
		if mode == "install-no-project" {
			return
		}
		check(projects.PrepareShared(ctx, db))
		inventory, err := projects.Inspect(ctx, db, "1111111111111111111111111111111111111111111111111111111111111111")
		check(err)
		_, err = projects.Install(ctx, db, inventory, []projectcatalog.Seed{{ID: "c2", Name: "c2"}}, io.Discard)
		check(err)
	}
	check(projects.Verify(ctx, db))
	runtime, err := pgruntime.NewSQLDB(ctx, db, pgruntime.Config{BlobStoreURI: os.Getenv("PGJOBDB_TEST_BLOBS")})
	check(err)
	defer runtime.Close(ctx)
	if mode == "serve" {
		handler := remote.NewConsumerServer(runtime)
		server := httptest.NewServer(celltest.Handler(handler))
		defer server.Close()
		check(json.NewEncoder(os.Stdout).Encode(server.URL))
		var stop [1]byte
		_, _ = os.Stdin.Read(stop[:])
		return
	}
	if mode == "probe" {
		_, err := runtime.CreateProject(ctx, jobdb.CreateProjectRequest{Name: "rollback-probe", RequestID: "rollback-probe"})
		check(err)
	}
	project, err := runtime.CreateProject(ctx, jobdb.CreateProjectRequest{Name: "historical", RequestID: "historical-receipt"})
	check(err)
	catalog, err := runtime.ListProjects(ctx, jobdb.ListProjectsRequest{PageSize: 1})
	check(err)
	jobs, err := runtime.ListJobs(ctx, jobdb.ListJobsRequest{TenantIds: []string{"c2"}, PageSize: 1})
	check(err)
	if catalog.NextPageToken == "" || jobs.NextPageToken == "" {
		log.Fatal("historical cursor missing")
	}
	check(json.NewEncoder(os.Stdout).Encode(map[string]any{"catalogCursor": catalog.NextPageToken, "jobCursor": jobs.NextPageToken, "project": project}))
}

func check(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

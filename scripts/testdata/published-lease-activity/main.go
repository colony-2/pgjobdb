//go:build ignore

package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	"github.com/colony-2/jobdb/pkg/jobdb/projectcatalog"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/remote"
	"github.com/colony-2/pgjobdb/pkg/pgjobdb"
	"github.com/colony-2/pgjobdb/pkg/pgjobdb/installer"
	"github.com/colony-2/pgjobdb/pkg/pgjobdb/projects"
	pgruntime "github.com/colony-2/pgjobdb/pkg/pgjobdb/runtime"
	_ "github.com/lib/pq"
)

type observer func(*http.Request) (*http.Response, error)

func (o observer) RoundTrip(request *http.Request) (*http.Response, error) { return o(request) }

func main() {
	ctx, cancel := context.WithTimeout(celltest.Context(context.Background()), time.Minute)
	defer cancel()
	ctx = jobdb.WithProjectAdministration(ctx, func(context.Context) error { return nil })
	db, err := sql.Open("postgres", os.Getenv("PGJOBDB_VERIFY_DSN"))
	check(err)
	defer db.Close()
	var occupied bool
	check(db.QueryRowContext(ctx, `SELECT to_regnamespace('pgjobdb') IS NOT NULL`).Scan(&occupied))
	assert(!occupied, "expected an empty disposable database")
	check((installer.Installer{DB: db}).Apply(ctx))
	check(projects.PrepareShared(ctx, db))
	inventory, err := projects.Inspect(ctx, db, strings.Repeat("1", 64), "c2")
	check(err)
	_, err = projects.Install(ctx, db, inventory, []projectcatalog.Seed{{ID: "c2", Name: "c2"}}, io.Discard)
	check(err)
	backend, err := pgruntime.NewSQLDB(ctx, db, pgruntime.Config{BlobStoreURI: os.Getenv("PGJOBDB_VERIFY_BLOBS")})
	check(err)
	defer backend.Close(ctx)
	_, err = backend.CreateCell(ctx, jobdb.CreateCellRequest{TenantID: "c2", Name: "test", Repo: "example.test/test"})
	check(err)
	leaseTimes, expiries := map[string]time.Time{}, map[string]time.Time{}
	for _, id := range []string{"active", "archived", "unknown"} {
		_, err := pgjobdb.SubmitJob(ctx, db, pgjobdb.SubmitJobRequest{TenantID: "c2", Cell: "test", CellRepo: "example.test/test", JobID: pgjobdb.JobID(id), WorkerID: "submitter", JobType: "activity"})
		check(err)
		if id == "unknown" {
			continue
		}
		lease, err := pgjobdb.GetJobLease(ctx, db, "c2", pgjobdb.JobID(id), "worker", pgjobdb.WorkSelector{JobTypes: []pgjobdb.JobType{"activity"}}, pgjobdb.GetJobLeaseOptions{LeaseDuration: time.Minute})
		check(err)
		assert(lease != nil, "missing lease for %s", id)
		expiries[id] = lease.ExpiresAt
		var at time.Time
		check(db.QueryRowContext(ctx, `SELECT occurred_at FROM pgjobdb.lease_events WHERE job_id=$1`, id).Scan(&at))
		leaseTimes[id] = at
		if id == "archived" {
			check(pgjobdb.CompleteJob(ctx, db, lease.Identity(), pgjobdb.Completion{Status: pgjobdb.CompletionSuccess}))
		}
	}
	server := httptest.NewServer(celltest.Handler(remote.NewConsumerServer(backend)))
	defer server.Close()
	for scope, constructor := range map[string]func(string, *http.Client, ...remote.Option) (*remote.Runtime, error){"tenants": remote.New, "projects": remote.NewProjectRuntime} {
		var raw []byte
		client, err := constructor(server.URL, &http.Client{Transport: observer(func(request *http.Request) (*http.Response, error) {
			assert(strings.Contains(request.URL.Path, "/v1/"+scope+"/c2/jobs"), "wrong list route %s", request.URL.Path)
			response, err := http.DefaultTransport.RoundTrip(request)
			if err != nil {
				return nil, err
			}
			defer response.Body.Close()
			raw, err = io.ReadAll(response.Body)
			response.Body = io.NopCloser(bytes.NewReader(raw))
			return response, err
		})})
		check(err)
		page, err := client.ListJobs(ctx, jobdb.ListJobsRequest{TenantIds: []string{"c2"}})
		check(err)
		var wire struct {
			Jobs []map[string]json.RawMessage `json:"jobs"`
		}
		check(json.Unmarshal(raw, &wire))
		assert(len(page.Jobs) == 3 && len(wire.Jobs) == 3, "wrong list length for %s", scope)
		for index, row := range page.Jobs {
			id := row.JobKey.JobId
			_, present := wire.Jobs[index]["lastLeaseUpdatedAt"]
			if id == "unknown" {
				assert(!present && row.LastLeaseUpdatedAt == nil && row.LeaseExpiresAt == nil, "unknown values were not omitted")
				continue
			}
			assert(present && row.LastLeaseUpdatedAt != nil && row.LastLeaseUpdatedAt.Equal(leaseTimes[id]), "wrong activity time for %s", id)
			assert(row.LeaseExpiresAt != nil && row.LeaseExpiresAt.Equal(expiries[id]), "changed lease expiry for %s", id)
			assert((row.ArchivedAt != nil) == (id == "archived"), "wrong store for %s", id)
		}
		fmt.Printf("%s: active/archived exact, unknown omitted, expiry unchanged\n", scope)
	}
}

func check(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
func assert(ok bool, message string, args ...any) {
	if !ok {
		log.Fatalf(message, args...)
	}
}

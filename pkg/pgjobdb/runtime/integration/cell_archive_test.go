package integration_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/remote"
	"github.com/colony-2/jobdb/pkg/jobdb/runtimetest"
	"github.com/colony-2/pgjobdb/internal/archivefixture"
	"github.com/colony-2/pgjobdb/internal/projectfixture"
	pg "github.com/colony-2/pgjobdb/pkg/pgjobdb/runtime"
	"github.com/colony-2/pgjobdb/pkg/pgjobdb/runtime/integration/internal/jobdbtest"
	"github.com/stretchr/testify/require"
)

func TestPostgresCellArchiveConformance(t *testing.T) {
	rt := conformanceRuntimeFactory(t)(t)
	runtimetest.RunCellArchiveConformance(t, jobdbtest.NewProjectBackend(rt, ""))
}

type archiveHTTPResponse struct {
	status int
	etag   string
	body   []byte
	err    error
}

func archiveHTTP(server *httptest.Server, method, path, token string, revisions ...string) archiveHTTPResponse {
	request, err := http.NewRequest(method, server.URL+path, nil)
	if err != nil {
		return archiveHTTPResponse{err: err}
	}
	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}
	for _, revision := range revisions {
		request.Header.Add("If-Match", revision)
	}
	response, err := server.Client().Do(request)
	if err != nil {
		return archiveHTTPResponse{err: err}
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	return archiveHTTPResponse{response.StatusCode, response.Header.Get("ETag"), body, err}
}

type archiveFixtureTransport struct {
	http.RoundTripper
}

func (transport archiveFixtureTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	request = request.Clone(request.Context())
	request.Header.Set("Authorization", "Bearer "+archivefixture.Writer)
	return transport.RoundTripper.RoundTrip(request)
}

func TestPostgresCellArchiveHTTP(t *testing.T) {
	factory := conformanceRuntimeFactory(t)
	for _, family := range []string{"tenants", "projects"} {
		t.Run(family, func(t *testing.T) {
			rt := factory(t)
			ctx := projectfixture.Context(context.Background())
			project, err := rt.CreateProject(ctx, jobdb.CreateProjectRequest{Name: "archive", RequestID: "archive"})
			require.NoError(t, err)
			other, err := rt.CreateProject(ctx, jobdb.CreateProjectRequest{Name: "other", RequestID: "other"})
			require.NoError(t, err)
			fixture, err := archivefixture.Seed(ctx, rt, project.Project.ID, other.Project.ID)
			require.NoError(t, err)
			server := httptest.NewServer(fixture.Handler(rt))
			defer server.Close()
			base := "/v1/" + family + "/" + fixture.Project + "/cells"
			ok := func(t *testing.T, response archiveHTTPResponse) jobdb.Cell {
				t.Helper()
				require.NoError(t, response.err)
				require.Equal(t, 200, response.status, "%s", response.body)
				var cell jobdb.Cell
				require.NoError(t, json.Unmarshal(response.body, &cell))
				require.NotEmpty(t, cell.Revision)
				require.Equal(t, archivefixture.Revision(cell), response.etag)
				return cell
			}
			original := fixture.Seeded[0]
			path := base + "/" + original.ID
			write := func(action string, revisions ...string) archiveHTTPResponse {
				return archiveHTTP(server, http.MethodPost, path+"/"+action, archivefixture.Writer, revisions...)
			}
			read := func(t *testing.T) jobdb.Cell {
				return ok(t, archiveHTTP(server, http.MethodGet, path, archivefixture.Writer))
			}
			t.Run("strong_preconditions", func(t *testing.T) {
				before := read(t)
				for _, action := range []string{"archive", "unarchive"} {
					missing := write(action)
					require.NoError(t, missing.err)
					require.Equal(t, 428, missing.status, "%s", missing.body)
					for _, bad := range [][]string{{"*"}, {`W/"weak"`}, {"bare"}, {`"a", "b"`}, {`"a"`, `"b"`}} {
						response := write(action, bad...)
						require.NoError(t, response.err)
						require.Equal(t, 400, response.status, "%s", response.body)
					}
				}
				require.Equal(t, before, read(t))
			})
			t.Run("revision_race_and_noop", func(t *testing.T) {
				before := read(t)
				revision := archivefixture.Revision(before)
				start := make(chan struct{})
				results := make(chan archiveHTTPResponse, 2)
				for range 2 {
					go func() { <-start; results <- write("archive", revision) }()
				}
				close(start)
				first, second := <-results, <-results
				if second.status == 200 {
					first, second = second, first
				}
				changed := ok(t, first)
				require.NoError(t, second.err)
				require.Equal(t, 412, second.status, "%s", second.body)
				require.NotNil(t, changed.ArchivedAt)
				require.NotEqual(t, revision, first.etag)
				require.Equal(t, changed, ok(t, write("archive", first.etag)))
				cleared := ok(t, write("unarchive", first.etag))
				require.Nil(t, cleared.ArchivedAt)
				require.NotEqual(t, changed.Revision, cleared.Revision)
				require.Equal(t, cleared, read(t))
			})
			t.Run("retirement", func(t *testing.T) {
				retired := fixture.Seeded[8]
				retiredPath := base + "/" + retired.ID
				before := ok(t, archiveHTTP(server, http.MethodGet, retiredPath, archivefixture.Writer))
				require.Equal(t, jobdb.CellRetired, before.State)
				require.Nil(t, before.ArchivedAt)
				for _, action := range []string{"archive", "unarchive"} {
					response := archiveHTTP(server, http.MethodPost, retiredPath+"/"+action, archivefixture.Writer, archivefixture.Revision(before))
					require.NoError(t, response.err)
					require.Equal(t, 409, response.status, "%s", response.body)
				}
				require.Equal(t, before, ok(t, archiveHTTP(server, http.MethodGet, retiredPath, archivefixture.Writer)))
			})
			t.Run("scoped_authorization", func(t *testing.T) {
				before := read(t)
				for token, status := range map[string]int{"": 403, archivefixture.Reader: 403, archivefixture.Wrong: 403, archivefixture.Outside: 404} {
					for _, action := range []string{"archive", "unarchive"} {
						response := archiveHTTP(server, http.MethodPost, path+"/"+action, token, archivefixture.Revision(before))
						require.NoError(t, response.err)
						require.Equal(t, status, response.status, "%s: %s", token, response.body)
					}
					require.Equal(t, before, read(t))
				}
			})
			t.Run("pagination_and_cursor_binding", func(t *testing.T) {
				for _, filter := range []string{"", "true", "false"} {
					query := url.Values{"pageSize": {"1"}}
					if filter != "" {
						query.Set("archived", filter)
					}
					var expected, got []string
					for index, cell := range fixture.Seeded {
						if fixture.InScope(index) && cell.State == jobdb.CellActive && (filter == "" || (filter == "true") == (cell.ArchivedAt != nil)) {
							expected = append(expected, cell.ID)
						}
					}
					slices.Sort(expected)
					for pageNumber := 1; ; pageNumber++ {
						require.LessOrEqual(t, pageNumber, len(expected))
						response := archiveHTTP(server, http.MethodGet, base+"?"+query.Encode(), archivefixture.Writer)
						var page jobdb.ListCellsResponse
						require.NoError(t, response.err)
						require.Equal(t, 200, response.status, "%s", response.body)
						require.NoError(t, json.Unmarshal(response.body, &page))
						require.Len(t, page.Cells, 1)
						got = append(got, page.Cells[0].ID)
						if page.NextPageToken == "" {
							break
						}
						query.Set("pageToken", page.NextPageToken)
						if pageNumber == 1 {
							for _, change := range []string{"archived=true", "archived=false", "state=ALL", "name=cell-01"} {
								changed, err := url.ParseQuery(change)
								require.NoError(t, err)
								for key, values := range query {
									if !changed.Has(key) {
										changed[key] = values
									}
								}
								if changed.Encode() != query.Encode() {
									invalid := archiveHTTP(server, http.MethodGet, base+"?"+changed.Encode(), archivefixture.Writer)
									require.NoError(t, invalid.err)
									require.Equal(t, 400, invalid.status, "%s", invalid.body)
								}
							}
							other := archiveHTTP(server, http.MethodGet, base+"?"+query.Encode(), archivefixture.Reader)
							require.NoError(t, other.err)
							require.Equal(t, 400, other.status, "%s", other.body)
						}
					}
					require.Equal(t, expected, got)
				}
				transport := &http.Client{Transport: archiveFixtureTransport{http.DefaultTransport}}
				client, err := remote.New(server.URL, transport)
				if family == "projects" {
					client, err = remote.NewProjectRuntime(server.URL, transport)
				}
				require.NoError(t, err)
				archived := true
				for _, state := range []string{"ACTIVE", "RETIRED", "ALL"} {
					filtered, err := client.ListCells(ctx, jobdb.ListCellsRequest{TenantID: fixture.Project, Archived: &archived, State: state, Name: "cell-01", PageSize: 1})
					require.NoError(t, err)
					if state == "RETIRED" {
						require.Empty(t, filtered.Cells)
					} else {
						require.Equal(t, []jobdb.Cell{fixture.Seeded[1]}, filtered.Cells)
					}
				}
				before := read(t)
				request := jobdb.ArchiveCellRequest{CellKey: jobdb.CellKey{TenantID: fixture.Project, CellID: before.ID}, IfMatch: archivefixture.Revision(before)}
				cell, err := client.ArchiveCell(ctx, request)
				require.NoError(t, err)
				require.NotNil(t, cell.ArchivedAt)
				request.IfMatch = archivefixture.Revision(cell)
				cleared, err := client.UnarchiveCell(ctx, request)
				require.NoError(t, err)
				require.Nil(t, cleared.ArchivedAt)
			})
		})
	}
}

func TestPostgresCellArchiveVisibilityAndReopen(t *testing.T) {
	dsn := conformanceDSNFactory(t)(t)
	ctx := celltest.Context(projectfixture.Context(context.Background()))
	config := pg.Config{BlobStoreURI: "blobfs://" + t.TempDir()}
	open := func() *pg.Runtime {
		runtime, err := pg.OpenDSN(ctx, dsn, config)
		require.NoError(t, err)
		t.Cleanup(func() { _ = runtime.Close(ctx) })
		return runtime
	}
	rt := open()
	project, err := rt.CreateProject(ctx, jobdb.CreateProjectRequest{Name: "archive-visibility", RequestID: "archive-visibility"})
	require.NoError(t, err)
	tenant := project.Project.ID
	cell, err := rt.CreateCell(ctx, jobdb.CreateCellRequest{TenantID: tenant, Name: "alias", Repo: "example.test/archive"})
	require.NoError(t, err)
	current := "current"
	cell, err = rt.UpdateCell(ctx, jobdb.UpdateCellRequest{CellKey: archivefixture.Key(cell), IfMatch: archivefixture.Revision(cell), Name: &current})
	require.NoError(t, err)
	submit := func(runtime *pg.Runtime, id string) jobdb.JobHandle {
		handle, err := runtime.SubmitJob(ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: tenant, Cell: "alias", JobID: id, JobType: "work", Data: jobdb.NewTaskDataOrPanic(1)}})
		require.NoError(t, err)
		require.Equal(t, cell.ID, handle.CellID)
		return handle
	}
	handle := submit(rt, "existing")
	routes := []jobdb.Route{{JobType: "work"}}
	lease, err := rt.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: handle.JobKey, WorkerID: "existing-worker", Routes: routes, LeaseDuration: time.Minute})
	require.NoError(t, err)
	require.NotNil(t, lease)
	marked, err := rt.ArchiveCell(ctx, archivefixture.ArchiveRequest(cell))
	require.NoError(t, err)
	_, err = rt.RetireCell(ctx, jobdb.RetireCellRequest{CellKey: archivefixture.Key(cell), IfMatch: archivefixture.Revision(marked)})
	require.ErrorIs(t, err, jobdb.ErrCellBusy)
	resolved, err := lease.LookupCell(ctx, "alias")
	require.NoError(t, err)
	require.Equal(t, marked.Resolved(), resolved)
	_, err = lease.(jobdb.RenewableExecutionLease).Renew(ctx)
	require.NoError(t, err)
	final := jobdb.Chapter{Ordinal: 1, TaskType: "work", CreatedAt: time.Now().UTC(), Body: jobdb.JobAttemptOutcomeChapter{
		Outcome: jobdb.ApplicationOutputOutcome{Output: jobdb.ApplicationOutputBytes{Data: []byte(`"done"`)}},
	}}
	require.NoError(t, lease.Complete(ctx, jobdb.CompleteExecutionRequest{Status: "success", Chapter: &final}))
	history, err := rt.ListChapters(ctx, jobdb.ListChaptersRequest{JobKey: handle.JobKey})
	require.NoError(t, err)
	require.NotEmpty(t, history)
	require.NoError(t, rt.Close(ctx))
	rt = open()
	got, err := rt.GetCell(ctx, archivefixture.Key(cell))
	require.NoError(t, err)
	require.Equal(t, marked, got)
	next := submit(rt, "new")
	leases, err := rt.PollWork(ctx, jobdb.PollWorkRequest{TenantId: tenant, WorkerID: "poll", Routes: routes, CellNames: []string{"alias"}, Limit: 1, LeaseDuration: time.Minute})
	require.NoError(t, err)
	require.Len(t, leases, 1)
	require.Equal(t, next.JobKey, leases[0].Job().JobKey)
	restarted, err := rt.SubmitRestartJob(ctx, jobdb.SubmitRestartJobRequest{Job: jobdb.SubmitRestartJob{PriorJobKey: handle.JobKey, Cell: "alias", JobID: "restarted", LastStepToKeep: 0}})
	require.NoError(t, err)
	require.Equal(t, cell.ID, restarted.CellID)
	retained, err := rt.ListChapters(ctx, jobdb.ListChaptersRequest{JobKey: handle.JobKey})
	require.NoError(t, err)
	require.Equal(t, history, retained)
	cleared, err := rt.UnarchiveCell(ctx, archivefixture.ArchiveRequest(marked))
	require.NoError(t, err)
	require.Nil(t, cleared.ArchivedAt)
	require.NoError(t, rt.Close(ctx))
	got, err = open().GetCell(ctx, archivefixture.Key(cell))
	require.NoError(t, err)
	require.Equal(t, cleared, got)
}

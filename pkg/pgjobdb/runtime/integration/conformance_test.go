package integration_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	remoteruntime "github.com/colony-2/jobdb/pkg/jobdb/runtime/remote"
	"github.com/colony-2/jobdb/pkg/jobdb/runtimetest"
	pgjobdbruntime "github.com/colony-2/pgjobdb/pkg/pgjobdb/runtime"
	"github.com/colony-2/pgjobdb/pkg/pgjobdb/runtime/internal/testsupport"
	"github.com/lib/pq"
)

func TestPostgresRuntimeConformance(t *testing.T) {
	// JobDB's suite shares operation deadlines across some groups of cases.
	// Start the server before those deadlines; database isolation belongs to
	// each fixture, without paying initdb/start/stop costs for every case.
	newRuntime := conformanceRuntimeFactory(t)
	routes := runtimetest.Capabilities{
		Leases: true, Schedules: true, SchemaRegistry: true, RuntimeStorage: true,
	}
	runtimetest.RunWorkflowRuntimeConformance(t,
		runtimetest.Harness{
			Name: "postgres", Capabilities: routes,
			New: func(tb testing.TB) runtimetest.Fixture {
				runtime := newRuntime(tb)
				return runtimetest.Fixture{Runtime: runtime, SchemaRegistry: runtime, Cleanup: runtime.Close}
			},
		},
		runtimetest.Harness{
			Name: "remote-postgres", Capabilities: routes,
			New: func(tb testing.TB) runtimetest.Fixture {
				runtime := newRuntime(tb)
				server := httptest.NewServer(remoteruntime.NewServer(runtime))
				tb.Cleanup(server.Close)
				remote, err := remoteruntime.New(server.URL, server.Client())
				if err != nil {
					tb.Fatalf("start remote Postgres runtime: %v", err)
				}
				return runtimetest.Fixture{
					Runtime: remote, SchemaRegistry: remote,
					Cleanup: func(ctx context.Context) error { server.Close(); return runtime.Close(ctx) },
				}
			},
		},
	)
}

func conformanceRuntimeFactory(t *testing.T) func(testing.TB) *pgjobdbruntime.Runtime {
	t.Helper()
	dsn, stop, err := testsupport.StartEmbeddedPostgres()
	if err != nil {
		t.Fatalf("start conformance Postgres: %v", err)
	}
	t.Cleanup(stop)
	admin, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = admin.Close() })
	var next atomic.Uint64
	return func(tb testing.TB) *pgjobdbruntime.Runtime {
		tb.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		database := fmt.Sprintf("conformance_%d", next.Add(1))
		if _, err := admin.ExecContext(ctx, "CREATE DATABASE "+pq.QuoteIdentifier(database)); err != nil {
			tb.Fatalf("create conformance database: %v", err)
		}
		tb.Cleanup(func() {
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			if _, err := admin.ExecContext(ctx, "DROP DATABASE "+pq.QuoteIdentifier(database)+" WITH (FORCE)"); err != nil {
				tb.Errorf("drop conformance database: %v", err)
			}
		})
		uri, err := url.Parse(dsn)
		if err != nil {
			tb.Fatal(err)
		}
		uri.Path = "/" + database
		runtime, err := pgjobdbruntime.OpenDSN(ctx, uri.String(), pgjobdbruntime.Config{BlobStoreURI: "blobfs://" + tb.TempDir()})
		if err != nil {
			tb.Fatalf("create conformance runtime: %v", err)
		}
		tb.Cleanup(func() { _ = runtime.Close(context.Background()) })
		return runtime
	}
}

func TestConformanceFixturesAreIsolated(t *testing.T) {
	newRuntime := conformanceRuntimeFactory(t)
	first, second := newRuntime(t), newRuntime(t)
	ctx := context.Background()
	req := jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", JobID: "same-id", JobType: "job", Data: jobdb.NewTaskDataOrPanic(1)}}
	handle, err := first.SubmitJob(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := second.GetJob(ctx, handle.JobKey); !errors.Is(err, jobdb.ErrJobNotFound) {
		t.Fatalf("scheduler leaked across fixtures: %v", err)
	}
	if _, err := second.GetChapter(ctx, jobdb.ChapterRef{JobKey: handle.JobKey, Ordinal: 0}); !errors.Is(err, jobdb.ErrChapterNotFound) {
		t.Fatalf("chapter leaked across fixtures: %v", err)
	}
	req.Job.Data = jobdb.NewTaskDataOrPanic(2)
	if _, err := second.SubmitJob(ctx, req); err != nil {
		t.Fatalf("independent submission: %v", err)
	}
	if err := first.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := second.GetJob(ctx, handle.JobKey); err != nil {
		t.Fatalf("closing a fixture stopped the other runtime: %v", err)
	}
}

package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	chapterpostgres "github.com/colony-2/jobdb/pkg/jobdb/chapterstore/postgres"
	"github.com/colony-2/jobdb/pkg/jobdb/configuration"
	runtimecore "github.com/colony-2/jobdb/pkg/jobdb/runtime/core"
	"github.com/colony-2/jobdb/pkg/jobdb/runtimetest"
	schemapostgres "github.com/colony-2/jobdb/pkg/jobdb/schemastore/postgres"
	"github.com/colony-2/pgjobdb/internal/projectfixture"
	"github.com/colony-2/pgjobdb/pkg/pgjobdb/projects"
	"github.com/colony-2/pgjobdb/pkg/pgjobdb/runtime/internal/runtimeadapter"
	"github.com/stretchr/testify/require"
)

func TestResourceBackoffConformance(t *testing.T) {
	newDSN := conformanceDSNFactory(t)
	runtimetest.RunResourceBackoffConformance(t, runtimetest.ResourceBackoffHarness{Name: "postgres", Durable: true, New: func(tb testing.TB) runtimetest.ResourceBackoffFixture {
		fixture := resourceFixture(tb, newDSN(tb))
		fixture.Runtime = sharedResourceRuntime{fixture.Runtime.(*runtimecore.Runtime)}
		return fixture
	}})
}

// The shared tests submit under symbolic names. Only the submission boundary
// translates them; handles and all subsequent calls use the real project ID.
type sharedResourceRuntime struct{ *runtimecore.Runtime }

func resourceTestProject(id string) string {
	switch id {
	case "resource-tenant":
		return "c2"
	case "another-tenant":
		return "40F0meyA7Npn0VoSVtJ3cd5gXdD"
	default:
		return id
	}
}

func (rt sharedResourceRuntime) CreateCell(ctx context.Context, req jobdb.CreateCellRequest) (jobdb.Cell, error) {
	req.TenantID = resourceTestProject(req.TenantID)
	return rt.Runtime.CreateCell(ctx, req)
}

func (rt sharedResourceRuntime) SubmitJob(ctx context.Context, req jobdb.SubmitJobRequest) (jobdb.JobHandle, error) {
	req.Job.TenantId = resourceTestProject(req.Job.TenantId)
	return rt.Runtime.SubmitJob(ctx, req)
}

func resourceFixture(tb testing.TB, dsn string, observers ...func(*sql.DB)) runtimetest.ResourceBackoffFixture {
	tb.Helper()
	ctx := celltest.Context(context.Background())
	blob := "blobfs://" + tb.TempDir()
	var db *sql.DB
	var chapters *chapterpostgres.Store
	var current *runtimecore.Runtime
	var scheduler runtimeadapter.Scheduler
	var mu sync.Mutex
	sample := .5
	stage := ""
	at := time.Now().UTC()
	var barrier chan struct{}
	var release chan struct{}
	fault := func(point string) error {
		mu.Lock()
		fail := stage == point
		entered, wait := barrier, release
		if point == "validated" && entered != nil {
			barrier = nil
		} else {
			entered = nil
		}
		mu.Unlock()
		if entered != nil {
			close(entered)
			<-wait
		}
		if fail {
			return fmt.Errorf("injected resource fault at %s", point)
		}
		return nil
	}
	setTime := func(value time.Time) {
		at = value.UTC()
		_, err := db.ExecContext(ctx, `CREATE OR REPLACE FUNCTION pgjobdb.scheduler_now() RETURNS TIMESTAMPTZ LANGUAGE sql VOLATILE AS $$ SELECT '`+at.Format(time.RFC3339Nano)+`'::timestamptz; $$;`)
		require.NoError(tb, err)
	}
	prepared := false
	open := func() jobdb.ConsumerRuntime {
		var err error
		db, err = sql.Open("postgres", dsn)
		require.NoError(tb, err)
		if !prepared {
			var installed bool
			require.NoError(tb, db.QueryRowContext(ctx, `SELECT to_regclass('pgjobdb.project_installation') IS NOT NULL`).Scan(&installed))
			if !installed {
				require.NoError(tb, projectfixture.Prepare(ctx, db))
			}
			prepared = true
		}
		require.NoError(tb, projects.Verify(ctx, db))
		control := projectfixture.Control(db)
		chapters, err = chapterpostgres.NewSQLDB(ctx, db, chapterpostgres.Config{Projects: control, BlobStoreURI: blob})
		require.NoError(tb, err)
		schemas, err := schemapostgres.NewSQLDB(ctx, db, control)
		require.NoError(tb, err)
		configs, err := configuration.NewSQLStore(ctx, db, configuration.Postgres, control)
		require.NoError(tb, err)
		scheduler = runtimeadapter.Scheduler{DB: db, Chapters: chapters, ResourceFault: fault, ResourceSample: func() float64 { mu.Lock(); defer mu.Unlock(); return sample }}
		current, err = runtimecore.NewRuntime(runtimecore.Config{Projects: control, Scheduler: scheduler, Chapters: chapters, Schemas: schemas, Configurations: configs})
		require.NoError(tb, err)
		setTime(at)
		for _, observe := range observers {
			observe(db)
		}
		return current
	}
	rt := open()
	tb.Cleanup(func() { require.NoError(tb, chapters.Close(ctx)); require.NoError(tb, db.Close()) })
	return runtimetest.ResourceBackoffFixture{Runtime: rt, SetTime: setTime, SetRandom: func(value float64) { mu.Lock(); sample = value; mu.Unlock() }, Fault: func(value string) { mu.Lock(); stage = value; mu.Unlock() },
		Barrier: func() (<-chan struct{}, func()) {
			mu.Lock()
			defer mu.Unlock()
			barrier = make(chan struct{})
			release = make(chan struct{})
			done := release
			return barrier, func() { close(done) }
		},
		Reopen: func() jobdb.ConsumerRuntime {
			require.NoError(tb, chapters.Close(ctx))
			require.NoError(tb, db.Close())
			return open()
		},
		ClaimAppend: func(ctx context.Context, key jobdb.JobKey, _ time.Time) error {
			id, err := scheduler.ClaimAppend(ctx, runtimecore.ClaimAppendRequest{JobKey: key, WorkerID: "jobdb-append-resource-test", Duration: time.Minute})
			if err == nil {
				err = scheduler.ReleaseAppend(ctx, id)
			}
			return err
		},
	}
}

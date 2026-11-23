//go:build integration

package projects_test

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"path/filepath"
	"strings"
	"testing"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	"github.com/colony-2/jobdb/pkg/jobdb/projectcatalog"
	"github.com/colony-2/pgjobdb/pkg/pgjobdb"
	"github.com/colony-2/pgjobdb/pkg/pgjobdb/installer"
	"github.com/colony-2/pgjobdb/pkg/pgjobdb/projects"
	pgruntime "github.com/colony-2/pgjobdb/pkg/pgjobdb/runtime"
	embeddedpostgres "github.com/fergusstrange/embedded-postgres"
	"github.com/lib/pq"
)

const participants = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

func admin() context.Context {
	return jobdb.WithProjectAdministration(celltest.Context(context.Background()), func(context.Context) error { return nil })
}
func database(t *testing.T) (context.Context, *sql.DB, string) {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	must(t, err)
	port := uint32(l.Addr().(*net.TCPAddr).Port)
	l.Close()
	dir := t.TempDir()
	pg := embeddedpostgres.NewDatabase(embeddedpostgres.DefaultConfig().Port(port).RuntimePath(filepath.Join(dir, "runtime")).DataPath(filepath.Join(dir, "data")))
	must(t, pg.Start())
	t.Cleanup(func() { pg.Stop() })
	dsn := fmt.Sprintf("postgres://postgres:postgres@localhost:%d/postgres?sslmode=disable", port)
	db, err := sql.Open("postgres", dsn)
	must(t, err)
	t.Cleanup(func() { db.Close() })
	return admin(), db, dsn
}
func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
func initialized(t *testing.T) (context.Context, *sql.DB, string) {
	t.Helper()
	ctx, db, dsn := database(t)
	_, err := projects.Initialize(ctx, db, participants, io.Discard)
	must(t, err)
	return ctx, db, dsn
}
func create(t *testing.T, ctx context.Context, db *sql.DB, name string) jobdb.ProjectCreation {
	t.Helper()
	p, err := projects.NewControl(db, nil).CreateProject(ctx, jobdb.CreateProjectRequest{Name: name, RequestID: name})
	must(t, err)
	return p
}

func TestProjectFreshInstallationAndReopen(t *testing.T) {
	ctx, db, _ := database(t)
	if _, err := pgruntime.NewSQLDB(ctx, db, pgruntime.Config{BlobStoreURI: "blobfs://" + t.TempDir()}); err == nil {
		t.Fatal("uninstalled open succeeded")
	}
	var count int
	must(t, db.QueryRow(`SELECT count(*) FROM pg_namespace WHERE nspname='pgjobdb'`).Scan(&count))
	if count != 0 {
		t.Fatal("constructor installed storage")
	}
	var report bytes.Buffer
	receipt, err := projects.Initialize(ctx, db, participants, &report)
	must(t, err)
	if report.Len() == 0 || receipt.Protocol != 1 {
		t.Fatal("missing receipt")
	}
	control := projects.NewControl(db, nil)
	list, err := control.ListProjects(ctx, jobdb.ListProjectsRequest{})
	must(t, err)
	if len(list.Projects) != 0 {
		t.Fatal("seeded projects")
	}
	cfg := pgruntime.Config{BlobStoreURI: "blobfs://" + t.TempDir()}
	for range 2 {
		rt, err := pgruntime.NewSQLDB(ctx, db, cfg)
		must(t, err)
		must(t, rt.Close(ctx))
		must(t, projects.Verify(ctx, db))
	}
	if _, err = projects.Initialize(ctx, db, participants, io.Discard); err == nil {
		t.Fatal("initialized occupied database")
	}
}
func TestProjectCatalogReceiptsAndDeletion(t *testing.T) {
	ctx, db, dsn := initialized(t)
	p := create(t, ctx, db, "first")
	reopened, err := sql.Open("postgres", dsn)
	must(t, err)
	defer reopened.Close()
	other := projects.NewControl(reopened, func(context.Context, string) error { return nil })
	again, err := other.CreateProject(ctx, jobdb.CreateProjectRequest{Name: "first", RequestID: "first"})
	must(t, err)
	if !bytes.Equal(p.Body, again.Body) || p.ETag != again.ETag || p.Location != again.Location || p.Status != again.Status {
		t.Fatal("receipt changed")
	}
	if _, err = other.CreateProject(ctx, jobdb.CreateProjectRequest{Name: "changed", RequestID: "first"}); !errors.Is(err, jobdb.ErrProjectConflict) {
		t.Fatal(err)
	}
	if err = projects.NewControl(db, nil).DeleteProject(ctx, jobdb.DeleteProjectRequest{ID: p.Project.ID, IfMatch: p.ETag}); !errors.Is(err, jobdb.ErrProjectConflict) {
		t.Fatalf("default deletion = %v", err)
	}
	must(t, other.DeleteProject(ctx, jobdb.DeleteProjectRequest{ID: p.Project.ID, IfMatch: p.ETag}))
	again, err = other.CreateProject(ctx, jobdb.CreateProjectRequest{Name: "first", RequestID: "first"})
	must(t, err)
	if !bytes.Equal(p.Body, again.Body) {
		t.Fatal("deleted receipt changed")
	}
	replacement, err := other.CreateProject(ctx, jobdb.CreateProjectRequest{Name: "first", RequestID: "replacement"})
	must(t, err)
	if replacement.Project.ID == p.Project.ID {
		t.Fatal("reused tombstone ID")
	}
}
func TestProjectAdministrationAndFirstUse(t *testing.T) {
	ctx, db, _ := initialized(t)
	p := create(t, ctx, db, "active")
	control := projects.NewControl(db, func(context.Context, string) error { return nil })
	if _, err := control.CreateProject(celltest.Context(context.Background()), jobdb.CreateProjectRequest{Name: "forbidden", RequestID: "forbidden"}); !errors.Is(err, jobdb.ErrProjectAccessDenied) {
		t.Fatal(err)
	}
	if err := control.AdmitWrite(ctx, "active"); !errors.Is(err, jobdb.ErrProjectNotFound) {
		t.Fatal(err)
	}
	must(t, control.AdmitWrite(ctx, p.Project.ID))
	after, err := control.GetProject(ctx, p.Project.ID)
	must(t, err)
	if after.Revision != p.Project.Revision {
		t.Fatal("seal changed public revision")
	}
	if err = control.DeleteProject(ctx, jobdb.DeleteProjectRequest{ID: p.Project.ID, IfMatch: p.ETag}); !errors.Is(err, jobdb.ErrProjectConflict) {
		t.Fatal(err)
	}
	if err = control.AdmitWrite(ctx, "missing"); !errors.Is(err, jobdb.ErrProjectNotFound) {
		t.Fatal(err)
	}
}
func TestProjectGuardsAndBorrowedTransactions(t *testing.T) {
	ctx, db, _ := initialized(t)
	p := create(t, ctx, db, "guarded")
	write := `INSERT INTO public.jobdb_configuration(tenant_id,snapshot) VALUES($1,$2)`
	if _, err := db.ExecContext(ctx, write, p.Project.ID, []byte(`{"opaque":90071992547409931234}`)); !errors.Is(pgjobdb.ProjectError(err), jobdb.ErrProjectUnavailable) {
		t.Fatalf("unsealed raw write: %v", err)
	}
	tx, err := db.BeginTx(ctx, nil)
	must(t, err)
	if err = pgjobdb.AdmitProjectWrite(ctx, tx, p.Project.ID); !errors.Is(err, jobdb.ErrProjectUnavailable) {
		t.Fatal(err)
	}
	tx.Rollback()
	must(t, pgjobdb.AdmitProjectWrite(ctx, db, p.Project.ID))
	tx, err = db.BeginTx(ctx, nil)
	must(t, err)
	must(t, pgjobdb.AdmitProjectWrite(ctx, tx, p.Project.ID))
	_, err = tx.ExecContext(ctx, write, p.Project.ID, []byte(`{"opaque":90071992547409931234}`))
	must(t, err)
	tx.Rollback()
	// Rollback of resources cannot erase the independently committed first seal.
	control := projects.NewControl(db, func(context.Context, string) error { return nil })
	if err = control.DeleteProject(ctx, jobdb.DeleteProjectRequest{ID: p.Project.ID, IfMatch: p.ETag}); !errors.Is(err, jobdb.ErrProjectConflict) {
		t.Fatal(err)
	}
	_, err = db.ExecContext(ctx, write, p.Project.ID, []byte(`{"opaque":90071992547409931234}`))
	must(t, err)
	for _, query := range []string{`UPDATE public.jobdb_configuration SET tenant_id='c2'`, `TRUNCATE public.jobdb_configuration`, `INSERT INTO public.jobdb_configuration VALUES('unknown','x')`} {
		if _, err = db.ExecContext(ctx, query); err == nil {
			t.Fatalf("guard allowed %s", query)
		}
	}
}
func TestProjectHistoricalAdoptionPreservesResources(t *testing.T) {
	ctx, db, _ := database(t)
	must(t, (installer.Installer{DB: db}).Apply(ctx))
	must(t, projects.PrepareShared(ctx, db))
	payload := []byte(`{"value":9007199254740993123456789,"tenant_id":"opaque"}`)
	_, err := db.ExecContext(ctx, `INSERT INTO public.jobdb_configuration VALUES('c2',$1)`, payload)
	must(t, err)
	inventory, err := projects.Inspect(ctx, db, participants)
	must(t, err)
	if _, err = projects.Install(ctx, db, inventory, nil, io.Discard); err == nil {
		t.Fatal("missing seed accepted")
	}
	receipt, err := projects.Install(ctx, db, inventory, []projectcatalog.Seed{{ID: "c2", Name: "c2"}}, io.Discard)
	must(t, err)
	var after []byte
	must(t, db.QueryRowContext(ctx, `SELECT snapshot FROM public.jobdb_configuration WHERE tenant_id='c2'`).Scan(&after))
	if !bytes.Equal(payload, after) {
		t.Fatal("historical bytes changed")
	}
	retry, err := projects.Install(ctx, db, inventory, []projectcatalog.Seed{{ID: "c2", Name: "c2"}}, io.Discard)
	must(t, err)
	if retry.CatalogID != receipt.CatalogID {
		t.Fatal("installation retry replaced catalog")
	}
	control := projects.NewControl(db, func(context.Context, string) error { return nil })
	p, err := control.GetProject(ctx, "c2")
	must(t, err)
	if err = control.DeleteProject(ctx, jobdb.DeleteProjectRequest{ID: "c2", IfMatch: projectcatalog.ETag(p.Revision)}); !errors.Is(err, jobdb.ErrProjectConflict) {
		t.Fatal(err)
	}
}

type shortWriter struct{}

func (shortWriter) Write(p []byte) (int, error) { return len(p) - 1, nil }
func TestProjectInstallationRollbackAndInventoryFailures(t *testing.T) {
	ctx, db, _ := database(t)
	must(t, (installer.Installer{DB: db}).Apply(ctx))
	must(t, projects.PrepareShared(ctx, db))
	inventory, err := projects.Inspect(ctx, db, participants)
	must(t, err)
	if _, err = projects.Install(ctx, db, inventory, nil, shortWriter{}); !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("short report: %v", err)
	}
	var installed bool
	must(t, db.QueryRowContext(ctx, `SELECT to_regclass('pgjobdb.project_installation') IS NOT NULL`).Scan(&installed))
	if installed {
		t.Fatal("failed install committed")
	}
	_, err = db.ExecContext(ctx, `INSERT INTO public.jobdb_configuration VALUES('c2','preserve')`)
	must(t, err)
	if _, err = projects.Install(ctx, db, inventory, nil, io.Discard); err == nil {
		t.Fatal("stale inventory accepted")
	}
	_, err = db.ExecContext(ctx, `INSERT INTO public.jobdb_configuration VALUES('unsupported-id','preserve')`)
	must(t, err)
	if _, err = projects.Inspect(ctx, db, participants); err == nil {
		t.Fatal("unsupported ID remapped")
	}
}
func TestProjectDamagedGuardsAndResolution(t *testing.T) {
	ctx, db, _ := initialized(t)
	_, err := db.ExecContext(ctx, `ALTER TABLE public.jobdb_configuration DISABLE TRIGGER project_admission_row`)
	must(t, err)
	if err = projects.Verify(ctx, db); err == nil {
		t.Fatal("disabled guard accepted")
	}
	if err = projects.NewControl(db, nil).AdmitWrite(ctx, "missing"); !errors.Is(err, jobdb.ErrProjectUnavailable) {
		t.Fatal(err)
	}
	_, err = db.ExecContext(ctx, `ALTER TABLE public.jobdb_configuration ENABLE TRIGGER project_admission_row`)
	must(t, err)
	must(t, projects.Verify(ctx, db))
	conn, err := db.Conn(ctx)
	must(t, err)
	defer conn.Close()
	_, err = conn.ExecContext(ctx, `CREATE TEMP TABLE jobdb_project_catalog(singleton integer,payload bytea)`)
	must(t, err)
	// A borrowed custom session must reject its own temporary shadow.
	if err = pgjobdb.AdmitProjectWrite(ctx, conn, "c2"); !errors.Is(err, jobdb.ErrProjectUnavailable) {
		t.Fatal(err)
	}
	// Make the good connection the most recently returned idle session. Startup
	// must inspect both held sessions, not accept whichever one it sees first.
	db.SetMaxOpenConns(2)
	clean, err := db.Conn(ctx)
	must(t, err)
	must(t, conn.Close())
	must(t, clean.Close())
	if err = projects.Verify(ctx, db); err == nil {
		t.Fatal("inconsistent pool accepted")
	}
	if rt, e := pgruntime.NewSQLDB(ctx, db, pgruntime.Config{BlobStoreURI: "blobfs://" + t.TempDir()}); e == nil {
		_ = rt.Close(ctx)
		t.Fatal("runtime accepted a shadowed pooled session")
	}
}
func TestProjectLargeNumericCatalogMetadata(t *testing.T) {
	ctx, db, _ := initialized(t)
	control := projects.NewControl(db, nil)
	p, err := control.CreateProject(ctx, jobdb.CreateProjectRequest{Name: "numeric", RequestID: "numeric", Metadata: json.RawMessage(`{"huge":1e9999999,"integer":9007199254740993123456789}`)})
	must(t, err)
	must(t, control.AdmitWrite(ctx, p.Project.ID))
	_, err = db.ExecContext(ctx, `INSERT INTO public.jobdb_configuration VALUES($1,'opaque')`, p.Project.ID)
	must(t, err)
	again, err := control.CreateProject(ctx, jobdb.CreateProjectRequest{Name: "numeric", RequestID: "numeric", Metadata: json.RawMessage(`{"huge":1e9999999,"integer":9007199254740993123456789}`)})
	must(t, err)
	if !strings.Contains(string(again.Body), "9007199254740993123456789") {
		t.Fatal("numeric precision lost")
	}
}

func TestProjectOpaqueCatalogStrings(t *testing.T) {
	ctx, db, dsn := initialized(t)
	unrelated := create(t, ctx, db, "unrelated")
	must(t, pgjobdb.AdmitProjectWrite(ctx, db, unrelated.Project.ID))
	opaque := []byte{0, 255, 'o', 'l', 'd'}
	_, err := db.ExecContext(ctx, `INSERT INTO public.jobdb_configuration VALUES($1,$2)`, unrelated.Project.ID, opaque)
	must(t, err)
	type retained struct {
		request jobdb.CreateProjectRequest
		result  jobdb.ProjectCreation
	}
	var receipts []retained
	for _, example := range []struct{ name, metadata, description string }{
		{"nul-value", `{"value":"\u0000"}`, ""},
		{"nul-key", `{"\u0000":"value"}`, ""},
		{"nested", `{"nested":[{"value":"before\u0000after"}]}`, ""},
		{"description", `{}`, "description\x00with NUL"},
		{"escapes", `{"literal":"\\u0000","astral":"\ud83d\ude80","huge":1e9999999}`, "Unicode 🚀"},
	} {
		t.Run(example.name, func(t *testing.T) {
			q := jobdb.CreateProjectRequest{Name: example.name, RequestID: example.name, Metadata: json.RawMessage(example.metadata), Description: example.description}
			p, err := projects.NewControl(db, nil).CreateProject(ctx, q)
			must(t, err)
			receipts = append(receipts, retained{q, p})
			must(t, pgjobdb.AdmitProjectWrite(ctx, db, p.Project.ID))
			_, err = db.ExecContext(ctx, `INSERT INTO public.jobdb_configuration VALUES($1,$2)`, p.Project.ID, opaque)
			must(t, err)
			_, err = db.ExecContext(ctx, `UPDATE public.jobdb_configuration SET snapshot=$2 WHERE tenant_id=$1`, unrelated.Project.ID, opaque)
			must(t, err)
			_, err = pgjobdb.CreateCell(ctx, db, jobdb.CreateCellRequest{TenantID: p.Project.ID, Name: example.name, Repo: "example.test/cell", RequestID: example.name})
			must(t, err)
		})
	}
	// Close and reopen the pool; constructors/read operations must preserve the
	// catalog, creation receipts and resource bytes exactly, including NUL escapes.
	var before, after []byte
	must(t, db.QueryRowContext(ctx, `SELECT payload FROM public.jobdb_project_catalog`).Scan(&before))
	must(t, db.Close())
	reopened, err := sql.Open("postgres", dsn)
	must(t, err)
	defer reopened.Close()
	must(t, projects.Verify(ctx, reopened))
	rt, err := pgruntime.NewSQLDB(ctx, reopened, pgruntime.Config{BlobStoreURI: "blobfs://" + t.TempDir()})
	must(t, err)
	must(t, rt.Close(ctx))
	control := projects.NewControl(reopened, nil)
	for _, receipt := range receipts {
		again, err := control.CreateProject(ctx, receipt.request)
		must(t, err)
		if !bytes.Equal(receipt.result.Body, again.Body) || receipt.result.ETag != again.ETag || receipt.result.Location != again.Location || receipt.result.Status != again.Status || !bytes.Equal(receipt.result.Project.Metadata, again.Project.Metadata) || receipt.result.Project.Description != again.Project.Description {
			t.Fatal("opaque creation receipt changed after reopen")
		}
		var resource []byte
		must(t, reopened.QueryRowContext(ctx, `SELECT snapshot FROM public.jobdb_configuration WHERE tenant_id=$1`, again.Project.ID).Scan(&resource))
		if !bytes.Equal(resource, opaque) {
			t.Fatal("opaque resource bytes changed")
		}
		// A borrowed writer must read the already committed seal without decoding
		// unrelated catalog metadata or opening another connection.
		tx, err := reopened.BeginTx(ctx, nil)
		must(t, err)
		must(t, pgjobdb.AdmitProjectWrite(ctx, tx, again.Project.ID))
		_, err = tx.ExecContext(ctx, `UPDATE public.jobdb_configuration SET snapshot=$2 WHERE tenant_id=$1`, again.Project.ID, opaque)
		must(t, err)
		must(t, tx.Commit())
	}
	must(t, reopened.QueryRowContext(ctx, `SELECT payload FROM public.jobdb_project_catalog`).Scan(&after))
	if !bytes.Equal(before, after) {
		t.Fatal("reopen or receipt replay rewrote catalog bytes")
	}
}

func TestProjectAdmissionProjectionIntegrity(t *testing.T) {
	ctx, db, _ := initialized(t)
	p := create(t, ctx, db, "projection")
	must(t, pgjobdb.AdmitProjectWrite(ctx, db, p.Project.ID))
	var payload, admission []byte
	must(t, db.QueryRowContext(ctx, `SELECT payload,admission FROM public.jobdb_project_catalog`).Scan(&payload, &admission))
	var changed map[string]any
	must(t, json.Unmarshal(admission, &changed))
	changed["id"] = "different-installation"
	wrongID, err := json.Marshal(changed)
	must(t, err)
	for _, example := range []struct {
		name               string
		payload, admission []byte
	}{
		{"missing", payload, nil},
		{"malformed", payload, []byte("not json")},
		{"stale", append(bytes.Clone(payload), ' '), admission},
		{"altered", payload, wrongID},
	} {
		t.Run(example.name, func(t *testing.T) {
			_, err := db.ExecContext(ctx, `UPDATE public.jobdb_project_catalog SET payload=$1,admission=$2`, example.payload, example.admission)
			must(t, err)
			defer func() {
				_, err := db.ExecContext(ctx, `UPDATE public.jobdb_project_catalog SET payload=$1,admission=$2`, payload, admission)
				must(t, err)
			}()
			if err := projects.Verify(ctx, db); err == nil {
				t.Fatal("invalid projection verified")
			}
			if err := pgjobdb.AdmitProjectWrite(ctx, db, p.Project.ID); !errors.Is(err, jobdb.ErrProjectUnavailable) {
				t.Fatalf("native admission: %v", err)
			}
			_, err = db.ExecContext(ctx, `INSERT INTO public.jobdb_configuration VALUES($1,'opaque')`, p.Project.ID)
			if !errors.Is(pgjobdb.ProjectError(err), jobdb.ErrProjectUnavailable) {
				t.Fatalf("SQL guard: %v", err)
			}
		})
	}
	// A rejected Store callback must retain both values, without leaving a
	// separately committed admission change or creation receipt.
	_, err = projects.NewControl(db, nil).CreateProject(ctx, jobdb.CreateProjectRequest{Name: "projection", RequestID: "conflicting"})
	if !errors.Is(err, jobdb.ErrProjectConflict) {
		t.Fatal(err)
	}
	var afterPayload, afterAdmission []byte
	must(t, db.QueryRowContext(ctx, `SELECT payload,admission FROM public.jobdb_project_catalog`).Scan(&afterPayload, &afterAdmission))
	if !bytes.Equal(payload, afterPayload) || !bytes.Equal(admission, afterAdmission) {
		t.Fatal("failed creation changed catalog or admission projection")
	}
	_, err = db.ExecContext(ctx, `INSERT INTO public.jobdb_configuration VALUES($1,'opaque')`, p.Project.ID)
	must(t, err)
}

func TestProjectInstallationConflictsAndExternalInventory(t *testing.T) {
	ctx, db, _ := database(t)
	must(t, (installer.Installer{DB: db}).Apply(ctx))
	must(t, projects.PrepareShared(ctx, db))
	in, err := projects.Inspect(ctx, db, participants, "0")
	must(t, err)
	if _, err = projects.Install(ctx, db, in, []projectcatalog.Seed{{ID: "0", Name: "embedded"}}, io.Discard); err != nil {
		t.Fatal(err)
	}
	if _, err = projects.Install(ctx, db, in, []projectcatalog.Seed{{ID: "0", Name: "changed"}}, io.Discard); !errors.Is(err, jobdb.ErrProjectConflict) {
		t.Fatal(err)
	}
	must(t, projects.Verify(ctx, db))
}
func TestProjectWriterExclusionAndUnknownParticipants(t *testing.T) {
	ctx, db, _ := database(t)
	must(t, (installer.Installer{DB: db}).Apply(ctx))
	must(t, projects.PrepareShared(ctx, db))
	in, err := projects.Inspect(ctx, db, participants)
	must(t, err)
	tx, err := db.BeginTx(ctx, nil)
	must(t, err)
	_, err = tx.ExecContext(ctx, `LOCK TABLE public.jobdb_configuration IN ROW EXCLUSIVE MODE`)
	must(t, err)
	if _, err = projects.Install(ctx, db, in, nil, io.Discard); err == nil {
		t.Fatal("busy writer allowed installation")
	}
	tx.Rollback()
	_, err = db.ExecContext(ctx, `CREATE TABLE public.jobdb_unreviewed(tenant_id text)`)
	must(t, err)
	if _, err = projects.Inspect(ctx, db, participants); err == nil {
		t.Fatal("unknown participant accepted")
	}
}
func TestProjectSQLNoopAdmissionAndCopy(t *testing.T) {
	ctx, db, _ := initialized(t)
	p := create(t, ctx, db, "sql-entry")
	for _, id := range []string{p.Project.ID, "missing"} {
		_, err := db.ExecContext(ctx, `SELECT pgjobdb.release_append($1,'missing','missing')`, id)
		if !errors.Is(pgjobdb.ProjectError(err), map[bool]error{true: jobdb.ErrProjectUnavailable, false: jobdb.ErrProjectNotFound}[id == p.Project.ID]) {
			t.Fatalf("no-op admission %q: %v", id, err)
		}
	}
	must(t, pgjobdb.AdmitProjectWrite(ctx, db, p.Project.ID))
	_, err := db.ExecContext(ctx, `SELECT pgjobdb.release_append($1,'missing','missing')`, p.Project.ID)
	must(t, err)
	// COPY invokes the same row guard as INSERT.
	tx, err := db.BeginTx(ctx, nil)
	must(t, err)
	stmt, err := tx.PrepareContext(ctx, pq.CopyInSchema("public", "jobdb_configuration", "tenant_id", "snapshot"))
	must(t, err)
	_, err = stmt.Exec("unknown", []byte("opaque"))
	must(t, err)
	_, err = stmt.Exec()
	if !errors.Is(pgjobdb.ProjectError(err), jobdb.ErrProjectNotFound) {
		t.Fatalf("COPY admission: %v", err)
	}
	stmt.Close()
	tx.Rollback()
}
func TestProjectSealSurvivesDownstreamFailureAndRelaxedCommit(t *testing.T) {
	ctx, db, dsn := initialized(t)
	p := create(t, ctx, db, "durable")
	relaxed, err := sql.Open("postgres", dsn+"&synchronous_commit=off")
	must(t, err)
	defer relaxed.Close()
	must(t, pgjobdb.AdmitProjectWrite(ctx, relaxed, p.Project.ID))
	tx, err := db.BeginTx(ctx, nil)
	must(t, err)
	_, err = tx.ExecContext(ctx, `INSERT INTO public.jobdb_configuration VALUES($1,NULL)`, p.Project.ID)
	if err == nil {
		t.Fatal("expected resource constraint failure")
	}
	tx.Rollback()
	control := projects.NewControl(db, func(context.Context, string) error { return nil })
	if err = control.DeleteProject(ctx, jobdb.DeleteProjectRequest{ID: p.Project.ID, IfMatch: p.ETag}); !errors.Is(err, jobdb.ErrProjectConflict) {
		t.Fatal(err)
	}
}

func TestProjectRefusesIncompleteSharedSchema(t *testing.T) {
	for _, damage := range []string{`DROP INDEX public.jobdb_schemas_list_idx`, `ALTER TABLE public.jobdb_configuration ALTER COLUMN snapshot DROP NOT NULL`, `ALTER TABLE public.jobdb_chapter_chapters DROP CONSTRAINT jobdb_chapter_chapters_anthology_id_story_id_fkey`} {
		t.Run(damage, func(t *testing.T) {
			ctx, db, _ := database(t)
			must(t, (installer.Installer{DB: db}).Apply(ctx))
			must(t, projects.PrepareShared(ctx, db))
			_, err := db.ExecContext(ctx, damage)
			must(t, err)
			if _, err = projects.Inspect(ctx, db, participants); err == nil {
				t.Fatal("certified incomplete shared schema")
			}
			var installed bool
			must(t, db.QueryRowContext(ctx, `SELECT to_regclass('pgjobdb.project_installation') IS NOT NULL`).Scan(&installed))
			if installed {
				t.Fatal("inspection installed projects")
			}
		})
	}
}

func TestProjectRejectsUncommittedSeal(t *testing.T) {
	ctx, db, _ := initialized(t)
	p := create(t, ctx, db, "uncommitted")
	for _, savepoint := range []bool{false, true} {
		tx, err := db.BeginTx(ctx, nil)
		must(t, err)
		if savepoint {
			_, err = tx.ExecContext(ctx, `SAVEPOINT seal`)
			must(t, err)
		}
		// Simulate an independently reachable raw writer attempting to couple a first
		// seal to resource effects. Both top-level and child transaction IDs must fail.
		_, err = tx.ExecContext(ctx, `WITH changed AS (
 SELECT convert_to(jsonb_set(convert_from(payload,'UTF8')::jsonb,ARRAY['projects',$1,'sealed'],'true')::text,'UTF8') AS payload
 FROM public.jobdb_project_catalog WHERE singleton=1)
 UPDATE public.jobdb_project_catalog c SET payload=changed.payload,
 admission=convert_to(jsonb_set(jsonb_set(convert_from(c.admission,'UTF8')::jsonb,ARRAY['projects',$1,'sealed'],'true'),
 ARRAY['payloadSHA256'],to_jsonb(encode(sha256(changed.payload),'hex')))::text,'UTF8') FROM changed WHERE c.singleton=1`, p.Project.ID)
		must(t, err)
		if savepoint {
			_, err = tx.ExecContext(ctx, `RELEASE SAVEPOINT seal`)
			must(t, err)
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO public.jobdb_configuration VALUES($1,'opaque')`, p.Project.ID)
		if !errors.Is(pgjobdb.ProjectError(err), jobdb.ErrProjectUnavailable) {
			t.Fatalf("uncommitted seal: %v", err)
		}
		tx.Rollback()
	}
	must(t, projects.NewControl(db, func(context.Context, string) error { return nil }).DeleteProject(ctx, jobdb.DeleteProjectRequest{ID: p.Project.ID, IfMatch: p.ETag}))
}

func TestProjectPartialAndFutureInstallations(t *testing.T) {
	ctx, db, _ := database(t)
	must(t, (installer.Installer{DB: db}).Apply(ctx))
	must(t, projects.PrepareShared(ctx, db))
	in, err := projects.Inspect(ctx, db, participants)
	must(t, err)
	_, err = db.ExecContext(ctx, `CREATE TABLE public.jobdb_project_catalog(singleton integer PRIMARY KEY,payload bytea); INSERT INTO public.jobdb_project_catalog VALUES(1,'opaque')`)
	must(t, err)
	if _, err = projects.Inspect(ctx, db, participants); err == nil {
		t.Fatal("orphan catalog accepted")
	}
	if _, err = projects.Install(ctx, db, in, nil, io.Discard); err == nil {
		t.Fatal("orphan catalog repaired")
	}
	var payload []byte
	must(t, db.QueryRowContext(ctx, `SELECT payload FROM public.jobdb_project_catalog`).Scan(&payload))
	if string(payload) != "opaque" {
		t.Fatal("orphan catalog overwritten")
	}
	_, err = db.ExecContext(ctx, `DROP TABLE public.jobdb_project_catalog`)
	must(t, err)
	_, err = projects.Install(ctx, db, in, nil, io.Discard)
	must(t, err)
	must(t, db.QueryRowContext(ctx, `SELECT payload FROM pgjobdb.project_installation`).Scan(&payload))
	var receipt map[string]json.RawMessage
	must(t, json.Unmarshal(payload, &receipt))
	receipt["protocol"] = json.RawMessage(`999`)
	future, err := json.Marshal(receipt)
	must(t, err)
	_, err = db.ExecContext(ctx, `UPDATE pgjobdb.project_installation SET payload=$1`, future)
	must(t, err)
	if err = projects.Verify(ctx, db); err == nil {
		t.Fatal("future protocol opened")
	}
	if _, err = projects.Install(ctx, db, in, nil, io.Discard); err == nil {
		t.Fatal("future protocol rewritten")
	}
	var after []byte
	must(t, db.QueryRowContext(ctx, `SELECT payload FROM pgjobdb.project_installation`).Scan(&after))
	if !bytes.Equal(future, after) {
		t.Fatal("future receipt changed")
	}
}

func TestProjectCommittedSealSurvivesVacuum(t *testing.T) {
	ctx, db, _ := initialized(t)
	p := create(t, ctx, db, "vacuum-seal")
	must(t, pgjobdb.AdmitProjectWrite(ctx, db, p.Project.ID))
	_, err := db.ExecContext(ctx, `VACUUM FREEZE public.jobdb_project_catalog`)
	must(t, err)
	must(t, projects.Verify(ctx, db))
	_, err = db.ExecContext(ctx, `INSERT INTO public.jobdb_configuration VALUES($1,'opaque')`, p.Project.ID)
	must(t, err)
}

func TestProjectCommittedCatalogNewerThanWriter(t *testing.T) {
	ctx, db, _ := initialized(t)
	p := create(t, ctx, db, "older-writer")
	must(t, pgjobdb.AdmitProjectWrite(ctx, db, p.Project.ID))
	tx, err := db.BeginTx(ctx, nil)
	must(t, err)
	defer tx.Rollback()
	var writerXID, catalogXID uint64
	must(t, tx.QueryRowContext(ctx, `SELECT pg_current_xact_id()::text`).Scan(&writerXID))
	// A different connection commits after the resource transaction has an XID.
	// The committed catalog tuple is newer than the writer, before any wraparound.
	create(t, ctx, db, "newer-catalog")
	must(t, db.QueryRowContext(ctx, `SELECT xmin::text FROM public.jobdb_project_catalog`).Scan(&catalogXID))
	if catalogXID <= writerXID {
		t.Fatalf("fixture did not advance catalog XID: catalog=%d writer=%d", catalogXID, writerXID)
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO public.jobdb_configuration VALUES($1,'opaque')`, p.Project.ID)
	must(t, err)
	// Advance it again between resource statements, as parallel admission can do
	// between the normalized cell registry's row writes. Exercise the native API.
	create(t, ctx, db, "newer-catalog-again")
	cell, err := pgjobdb.CreateCell(ctx, tx, jobdb.CreateCellRequest{TenantID: p.Project.ID, Name: "newer-catalog-cell", Repo: "example.test/cell", RequestID: "newer-catalog-cell"})
	must(t, err)
	must(t, tx.Commit())
	stored, err := pgjobdb.GetCell(ctx, db, jobdb.CellKey{TenantID: p.Project.ID, CellID: cell.ID})
	must(t, err)
	if stored.ID != cell.ID || stored.Repo != cell.Repo {
		t.Fatal("cell write was not retained")
	}
	var payload []byte
	must(t, db.QueryRowContext(ctx, `SELECT snapshot FROM public.jobdb_configuration WHERE tenant_id=$1`, p.Project.ID).Scan(&payload))
	if string(payload) != "opaque" {
		t.Fatal("resource bytes changed")
	}
}

func TestProjectConcurrentRetainedCreationAndDeletion(t *testing.T) {
	ctx, db, dsn := initialized(t)
	other, err := sql.Open("postgres", dsn)
	must(t, err)
	defer other.Close()
	type result struct {
		p   jobdb.ProjectCreation
		err error
	}
	results := make(chan result, 8)
	start := make(chan struct{})
	for i := 0; i < 8; i++ {
		go func(i int) {
			<-start
			pool := db
			if i%2 == 0 {
				pool = other
			}
			p, e := projects.NewControl(pool, nil).CreateProject(ctx, jobdb.CreateProjectRequest{Name: "concurrent", RequestID: "concurrent"})
			results <- result{p, e}
		}(i)
	}
	close(start)
	first := <-results
	must(t, first.err)
	for i := 1; i < 8; i++ {
		r := <-results
		must(t, r.err)
		if r.p.Project.ID != first.p.Project.ID || !bytes.Equal(r.p.Body, first.p.Body) || r.p.Location != first.p.Location || r.p.ETag != first.p.ETag || r.p.Status != first.p.Status {
			t.Fatal("creation receipt split across connections")
		}
	}
	// Catalog row locking orders deletion and first use: exactly one can succeed.
	control := projects.NewControl(db, func(context.Context, string) error { return nil })
	admits := make(chan error, 1)
	deletes := make(chan error, 1)
	start = make(chan struct{})
	go func() { <-start; admits <- control.AdmitWrite(ctx, first.p.Project.ID) }()
	go func() {
		<-start
		deletes <- control.DeleteProject(ctx, jobdb.DeleteProjectRequest{ID: first.p.Project.ID, IfMatch: first.p.ETag})
	}()
	close(start)
	admitted, deleted := <-admits, <-deletes
	if admitted == nil {
		if !errors.Is(deleted, jobdb.ErrProjectConflict) {
			t.Fatalf("seal did not exclude deletion: %v", deleted)
		}
	} else {
		must(t, deleted)
		if !errors.Is(admitted, jobdb.ErrProjectNotFound) {
			t.Fatalf("deleted project admitted: %v", admitted)
		}
	}
	replay, err := projects.NewControl(other, nil).CreateProject(ctx, jobdb.CreateProjectRequest{Name: "concurrent", RequestID: "concurrent"})
	must(t, err)
	if !bytes.Equal(first.p.Body, replay.Body) {
		t.Fatal("concurrent first-use/deletion changed retained creation")
	}
}

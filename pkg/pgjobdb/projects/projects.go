// Package projects supplies explicit PostgreSQL project maintenance. Opening a
// runtime never invokes these operations. Operators must fence all participants
// and review external inventory before initializing or adopting a database.
package projects

import (
	"context"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"slices"
	"sort"
	"strings"

	"github.com/colony-2/jobdb/pkg/jobdb"
	chapterpostgres "github.com/colony-2/jobdb/pkg/jobdb/chapterstore/postgres"
	"github.com/colony-2/jobdb/pkg/jobdb/configuration"
	"github.com/colony-2/jobdb/pkg/jobdb/projectcatalog"
	schemapostgres "github.com/colony-2/jobdb/pkg/jobdb/schemastore/postgres"
	"github.com/colony-2/pgjobdb/pkg/pgjobdb/installer"
	"github.com/colony-2/pgjobdb/pkg/pgjobdb/internal/projectstorage"
	"github.com/segmentio/ksuid"
)

const Protocol = projectstorage.Protocol

// Inventory binds exact database content and separately reviewed participants.
// ExternalIDs explicitly enumerates identities found only in that external review.
// Neither the digest nor this operation proves external fencing or coverage.
type Inventory struct {
	Protocol           int      `json:"protocol"`
	ParticipantsSHA256 string   `json:"participantsSHA256"`
	ResourcesSHA256    string   `json:"resourcesSHA256"`
	StructureSHA256    string   `json:"structureSHA256"`
	TenantIDs          []string `json:"tenantIDs"`
	ExternalIDs        []string `json:"externalIDs"`
}
type Receipt = projectstorage.Receipt

func validDigest(s string) bool {
	b, err := hex.DecodeString(s)
	return err == nil && len(b) == 32 && s == strings.ToLower(s)
}
func validID(id string) bool {
	if id == "c2" || id == "0" {
		return true
	}
	_, err := ksuid.Parse(id)
	return err == nil
}
func begin(ctx context.Context, db *sql.DB, readOnly bool) (*sql.Tx, error) {
	if ctx == nil || db == nil {
		return nil, fmt.Errorf("projects require context and database")
	}
	level := sql.LevelReadCommitted
	if readOnly {
		level = sql.LevelRepeatableRead
	}
	return db.BeginTx(ctx, &sql.TxOptions{ReadOnly: readOnly, Isolation: level})
}

// Inspect captures a read-only, repeatable snapshot. It does not create missing
// tables or normalize stored JSON, bytes, identities or historical records.
func Inspect(ctx context.Context, db *sql.DB, participantsSHA256 string, externalIDs ...string) (Inventory, error) {
	if !validDigest(participantsSHA256) {
		return Inventory{}, fmt.Errorf("participants SHA-256 must be 64 lowercase hex digits")
	}
	tx, err := begin(ctx, db, true)
	if err != nil {
		return Inventory{}, err
	}
	defer tx.Rollback()
	return inspect(ctx, tx, participantsSHA256, externalIDs)
}
func inspect(ctx context.Context, tx *sql.Tx, participants string, external []string) (Inventory, error) {
	in := Inventory{Protocol: Protocol, ParticipantsSHA256: participants, TenantIDs: []string{}, ExternalIDs: append([]string{}, external...)}
	sort.Strings(in.ExternalIDs)
	for i, id := range in.ExternalIDs {
		if !validID(id) || i > 0 && id == in.ExternalIDs[i-1] {
			return in, fmt.Errorf("invalid or duplicate external project ID")
		}
	}
	if err := projectstorage.CheckResolution(ctx, tx); err != nil {
		return in, err
	}
	var version int
	if err := tx.QueryRowContext(ctx, `SELECT format_version FROM pgjobdb.installation WHERE name='pgjobdb'`).Scan(&version); err != nil {
		return in, err
	}
	if version != installer.FormatVersion {
		return in, fmt.Errorf("project installation requires scheduler format %d, found %d; convert explicitly first", installer.FormatVersion, version)
	}
	if err := installer.VerifyTransaction(ctx, tx); err != nil {
		return in, err
	}
	// No extension participant may disappear into an assertion of empty storage.
	rows, err := tx.QueryContext(ctx, `SELECT n.nspname||'.'||c.relname,c.relkind::text,c.relispartition FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE (n.nspname='pgjobdb' OR n.nspname='public' AND c.relname LIKE 'jobdb_%') AND c.relkind IN ('r','p','f','m') ORDER BY 1`)
	if err != nil {
		return in, err
	}
	seen := map[string]bool{}
	for rows.Next() {
		var name, kind string
		var partition bool
		if err = rows.Scan(&name, &kind, &partition); err != nil {
			break
		}
		if kind != "r" || partition {
			err = fmt.Errorf("unsupported participant arrangement: %s", name)
			break
		}
		if _, known := projectstorage.Tables[name]; !known && name != "pgjobdb.resource_configuration" && name != "pgjobdb.installation" && name != "pgjobdb.project_installation" && name != "public.jobdb_project_catalog" {
			err = fmt.Errorf("unknown participant table: %s", name)
			break
		}
		seen[name] = true
	}
	scanErr := rows.Err()
	rows.Close()
	if err != nil {
		return in, err
	}
	if scanErr != nil {
		return in, scanErr
	}
	for _, name := range projectstorage.TableNames() {
		if !seen[name] {
			return in, fmt.Errorf("missing participant table: %s", name)
		}
	}
	if err := projectstorage.VerifyShared(ctx, tx); err != nil {
		return in, err
	}
	var projectArtifacts bool
	if err := tx.QueryRowContext(ctx, `SELECT to_regclass('public.jobdb_project_catalog') IS NOT NULL OR to_regclass('pgjobdb.project_installation') IS NOT NULL OR EXISTS(SELECT 1 FROM pg_proc WHERE pronamespace='pgjobdb'::regnamespace AND proname IN ('_project_write_check','_project_row_guard','_project_no_truncate')) OR EXISTS(SELECT 1 FROM pg_trigger WHERE tgname IN ('project_admission_row','project_admission_truncate'))`).Scan(&projectArtifacts); err != nil {
		return in, err
	}
	if projectArtifacts {
		if err := projectstorage.Verify(ctx, tx); err != nil {
			return in, fmt.Errorf("partial or unsupported project installation: %w", err)
		}
	}
	// Server encodes each typed value, including BYTEA and raw JSON tokens. UTC and
	// ISO dates keep output stable; only these local snapshot settings are changed.
	if _, err = tx.ExecContext(ctx, `SET LOCAL timezone='UTC'; SET LOCAL datestyle='ISO, YMD'; SET LOCAL bytea_output='hex'; SET LOCAL extra_float_digits=3`); err != nil {
		return in, err
	}
	allRows := []string{}
	ids := map[string]bool{}
	for _, table := range projectstorage.TableNames() {
		column := projectstorage.Tables[table]
		query := `SELECT row_to_json(t)::text,` + column + ` FROM ` + table + ` t`
		rows, err = tx.QueryContext(ctx, query)
		if err != nil {
			return in, err
		}
		for rows.Next() {
			var raw, id string
			if err = rows.Scan(&raw, &id); err != nil {
				break
			}
			ids[id] = true
			allRows = append(allRows, fmt.Sprintf("%d:%s%d:%s", len(table), table, len(raw), raw))
		}
		scanErr = rows.Err()
		rows.Close()
		if err != nil {
			return in, err
		}
		if scanErr != nil {
			return in, scanErr
		}
	}
	rows, err = tx.QueryContext(ctx, `SELECT DISTINCT base_anthology_id FROM public.jobdb_chapter_stories WHERE base_anthology_id IS NOT NULL`)
	if err != nil {
		return in, err
	}
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			break
		}
		ids[id] = true
	}
	scanErr = rows.Err()
	rows.Close()
	if err != nil {
		return in, err
	}
	if scanErr != nil {
		return in, scanErr
	}
	for id := range ids {
		if !validID(id) {
			return in, fmt.Errorf("unsupported historical project ID %q; no remapping performed", id)
		}
		in.TenantIDs = append(in.TenantIDs, id)
	}
	sort.Strings(in.TenantIDs)
	for _, id := range in.ExternalIDs {
		if ids[id] {
			return in, fmt.Errorf("external-only ID is already in database inventory: %s", id)
		}
	}
	sort.Strings(allRows)
	raw, _ := json.Marshal(allRows)
	in.ResourcesSHA256 = projectstorage.Digest(raw)
	in.StructureSHA256, err = projectstorage.Structure(ctx, tx)
	return in, err
}

// Initialize explicitly prepares empty storage and installs zero projects. A
// preparation error can leave infrastructure, never a completion receipt. Do not
// use Initialize to repair partially prepared or historical databases.
func Initialize(ctx context.Context, db *sql.DB, participantsSHA256 string, report io.Writer) (Receipt, error) {
	if err := jobdb.RequireProjectAdministration(ctx); err != nil {
		return Receipt{}, err
	}
	if !validDigest(participantsSHA256) {
		return Receipt{}, jobdb.ErrInvalidProject
	}
	if report == nil {
		return Receipt{}, fmt.Errorf("staged installation report writer is required")
	}
	if db == nil || ctx == nil {
		return Receipt{}, fmt.Errorf("projects require context and database")
	}
	if db.Stats().MaxOpenConnections == 1 {
		return Receipt{}, fmt.Errorf("fresh preparation requires at least two pool connections")
	}
	// A separate session lock serializes fresh initializers across the public
	// schema constructors, whose transactions cannot share our installer lock.
	conn, err := db.Conn(ctx)
	if err != nil {
		return Receipt{}, err
	}
	defer conn.Close()
	if _, err = conn.ExecContext(ctx, `SELECT pg_advisory_lock(hashtextextended('pgjobdb.project.initialize',0))`); err != nil {
		return Receipt{}, err
	}
	defer conn.ExecContext(context.WithoutCancel(ctx), `SELECT pg_advisory_unlock(hashtextextended('pgjobdb.project.initialize',0))`)
	var occupied bool
	if err = conn.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='pgjobdb' OR n.nspname='public' AND c.relname LIKE 'jobdb_%')`).Scan(&occupied); err != nil {
		return Receipt{}, err
	}
	if occupied {
		return Receipt{}, fmt.Errorf("initialize requires empty JobDB storage; inspect existing or partial storage explicitly")
	}
	if err = (installer.Installer{DB: db}).Apply(ctx); err != nil {
		return Receipt{}, fmt.Errorf("prepare scheduler: %w", err)
	}
	if err = PrepareShared(ctx, db); err != nil {
		return Receipt{}, fmt.Errorf("prepare shared storage: %w", err)
	}
	inventory, err := Inspect(ctx, db, participantsSHA256)
	if err != nil {
		return Receipt{}, err
	}
	if len(inventory.TenantIDs) != 0 {
		return Receipt{}, fmt.Errorf("fresh initialization acquired resources; keep writers fenced")
	}
	return Install(ctx, db, inventory, nil, report)
}

// PrepareShared is an explicit maintenance step for historical databases which
// have never used shared stores. It refuses partial sets; it imports no identity.
// Fence participants before calling it. Existing resource tables are untouched.
func PrepareShared(ctx context.Context, db *sql.DB) error {
	if err := jobdb.RequireProjectAdministration(ctx); err != nil {
		return err
	}
	var count int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM pg_class WHERE relnamespace='public'::regnamespace AND relname IN ('jobdb_configuration','jobdb_schemas','jobdb_chapter_stories','jobdb_chapter_chapters','jobdb_chapter_artifacts') AND relkind='r'`).Scan(&count); err != nil {
		return err
	}
	if count != 0 && count != 5 {
		return fmt.Errorf("partial shared storage requires diagnosis")
	}
	if count == 5 {
		return nil
	}
	if err := projectstorage.CheckResolution(ctx, db); err != nil {
		return err
	}
	dir, err := os.MkdirTemp("", "pgjobdb-maintenance-blobs-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)
	chapters, err := chapterpostgres.NewSQLDB(ctx, db, chapterpostgres.Config{BlobStoreURI: "blobfs://" + dir})
	if err != nil {
		return err
	}
	if err = chapters.Close(ctx); err != nil {
		return err
	}
	if _, err = schemapostgres.NewSQLDB(ctx, db); err != nil {
		return err
	}
	_, err = configuration.NewSQLStore(ctx, db, configuration.Postgres)
	return err
}

func validateInventory(in Inventory, seeds []projectcatalog.Seed) error {
	if in.Protocol != Protocol || !validDigest(in.ParticipantsSHA256) || !validDigest(in.ResourcesSHA256) || !validDigest(in.StructureSHA256) {
		return jobdb.ErrInvalidProject
	}
	ids := append(slices.Clone(in.TenantIDs), in.ExternalIDs...)
	sort.Strings(ids)
	if len(ids) != len(seeds) {
		return fmt.Errorf("approved seeds must cover exactly database and external-only inventory IDs")
	}
	mapping := map[string]string{}
	for _, s := range seeds {
		if !validID(s.ID) || mapping[s.ID] != "" || s.ID == "c2" && s.Name != "c2" {
			return jobdb.ErrInvalidProject
		}
		mapping[s.ID] = s.Name
	}
	for i, id := range ids {
		if !validID(id) || i > 0 && id == ids[i-1] || mapping[id] == "" {
			return fmt.Errorf("missing or duplicate inventory mapping: %s", id)
		}
	}
	return nil
}

// Install rechecks inventory under NOWAIT table exclusion and atomically commits
// public catalog installation, resource guards and the completion receipt. The
// report is staged before commit; only a nil return confirms this commit.
func Install(ctx context.Context, db *sql.DB, in Inventory, seeds []projectcatalog.Seed, report io.Writer) (Receipt, error) {
	var receipt Receipt
	if err := jobdb.RequireProjectAdministration(ctx); err != nil {
		return receipt, err
	}
	if report == nil {
		return receipt, fmt.Errorf("staged installation report writer is required")
	}
	if err := validateInventory(in, seeds); err != nil {
		return receipt, err
	}
	seeds = append([]projectcatalog.Seed{}, seeds...)
	slices.SortFunc(seeds, func(a, b projectcatalog.Seed) int { return strings.Compare(a.ID, b.ID) })
	tx, err := begin(ctx, db, false)
	if err != nil {
		return receipt, err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, `SET LOCAL synchronous_commit=on; SELECT pg_advisory_xact_lock(hashtextextended('pgjobdb.installation',0))`); err != nil {
		return receipt, err
	}
	// Retry after uncertain commit is read/verify only, including after resource
	// traffic. It must not substitute a fresh inventory for the retained descriptor.
	var exists bool
	if err = tx.QueryRowContext(ctx, `SELECT to_regclass('pgjobdb.project_installation') IS NOT NULL`).Scan(&exists); err != nil {
		return receipt, err
	}
	raw, _ := json.Marshal(in)
	seedRaw, _ := json.Marshal(seeds)
	if exists {
		receipt, err = projectstorage.ReadReceipt(ctx, tx)
		if err != nil {
			return receipt, fmt.Errorf("partial installation receipt: %w", err)
		}
		oldSeeds, _ := json.Marshal(receipt.Seeds)
		if string(receipt.Inventory) != string(raw) || string(oldSeeds) != string(seedRaw) {
			return receipt, jobdb.ErrProjectConflict
		}
		if err = projectstorage.Verify(ctx, tx); err != nil {
			return receipt, err
		}
		svc := projectcatalog.Service{Store: projectstorage.TxStore{Tx: tx}}
		if err = svc.Install(ctx, projectcatalog.Installation{Manifest: projectstorage.Digest(raw), Seeds: seeds}); err != nil {
			return receipt, err
		}
		if err = writeReport(report, receipt); err != nil {
			return receipt, err
		}
		return receipt, nil
	}
	if _, err = tx.ExecContext(ctx, "LOCK TABLE "+strings.Join(append(projectstorage.TableNames(), "pgjobdb.installation"), ",")+" IN ACCESS EXCLUSIVE MODE NOWAIT"); err != nil {
		return receipt, fmt.Errorf("exclude installation writers: %w", err)
	}
	actual, err := inspect(ctx, tx, in.ParticipantsSHA256, in.ExternalIDs)
	if err != nil {
		return receipt, err
	}
	current, _ := json.Marshal(actual)
	if string(raw) != string(current) {
		return receipt, fmt.Errorf("inventory changed; inspect and review again")
	}
	if _, err = tx.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS public.jobdb_project_catalog(singleton INTEGER PRIMARY KEY CHECK(singleton=1),payload BYTEA,admission BYTEA); INSERT INTO public.jobdb_project_catalog(singleton,payload,admission) VALUES(1,NULL,NULL) ON CONFLICT(singleton) DO NOTHING; CREATE TABLE pgjobdb.project_installation(singleton INTEGER PRIMARY KEY CHECK(singleton=1),payload BYTEA NOT NULL)`); err != nil {
		return receipt, err
	}
	svc := projectcatalog.Service{Store: projectstorage.TxStore{Tx: tx}}
	if err = svc.Install(ctx, projectcatalog.Installation{Manifest: projectstorage.Digest(raw), Seeds: seeds}); err != nil {
		return receipt, err
	}
	if err = projectstorage.InstallGuards(ctx, tx, projectstorage.TableNames()); err != nil {
		return receipt, err
	}
	state, err := (projectstorage.TxStore{Tx: tx}).Read(ctx)
	if err != nil {
		return receipt, err
	}
	receipt = Receipt{Protocol: Protocol, Inventory: raw, Seeds: seeds, CatalogID: state.ID, CatalogManifest: state.Manifest, GuardSource: projectstorage.GuardDigest()}
	receipt.Structure, err = projectstorage.Structure(ctx, tx)
	if err != nil {
		return receipt, err
	}
	payload, _ := json.Marshal(receipt)
	if _, err = tx.ExecContext(ctx, `INSERT INTO pgjobdb.project_installation VALUES(1,$1)`, payload); err != nil {
		return receipt, err
	}
	if err = projectstorage.Verify(ctx, tx); err != nil {
		return receipt, err
	}
	if err = writeReport(report, receipt); err != nil {
		return receipt, err
	}
	if err = tx.Commit(); err != nil {
		return receipt, fmt.Errorf("installation commit uncertain; reconcile retained receipt before retry: %w", err)
	}
	return receipt, nil
}
func writeReport(w io.Writer, r Receipt) error {
	b, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return err
	}
	b = append(b, '\n')
	n, err := w.Write(b)
	if err == nil && n != len(b) {
		err = io.ErrShortWrite
	}
	return err
}
func Verify(ctx context.Context, db *sql.DB) error {
	if ctx == nil || db == nil {
		return fmt.Errorf("projects require context and database")
	}
	if err := (installer.Installer{DB: db}).Verify(ctx); err != nil {
		return err
	}
	// Hold several sessions at once to expose inconsistent existing pool settings.
	// Changing a caller-owned pool's configuration after this check is unsupported.
	count := db.Stats().MaxOpenConnections
	if count == 0 || count > 8 {
		count = 8
	}
	conns := []*sql.Conn{}
	defer func() {
		for _, c := range conns {
			c.Close()
		}
	}()
	for i := 0; i < count; i++ {
		conn, err := db.Conn(ctx)
		if err != nil {
			return err
		}
		conns = append(conns, conn)
		if err = projectstorage.Verify(ctx, conn); err != nil {
			return err
		}
	}
	return nil
}
func NewControl(db *sql.DB, canDelete func(context.Context, string) error) jobdb.ProjectControl {
	return projectstorage.Control(db, canDelete)
}

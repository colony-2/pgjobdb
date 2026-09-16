# pgjobdb

`pgjobdb` is the typed Postgres scheduler used by JobDB's direct runtime.
It has no dependency on JobDB.

Import `github.com/colony-2/jobdb/pkg/jobdb/runtime/direct` to use the JobDB
workflow runtime on Postgres. Its `NewSQLDB(ctx, db, cfg)` constructor wraps a
caller-owned `*sql.DB`; `OpenDSN(ctx, dsn, cfg)` owns the connection. Both
require a `BlobStoreURI` for chapter artifacts. Call `Close` when finished.

The first deployment requires a brand-new empty Postgres database. Startup
creates the `pgjobdb` schema and the JobDB chapter and schema tables. Later
starts accept an initialized `pgjobdb` installation. Startup rejects old
`pgwf` databases, other configured scheduler schemas, and a fresh database
that already contains user tables. There is no data migration or dual-read
period.

The `github.com/colony-2/pgjobdb/pkg/pgjobdb` package exposes typed scheduler
operations. Its `installer` subpackage installs and verifies the embedded
Postgres schema. JobDB's direct runtime adapts those operations to its public
workflow core. Applications using another backend can import only that
backend and JobDB core.

The repository layout follows JobDB's module: public Go packages live under
`pkg/pgjobdb`, while the module root holds documentation and module metadata.

Run the current checks with:

```sh
go test ./...
```

The JobDB and pgjobdb commits in this workspace are local. Until both are
published, check JobDB with a temporary Go workspace containing the two
checkouts and a replacement for the pinned pgjobdb version. Do not commit
absolute-path replacements to either module.

Publish in this order: this pgjobdb module, then JobDB with the direct runtime
that imports it. The JobDB CLI continues to provide the Postgres backend.

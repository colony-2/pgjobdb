# pgjobdb

`pgjobdb` is the Postgres scheduler and optional runtime adapter for JobDB.
Applications that choose another runtime do not need to import this module.

Import `github.com/colony-2/pgjobdb/runtime` to use the JobDB workflow
runtime on Postgres. Its `New(ctx, db, cfg)` constructor initializes a new
database and wraps a caller-owned `*sql.DB`; `OpenDSN(ctx, dsn, cfg)` owns the
connection. Both require a `BlobStoreURI` for chapter artifacts. Call `Close`
when finished.

The first deployment requires a brand-new empty Postgres database. Startup
creates the `pgjobdb` schema and the JobDB chapter and schema tables. Later
starts accept an initialized `pgjobdb` installation. Startup rejects old
`pgwf` databases, other configured scheduler schemas, and a fresh database
that already contains user tables. There is no data migration or dual-read
period.

The root `github.com/colony-2/pgjobdb` package exposes typed scheduler
operations. The optional `runtime` package composes those operations with
JobDB's public workflow core. JobDB's `pkg/jobdb` and `runtime/core` packages
do not import this module, so an application using another backend can import
only that backend.

Run the current checks with:

```sh
go test ./...
```

The JobDB and pgjobdb commits in this workspace are local. Until both are
published, run checks with a temporary Go workspace containing the two
checkouts and replacements for the pinned module versions. Do not commit
absolute-path replacements to either module.

Publish in this order: the JobDB core commit that adds the public runtime
ports and facade, this pgjobdb module, then the JobDB direct wrapper that
imports pgjobdb. The JobDB CLI uses that wrapper; applications can import
`github.com/colony-2/pgjobdb/runtime` directly.

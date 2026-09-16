# pgjobdb

`pgjobdb` is the Postgres scheduler and optional runtime adapter for JobDB.
Applications that choose another runtime do not need to import this module.

Import `github.com/colony-2/pgjobdb/runtime` to use the JobDB workflow
runtime on Postgres. Its `New(ctx, db, cfg)` constructor initializes a new
database and wraps a caller-owned `*sql.DB`; `OpenDSN(ctx, dsn, cfg)` owns the
connection. Both require a `BlobStoreURI` for chapter artifacts. Call `Close`
when finished.

This repository is under construction. Its first deployment requires a new,
empty Postgres database. It does not migrate or adopt existing `pgwf` data.

Run the current checks with:

```sh
go test ./...
```

Until the pinned JobDB core commit is available remotely, run checks in a
temporary Go workspace containing local `jobdb` and `pgjobdb` checkouts, with
a workspace replacement for the pinned JobDB version. The workspace file is
local to the developer and is not committed to this module.

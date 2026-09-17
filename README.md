# pgjobdb

`pgjobdb` provides a typed Postgres scheduler and a JobDB workflow runtime.
The scheduler is independent of JobDB; the runtime adapter uses JobDB's public
core and Postgres chapter and schema stores.

Import `github.com/colony-2/pgjobdb/pkg/pgjobdb/runtime` to use the JobDB
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
Postgres schema. The `runtime` subpackage adapts those operations to JobDB's
public workflow core. Applications using another backend can import only that
backend and JobDB core.

The `cmd/jobdb` executable adds `direct` and `serve` commands to JobDB's base
SQLite, toy, and healthcheck CLI. Build or run it from this repository to serve
the Postgres runtime:

```sh
go run ./cmd/jobdb direct --postgres-dsn "$JOBDB_POSTGRES_DSN" --blob-store-uri 'blobfs:///tmp/jobdb-blobs'
```

## Container image

Tagged pgjobdb releases publish Linux AMD64 and ARM64 images at
`ghcr.io/colony-2/pgjobdb:<release-tag>`. The image runs `/jobdb serve`, with
a fixed stateless Postgres and remote blob-store configuration. It runs as
UID/GID `65532:65532`, supports a read-only root filesystem, and exposes
`GET /healthz` with a container health check.

Set `JOBDB_POSTGRES_DSN`, `JOBDB_BLOB_STORE_URI` (an `s3://`, `gs://`, or
`azblob://` URI), and either `JOBDB_LEASE_TOKEN_SIGNING_KEY` or
`JOBDB_LEASE_TOKEN_SIGNING_KEY_FILE`. The signing key must be base64 encoded
and decode to at least 32 bytes. Optional `JOBDB_LISTEN` defaults to
`0.0.0.0:8080`, and `JOBDB_MAX_INLINE_ARTIFACT_BYTES` defaults to `4096`.

```sh
docker run --rm --read-only --tmpfs /tmp:rw,noexec,nosuid,size=64m \
  --publish 8080:8080 \
  --env JOBDB_POSTGRES_DSN \
  --env JOBDB_BLOB_STORE_URI='s3://jobdb-artifacts?region=us-east-1' \
  --env JOBDB_LEASE_TOKEN_SIGNING_KEY \
  ghcr.io/colony-2/pgjobdb:<release-tag>
```

The image release and its Postgres/MinIO smoke test are defined in this repo.
The base JobDB release publishes only its SQLite and toy CLI artifacts.

The repository layout follows JobDB's module: public Go packages live under
`pkg/pgjobdb`, while the module root holds documentation and module metadata.

Run the current checks with:

```sh
go test ./...
```

The JobDB and pgjobdb commits in this workspace are local. Until both are
published, check them with a temporary Go workspace containing the two
checkouts and replacements for the pinned module versions. Do not commit
absolute-path replacements to either module.

Publish the JobDB core and base CLI version first, then this pgjobdb module.

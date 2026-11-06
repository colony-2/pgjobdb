# pgjobdb

`pgjobdb` provides a typed Postgres scheduler and a JobDB workflow runtime.
The scheduler owns PostgreSQL state and uses JobDB's public goal/dependency types;
the runtime adapter uses JobDB's core and Postgres chapter and schema stores.

Import `github.com/colony-2/pgjobdb/pkg/pgjobdb/runtime` to use the JobDB
workflow runtime on Postgres. Its `NewSQLDB(ctx, db, cfg)` constructor wraps a
caller-owned `*sql.DB`; `OpenDSN(ctx, dsn, cfg)` owns the connection. Both
require a `BlobStoreURI` for chapter artifacts. Call `Close` when finished.

The first deployment requires a brand-new empty Postgres database. Startup
creates the `pgjobdb` schema and the JobDB chapter and schema tables. Later
starts accept an initialized format-8 `pgjobdb` installation. The cells
upgrade requires a fresh database; format 7 and older are rejected unchanged.
Startup also rejects old `pgwf` databases, other configured scheduler schemas, and a fresh database
that already contains user tables. There is no migration from earlier formats or dual-read period.

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

## Cells

Every submitted job requires an explicitly registered cell name. Submission never
creates cells; `cellRepo` optionally asserts the existing repository. Cell names are
unique and mutable; IDs and job repository snapshots are stable. PATCH supports
repo, name, description, and metadata updates. Cells are retired rather than deleted.

Normal execution leases can explicitly register cells with `lease.CreateCell`.
Delegated leases require a `cells.create` name selector. Creation is restricted to
the lease's tenant and does not expand its job-submission scope.

```sh
jobdb cell create --tenant acme --name root --repo github.com/acme/root --url http://localhost:8080
jobdb cell create --tenant acme --name payments --repo github.com/acme/payments --url http://localhost:8080
```

See [cells](docs/cells.md) for API, authorization, transaction guarantees, and the
fresh-state cutover. `root` is an ordinary name with no special authority or
rename restrictions. The `create-root` command has been removed.

## JobDB consumer API

The current dependency is JobDB `v0.0.68-0.20261104205641-8797908eaf7c`.
Engine submission returns a `workflow.JobHandle`; use its `JobKey`
for job operations and `GoalID` for goal dependencies. Engine restart submission
takes `SubmitRestartJobRequest{Job: ...}` and also returns a handle. Restarts
inherit the source job's metadata. Goal and pending-dependency discovery are
available through both direct and HTTP runtimes.

Ordinary runtime job lists return compact summaries using one cell-registry
snapshot per requested tenant and one row query per page. They retain full metadata, status, stored route, primary goal ID,
payload revision, and a small pending-wait summary. Use `GetJob` for payloads,
execution state, prerequisite lists, additional goal memberships, completion
text, and dependency explanations. The native scheduler exposes this path as
`ListJobSummaries`; its existing `ListJobs` detail API remains available. See
[compact job lists](docs/compact-job-lists.md) for schema updates, verification,
and query plans.

JobDB supports versioned execution scopes, deferred task input construction,
and scope-based restarts through `workflow.ExecutionWorker`. See the
[pgjobdb integration guide](GUIDE-PGJOBDB-EXECUTION-SCOPES.md) for consumer cutover,
server upgrades, and dependency-deadline behavior. JobDB now uses only the
versioned execution runner: direct `JobWorker.Run` implementations take
`ExecutionContext`, and callbacks use `ExecutionWorker`. Task declarations require
stable sites, contract versions, valid JSON commands, and deferred input builders.
Execution protocol 3 requires a fresh-job cutover: delete pre-cutover workflow
jobs, including those with protocol 1 or 2 execution histories, before deployment.
See the [legacy-runner removal review](REVIEW-Legacy-Runner-Removal.md) for the
pgjobdb migration and verification coverage.

Job goals support shared restart candidates, explicit goal failure, and durable
job/goal dependency waits through both direct and HTTP runtimes. Each job has an
immutable primary goal ID on its active row and retained facts. Successful
completion resolves open goals and their waits in the scheduler transaction;
failed candidates leave their goals open. See the [job-goals implementation
review](REVIEW-PGJOBDB-JOB-GOALS.md) for storage, concurrency, and rollout details.

Job deletion hides jobs, revokes leases, and cancels unfinished work while
retaining identity, history, and artifacts. The runtime supports embedded and
HTTP deletion, hidden-inclusive history listings, and available-route discovery.
See the [upgrade and deletion review](REVIEW-PGJOBDB-JOB-DELETION.md) for the pinned
version, storage behavior, installer requirements, and opt-in timeout defaults.

## Crash concern and ready work

Native polling, targeted lease acquisition, and external-task claims require
`jobs_with_status.status = 'READY'`. A `CRASH_CONCERN` job cannot bypass this
classification through an alternate route. Acquisition still locks the base job
row with `FOR UPDATE ... SKIP LOCKED`.

The existing default threshold is five consecutive expired-lease reclaims,
configurable with `pgjobdb.set_crash_concern_threshold`. The counter advances on
reacquisition, not on reads or renewal. A held lease is `ACTIVE`; once it expires,
the threshold determines whether the job is `CRASH_CONCERN`. With the default,
an uninterrupted failure cycle stops after the sixth lease expires. Successful
rescheduling resets the streak. An operator can restore normal scheduling
eligibility through `WorkflowRuntime.ClearCrashConcern`, or:

```sh
jobdb jobs clear-crash-concern -t TENANT JOB_ID --reason 'worker issue fixed'
```

The REST endpoint is `POST /v1/tenants/{tenantId}/jobs/{jobId}/clear-crash-concern`
with a JSON body such as `{"reason":"worker issue fixed","workerId":"operator"}`;
`{}` is also valid. Success returns `204`, missing jobs return `404`, and live,
completed, cancelled, or hidden jobs return `409`. Delegated lease credentials
cannot call this operator endpoint. `reset-crash-concern` is a CLI alias.

Native Go callers can use `pgjobdb.ClearCrashConcern(ctx, db, req)` with a
`jobdb.ClearCrashConcernRequest`. It accepts a database or borrowed transaction,
locks the job, checks eligibility, and calls the existing traced
`pgjobdb.clear_crash_concern` SQL operation atomically. Only the consecutive
expiration count is reset; history, routing, dependencies, availability,
deadlines, and lifetime expiration statistics remain intact. The old lease stays
invalid. Normal acquisition reclaims it with a new lease and advances the count
from zero to one. Repeating the clear while inactive succeeds; a concurrent live
lease causes `jobdb.ErrConflict`. No schema migration is required for this API.

The acquisition fix itself changes functions rather than the database format.
The current release also includes job goals and requires the format-8 cutover
described above.

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
GOWORK=off go test -tags=integration -p 1 ./...
```

The JobDB dependency is pinned to a published release. Use `GOWORK=off` when
verifying that release so a local workspace cannot substitute another checkout.

Publish the JobDB core and base CLI version first, then this pgjobdb module.

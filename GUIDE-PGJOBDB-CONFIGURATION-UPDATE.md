# Tenant configuration in pgjobdb

pgjobdb pins JobDB `v0.0.53-0.20261024021812-70fda67ff4d5` (`ba8ecfd3c373`), containing configuration commit
`ab3ac0208ac430590c777ad9bc65c2cce4a48396`. Rebuild the pgjobdb executable or
container to expose the configuration API and inherited `jobdb config` commands.
Updating JobDB in another application does not update a deployed pgjobdb server.

## Storage and runtime integration

`runtime.NewSQLDB` initializes JobDB's shared `configuration.SQLStore` with the
Postgres dialect after the scheduler, chapter store, and schema registry. The
store is passed to `runtimecore.NewRuntime`; the embedded core provides the
administration and lease-scoped read APIs, including HTTP transport support.
This follows the proposed patch's production wiring without duplicating JobDB's
validation, revision, authorization, or HTTP implementation in the scheduler.

Startup creates an additive table using the connection's schema/search path:

```sql
CREATE TABLE IF NOT EXISTS jobdb_configuration (
    tenant_id TEXT PRIMARY KEY,
    snapshot BYTEA NOT NULL
);
```

Each row contains a JSON snapshot with a revision and all tenant values. BYTEA
preserves JSON numbers exactly. Reads fetch one coherent snapshot; writes lock
the tenant row and commit atomically, including concurrent first writes from
separate server instances. Expected-revision conflicts do not change the row.
Removing the last value retains the tenant revision.

**The scheduler remains format 6. No database reset or data conversion is
required for an existing format-6 installation.** Earlier scheduler formats
still follow the existing hard-break policy. Configuration is durable tenant
state, independent of jobs, chapters, job deletion, and job-history exports;
include its table in normal database backups. Historical snapshot lookup is not
provided.

Let the runtime install the scheduler first on a fresh database. Precreating
application tables can trip the installer's nonempty-database check. The startup
role needs table-creation privileges; normal configuration operations need
SELECT, INSERT, and UPDATE. `NewSQLDB` keeps the supplied pool caller-owned;
`OpenDSN` owns and closes its pool.

## Limits and access

Embedded applications can set `runtime.Config.ConfigurationLimits`. Zero fields
select JobDB defaults: 1,024 entries per tenant, 256-byte names, 64 KiB values,
1 MiB snapshots and request bodies, and a 10-second read timeout. No new CLI
flags or environment variables are needed. Keep limits consistent across
replicas and clients when changing defaults.

| Method | Route | Access |
| --- | --- | --- |
| GET | `/v1/tenants/{tenantId}/configuration` | Full tenant snapshot for administration |
| POST | `/v1/tenants/{tenantId}/configuration/set` | Set one JSON value |
| POST | `/v1/tenants/{tenantId}/configuration/remove` | Remove one value |
| GET | `/v1/tenants/{tenantId}/jobs/{jobId}/leases/{leaseId}/configuration` | Current snapshot with `X-JobDB-Lease-Token` |

Use the deployment's tenant/admin authorization for administration routes. An
execution lease grants only the worker read capability. Administrative and direct
lease reads fetch current storage. Since v0.0.32, `ctx.GetConfiguration` caches the
first successful snapshot for the live workflow attempt and returns owned copies;
inline tasks share that snapshot. A new attempt gets a new cache. Configuration
is not automatically recorded in workflow history.

## Secrets compatibility

PostgreSQL is **not a secrets provider**, and this change adds no secret storage,
encryption keys, or provider configuration. The inherited JobDB runtime and HTTP
server expose the secrets protocol. With a valid lease, resolution returns
`jobdb.ErrNoSecretProvider`; invalid or cancelled leases remain rejected.

Review of the proposed patch found one additional adapter requirement: JobDB's
secrets broker needs `runtimecore.StoredJob.Lease`, which pgjobdb's public job
projection intentionally omits. The internal scheduler adapter now fetches the
lease identity for active leased jobs. The broker still calls `ValidateLease`
before granting access. Lease IDs are not added to public job listings. Test
runtime and lease wrappers forward both configuration and secrets methods.

## Validation

Run against the published dependency, without a local workspace override:

```sh
GOWORK=off go test ./pkg/pgjobdb/runtime/... ./cmd/jobdb -run '^$'
GOWORK=off go test ./pkg/pgjobdb/runtime/integration \
  -run '^Test(TenantConfiguration|NativeRuntimeCore|PostgresSecretsWithoutProvider)' -count=1
GOWORK=off go test -race ./pkg/pgjobdb/runtime/integration \
  -run '^Test(TenantConfiguration|PostgresSecretsWithoutProvider)$' -count=1
GOWORK=off go test -tags=integration -p 1 ./...
GOWORK=off go build -o ./bin/jobdb ./cmd/jobdb
```

Configuration tests use real PostgreSQL with embedded and HTTP access. They
cover the upstream configuration contract, concurrent writes and revision
conflicts across separate connection pools, reopening with preserved values and
revisions, configured limits with atomic rejection, workflow-attempt snapshot
reads, and additive startup on an existing database without losing job data.
Secrets tests cover the missing-provider response and lease authorization.
The full suite uses serialized packages to avoid contention between embedded
PostgreSQL suites with short workflow deadlines.

### Workflow regressions resolved in JobDB v0.0.33

Verified on 2026-10-13: the full `GOWORK=off go test -tags=integration -p 1 ./...`
suite passes, as do configuration/secrets checks under the race detector and the
CLI build.

JobDB v0.0.31 and v0.0.32 left jobs ACTIVE on live determinism errors and
failed RestartExtra prerequisites. The upstream fixes in v0.0.33 now pass all
four previously failing pgjobdb tests:

- `TestRestartWithExtraOutputDeterminismErrorOnDirectRuntime`
- `TestDirectRestartWithExtraOutputDeterminismErrorParity`
- `TestChapterConstraintsAcrossEngines` (including toy and Postgres)
- `TestRestartPrerequisitesCheckedAtRestartExtra`

The independent toy/SQLite reproductions also reach COMPLETED with a failed
outcome, one lease completion, and a durable JobAttemptOutcome chapter. No local
JobDB patch or workspace override is required. The [historical core bug
report](BUG-JOBDB-WORKFLOW-TERMINAL-FAILURES.md) retains the v0.0.32 investigation
and reproduction instructions.

## Rollout

Upgrade all server replicas before enabling configuration reads in workers.
Keep the database, artifact storage, and lease-signing settings. There is no
configuration enablement flag. To roll back, first revert workers that need the
new APIs, then revert the server; retain the additive configuration table.

For a smoke test against a rebuilt running server:

```sh
printf '%s' '{"region":"eu-west-1","parallelism":4}' |
  ./bin/jobdb config set build.defaults -t configuration-smoke -f - \
    -u http://127.0.0.1:9047
curl --fail-with-body -i \
  http://127.0.0.1:9047/v1/tenants/configuration-smoke/configuration
./bin/jobdb config remove build.defaults -t configuration-smoke \
  -u http://127.0.0.1:9047
```

GET returns a snapshot, its revision in a quoted `ETag`, and
`Cache-Control: no-store`. Supply deployment authentication when applicable.

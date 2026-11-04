# Tenant cells

pgjobdb implements the JobDB cell contract using storage format **9** and JobDB
commit `8797908eaf7c`. Jobs require an explicitly registered cell name and retain
tenant-wide job IDs. Cells have server-generated immutable IDs, tenant-unique
mutable names and canonical repository locators, descriptions, arbitrary JSON
metadata, revisions, and ACTIVE/RETIRED state. Previous names remain aliases.
Retirement preserves identity and reserves names/repo; there is no delete or
restore operation.

Register a cell through the administrative API or shared CLI before submission:

```sh
jobdb cell create --tenant acme --name payments --repo github.com/acme/payments --url https://jobs.example.com
```

`--url` identifies JobDB; `--repo` identifies Git. `JOBDB_BEARER_TOKEN` supplies
deployment authentication. All creation requires `cells.create`. The name
`root` is ordinary: it conveys no authority and has the same rename/retirement
rules as any other name. There is no root flag, binding, bootstrap permission,
list filter, or root-only command.

Submission never creates cells, including when the caller has `cells.create`.
Unknown targets return `not_found` with or without `cellRepo`. For an existing
cell, `cellRepo` is an optional repository assertion; name-only submission works.
A lease defaults to its own cell. Trusted backend policy may grant additional
existing target IDs; signed delegation restrictions intersect that ceiling.
No lease inherits cell creation or tenant-wide submission from a cell name.

The standard cell endpoints support creation, filtered/paginated listing,
retrieval, conditional PATCH, and retirement. PATCH accepts name, repository,
description, and arbitrary metadata. Metadata replaces the map; `{}` clears it.
Use a strong current ETag in `If-Match`; missing validators return 428, malformed
validators 400, and stale validators 412. Changes are atomic. Creation request
IDs are scoped to tenant/principal; matching replay returns the original
snapshot after authority and retirement checks.

Job facts, goals, preparations, schedules, handles, and leases carry cell
identity. Jobs keep the repository captured at submission across retries and
restarts after repo edits. Reads show the current name; leases retain their
issued snapshot. Schedules pin existing cell IDs and repositories. Repo changes
pause active schedules with `cell_repo_mismatch`; update the target explicitly
before resuming. Already emitted jobs retain their snapshots.

## Storage and transaction boundaries

`cells` stores fixed properties in ordinary columns; only arbitrary `metadata`
is JSON. `cell_names` reserves names/aliases. `cell_catalog` stores the byte
cursor key; `cell_requests` stores principal/request ID, normalized request hash,
and the original result in ordinary columns, plus its arbitrary metadata.
Nanosecond timestamps preserve exact replay results. Registry reads use one SQL
statement over these rows for a consistent MVCC snapshot.

Job facts, goals, preparations, and schedules reference existing cells through
same-tenant foreign keys. Preparation requested name/repo, resolved ID/name/repo,
and parent/prior job IDs are columns. No provisional cell identity is reserved.
Schedule ownership columns are ordinary columns; their legacy target JSON does
not duplicate cell fields. Go reconstructs the core's wire snapshot on reads.
Active/archive rows retain ownership through required job facts. Broader
normalization of old non-cell structured JSON is a separate change.

Registry mutations, preparation/publication, scheduling, retirement, and work
acquisition take the tenant admission advisory lock before row locks.
Multi-tenant acquisition locks tenants in sorted order. Caller-owned
transactions must use the same order. Savepoints ensure a failed borrowed
operation leaves no partial writes even if the caller commits afterward.
Publication atomically writes the job, goal, and dependencies against an
existing cell; it never writes a new cell. Chapters are staged before publication
and are exposed only through a published job.

The backend validates the stored parent lease at preparation and publication.
Completion, cancellation, replacement, or expiry prevents delayed publication.
Registration conflicts and concurrent repo claims leave no partial cell records.
Retirement checks active jobs/leases, open goals, active schedules, and
unpublished preparations by immutable ID, including after rename.

Go mutations require a transaction-capable database (`*sql.DB`, `*sql.Tx`, or a
wrapper exposing `BeginTx`). Raw SQL is a trusted database boundary; applications
use the typed API for validation and policy.

## Policy and queries

Embeddings install verified `jobdb.CellAuthority` context. Request bodies and
client headers cannot establish it. The standalone server retains explicit
administrative ingress; authenticate at the deployment boundary or embed your
own policy middleware. This release covers registry, submission, listing, and
acquisition hooks. It does not retrofit full RBAC onto every history, artifact,
goal, wait, secret, or mutation operation.

Native `GetWork` requires explicit tenant IDs. `CellNames` resolve aliases and
intersect allowed IDs before pagination/acquisition. Unknown or inaccessible
names fail. Cursors bind principal, authority, filters, and resolved IDs and
recheck access on each page. Listing takes one registry snapshot per tenant and
one compact row query, independently of page size; no per-job history, registry,
or artifact lookup is needed.

## Hard cutover and validation

Use a new database and artifact location. Format 8 and earlier are rejected
unchanged. No jobs, histories, schedules, leases, or metadata-derived identities
are migrated. Explicitly register configured cells before starting compatible
clients and workers. Reinstalling the current schema is supported.

The module pins the exact JobDB commit and checksums. Publish JobDB commit
`8797908eaf7c` before building this pgjobdb revision elsewhere. Local tests use
that committed module archive, with no checked-in filesystem replacement.

Validation covers direct/HTTP shared conformance, native SQL integration,
previous-format rejection, normalized storage, original-result replay,
registration/retirement/schedule races, lease loss between preparation and
publication, borrowed-transaction rollback, and authorized discovery/acquisition.

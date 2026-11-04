# Tenant cells

pgjobdb implements the JobDB cell contract using storage format **8** and JobDB
commit `a21636d234ce`. Every job requires a cell name. Job IDs remain tenant-wide.
Cells have server-generated IDs, tenant-unique names and canonical repository
locators, descriptions, arbitrary JSON metadata, revisions, and active/retired
state. Renaming reserves the previous name as an alias. Retiring reserves both
names and the repository; there is no delete or restore operation.

Use the shared JobDB administrative API and CLI:

```sh
jobdb cell create --tenant acme --name root --repo github.com/acme/root --url https://jobs.example.com
jobdb cell create --tenant acme --name payments --repo github.com/acme/payments --url https://jobs.example.com
```

`--url` identifies the JobDB API server. `--repo` identifies the Git repository.
`JOBDB_BEARER_TOKEN` supplies deployment authentication. The old `create-root`
command is removed. Creating `root` requires `cells.bootstrapRoot` as well as
`cells.create`; its name and root designation are permanent. Its repository,
description, and metadata remain editable. The standard cell endpoints support
creation, listing/name lookup, retrieval, revision-checked PATCH, and retirement.
Metadata PATCH replaces the map; `{}` clears it. Use the returned strong ETag in
`If-Match` for PATCH and retirement.

A live root lease can submit to any cell name in its tenant. A missing ordinary
cell additionally requires `cellRepo`; subsequent submissions may use only the
name. Root itself always requires explicit administrative creation. Ordinary
leases submit to their own cell unless verified backend policy grants additional
existing targets. Signed delegation restrictions still intersect these rights.
Administrative submission requires `jobs.submit`, plus `cells.create` when it
implicitly registers a cell.

Jobs, goals, preparations, schedules, handles, and leases carry cell identity.
Jobs retain the repository captured at submission, including retries and
restarts after a repository edit. Job reads show the current cell name; leases
keep their original snapshot. Schedules pin a cell ID and expected repository.
Changing the repository pauses active schedules with `cell_repo_mismatch`;
explicitly update their target before resuming. Already emitted jobs retain their
snapshots.

## Storage and transaction boundaries

`cells` stores the registry, `cell_names` reserves current names and aliases, and
`cell_catalog` stores replay records and the cell-list cursor signing key.
Postgres uses case-sensitive C collation for names and repository identity.
Registry payloads use JSON to preserve nested metadata and numeric
representations during replay and no-op revision checks.
Job facts and goals have required same-tenant cell foreign keys. Active/archive
rows derive ownership through their required retained job facts. Schedules have
required generated cell ownership columns and the same foreign key. Private
preparations reserve an identity without making a cell visible.

Cell operations, job preparation/publication, scheduling, retirement, and work
acquisition take the tenant goal/admission advisory lock **before row locks**.
Multi-tenant acquisition takes tenant locks in sorted order. Callers supplying
an existing transaction must follow this order. Operations use a savepoint in
borrowed transactions so a failed publication cannot leave a cell behind if the
caller later commits. An owned transaction commits the cell, job, goal, and
initial dependencies together. Chapter storage is staged before publication;
the runtime exposes chapters only through a published job.

The backend validates a parent lease against the locked stored job during both
preparation and publication. Completion, cancellation, and lease loss therefore
prevent delayed publication. Concurrent matching first submissions converge on
one cell; competing repository claims fail without partial registration.
Retirement checks active jobs (including hidden/waiting jobs), pending
preparations, open goals, and active schedules under the same lock.

All Go mutations require a transaction-capable database (`*sql.DB`, `*sql.Tx`, or
a wrapper exposing `BeginTx`). Raw SQL access is a trusted database boundary;
applications should use the typed API for registry validation and policy.

## Policy and queries

Embeddings install `jobdb.CellAuthority` from authenticated policy in context.
Request bodies and client headers cannot establish this authority. The standalone
server retains an administrative ingress policy: authenticate and restrict access
at the deployment boundary, or embed the server with your own policy middleware.
This release adds policy hooks for registry operations, submission, job listing,
and acquisition. It does not claim complete RBAC for history, artifacts, goals,
waits, or every other mutation. Root submission rights do not grant those rights.

Native `GetWork` requires explicit tenant IDs. `CellNames` selectors resolve
aliases and intersect allowed IDs before SQL pagination or claiming work.
Unknown or inaccessible named selectors fail. Job cursors bind the principal,
authority, filters, and resolved cell IDs; access is reevaluated on every page.
Listing takes one registry snapshot per requested tenant and one compact row
query, independent of the number of jobs returned. It never issues per-job
registry, history, or artifact queries.

## Hard cutover and validation

Use a new empty database and a fresh artifact location. Format 7 and all earlier
formats are rejected unchanged, including populated installations. No job,
chapter, schedule, lease, or metadata-based identity migration is provided.
Recreate tenant administration and root/ordinary cells, then use compatible
clients and workers. Reinstalling the current schema is supported.

The Go module pins the exact JobDB commit with checksums. Publish JobDB commit
`a21636d234ce` before building this pgjobdb commit on another machine. Local tests
also run against the exact committed JobDB module archive, without a checked-in
filesystem replacement.

Validation includes shared workflow, compact-list and cell conformance over
both direct and HTTP runtimes; previous-format rejection; concurrent registration,
repository claims, retirement/admission and schedule/edit races; lease loss after
preparation; borrowed-transaction rollback; pending-preparation retirement; and
cell filtering before pagination and acquisition.

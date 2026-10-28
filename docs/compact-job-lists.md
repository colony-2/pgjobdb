# Compact job lists

Runtime `ListJobs` and HTTP job queries use one SQL query per page. They retain
identity, primary goal, full metadata, schema/parent, status, completion status,
stored route, timestamps, cancellation, payload revision, and the persisted
pending wait's ID/deadline. Deprecated detail fields are empty by default.
`GetJob`, dependency discovery, execution, and schedule-run inspection keep
returning their details.

The adapter now calls `pgjobdb.ListJobSummaries`, which has a dedicated decoder
and explicit SQL projection. The native `pgjobdb.ListJobs` API still returns
`JobDetail` for callers that intentionally request full scheduler rows. Both
paths share filter validation, cursor encoding, and pagination. No JobDB runtime
list calls the old detail projection or `BlockingDependencyWait`.

The query reads active/archive rows and immutable facts, applies filters, sorts,
and materializes at most the requested page plus one lookahead row. Output joins
fetch only client-state revision and the current persisted wait's small columns.
Without a status or availability filter, status calculation occurs only after
the page limit. With those filters, readiness is evaluated before pagination.
No list output fetches payload content, policies, task coordinates, prerequisite
arrays, completion text, or memberships. No dependency explanations, chapters,
or artifacts are read.

Goal filters match primary goals or an `EXISTS` predicate on secondary membership;
they do not enumerate memberships for output or duplicate jobs. Available-route
filters use the effective route, including due alternates, while returned routes
remain the stored route. Status and wait expiry share one observation time.
Legacy prerequisite readiness still checks archived prerequisites. Wait summaries
use persisted state without evaluating targets; expired, terminal, cancelled, and
archived waits are omitted.

## Schema update

Startup adds `dependency_waits.deadline_exact`, a stored generated text column
extracted from the wait outcome. This avoids loading the full outcome JSON to
return the original deadline. New waits preserve the API deadline's nanoseconds;
PostgreSQL's indexed `TIMESTAMPTZ` deadline still controls readiness at PostgreSQL
precision. Existing rows retain their persisted precision.

This is an additive, idempotent update to format 7. Adding the stored generated
column backfills existing rows and requires PostgreSQL's table rewrite/DDL lock
on its first installation. Subsequent starts leave the column in place. Older
formats remain unsupported; this change does not introduce migration from them.

## Verification

- Upstream `RunCompactListConformance` runs against direct and HTTP runtimes,
  including full detail replacements and exact deadline round trips.
- Driver-level instrumentation checks exactly one database operation for each
  page at sizes 1, 100, and 200, including pending waits, archived outcomes, and
  secondary-goal filters. Metadata is checked without truncation. These fixtures
  have no chapter/artifact records, so listing cannot depend on those stores.
- SQL guards replace dependency evaluation, explanation, and goal-candidate
  enumeration functions with failures during listing. Ordinary lists and goal
  filtering still succeed.
- Storage tests cover cursor ties, filter parity with the existing detail path,
  hidden jobs, metadata equality, parents/roots, creation bounds, legacy waits,
  active leases, cancellation, stored task routes, due alternates, wait expiry,
  persisted terminal waits, and agreement between listing and lease acquisition.
- Reinstallation tests cover an existing format-7 table without the new column,
  preserving jobs, waits, and exact deadlines across repeated installs.
- Existing workflow tests now read prerequisite lists and task coordinates from
  `GetJob` and assert those fields are empty in ordinary lists.

## Query plans

Captured `EXPLAIN (ANALYZE, BUFFERS)` output from local embedded PostgreSQL is
checked in under [plans/compact-job-lists](plans/compact-job-lists). These are
small warm-cache correctness fixtures, not production throughput benchmarks.
The pending-only fixture has 240 consumers, two producers, 4 KiB of metadata and
32 KiB of payload per consumer, with secondary membership on 12 consumers. Its
tables are analyzed before capture. The mixed fixture has 160 pending consumers
and 80 archived consumers plus two producers; it uses normal post-setup statistics.

| Query | Output rows, including lookahead | Execution time | Shared buffer hits |
| --- | ---: | ---: | ---: |
| [Page size 1](plans/compact-job-lists/page-1.txt) | 2 | 0.775 ms | 64 |
| [Page size 100](plans/compact-job-lists/page-100.txt) | 101 | 2.844 ms | 960 |
| [Page size 200](plans/compact-job-lists/page-200.txt) | 201 | 5.035 ms | 1860 |
| [Secondary goal + metadata](plans/compact-job-lists/secondary-goal-metadata.txt) | 12 | 0.590 ms | 160 |
| [Mixed active/archive](plans/compact-job-lists/mixed-active-archive.txt) | 101 | 5.569 ms | 3260 |

All captured executions report buffer hits without disk reads. The plans show
filters below `CTE page`'s limit and output joins above it. Small fixtures favor
sequential scans/hash joins for candidate filtering, with indexed wait/status
lookups after pagination; larger pages can use a hash join for client revisions.
Secondary membership can be hashed for filtering, without becoming output.
These plans do not justify new indexes. Larger deployments and different filter
selectivity should be measured independently. Full metadata and broad filters
can still require work beyond the number of returned rows.

Reproduce the captures with:

```sh
GOWORK=off PGJOBDB_EXPLAIN_DIR=/tmp/compact-list-plans go test -tags=integration \
  ./pkg/pgjobdb/runtime/integration \
  -run 'TestCompactListStoragePagesAndPlans|TestCompactListRuntimeQueryCounts' -count=1
```

The export includes full verbose JSON plans and text plans. Query counts are
asserted separately from the optional `EXPLAIN` calls.

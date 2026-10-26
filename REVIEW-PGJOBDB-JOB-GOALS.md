# Job goals: pgjobdb design review

Reviewed 2026-10-24 against `GUIDE-PGJOBDB-JOB-GOALS.md` and the current
pgjobdb implementation. The design below is now implemented, with verification
results recorded at the end.

## Upstream availability

After the goals push, direct inspection of remote `main` resolves to
`70fda67ff4d5714ccb7c4e8b635846746e1703c1`
(`v0.0.53-0.20261024021812-70fda67ff4d5`). Downloaded and inspected this revision:
it contains `GoalRuntime`, `GoalScheduler`, the exported preparation aliases,
the job-goals spec, and prepare-before-history calls in submit/restart. The
previous upstream availability blocker is resolved. pgjobdb now pins
`v0.0.53-0.20261024021812-70fda67ff4d5` with the expanded adapter.

Use the exported interfaces and validators; inspect reference tests and typed
errors during implementation. Do not import upstream internal goalstate packages.

## Ownership and storage

Implement goals in pgjobdb's native SQL scheduler, with typed Go operations and
a thin runtime adapter. Reuse JobDB's runtime facade, HTTP server, history codecs,
and workflow SDK. There is no separate goal service or fallback store.

Keep the immutable primary goal ID in **both `jobs.goal_id` and
`job_facts.goal_id`**. The active row supplies it directly for scheduling and
completion updates, avoiding a lookup in the growing facts table solely to
find the goal. Retained facts preserve primary membership across archival and
soft deletion. Write both values atomically at publication and never update
them independently. Carry the value into `jobs_archive` on archival as well.
Retained membership queries and foreign keys continue to use `job_facts`. An
insertion trigger verifies the active/facts mapping; update triggers reject
primary-goal changes. Ordinary active-row updates do not repeat that facts lookup.

| Storage | Responsibility |
| --- | --- |
| `goals` | Tenant-scoped identity, OPEN/SUCCEEDED/FAILED state, revision, winner or immutable failure snapshot, timestamps |
| `job_facts.goal_id` | Required immutable primary membership, indexed by tenant/goal/job, referencing `goals` |
| `goal_candidates` | Extra membership only; foreign keys to goals and retained job facts; indexes in both directions |
| `job_preparations` | Reserved destination and goal, core semantic descriptor, source/sharing information, publication flag; unique tenant/destination and nonempty tenant/request ID |
| `dependency_waits` | Immutable ordered descriptor and deadline, durable group outcome and failed dependency index; key tenant/consumer/wait |
| `dependency_targets` | Indexed pending target subscriptions; ordered bindings and failures remain in the wait outcome JSONB |
| `jobs` | Immutable primary `goal_id` for active operations; reference to the currently blocking structured wait, separate from legacy `wait_for` |
| `jobs_archive.goal_id` | Primary goal ID copied from the active row on every archival path |

Outcome snapshots retain the full upstream structure in JSONB. Separate indexed
subscription rows support target lookup, while the wait row retains its ordered
descriptor, state, deadline, and outcome. Closing a group removes subscriptions
without discarding earlier bindings or failures. Deadline indexes support both
tenant-specific and global polling.

Store no extra candidate row for primary membership. Derive sorted
`OtherGoalIDs`; do not put membership counts or progress counters on goal rows.
Foreign keys target retained identities, never active job rows. A preparation
does not expose a public goal or job before publication.

## Transaction boundary and locking

Use one transaction-scoped advisory lock per tenant for topology and outcome
mutations initially. Define a shared lock-key helper and take the lock **before
any job, facts, preparation, goal, or wait row lock**. A hash collision may reduce
concurrency but must not affect correctness. Multi-tenant maintenance processes
one tenant per transaction, or takes all required tenant locks in stable order.

Participation includes publication, attachment, explicit failure, completion,
wait registration/replacement, cancellation, deletion, cancellation archival,
and timeout settlement. An adapter-only mutex or lock around just the new API
methods is insufficient: native SQL callers must obey the same protocol.

Retain ordinary `SKIP LOCKED` work claiming. A claim path that settles an expired
wait must first participate in tenant coordination, before taking the consumer
row lock. Do not add a tenant lock inside a completion helper after its caller
has already locked the job. Review the existing child-publication facts lock
and deletion's facts-then-active-row locks under this ordering too.

Keep chapter/artifact preparation outside scheduler transactions. Core prepares
history first; one scheduler transaction publishes the job and initial goal or
commits completion, goal resolutions, and affected wait outcomes. PostgreSQL
notifications are delivered after commit; polling must work without them.

## Publication and completion

A preparation reservation is a durable operation record written before core
creates or clones history. It associates the destination job ID and goal ID
with the request's semantic descriptor and, for restarts, its source and optional
request ID. It is not a runnable job, public goal, execution lease, or a database
transaction held open while artifacts are copied.

For example, restart request `R` reserves destination `J` and goal `G`, then core
copies history. If the process crashes during that copy, retrying `R` recovers
the same `J` and `G`. Once publication commits, retrying `R` returns that original
handle even if `J` has progressed. Reusing `R` for different semantic inputs
conflicts. Explicit destination IDs provide another operation identity; requests
without a stable operation identity do not gain automatic retry deduplication.

The upstream `PrepareJob` returns `Key`, `GoalID`, and `Published`. Core skips
history preparation when `Published` is true; otherwise it prepares history and
calls `CreateJob`, which must publish and mark the reservation in one scheduler
transaction. This preserves the separate chapter/artifact transaction boundary.

Add native prepare/publish operations. Preserve the semantic descriptor supplied
by core, reserve generated identities durably, and resolve equivalent retries by
explicit destination or tenant-scoped request ID. Published equivalent retries
return the original mapping even after destination progress or goal closure.
Recheck shared-goal openness and cycles at publication, not just preparation.
Independent restarts and new schedule occurrences receive independent goals;
shared restarts inherit only the source's primary goal.

Existing `submit_native_job` compares immutable facts and initial payload; that
does not replace preparation idempotency. Native submission also needs a defined
goal creation path so every published native job has primary membership.
Core reconciles conflicts while creating/cloning history; the persisted semantic
descriptor rejects conflicting operations before those writes. Publication checks
the reserved goal, sharing policy, and dependencies under the tenant lock.

Factor terminal-transition handling so normal completion, unheld completion,
deletion, and cancellation archival all settle exact-job dependencies and close
consumer-owned subscriptions. Current `delete_native_jobs` and
`archive_cancelled_jobs` insert archive rows directly and bypass
`_complete_native_locked_job`; modifying only that helper would miss them.

Successful terminal completion resolves every still-open primary or extra goal
in the same transaction, then settles indexed affected waits. A closed goal
does not block other resolutions. Unsuccessful completion leaves goals open.
Do not classify transient retries, crash concern, or an expired execution lease
as terminal failure. Native cancellation now archives the cancelled job in its scheduler transaction,
revokes its lease, and immediately settles exact-job dependencies. Archive
triggers also cover deletion and cancellation maintenance. They obtain primary
goal identity from the active row and retain it on the archive.

Candidate attachment must check primary and extra membership before rejecting a
closed goal. Attaching an already successful job resolves an open goal in the
same transaction. Explicit failure freezes sorted candidate lists, including
hidden jobs, and never cancels candidates or rewrites their own outcomes.

## Waits, deadlines, and cycles

Do not encode structured waits into `wait_for` or reuse `available_at` as their
only state. Those fields cannot retain goal winners, declaration order, typed
failures, immutable descriptors, or recovery bindings. Preserve their existing
legacy behavior alongside structured waits, and reject mixed requests.

Use one evaluator for registration, target settlement, and timeout handling.
Check the absolute deadline before assigning any new outcome. Preserve assigned
bindings; fail on the lowest declaration index among failures found in the
same evaluation. A terminal group makes its consumer eligible to resume under
ordinary lease, cancellation, expiration, and scheduling rules. It does not
automatically mean the consumer job succeeded or failed.

Reads project deadline expiry coherently without mutations. Before a due consumer
can be leased, persist the timeout through the coordinated write path. Polling
and targeted acquisition must trigger this without requiring a producer event
or a background daemon. Restrict processing with tenant/deadline indexes and
avoid locking unrelated tenants during ordinary claims. `job_status_at`, listing
at a supplied `Now`, route discovery, task claims, and lease acquisition must
agree about readiness.

Cycle checks use unresolved job-to-target edges and open-goal-to-nonterminal-job
edges through both membership representations. Traverse the prospective graph
under the tenant lock, including hidden candidates. Check registration,
attachment, and shared publication; evaluate an already successful attachment
as a prospective resolution. A recursive SQL query can implement reachability;
separate unlocked checks on different goals cannot prevent graph write skew.

## Go and transport changes

Extend native request/result types and SQL projections, then map them in
`runtime/internal/runtimeadapter`:

- `mutations.go`: preparation/publication fields and structured rescheduling.
- `jobs.go`: primary and extra goal IDs, goal query filters.
- `leases.go`: goal identity on acquisition, validation, renewal, and import paths.
- `schedules.go` and native schedule-run projections: goal identity on summaries.
- New goal operations: reads, candidate attachment, explicit failure, wait reads.

Apply goal filtering in SQL before pagination using indexed `EXISTS`, and include
the normalized goal filter in `list_jobs.go`'s cursor fingerprint. Load
extra associations through an indexed ordered subquery in the same SQL statement,
avoiding Go queries per result and join multiplication. Include archived and hidden provenance
where the contract requires it.

Translate stable SQL error codes into upstream typed errors, preserving complete
failure snapshots. Reuse upstream validators plus persistent-state checks. The
shared HTTP server should provide routes and authority checks; verify tenant
lifecycle authority for attach/fail and denial for execution-only capabilities.
Do not build a second HTTP protocol in pgjobdb.

## Rollout

Given the established discard-old-data direction for this deployment, this
implementation uses scheduler format **7** and requires a fresh database. The
installer rejects format 6 and older without changing them; it never erases an
existing database. Deploy matching servers/workers and start fresh jobs. No
existing deployment or data was modified during implementation.

If preserving a populated installation becomes a requirement, the guide's
transactional/resumable one-goal-per-job backfill is separate work. It must include
active, archived, and hidden jobs, use authoritative completion outcomes, and
coordinate old writers. It cannot infer success from an archived row alone or
infer shared goals from restart ancestry.

## Implementation and validation sequence

1. Use the now-available goals revision/spec; verify public contracts and compile-time
   interface changes. Upgrade the dependency together with a complete adapter.
2. Add schema, retained identities, prepare/publish, and all query/lease mappings.
3. Implement waits, cycle checks, settlement, and terminal-path integration under
   the shared transaction protocol. Add deadline processing to acquisition.
4. Expose goal operations through the shared runtime/server and preserve typed
   failures and recovery history.
5. Run native SQL tests and direct/HTTP runtime conformance; exercise controlled
   races through separate database connections/runtime instances.

Release gates include competing successes; attach/complete/fail races; shared
publication versus closure; concurrent cycle creation; deadline expiry without
producer events; mixed ordered dependencies; cancellation/deletion; immutable
bindings after runtime restart and before history append; idempotent preparation
after lost responses; tenant isolation; filtered pagination; and lease/import/
schedule field preservation. Assert no primary extra row and no goal timestamp
or revision rewrite on unsuccessful completion or ordinary progress.

## Implementation verification

The adapter now implements all goal methods and reservation/publication mappings.
Native SQL persists goal identity on active jobs, retained facts, and archived
jobs. Goals and pending dependency outcomes settle synchronously with terminal
scheduler writes. Read-only timeout projections use the same evaluator as write
paths; polling and targeted claims persist due timeouts before granting leases.

Added tests cover direct and HTTP goal lifecycle, shared and independent restarts,
request-ID recovery, primary membership after closure, typed failures, competing
successes, immutable wait reuse, and goal-filtered pagination. PostgreSQL-specific
races use separate connections blocked on the tenant advisory lock before release:
attachment/completion, attachment/failure, wait registration/resolution, opposing
graph changes, and shared publication/closure. Further tests cover unpublished
reservations, hidden failure provenance, deadline settlement without producer
events, late producer completion, cancellation/deletion, and runtime reopen after
settlement but before consumer history recording. Workflow tests exercise joins
inside jobs and tasks, failure admission, restarts, and immutable replay bindings.

The upstream HTTP error-decoding issue is fixed in JobDB v0.0.57, now pinned by
pgjobdb. Generated-ID restart request-ID mismatches match both
`ErrExistingJobMismatch` and `ErrConflict` through direct and HTTP runtimes.
See [the resolved core bug report](BUG-JOBDB-GOAL-RESTART-CONFLICT.md) and its
standalone toy/HTTP check. Tests require both typed errors on both transports;
no pgjobdb transport workaround was introduced.

Validation on 2026-10-24:

- Targeted goals/workflow race tests passed, including the controlled independent
  PostgreSQL writer races.
- CLI and standalone core-bug reproducer builds passed.
- The full integration suite passed, including installer, runtime conformance,
  engine conformance, usage parity, workflow, and PostgreSQL integration tests.

```sh
GOWORK=off go test -tags=integration -p 2 ./...
GOWORK=off go test -race ./pkg/pgjobdb/runtime/integration \
  -run 'TestPostgresGoal|TestExecutionGoal|TestExecutionRestartGoal|TestConcurrentGoalResolution' -count=1
GOWORK=off go build -o /tmp/pgjobdb-goals ./cmd/jobdb
GOWORK=off go run ./docs/repros/jobdb-goal-restart-conflict
```

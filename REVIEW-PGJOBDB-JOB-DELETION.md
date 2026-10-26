# JobDB upgrade and deletion review — 2026-10-19

## Dependency and required changes

Deletion support was implemented against JobDB
`v0.0.46-0.20261019210628-20269bd76cd7`, main commit
`20269bd76cd77572b71bb8811b4efb6dcba7b2fb`. This includes the deletion API in the
[supplied guide](GUIDE-PGJOBDB-JOB-DELETION.md). The preceding v0.0.45 tag did not.
The guide's relative source links refer to the JobDB repository.

A dependency update alone was insufficient. This implementation adds the optional
`runtimecore.JobDeletionScheduler` capability and honors `ListJobs.IncludeHidden`.
The shared core and HTTP server supply the public deletion API and CLI support;
pgjobdb implements selection, visibility, cancellation, and concurrency in SQL.
It also implements the available-route discovery contract introduced in v0.0.45.

## Deletion storage and behavior

A nullable `job_facts.hidden_at` timestamp records visibility on the retained
identity shared by active and archived jobs. This is additive within pgjobdb's
existing format 6: old rows remain visible and installer application is
idempotent. Rebuild/redeploy the server and apply the updated installer (runtime
startup applies it) before using the new APIs. No chapter/blob format changes or
data reset are required for this update.

`delete_native_jobs` selects the full tenant-scoped intersection in one
transaction, without pagination or `SKIP LOCKED`. Creation cutoffs are exclusive;
metadata uses the existing application-metadata predicates. Dry runs only count.
Repeated and duplicate IDs do not add matches. An error rolls back the entire
operation, including lease revocation and traces.

Unfinished jobs move to the archive as `cancelled` / `job deleted`, with no valid
lease. Already-terminal outcomes remain unchanged. Hidden, cancellation, archive,
and deletion-event timestamps use the supplied `Now`. Existing dependency
completion handling wakes waiting parents. Identity locks serialize repeated
requests; active-row locks serialize acquisition, completion, and renewal. As
with other database transactions, a PostgreSQL deadlock or serialization error
fails the whole request and callers may retry.

Default job listings and schedule-run listings exclude hidden records before
pagination. `IncludeHidden` returns both hidden and visible jobs, preserving all
other filters; its value is bound into pagination tokens. It does not restore
execution eligibility. Direct job/history/artifact reads and clone source access
remain available. Explicit-ID resubmission cannot recreate or unhide a deleted
identity. Deletion does not cascade to children, clones, or schedules and does
not reclaim chapter or artifact storage.

Child creation takes a shared lock on its parent's identity and checks visibility
inside the submission transaction. This closes the race between core's initial
lease validation and storage creation: if deletion commits first, creation fails
with `ErrExecutionLeaseLost`; if creation commits first, the child is retained.
Chapter insertion remains independent, as elsewhere in the runtime.

## Other upgrade requirements

`runtimecore.ListJobsRequest.AvailableRoutes` and `Now` were previously ignored by
the adapter. Discovery now filters before pagination and shares status and
alternate-route evaluation with polling. A job-only route matches no task type;
routes are ORed, while metadata, status, job/task, and tenant filters intersect.
Discovery preserves stored routing and task waits without acquiring a lease or
promoting an alternate. Eligibility uses the supplied time. Cursor fingerprints
include the normalized route set but exclude time, so subsequent pages reevaluate
availability. Ordinary listing tokens retain their existing fingerprint.

Default execution timeouts are now opt-in. `DefaultRunPolicy()` retains retries
but no longer inserts invocation or total timeouts. pgjobdb already preserves
omitted and explicit timeout values, so no adapter conversion change is needed.
Existing jobs keep their recorded policies. Applications must rebuild to use the
new defaults; clients that need the former 10-minute invocation and 30-minute
total limits must set them explicitly. See JobDB's
[timeout migration guide](https://github.com/colony-2/jobdb/blob/20269bd76cd77572b71bb8811b4efb6dcba7b2fb/docs/MIGRATION-OPT-IN-EXECUTION-TIMEOUTS.md).

## Verification coverage

PostgreSQL tests adapt the upstream deletion scenarios for embedded and HTTP
runtimes: selectors/counts, tenant isolation, exclusive cutoff precision, dry
runs, repeated deletion, selections larger than one listing page, schedule runs,
thin-clone replay, restart from hidden history, and retained artifact bytes.
Additional tests cover hidden-inclusive pagination, reopening, identity retention,
no child cascade, transaction rollback, request timestamps, and races with lease
acquisition, completion, repeated deletion, and in-flight child creation.

Renewal assertions use `RenewableExecutionLease.Renew`: embedded `KeepAlive`
starts a background loop and returns without attempting synchronous renewal.
Available-route tests cover the supplied clock, alternate activation, pagination,
normalized route filters, and unchanged scheduler state.

Validation passed at the deletion implementation commit:

```sh
GOWORK=off go test -tags=integration -p 1 ./...
GOWORK=off go test -race -p 1 ./pkg/pgjobdb/installer ./pkg/pgjobdb/runtime/integration \
  -run 'TestNative(JobDeletion|AvailableRoute)|TestPostgresJobDeletion' -count=1
GOWORK=off go build -o /tmp/pgjobdb-main-jobdb ./cmd/jobdb
```

An additional database smoke check installed the committed format-6 schema,
created a job, applied the new installer, deleted the retained job, and reapplied
the installer. Visibility and retained cancelled history remained correct.

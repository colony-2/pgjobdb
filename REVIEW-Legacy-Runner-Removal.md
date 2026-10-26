# Legacy runner removal

Reviewed c2j main at `165adbd2be0dbf6f8a7cc404849bd77001d2a051` on
2026-10-24 before removing the sequential workflow runner.

## c2j callers

The production entry points already select the versioned execution engine:

- `pkg/worker/compiler/job_worker.go`: `NewRecipeJobWorker` returns
  `jobworkflow.ExecutionWorker`, whose callback takes `ExecutionContext`.
  `NewRecipeWorkerWithOptions` registers that worker and its task implementations.
- `cmd/c2j/internal/runjob/service.go`: live runs and replay use that factory
  through `newStoryJobWorker`.
- `cmd/c2j/internal/runjob/supplied_lease.go`: supplied-lease execution also uses
  `newStoryJobWorker`.
- `pkg/story/live/recorder.go` and `restart_context.go`: story reconstruction
  and restart replay wrap the same execution worker.
- `pkg/recipetest/execution_scope.go` and the recipe test executor use
  `ExecutionWorker` or worksets from the same compiler factory.

No Go references to JobDB's `JobContext`, `WithTaskOptions`, `TaskOptionsFor`, or
legacy `TaskAlternate` were found. c2j's own contextual `JobContext` data type is
unrelated. Uses of `jobworkflow.JobWorker` as a registration interface remain
valid because their values are execution workers.

A compilation check of c2j's compiler, story/live, runjob, and recipetest packages
fails identically against JobDB before and after this removal: c2j's
`validationJobContext` lacks the already-required `ResolveSecretsFresh` method.
The reviewed c2j revision pins JobDB v0.0.46. This is an existing dependency
migration issue, not evidence of a legacy runner dependency; it prevents claiming
a successful end-to-end c2j build against current JobDB.

## Other consumers

The sibling pgjobdb checkout still has legacy JobContext implementations in
`pkg/pgjobdb/runtime/integration/`, its copied test harness/conformance fixtures,
and `docs/repros/jobdb-workflow-stalls/main.go`. Its task-alternate tests also use
`WithTaskOptions`. These fixtures/reproductions need migration when pgjobdb
updates its JobDB dependency. No changes to the separate pgjobdb repository are
included here.

## Removal and validation coverage

`JobWorker.Run` now receives `ExecutionContext`. `ExecutionWorker` remains its
callback implementation. Registration APIs and c2j's factory calls retain their
shapes. There is one invocation/scope execution path; the old task retry loop,
job retry loop, ordinal cursor, timers, and Goexit-based handoff are removed.
Legacy `JobContext` and data-attached alternate options are removed as well.
The low-level runtime and chapter APIs remain available.

Internal workers and runtime conformance fixtures now use task declarations with
stable sites and input builders. Tests asserting the retired sequential chapter
layout, manually overwriting those chapters, or comparing their physical
ordinals with the execution engine are retired. Relevant current coverage is:

- `execution_compact_test.go`: compact histories, recovery, task-boundary
  restarts, and committed outcomes.
- `execution_integration_test.go`: retries, nested scopes, deadlines, input
  snapshots, replay, restart budgets, artifacts, and child joins across runtimes.
- `execution_failure_test.go`: non-retryable errors, prerequisite admission,
  deterministic-history failures, uncertain completion, and external results.
- `execution_alternate_test.go`: alternate dispatch, claims, lease replacement,
  expiry, retries, cancellation, and recovery.
- `execution_handoff_test.go` and `supplied_lease_integration_test.go`: handoff,
  renewal failure, cancellation, and supplied leases.
- `TestExecutionReplayPreservesObserverHistoryAndExpiredBudgets`: identical
  observer events and immutable history when replaying after execution budgets
  have expired, without rerunning task workers.

Both direct `JobWorker` implementations and callback `ExecutionWorker`
registrations are exercised by the timeout, secrets, and handoff suites.

## pgjobdb update — 2026-10-24

pgjobdb now pins JobDB main `ba8ecfd3c373a70445bda05abaa5200fe6f5b992`
(`v0.0.51-0.20261024003652-ba8ecfd3c373`). The observations above about pgjobdb's
legacy fixtures describe its state before this update. This review does not
change or validate the separate c2j checkout.

The scheduler adapter and PostgreSQL storage APIs remain compatible: no further
production adapter or SQL changes are needed for runner removal. Existing
configuration, secrets delegation, deletion, supplied-lease, and route-discovery
support remain in place. The dependency adds transitive packages for upstream
remote capabilities and secret plugins; pgjobdb does not implement a secrets
provider.

All Go consumers in this repository now use `ExecutionContext`. Task calls use
`TaskCall` with stable sites, contract versions, valid JSON commands, and deferred
input builders. Direct `JobWorker` implementations remain supported through the
new `Run` signature; callback workers continue to use `ExecutionWorker`.
Alternates use `TaskCall.Alternate` / `TaskAlternateAfter`, with relative delays
anchored at the persisted wait. Tests wait for the recorded alternate deadline.

### Fixture and diagnostic changes

The copied engine-conformance and usage-parity fixtures were refreshed from this
JobDB revision while retaining pgjobdb's PostgreSQL and HTTP harnesses. Further
PostgreSQL fixture updates preserve behavioral checks without relying on the
removed runner's implementation:

- Retry assertions inspect logical execution attempts and committed retry
  decisions. A pending retry has no synthesized future attempt record.
- Parity checks compare execution-record identities, outcomes, and input/output
  data. Wait digests include activation-specific history, so comparisons between
  separate runs normalize nonempty digests; claim and completion tests still
  verify exact fencing values.
- The five-task workflow verifies its logical intermediate outputs. Error tests
  inspect task result records, and artifact tests follow recorded artifact keys
  for every failed attempt instead of assuming fixed chapter ordinals.
- Supplied-lease cancellation and renewal failures must not commit execution or
  job outcomes. Execution declarations and waits may already be durable.
- Versioned external waits require both input ordinal and input digest on
  `CompleteTaskIfWaiting`; the claim-race fixture now supplies both and verifies
  the scheduler rechecks state after preflight.
- Explicit job invocation deadlines remain effective across suspended external
  waits. The old alternate test that expected an invocation timeout to disappear
  now verifies timeout classification without an alternate handler.
- Restart prerequisite tests select a task ID from `GetJobRun.Execution` through
  `ExecutionRestart`, rather than injecting `RestartExtra` chapters. Tests of
  retired sequential-history layouts, extra-output mutation, and synthesized
  attempts were removed or replaced by execution-history checks.
- The private step-reset test was replaced with successful replay followed by a
  changed-contract replay. The latter must return `ErrWorkflowNotDeterministic`
  and leave recorded history unchanged, on toy and PostgreSQL engines.

The standalone historical stall diagnostic also uses public execution APIs. Its
`determinism` case suspends, then resumes with a conflicting declaration;
`prerequisite` restarts at a task boundary with a failed success prerequisite.
Both must commit a terminal failure. `task-restart` intentionally changes the
selected task's contract and succeeds. The old report and experimental core
patch remain historical evidence, not instructions for the current runner.

### Consumer upgrade

Rebuild pgjobdb servers and submitting applications with the updated dependency.
Implement `Run(ExecutionContext, JobData)` or register `ExecutionWorker`, move
input creation into the task builder, and replace data-attached alternate
options with task declarations. Read logical execution history and use its
restart IDs; physical chapter positions are storage addresses, not task steps.
The pinned dependency also uses compact execution protocol 3. Delete pre-cutover
workflow jobs and submit fresh jobs: sequential histories and protocol 1 or 2
execution histories are not migrated. The runner rejects protocol 1 and 2
histories before invoking consumer code. Multiple logical inspection records may
share a physical chapter ordinal. See [the execution guide](GUIDE-PGJOBDB-EXECUTION-SCOPES.md)
and upstream's [compact-history cutover requirements](https://github.com/colony-2/jobdb/blob/ba8ecfd3c373a70445bda05abaa5200fe6f5b992/docs/MIGRATION-COMPACT-EXECUTION-HISTORY.md).

### Validation

Passed on 2026-10-24 with workspace overrides disabled:

```sh
GOWORK=off go test -tags=integration -p 2 ./...
GOWORK=off go test -race -p 2 \
  ./pkg/pgjobdb/runtime/integration \
  ./pkg/pgjobdb/runtime/integration/workflow \
  -run 'TestPostgres(SuppliedLease|TaskAlternates)|TestTaskCompletionClaimsBeforeAppending|TestPostgresExecutionScopes|TestArtifactStorageOn|TestExecutionHistoryConstraints' -count=1
GOWORK=off go build -o /tmp/pgjobdb-legacy-jobdb ./cmd/jobdb
GOWORK=off go build -o /tmp/pgjobdb-stall-repro ./docs/repros/jobdb-workflow-stalls
```

All six diagnostic runs passed: toy and SQLite each completed the `determinism`,
`task-restart`, and `prerequisite` cases with the expected durable outcome.
The full suite includes direct PostgreSQL and HTTP/PostgreSQL conformance,
usage parity, configuration, secrets without a provider, deletion, supplied
leases, alternates, task completion recovery, and workflow behavior.

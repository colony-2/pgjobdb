# JobDB execution scopes and runner migration

The subsequent [job-goals update](REVIEW-PGJOBDB-JOB-GOALS.md) uses scheduler
format 7 and requires a fresh database. The additive dependency-deadline schema
discussion below describes the earlier format-6 change.

Execution scopes were introduced in JobDB v0.0.41
(`623a8276fedcce531a7c216a8c7ca812efb55c53`). pgjobdb now pins
`v0.0.51-0.20261024003652-ba8ecfd3c373`, which removes the sequential runner
and uses compact execution protocol 3.
The scheduler interface and upstream PostgreSQL chapter/schema stores remain
compatible. The shared JobDB core supplies execution-record serialization,
remote transport, scope restarts, and completion-summary projections.

## Consumer integration

Consumers register `workflow.ExecutionWorker` callbacks or implement
`JobWorker.Run(ExecutionContext, JobData)` directly.
Use `ExecutionContext.DoTask` with a stable `TaskCall` and deferred input builder,
and `DoScope` for structured retries, catches, and timeouts. `TaskWorker.Run`
keeps its signature. Execution histories are exposed through
`GetJobRunResponse.Execution`; scope restarts use `ExecutionRestart.ScopeID`.
`Yield` takes `workflow.YieldRequest`, and acknowledged handoffs are returned in
`JobRunOutcome.Handoff`.

See JobDB's [execution-scope migration guide](https://github.com/colony-2/jobdb/blob/ba8ecfd3c373a70445bda05abaa5200fe6f5b992/docs/MIGRATION-EXECUTION-SCOPES.md)
for the consumer contract and versioning rules. Rebuild and upgrade pgjobdb
servers before using the new engine remotely. `JobContext`, the old three-argument
`DoTask`, and data-attached `WithTaskOptions` alternates have been removed. Use
`TaskCall.Alternate` with a relative `TaskAlternateAfter` duration. Task declarations
must contain a stable `Site`, `ContractVersion`, and valid JSON `Command`; input
construction belongs inside the deferred builder.

This is a hard history-format cutover. Delete pre-cutover workflow jobs and submit
fresh jobs: neither sequential histories nor execution protocol 1 or 2 histories
are migrated. The runner rejects protocol 1 and 2 histories before invoking
consumer code. Deploy matching clients and servers. See the upstream
[compact-history migration guide](https://github.com/colony-2/jobdb/blob/ba8ecfd3c373a70445bda05abaa5200fe6f5b992/docs/MIGRATION-COMPACT-EXECUTION-HISTORY.md).
Tenant chapter schemas must
admit the `execution` body. Inspect logical records through `GetJobRun.Execution`
and use `ExecutionRestart.ScopeID` for task/scope boundaries instead of assuming
that physical chapter ordinals correspond to task steps.

## Dependency deadlines

There is one pgjobdb scheduler adaptation. A JobDB reschedule with both
`WaitForJobIDs` and `WaitUntil` becomes eligible when **all dependencies terminate
or the deadline arrives**, whichever happens first. Previously pgjobdb required
both, delaying completed joins and preventing deadline-driven recovery while a
child remained unfinished.

The adapter opts into `RescheduleRequest.WaitUntilIsDeadline`. The scheduler
persists that mode in the additive `pgjobdb.jobs.wait_until_is_deadline` boolean.
Completion of the last dependency clears the mode and makes the job available
immediately. Deadline expiry permits acquisition even with unfinished children.
Subsequent rescheduling replaces the mode. Polling, targeted acquisition, and
external-task claims all use the same READY classification.

Native callers retain the default not-before meaning of `AvailableAt`, including
when `WaitFor` is present. The original SQL reschedule signature retains that
behavior too. To opt into a dependency deadline, native Go callers set
`WaitUntilIsDeadline: true` together with `WaitFor` and `AvailableAt`. A JobDB
reschedule with only `WaitUntil` remains an ordinary delay.

The dependency-deadline schema update alone is additive: startup installs the
new column and functions on existing format-6 databases. Existing scheduler rows default to
their previous availability semantics. Upgrade all server replicas together so
an old installer cannot restore the previous function definitions. The new
execution protocol's required history cutover still applies; an additive scheduler
update does not make old workflow histories compatible. Chapter insertion remains
independent of scheduler transactions.

## Verification

Runner-removal validation on 2026-10-24 passed the full integration suite,
targeted race tests, and CLI/reproducer builds. See the
[runner-removal review](REVIEW-Legacy-Runner-Removal.md) for commands and coverage.

Verified on 2026-10-15: the full integration suite, focused execution/configuration/
secrets race tests, and CLI build pass. An additional upgrade check installed over
the previous populated format-6 schema and preserved the existing job's scheduled
time; applying the installer again also succeeded.

`TestPostgresExecutionScopes` covers direct and HTTP execution, task retries,
read-only replay, external input snapshots and stale completion rejection,
scope-patched restarts with preserved source history, and handoff receipts.
`TestPostgresDependencyDeadline` covers both wakeup paths, dependencies already
completed at rescheduling, waits without deadlines, and standalone delays.
`TestNativeDependencyDeadline` covers multiple children, failed terminal children,
polling, deadline-based task claims, reinstallation, and clearing deadline mode.
The upstream runtime conformance suite also includes completion summaries.

Run against the published dependency without workspace overrides:

```sh
GOWORK=off go test -tags=integration -p 1 ./...
GOWORK=off go test -race ./pkg/pgjobdb/runtime/integration \
  -run '^Test(PostgresExecutionScopes|PostgresDependencyDeadline|TenantConfiguration|PostgresSecretsWithoutProvider)$' -count=1
GOWORK=off go build ./cmd/jobdb
```

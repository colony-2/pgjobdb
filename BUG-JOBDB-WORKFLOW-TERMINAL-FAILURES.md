# Core JobDB: live determinism and restart-prerequisite errors leave jobs ACTIVE

**CI follow-up (2026-10-26):** the supplied October 21 failure at
`engine_additional_conformance_test.go:661` is the wait for the restarted job in
the retired `RestartExtra` test below. That API no longer exists in the current
runner. Before upgrading, the SQLite-only live determinism reproduction passed
on `70fda67ff4d5`: `status=COMPLETED Complete=1 Reschedule=0` with a failed
outcome. The current PostgreSQL checkout could not compile on that pin because
the newly committed goal-discovery adapter requires newer JobDB APIs.

The replacement `TestPostgresLiveDeterminismFailureCompletes` covers direct and
HTTP PostgreSQL using public execution APIs: suspend after a recorded task,
complete the external wait, then resume with a conflicting task contract. It
asserts a persisted failure, terminal scheduler state, unchanged existing
chapters, no repeated task execution, and no subsequent lease acquisition.
It also restarts at the final task boundary with a conflicting retained prefix
and verifies that the restarted job persists a terminal determinism failure.
This guards the actual ACTIVE-job stall rather than increasing a polling
timeout or retaining a test of removed sequential-history APIs.

Verified with JobDB v0.0.55 (`9c9d68441990`) on Linux, with workspace
overrides disabled:

```sh
GOWORK=off go test -tags=integration -p 2 ./...
GOWORK=off go test -race ./pkg/pgjobdb/runtime/integration \
  -run 'TestPostgresLiveDeterminismFailureCompletes|TestGoalDiscovery|TestDependencyDiscovery|TestPostgresGoalSharedRestartIdentity' -count=1
CGO_ENABLED=0 GOWORK=off go build ./cmd/jobdb
```

All passed, as did all six current toy/SQLite diagnostic cases. The fixture
upgrade extracts `JobKey` from engine submission handles and wraps restart
requests; the regression also checks the returned goal identities. macOS was
not available for a local rerun.

**Resolved in JobDB v0.0.33**, latest main commit
`0641e8940fad29ce9ce98768efe947c0dc65522a`, verified 2026-10-13. pgjobdb was upgraded to
that release during verification. All four previously failing pgjobdb tests pass unchanged, and all
four toy/SQLite reproductions below report `status=COMPLETED Complete=1
Reschedule=0` with a failed outcome and a durable JobAttemptOutcome chapter.
Verification uses `GOWORK=off` and the published dependency, with no local core
patch. The full `go test -tags=integration -p 1 ./...` suite and the
configuration/secrets race tests also pass with `GOWORK=off`. The remainder of
this report records the historical v0.0.32 analysis.

**Runner removal update (2026-10-24):** the linked executable now uses
`ExecutionContext` and public execution-history boundaries. Its current cases
are `determinism`, `task-restart`, and `prerequisite`; the selected task restart
may change its contract and is expected to succeed. The determinism and failed
prerequisite cases still verify durable terminal failures. The commands, private
step-reset hook, and v0.0.32 experiment described below are historical; the
current executable cannot be compiled against that old release.

Investigated 2026-10-13 after updating pgjobdb from JobDB v0.0.31 to the latest
published release, **v0.0.32**, commit
`b05f903e5bbdde192f92edf1d63077c45a839600`.

## Conclusion and impact

These failures originate in JobDB's shared workflow runner. They are not caused
by PostgreSQL scheduling, chapter uniqueness, configuration storage, or lease
expiry. There are two faulty error paths, described below. Both return without
calling `ExecutionLease.Complete` or `Reschedule`, leaving the job ACTIVE and
without a durable job outcome. A background worker discards the returned error,
so callers waiting for completion see a timeout. A later lease acquisition can
encounter the same unchanged history again.

The decisive evidence is a reproducer using only JobDB's built-in toy and SQLite
runtimes, with **no pgjobdb imports and no PostgreSQL server**. It traces actual
lease completion and reschedule calls. The failing runs report:

```text
status=ACTIVE Complete=0 Reschedule=0
```

A temporary core-only experiment also makes all four failing pgjobdb tests pass
without changing pgjobdb's runtime or scheduler. The experimental changes are
not applied to the production dependency.

## Reproducing without pgjobdb

[The standalone reproducer](docs/repros/jobdb-workflow-stalls/main.go) imports only
`github.com/colony-2/jobdb`. It uses synchronous `GetJobForRun` execution instead
of waiting for a background worker, so each reproduction takes seconds and
prints the actual error chain and surviving chapter types.

Historical commands used with the v0.0.33 checkout and original reproducer:

```sh
GOWORK=off go run ./docs/repros/jobdb-workflow-stalls -backend toy -case determinism
GOWORK=off go run ./docs/repros/jobdb-workflow-stalls -backend sqlite -case determinism
GOWORK=off go run ./docs/repros/jobdb-workflow-stalls -backend sqlite -case restart-mismatch
GOWORK=off go run ./docs/repros/jobdb-workflow-stalls -backend sqlite -case prerequisite
```

Historical isolated reproduction using the original `main.go`:

```sh
go mod init jobdb-stall-repro
go get github.com/colony-2/jobdb@v0.0.32
go mod tidy
go run . -backend sqlite -case prerequisite
```

The executable prints observations and exits normally after a reproduced stall;
an exit status of zero does not mean the job completed. The wrapper preserves
lease renewal and forwards operations unchanged; only call counters are added.
The determinism case uses JobDB's existing `ManipulateStepForTest` hook to reuse
ordinal 1 with different input, as the chapter-constraints test does. The
restart cases use ordinary public APIs and no test hook.

Observed on the unmodified published v0.0.32:

| Case | Backend | Job status | Complete calls | Remaining history |
| --- | --- | --- | --- | --- |
| Determinism mismatch | toy | ACTIVE | 0 | JobStart, TaskAttemptOutcome |
| Determinism mismatch | SQLite | ACTIVE | 0 | JobStart, TaskAttemptOutcome |
| Restart with mismatching extra input | SQLite | ACTIVE | 0 | JobStart, RestartExtra |
| Restart with failed success prerequisite | SQLite | ACTIVE | 0 | JobStart, RestartExtra |

All four also report zero reschedule calls. Their setup jobs complete normally
with exactly one completion call. All use one permitted job attempt and a
one-minute lease, and the failures return immediately while the lease is live.

The toy runtime does not reproduce either restart-specific case in this
reproducer: it completes them, and the resulting history contains an ordinary
task outcome rather than RestartExtra. Therefore, toy restart behavior is not
being used as evidence of correct restart handling; SQLite supplies the
independent reproduction of those paths.

## Bug 1: live determinism errors take a return path intended to protect history

Trigger: execute a live job whose task input differs from the input recorded at
the current ordinal, either through a conflicting restart extra or a reused
ordinal.

1. `DoTask` detects the input hash mismatch and returns `TaskInputMismatchError`.
2. `DoJob` returns immediately for `ErrWorkflowNotDeterministic` or
   `ErrMissingInputHash`. This guard applies to live execution as well as replay.
3. No terminal chapter is appended and no scheduler completion is requested.

The immediate error is:

```text
job tenant/mismatch did not reach a terminal state after execution: status=ACTIVE:
workflow was not deterministic: task echo ordinal 1 input hash mismatch ...
```

Expected: live execution should terminate with the original determinism error
available as the job result, preserving existing chapters and placing any new
terminal outcome at a valid append ordinal. Read-only replay must still return
its original mismatch without persisting anything.

Relevant source at the pinned commit:

- [Mismatch detection in DoTask](https://github.com/colony-2/jobdb/blob/b05f903e5bbdde192f92edf1d63077c45a839600/pkg/workflow/worker_runner.go#L909).
- [Unconditional early return in DoJob](https://github.com/colony-2/jobdb/blob/b05f903e5bbdde192f92edf1d63077c45a839600/pkg/workflow/worker_runner.go#L1492).
- [Background worker discards the returned error](https://github.com/colony-2/jobdb/blob/b05f903e5bbdde192f92edf1d63077c45a839600/pkg/workflow/worker_engine.go#L351).

This accounts for three failing test functions:

- `TestRestartWithExtraOutputDeterminismErrorOnDirectRuntime`
- `TestDirectRestartWithExtraOutputDeterminismErrorParity`
- `TestChapterConstraintsAcrossEngines` / `non-deterministic_job_fails_with_determinism_error`
  (both toy and PostgreSQL)

## Bug 2: failed RestartExtra prerequisites leave the cursor on an occupied ordinal

Trigger: complete an ordinary job, complete a separate prerequisite with an
application error, then restart the first job with `LastStepToKeep: 0`, an extra
task input/output, and a success prerequisite referring to the failed job.
The extra input matches the original task input, so this is not an input-hash
mismatch.

1. The restarted history contains JobStart at ordinal 0 and RestartExtra at 1.
2. `DoTask` reads RestartExtra, checks its prerequisite, and correctly finds that
   the prerequisite did not succeed.
3. It returns that error **before** `markStoryOrdinalConsumed(ordinal)`. The
   cursor remains 1.
4. `DoJob` copies that cursor back from the attempt runner and calls
   `checkCachedJobResult` at ordinal 1.
5. That lookup expects JobAttemptOutcome, finds RestartExtra, and returns a new
   determinism error. It masks the prerequisite error and prevents completion.

Observed error:

```text
job tenant/restart did not reach a terminal state after execution: status=ACTIVE:
workflow was not deterministic: unexpected chapter type "RestartExtra" at ordinal 1
```

Expected: the job should terminate with the failed-prerequisite result, retaining
RestartExtra and appending the terminal outcome after the retained history.

Relevant source:

- [Prerequisite error returns before cursor advancement](https://github.com/colony-2/jobdb/blob/b05f903e5bbdde192f92edf1d63077c45a839600/pkg/workflow/worker_runner.go#L900).
- [Cursor advancement occurs later](https://github.com/colony-2/jobdb/blob/b05f903e5bbdde192f92edf1d63077c45a839600/pkg/workflow/worker_runner.go#L934).
- [Post-execution cached-outcome lookup](https://github.com/colony-2/jobdb/blob/b05f903e5bbdde192f92edf1d63077c45a839600/pkg/workflow/worker_runner.go#L1513).
- [Unexpected chapter type becomes a cache error](https://github.com/colony-2/jobdb/blob/b05f903e5bbdde192f92edf1d63077c45a839600/pkg/workflow/worker_runner.go#L767).

This accounts for `TestRestartPrerequisitesCheckedAtRestartExtra`.

## Regression source and controlled experiment

[Commit ffe70ccf111c6efe17662e239b19cf40b36f708d](https://github.com/colony-2/jobdb/commit/ffe70ccf111c6efe17662e239b19cf40b36f708d),
“Fix replay of interrupted job attempts and isolate timeout state,” introduced
both the unconditional determinism-error return and the move of cursor
advancement below the RestartExtra prerequisite check. This is confirmed by its
diff; a full release-by-release bisect was not performed. Previous investigation
also established that the focused engine-conformance and workflow cases pass
on pgjobdb's previous v0.0.23 pin.

In a disposable copy of v0.0.32, I made just two diagnostic changes:

1. Keep the immediate determinism-error return for replay, but for live execution
   seek past durable task/restart chapters before attempting to record failure.
2. Advance past the already-read RestartExtra before returning its prerequisite
   failure.

[Experimental patch](docs/repros/jobdb-workflow-stalls/core-experiment.patch).
With only that temporary JobDB substitution, and the original pgjobdb tests
unchanged, the previously failing packages' focused runs passed:

```text
engineconformance  3.083s  PASS
usageparity        5.962s  PASS
workflow          7.132s  PASS
```

The same commands against published v0.0.32 fail in 12.808s, 13.003s, and 96.977s
respectively. No SQL, PostgreSQL adapter, lease duration, or test timeout was
changed between these two runs.

```sh
GOWORK=off go test -p 1 \
  ./pkg/pgjobdb/runtime/integration/internal/jobdbtest/engineconformance \
  ./pkg/pgjobdb/runtime/integration/internal/jobdbtest/usageparity \
  ./pkg/pgjobdb/runtime/integration/workflow \
  -run '^(TestRestartWithExtraOutputDeterminismErrorOnDirectRuntime|TestDirectRestartWithExtraOutputDeterminismErrorParity|TestChapterConstraintsAcrossEngines|TestRestartPrerequisitesCheckedAtRestartExtra)$' \
  -count=1
```

The patch is an isolation experiment, **not a production-ready fix**. Before
merging a core fix, test replay immutability, existing terminal outcomes after a
crash, retry boundaries, interrupted job timeouts, and lease loss. In particular,
seeking an existing outcome must not hide the original mismatch or overwrite
history. Background worker error reporting should also expose unhandled runner
errors instead of silently waiting for lease expiry.

## Separate v0.0.32 configuration contract change

This release intentionally caches the first successful configuration read for a
live workflow attempt. Administrative and direct lease reads remain fresh. The
previous pgjobdb workflow-context test expected freshness within the same
attempt; it was updated to assert the new snapshot contract while checking that
administrative reads see the mutation. Configuration and secrets checks pass
under the race detector with the **unmodified published v0.0.32**.

That intended change is separate from the four workflow failures above. At the
time of the investigation, the production dependency was v0.0.32 with no local replace or experimental core
patch applied.

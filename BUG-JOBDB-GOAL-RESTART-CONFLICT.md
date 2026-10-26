# JobDB HTTP client loses typed request-ID restart conflicts

**Resolved in JobDB v0.0.57 (`192d83188d386657970c2c69fdf43a4de052470c`),
verified 2026-10-26.** pgjobdb now uses this release. Its direct and HTTP
restart tests both require `ErrExistingJobMismatch` and `ErrConflict`, with no
HTTP-specific exception. The standalone toy/HTTP check also passes and exits
unsuccessfully if either transport loses either error identity:

```text
direct: mismatch=true conflict=true error=different goal sharing
http: mismatch=true conflict=true error=different goal sharing
```

Validation passed on Linux with `GOWORK=off`: the full
`go test -tags=integration -p 2 ./...` suite, focused restart-conflict and live
determinism race tests, and the CLI build. JobDB's upstream
`TestGoalRestartRequestIDConflicts` also passes, covering direct and
lease-authorized restarts, retries after destination progress, and goal closure.

The remaining report describes the historical failure.

Reproduced against JobDB `70fda67ff4d5714ccb7c4e8b635846746e1703c1`
(`v0.0.53-0.20261024021812-70fda67ff4d5`) on 2026-10-24.
This is a core remote-client issue, reproduced with the toy runtime and no
PostgreSQL code or database.

It was still reproducible on 2026-10-26 with JobDB v0.0.55
(`9c9d68441990b0a5de42363c1bfef248175ddd5f`), with the same output below.

A generated-ID restart with a stable request ID succeeds. Reusing that ID with
changed semantic parameters correctly conflicts. The server returns HTTP 409:

```json
{"code":"existing_job_mismatch","message":"different goal sharing"}
```

The direct runtime error matches both `ErrExistingJobMismatch` and `ErrConflict`.
The HTTP client returns a formatted error matching neither sentinel, so callers
cannot classify this new restart-idempotency conflict through the public errors.
The operation is rejected correctly and the original job remains intact.

Run the standalone reproduction from pgjobdb:

```sh
GOWORK=off go run ./docs/repros/jobdb-goal-restart-conflict
```

Observed output:

```text
direct: mismatch=true conflict=true error=different goal sharing
http: mismatch=false conflict=false error=submit restart job: http 409: {"code":"existing_job_mismatch","message":"different goal sharing"}
```

In `pkg/jobdb/runtime/remote/runtime.go`, `Runtime.SubmitRestartJob` routes the
explicit-destination PUT error through `explicitJobCreateError`, which recognizes
`existing_job_mismatch`. The generated-destination POST branch uses
`responseError`, which does not recognize that code. New goal-error decoding
also omits it.

Use the same typed conflict decoding for the POST branch (and audit the
lease-authorized generated-ID restart path). Add a regression covering a
successful shared restart, changed parameters with the same request ID, and
`errors.Is(err, ErrExistingJobMismatch)` through both local and HTTP runtimes.
Equivalent retries after destination progress or goal closure must still return
the original handle.

Before the fix, pgjobdb's test asserted the direct typed error and the correct
HTTP 409/code. No alternate transport or error-rewriting workaround was added
to pgjobdb.

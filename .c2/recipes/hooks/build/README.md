# Native verification

`verify.yaml` is a repository-local `c2.build-hook/v1` hook. It uses the supported
legacy recipe input boundary without an input schema so optional settings can be
absent, null, or native/value artifacts. Do not add `schema_version: 2` without
migrating and testing that input boundary: v2 rejects undeclared caller fields.
Request, target-directory and settings text are never interpolated into commands.
Checks always select packages from the candidate root.

Each named const operation independently declares pinned Go 1.26.1, Python and Git.
`verify.py` creates its own temporary module file, module/build caches, binaries,
home and temporary directory. It preserves scoped Git environment only during
compilation. Compilation explicitly sets `GOWORK=off` and clears inherited `GOFLAGS` so neither a workspace nor an overlay can substitute candidate dependencies. Named pass/fail/skip counts are retained; any skipped native test fails its behavior.
Tests receive an environment allowlist without C2J, Pulse, JobDB,
cloud or inherited test-endpoint settings. Tests run as the current non-root user,
or UID/GID 65534 when the worker is root; supplementary groups are cleared.
Tests use `LANG=LC_ALL=C.UTF-8` so embedded initdb creates UTF-8 databases
regardless of a worker's C/POSIX locale. The chosen locale is recorded in evidence.
No Docker or remote CI is invoked. CGO is disabled; this hook does not claim race
coverage, which the pgjobdb test workflow does not request.

| Workflow behavior | Hook task or held reason |
| --- | --- |
| test.yml native arm64 `go test -v -tags=integration ./...` | `service_free`: all packages outside installer and runtime/integration, including CLI configuration/help/completion/rejection tests and compilation of packages with no test files. Database command tests run in `runtime`. |
| Same native suite: installer | `installer`: complete pkg/pgjobdb/installer and pkg/pgjobdb/projects suites, including explicit installation, inventory preservation, receipts and independent SQL guards. Required coverage includes opaque catalog strings, admission projection integrity, committed catalog updates newer than an active resource writer's transaction ID, plus rejection of uncommitted seals. |
| Same native suite: runtime conformance | `conformance`: all names containing Conformance, enumerated from the compiled integration test binary, including workflow runtime, start-or-append, fixture isolation and cell conformance. |
| Same native suite: other runtime tests | `runtime`: remaining tests in runtime/integration and all descendant packages, including engineconformance, usageparity and workflow, plus PostgreSQL consumer-goal, project and resource migration CLI tests in cmd/jobdb. The hook builds and supplies the actual production CLI. No package is discarded because it also contains conformance tests. |
| test-summary | `report`: checks all four behavior statuses and required logs/JSON; exports verification.md and environment.json. |
| test.yml architecture/macOS matrix | Each run covers its recorded native Linux arm64 or amd64 host. Other architecture and macOS runs remain held. |
| test.yml container-smoke | Held: Docker daemon/buildx, amd64 release-shaped image, Postgres/MinIO service topology and container persistence smoke. |
| release.yaml tests | Covered by the same native selection above. |
| release.yaml wait/tag/release/image/signing/login/push | Held: remote Actions, publication credentials, Docker and signing. |

The Go command is translated into `go list -tags=integration ./...`, per-package
`go test -tags=integration -c -o <scratch>`, then native test binaries with
`-test.v=test2json -test.count=1 -test.timeout=25m`. Splitting compilation from execution
keeps private module credentials out of non-root test processes. Test-name
selection partitions the integration and command packages; nested packages run completely.
Enumeration requires the focused consumer and database command tests and all existing
nested packages. Go test2json records every selected test outcome; a failed, absent
or skipped required top-level test fails acceptance even after a zero process exit.
Focused consumer/database-command subtests and required PostgreSQL task-delegation
runner/harness subtests cannot pass through skips. The task-delegation group in
internal/jobdbtest is required during enumeration, including default and signed
consumer servers, nonempty namespaces, and the standard remote PostgreSQL factory.
Every selected child outcome, including shared conformance and availability checks,
must pass; any skipped native test fails its behavior and remains recorded.
There are no pin-specific failure exceptions. The gate requires the shared
cell archive conformance test, each tenant/project HTTP case, the visibility
and reopen check, and migration/rollback for all three authentic format-14
layouts. The installer stage checks out the historical module by exact Git SHA
and checks its module hash before compiling a fixture builder, then runs it
without acquisition credentials. The report records the candidate and exact
selected JobDB module identity and checksums.
The runtime stage requires the two-connection initial-history barrier,
concurrent selection with equivalent and conflicting requests, and initial
history rollback with inline and blob-backed artifact readback on retry.
The service-free selection first executes the bounded hook unit tests from an
isolated source copy as the same unprivileged UID, without acquisition credentials.
Per-command execution is bounded to 30 minutes. Unprivileged Linux subreaper
semantics adopt daemonized PostgreSQL descendants; timeout/cancellation cleanup
stops process groups and adopted children, then reaps them. Signal cancellation
unwinds any active child wait before it stops and reaps the process group.
Ordinary test/compile failures retain evidence and allow independent packages and
recipe tasks to continue. Launch/setup errors fail the behavior, recording that
dependent tests were not run. Evidence/report generation failures fail the hook;
the standard shared caller handles operation failures and cancellation.

The installer lane builds the standalone format-13 producer and authentic
format-14/15 producers from the pinned published modules. The lease activity,
archive preservation, concurrent initial-history, and append conformance checks
are required with the current JobDB dependency; skips and failures reject the candidate.

Embedded PostgreSQL uses the existing test helpers and pinned embedded-postgres
v1.32.0 default PostgreSQL 17.5.0. Native Linux binaries are downloaded by that
library using its checked checksum behavior. Startup failure is a failure, never a
passing skip. If a future binary becomes unavailable, report its exact limitation
and make any held-suite change explicit; do not silently skip database tests.

Run bounded failure tests with `python3 .c2/recipes/hooks/build/tests/test_verify.py`.
Generate candidate-loader fixtures with that script's `--fixtures <scratch-dir>`
and run `c2j test run --directory <scratch-dir> --out-dir <scratch-results>`.
Loader fixtures read current source, use actual cell:// loading and result-schema
validation, mock the expensive suites, and execute the report command for pass
and failure cases. Acceptance separately runs the real native suites.

Resource acceptance requires the PostgreSQL conformance, boundary and installed-protection suites,
format-13 conversion, project preservation and rollback suites, and migration command tests during
enumeration. Every one of the 29 shared resource cases must appear with a passing
outcome, including B09b, B11a and real workflow B11 timeout recovery. A passing
wrapper, empty selection, missing child, failure or skip cannot certify support.
Named children are also required for workflow resumption and acknowledgment
failures, and for migration rejection of invalid legacy crash data.
The runtime partition also requires cancellation during resumed EAGAIN execution
and ordinary HTTP operations from a pre-EAGAIN client. The hook builds that
client from its standalone module in `scripts/testdata/pgjobdb-legacy-client`,
then supplies the binary via `PGJOBDB_TEST_LEGACY_CLIENT`. The server test
checks the binary's linked JobDB version and rejects module replacements.
Without a supplied binary, ordinary `go test` builds the same isolated module in
a temporary directory. Release tests prebuild it before switching to their
separate test home so the build can use the configured private-module access.
The hook also requires passing child results for reopening tenant configuration
on the current format and for verifying the project search path during migration.
No resource case or new assertion is exempt.

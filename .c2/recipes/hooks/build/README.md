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
compilation. Tests receive an environment allowlist without C2J, Pulse, JobDB,
cloud or inherited test-endpoint settings. Tests run as the current non-root user,
or UID/GID 65534 when the worker is root; supplementary groups are cleared.
Tests use `LANG=LC_ALL=C.UTF-8` so embedded initdb creates UTF-8 databases
regardless of a worker's C/POSIX locale. The chosen locale is recorded in evidence.
No Docker or remote CI is invoked. CGO is disabled; this hook does not claim race
coverage, which the pgjobdb test workflow does not request.

| Workflow behavior | Hook task or held reason |
| --- | --- |
| test.yml native arm64 `go test -v -tags=integration ./...` | `service_free`: all packages outside installer and runtime/integration, including CLI configuration tests and compilation of packages with no test files. |
| Same native suite: installer | `installer`: complete pkg/pgjobdb/installer suite. |
| Same native suite: runtime conformance | `conformance`: TestPostgresRuntimeConformance, TestConformanceFixturesAreIsolated and TestPostgresCellConformance, enumerated from the compiled integration test binary. |
| Same native suite: other runtime tests | `runtime`: remaining tests in runtime/integration and all descendant packages, including engineconformance, usageparity and workflow. No package is discarded because it also contains conformance tests. |
| test-summary | `report`: checks all four behavior statuses and required logs/JSON; exports verification.md and environment.json. |
| test.yml amd64/macOS matrix | Held: alternate architecture/OS. |
| test.yml container-smoke | Held: Docker daemon/buildx, amd64 release-shaped image, Postgres/MinIO service topology and container persistence smoke. |
| release.yaml tests | Covered by the same native selection above. |
| release.yaml wait/tag/release/image/signing/login/push | Held: remote Actions, publication credentials, Docker and signing. |

The Go command is translated into `go list -tags=integration ./...`, per-package
`go test -tags=integration -c -o <scratch>`, then native test binaries with
`-test.v -test.count=1 -test.timeout=25m`. Splitting compilation from execution
keeps private module credentials out of non-root test processes. Test-name
selection partitions only the integration package; nested packages run completely.
Per-command execution is bounded to 30 minutes. Unprivileged Linux subreaper
semantics adopt daemonized PostgreSQL descendants; timeout/cancellation cleanup
stops process groups and adopted children, then reaps them.
Ordinary test/compile failures retain evidence and allow independent packages and
recipe tasks to continue. Launch/setup errors fail the behavior, recording that
dependent tests were not run. Evidence/report generation failures fail the hook;
the standard shared caller handles operation failures and cancellation.

Embedded PostgreSQL uses the existing test helpers and pinned embedded-postgres
v1.32.0 default PostgreSQL 17.5.0. Linux arm64v8 binaries are downloaded by that
library using its checked checksum behavior. Startup failure is a failure, never a
passing skip. If a future binary becomes unavailable, report its exact limitation
and make any held-suite change explicit; do not silently skip database tests.

Run bounded failure tests with `python3 .c2/recipes/hooks/build/tests/test_verify.py`.
Generate candidate-loader fixtures with that script's `--fixtures <scratch-dir>`
and run `c2j test run --directory <scratch-dir> --out-dir <scratch-results>`.
Loader fixtures read current source, use actual cell:// loading and result-schema
validation, mock the expensive suites, and execute the report command for pass
and failure cases. Acceptance separately runs the real native suites.

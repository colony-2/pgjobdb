# Verify resource waits against a published pgjobdb module

Run after publication, as a non-root user on Linux with Go and network access
for the pinned Go modules and embedded PostgreSQL. Use an isolated database, not
a production DSN. This fixture does not need access to pgjobdb internals.

1. Create a fresh external Go module and initially empty `GOMODCACHE` and
   `GOCACHE`. Set `GOWORK=off`, clear `GOFLAGS`, and use no `replace` or overlays.
   Copy only `main_test.go` from this directory to the consumer. Resolve
   `github.com/colony-2/pgjobdb` at the actual published full Git revision.
2. Save `go mod download -json` receipts for `github.com/colony-2/pgjobdb` and
   `github.com/colony-2/jobdb` and verify their versions, `Origin.Hash`, `Sum` and
   `GoModSum` against the publication records. The expected JobDB pin is
   `v0.0.99-0.20261122051819-86f7ba9bc009` at
   `86f7ba9bc009ca5dc851a7e8a0057c63d788bc8b`. Run `go mod tidy`, retain
   `go list -m -json all`, and verify both selections and the absence of
   replacements before testing.
3. Get the cached pgjobdb directory with
   `go list -m -f '{{.Dir}}' github.com/colony-2/pgjobdb`. Set
   `PGJOBDB_HISTORY_FIXTURES` to `pkg/pgjobdb/installer/testdata` beneath that
   directory. Verify it is under the fresh module cache. Run
   `go test -count=1 -json .` in the consumer and retain its output. The fixture
   checks explicit offline migration paths from formats 9–13 and public durable
   wait/replay/resumption after an actual PostgreSQL stop and restart. For the
   authentic 9–11 fixtures, the converter preserves the retired schedules table;
   project inspection must still reject that participant. Do not remove it from
   the original database just to pass this check. The 12 and 13 fixtures also
   exercise project installation and public runtime reopen.
4. Run the exact published integration and installer packages by import path
   from the same consumer, after confirming their `go list -f '{{.Dir}}'` entries
   resolve within the verified pgjobdb cache directory. Use
   `go test -count=1 -json -tags=integration` with the appropriate package and
   `-run` expression and retain the named outcomes. Include
   `TestResourceBackoffConformance` (all 29 child cases, P01), historical and
   resource migration tests (P02, P02a–P02b),
   `TestResourceBackoffPostgresProcessRestart` (P03), and countdown/default
   boundary and installed-protection cases (P04, P04a–P04d).

Match the actual named outcomes to the acceptance checks; a missing, failed, or
skipped mandatory case leaves release acceptance incomplete. A build against a
source checkout can validate the fixture before merge but is not release evidence.

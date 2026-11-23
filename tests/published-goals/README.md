# Exact-publication acceptance

Run this harness after native merge and module publication, from the published
checkout or its retained source archive. Supply the actual pgjobdb version, full
merged commit, module sum and go.mod sum from that publication:

```sh
python3 tests/published-goals/accept.py \
  --version "$PGJOBDB_VERSION" --commit "$PGJOBDB_COMMIT" \
  --sum "$PGJOBDB_SUM" --gomodsum "$PGJOBDB_GOMODSUM" \
  --out /tmp/pgjobdb-published-acceptance
```

The script starts with empty module/build caches and creates a standalone consumer
outside this checkout. It uses normal direct module acquisition, checks both
origin hashes and both checksums for pgjobdb and the exact JobDB project publication,
rejects replacements anywhere in the graph, and independently builds a linked
identity probe. The Go suite starts real PostgreSQL as an unprivileged user and
runs public admission/protected-record conformance through direct, default HTTP
and signed HTTP paths. It also exercises explicit project installation, invalid/partial inventory,
first-use admission, administration, default-deny deletion, retained creation
receipts, reopen, raw SQL guards and the actual `direct`/`serve` CLI binaries.
Required catalog regressions include opaque NUL strings and metadata across
projects, exact retained receipts after reopen, and fail-closed admission when
the derived projection is missing, stale or altered.
The harness copies consumer-only fixture adapters and public project test cases
from this checkout into its isolated module; production code is linked exclusively
from the verified published dependency graph. It converts authentic format 11 and inspects the migrated
protected record through all three readers after reopening.

Missing publications, changed selections, origin/checksum/linked identity mismatch,
required startup/SQL errors, absent transport groups, failed behavior or any skip
fail acceptance. The output keeps the actual consumer source, historical fixture,
module files, acquisition/selection/identity receipts and command/behavior logs.
`acceptance.json` is created only after every check passes. Existing success output
is removed before a new attempt.

Validate the publication checker before merge with:

```sh
python3 -B -m unittest discover -s tests/published-goals -p 'test_*.py'
```

A candidate-linked run can validate this harness before merge, but it does not
satisfy exact-publication acceptance. No replacements or candidate-linked results
are accepted by `accept.py`. This harness performs no deployment or production
activation.

For candidate compilation, call `accept.prepare_harness(source, scratch)` from
Python with `source` pointing at this directory, then compile its generated
`*_test.go` files with `go test -tags=published_goals -c` from the candidate module.
Run that binary unprivileged with `GOALS_HISTORY_FIXTURE` pointing at the authentic
format-11 fixture and `PGJOBDB_TEST_CLI` pointing at the candidate-built command.
This source-linked check is preparatory evidence only.

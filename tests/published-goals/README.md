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
origin hashes and both checksums for pgjobdb and the exact JobDB goal publication,
rejects replacements anywhere in the graph, and independently builds a linked
identity probe. The Go suite starts real PostgreSQL as an unprivileged user and
runs public admission/protected-record conformance through direct, default HTTP
and signed HTTP paths. It converts authentic format 11 and inspects the migrated
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
go test -tags=published_goals -run '^$' ./tests/published-goals
```

A candidate-linked run can validate this harness before merge, but it does not
satisfy exact-publication acceptance. No replacements or candidate-linked results
are accepted by `accept.py`. This harness performs no deployment or production
activation.

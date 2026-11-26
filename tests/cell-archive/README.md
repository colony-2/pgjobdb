# Disposable PostgreSQL cell archive fixture

Run these commands on native Linux with Go 1.26.1, Python 3, and a non-root
account (PostgreSQL `initdb` rejects root). The fixture downloads the same
embedded PostgreSQL binaries used by the integration suite. Do not point any
of these commands at an existing store. The fixture starts a private disposable
database and binds the HTTP listener to `127.0.0.1` on an ephemeral port.

```bash
scratch=$(mktemp -d)
GOWORK=off go build -mod=readonly -o "$scratch/fixture" ./pkg/pgjobdb/runtime/integration/cmd/cellarchivefixture
"$scratch/fixture" -info "$scratch/fixture.json" >"$scratch/fixture.log" 2>&1 &
fixture_pid=$!
trap 'kill -TERM "$fixture_pid" 2>/dev/null || true; wait "$fixture_pid"; rm -rf "$scratch"' EXIT
for attempt in $(seq 1 90); do
  test -s "$scratch/fixture.json" && break
  kill -0 "$fixture_pid" || break
  sleep 1
done
base=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["baseURL"])' "$scratch/fixture.json")
curl --fail --silent --show-error "$base/readyz"
python3 tests/cell-archive/smoke.py "$scratch/fixture.json"
# Explicit cleanup, or exit the shell to invoke the trap:
kill -TERM "$fixture_pid"
wait "$fixture_pid"
test ! -e "$scratch/fixture.json"
rm -rf "$scratch"
trap - EXIT
```

The launcher seeds nine named cells on startup, including an alias, an
interleaved out-of-scope cell for each archive state, and one retired cell.
The descriptor exists only after database initialization and seeding. It
contains `baseURL`, the disposable PostgreSQL `dsn`, the `project` identifier
(also the tenant route identifier), `otherProject`, `seeded` cells and
`hiddenCellIds`. The HTTP server only exposes readiness and the existing JobDB
cell list, get, archive and unarchive routes. Send an `Authorization: Bearer`
header with one of the following test-only strings:

| Token | Fixture scope |
| --- | --- |
| `fixture-writer` | Scoped list, get and archive/unarchive |
| `fixture-reader` | Same scope, list and get only |
| `fixture-wrong-tenant` | Writer in a different project |
| `fixture-outside-scope` | Writer for an interleaved cell, not the smoke target |
| No header | No authority |

Use `/v1/tenants/{project}/cells` or `/v1/projects/{project}/cells`. Send the
returned quoted `ETag` as `If-Match` on `POST /cells/{id}/archive` and
`POST /cells/{id}/unarchive`. The smoke checks both route families, all three
archive list modes across multiple pages, cursor rejection, scoped denials and
state round trips. Stopping the launcher shuts down PostgreSQL and removes its
data, blob directory and connection descriptor. These credentials are only
valid for this disposable launcher and must never be added to a real service.

## After Git publication

Run the following with the **full SHA from recipe-managed publication** and a
new receipt directory. Use ordinary module acquisition credentials if needed;
the fixture process receives none of them.

```bash
python3 tests/cell-archive/published.py --commit "$PGJOBDB_PUBLISHED_SHA" \
  --out /tmp/pgjobdb-archive-published-receipt
```

The checker resolves pgjobdb and the pinned JobDB in fresh Go caches with
`GOWORK=off` and no replacements. It builds an external consumer and the
launcher **from the downloaded published module**, checks both linked module
versions and sums, starts the fixture, and runs the external consumer and HTTP
smoke from that published source. It writes `receipt.json` only on success;
send it to Root and Cortex along with the linked identities, smoke log, and
these commands. A rehearsal against a worktree is not publication evidence.

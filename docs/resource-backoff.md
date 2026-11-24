# Resource waits and crash credit

`workflow.EAGAINWait(ctx, options...)` suspends the unfinished attempt when a
resource is temporarily unavailable. Completed work, original durable input,
selected task implementation and original deadlines survive the wait. It does
not enter ordinary retry/catch paths or create a new recipe attempt.

The PostgreSQL adapter implements JobDB resource protocol 1 with durable exact
receipts. The default policy waits two seconds, doubles subsequent waits up to
30 seconds, samples 20% jitter, and binds the configured crash allowance on the
first accepted wait for an invocation/attempt/reason. Options can choose an
initial wait, growth, maximum, explicit zero jitter and a positive int64 crash
allowance. There is no resource retry ceiling. An invocation can be 16 KiB;
sequences use digest indexing and compare the full retained key.

Each accepted transaction validates the current lease, route/task generation,
immutable chapter checkpoint and payload revision. It binds policy/allowance,
samples once, selects the continuation, updates the payload once, assigns credit,
records the exact wake/receipt and releases the lease atomically. An exact retry
returns the original receipt before reading current lease, default or checkpoint
state. A changed binding rejects. Receipts survive completion and soft deletion;
actual identity cleanup removes them.

PostgreSQL indexes an upward-rounded microsecond wake while retaining exact
nanoseconds and immutable receipt bytes. Polling, targeted/task acquisition,
external successful completion and append cannot bypass the full resource wake.
An earlier original deadline independently admits job timeout handling. That
route remains recoverable after repeated handler crashes at zero credit; it
never grants task-body or append admission or refills the balance.

Crash credit counts remaining expired execution-lease recoveries. At the default
five, the initial claim keeps five; successive successful recovery claims install
4, 3, 2, 1 and 0. The final live zero lease remains ACTIVE and renewable. After its
expiry, body execution is blocked. Reads, renewal, waiting, child submission,
receipt replay and existing-job append never refill credit. Internal append
leases retain their expiry exemption. Lifetime expiry statistics stay separate.

`pgjobdb.set_crash_concern_threshold(positive_integer)` now sets the default for
future assignments. Fresh generations, ordinary successful reschedules and
guarded crash clearing use that default. Existing credit and bound resource
policies do not change. Raising the default does not revive exhausted jobs;
use guarded clearing after diagnosing the failure.

## Existing databases

Stop incompatible scheduler writers and configuration setters, back up the
store, then explicitly inspect and convert it before starting upgraded servers:

```sh
jobdb direct check-migrate --postgres-dsn "$PGJOBDB_MIGRATION_DSN"
jobdb direct migrate --postgres-dsn "$PGJOBDB_MIGRATION_DSN" --report-file cutover.json
```

Both commands require an explicit database flag. Migration requires a new report
file; it never overwrites prior evidence. It fully writes and syncs staged bytes
before database commit, then emits a separate commit confirmation containing the
exact report digest. Staged bytes alone do not prove commit. If acknowledgment is
uncertain, retain the file and reconcile the marker, balances, retained facts and
exact allocated failure IDs on a fresh connection before retrying.

Fresh runtimes require an explicit [project initialization](projects.md) on format 14. Startup/Apply reject formats 9–13 unchanged with
an explicit migration error. The explicit migration composes historical 9–12
retained-goal conversion and countdown conversion in one transaction; authentic
13 converts directly. It captures the original threshold T before replacing any
functions and converts each original execution count C to `max(T-C, 0)` once.
Protected goal failures remain inert at zero. Leases, original expired identities,
routes, payload revisions, history, waits and retained-goal protections survive.
Writer exclusion, validation or report failure rolls back both phases. Repeating
migration on 14 is a no-op and never converts balances again. For a format-13
store with an installed project catalog, the cutover validates its original
fingerprint, preserves catalog and receipt bytes, and records the new fingerprint
with the same commit. Guards cover the new retained tables. Use the installer for
reapplication on project-enabled stores; standalone schema SQL refuses them to
protect existing guards.

Version-two reports preserve historical arrays and add phase path, original and
target defaults, formula and per-tenant credit counts. Legacy inputs are available
only when the original source was 9–12. Source 13 cannot reconstruct discarded
secondary links. See [retained goals](retained-goals.md) for those guarantees.

Formats 1–8, future formats, old pgwf, unmarked/corrupt stores, mixed old/new
scheduler writers and downgrade remain unsupported. Upgrade every execution
reader on a route before enabling resource checkpoints. Durable HTTP receipt
recovery needs stable signing authority and audience; ephemeral authority
advertises durability zero. Source publication does not establish a server or
worker upgrade.

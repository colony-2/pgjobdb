# PostgreSQL projects

Every resource writer uses an immutable project ID. Project names support
discovery; resource addressing retains the ID in scheduler rows, chapter keys,
schemas, configuration, artifact paths and provider namespaces. Installation
preserves historical bytes.

`runtime.NewSQLDB`, `OpenDSN`, `direct` and `serve` only open prepared storage.
They refuse missing, partial, damaged or unsupported project installations. They
never initialize a database, convert scheduler formats or import projects.
Scheduler format remains 13; project installation has independent protocol 1.

## Prepare empty storage

Fence all participants before maintenance. Inventory every scheduler, independent
chapter/schema/configuration store, artifact bucket, secret provider, external
writer, backup/restore path and their identity namespaces. Record their versions,
immutable IDs, owners and fencing evidence in a separately reviewed document.
`--participants-sha256` binds the SHA-256 of that document; a digest does not prove
its completeness or establish a fleet fence. Keep its original content and keys.

With a database that contains no JobDB storage:

```sh
jobdb project postgres installation initialize \
  --participants-sha256 "$PARTICIPANTS_SHA256"
jobdb project postgres create --name example --request-id "$REQUEST_ID"
```

These commands use `JOBDB_POSTGRES_DSN`, or an explicit `--postgres-dsn`. Local DSN
access is explicit operator authority. Initialization prepares scheduler/shared
storage and installs **zero** projects. Project creation returns the generated
immutable ID and a retained receipt. If omitted, the creation request ID is printed
to stderr before any database work; save it and reuse it after uncertainty.

The initializer serializes its public store preparation using a dedicated session,
so its pool needs at least two connections. Failed preparation can leave empty
infrastructure. Diagnose that partial state explicitly; do not repeatedly run the
fresh initializer or delete historical storage to make it appear empty.

## Adopt historical storage

Back up first and keep every participant fenced through reconciliation and reopen.
Convert supported scheduler formats 9–12 explicitly with
`installer.Installer.ApplyWithReport`, retaining the staged report and committed
confirmation. Opening a runtime no longer performs that conversion.

If all five shared tables are absent, explicitly prepare them:

```sh
jobdb project postgres installation prepare-shared
jobdb project postgres installation inspect \
  --participants-sha256 "$PARTICIPANTS_SHA256" > inventory.json
```

A partial shared set requires diagnosis. Inspection is read-only and validates
the scheduler/shared contract. Unknown JobDB participant tables, unsupported
partitions/foreign tables, retained retired tables such as `pgjobdb.schedules`, unstable resolution and invalid historical IDs are
rejected. Inspection hashes typed database rows, including raw BYTEA and JSON,
and collects every tenant/anthology/base-anthology identity. It does not scan
external stores. Add `--external-id ID` for each reviewed external-only identity.
Supported historical identities are KSUIDs, `c2` and `0`; unsupported IDs require
a separate design, never automatic renaming or remapping.

Review the inventory and a complete ID-to-name mapping before installing:

```sh
jobdb project postgres installation install --inventory inventory.json \
  --seed c2 --seed 'IMMUTABLE_ID=discovery-name'
```

Mappings must cover exactly the database and external-only identities. `c2`
retains name `c2`. Imported projects are permanently sealed. The installer takes
NOWAIT exclusive participant-table locks, rechecks the approved inventory, and
atomically commits catalog, guards and a completion receipt. Busy writers, changed
content, report failure and invalid mappings leave no completion. Empty/partial
project artifacts and future protocols are refused without repair.

The report on stderr is staged **before** commit. Only a successful result with
`committed:true` confirms the operation. After an uncertain result, retry the same
approved inventory and mappings. A matching retained receipt verifies the original
installation even after normal resource traffic. A different descriptor conflicts;
do not replace the saved inventory with a new one to conceal uncertainty.

Restore catalog, receipt, scheduler and all shared resources together. The receipt
binds relation OIDs: consistent physical restores preserve them. Logical restores
that replace relation identities are refused and need a separately reviewed
recovery operation; this version does not recertify them automatically. Retain
external inventory, artifact/provider namespaces, backups and catalog cursor keys.
Source tests establish no deployment or fleet migration evidence.

The receipt also binds scheduler definitions. A later scheduler schema upgrade
must explicitly preserve the catalog, creation receipts and seals, reconcile the
guard bindings and certify the new definitions under the participant fence.
Replaying scheduler SQL or copying a new structure digest into the receipt is
not a supported upgrade operation. This version provides no format-14 upgrade.

## Compose trusted policy and independent writers

`projects.NewControl(db, canDelete)` uses the public JobDB project catalog service
with PostgreSQL storage. A runtime composes the same control into core, chapter,
schema, configuration and its separately exposed schema registry. Pass it
explicitly to independently constructed public stores as their project admission.
Missing admission remains unavailable. Secret providers are caller-owned;
`runtime.Config.SecretProvider` and `SecretLimits` forward to core, which admits
the project before provider effects.

Project administration requires `jobdb.WithProjectAdministration` installed by
trusted host middleware. Ordinary cell permission, a signing key, development
mode or JSON fields do not grant project administration. Standalone `direct` and
`serve` keep HTTP project administration denied. Hosts can compose the public
project server with their authentication policy.

Deletion is denied by default. A host may configure `CanDeleteProject` only when
it can prove complete, stable absence across all independent participants and
external stores. First use commits a permanent seal separately, with synchronous
commit, before resource effects. A sealed project cannot be deleted even if the
resource operation fails or rolls back. Creation retries retain their exact
status, headers and response bytes after update, deletion and reopen. Deleted IDs
are tombstoned and never reused.

Native scheduler writers admit through the pool before resource transactions.
For a borrowed transaction, call `pgjobdb.AdmitProjectWrite(ctx, pool, id)` first;
the borrowed transaction can only verify a previously committed seal. It never
opens a hidden pool connection. SQL entry/row guards check seals and reject
identity rewrites and TRUNCATE. Raw writers must pre-admit every project; scoped
scheduler array writers require explicit, nonempty identities. The guards do not
create seals in resource transactions, including subtransactions.
Committed catalog changes may become visible after a resource transaction has
already received its transaction ID. Guard checks use the current snapshot's
upper bound when reconstructing catalog transaction IDs, so those later commits
remain admissible while uncommitted first-use seals still fail.

Catalog metadata, descriptions and retained receipt bodies stay in the public
BYTEA encoding. They may contain valid JSON escapes, including NUL, that
PostgreSQL cannot decode into SQL text. Each catalog update atomically saves a
minimal admission projection in the same row: catalog binding, project IDs,
tombstones and seals, without opaque application strings. Resource guards parse
only this projection and verify its SHA-256 binding to the exact catalog bytes.
Catalog operations also compare the projection with one derived from public
state; missing, stale or altered projections fail closed. This derived storage
does not provide a second administration path or change public catalog rules.
Constructors never backfill it. Earlier unpublished candidate installations lack
this guard/structure contract and are refused; they are not silently upgraded or
certified by rewriting a receipt. No deployed installation migration is provided.

Shared public stores require the stable, unshadowed `public` search path. Do not
change a caller-owned pool's session configuration after opening a runtime.
Catalog operations verify their own transaction's connection; startup checks up
to eight held connections. Dedicated SQL diagnostics map unavailable admission,
missing/deleted projects and immutable identity errors without exposing payloads.
Database owners capable of disabling guards or rewriting catalog storage remain
trusted maintenance actors; application admission is not a PostgreSQL privilege
boundary against a hostile database owner.

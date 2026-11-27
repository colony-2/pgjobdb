# Offline PostgreSQL format 16 upgrade

Format 16 retains resource backoff and cell archiving and adds nullable `last_lease_updated_at`
on the active and archived scheduler tables. A timestamp is recorded in the same
transaction as a successful lease acquisition or renewal, including one that
shortens expiry. It is not a deadline or a lease-history lookup. Release,
reschedule, timeout and archival do not refresh it. Upgraded rows start unknown,
even if their expiry, lease ID or lease events survive; a successful renewal of a
lease held at cutover can populate it. Both existing job-list routes omit unknown
`lastLeaseUpdatedAt` values. Expiry and cursors are unchanged.

## Before changing storage

1. Inventory all participants and dependencies. Stop and fence old **and** new
   binaries, workers and runtimes, tenant and project clients, catalog and admin
   tools, independent configuration/schema/chapter/artifact writers, direct SQL
   and maintenance jobs, and external writers (including shared blob stores).
   Prevent reconnects and check for residual transactions. Keep the fence from
   before the backup until verification is complete. The database locks alone
   cannot fence external systems or old binaries after commit.
2. Take and validate a consistent, **OID-preserving physical PostgreSQL backup**
   of the scheduler, project catalog and original receipt, and shared stores.
   Retain external state at the same recovery point. Record the backup identity,
   recovery point, source version, participant inventory, and how to restore them
   together. Do not proceed without a restorable backup. Catalog attestations
   bind relation OIDs; a logical dump/reload is not a supported substitute.
3. With the new binary, run `jobdb direct check-migrate --postgres-dsn "$DSN"`
   and retain the report. `installer.Installer.Preflight` is the read-only
   equivalent. Neither authorizes writes nor proves exclusion. Valid released
   formats 13–15 with or without projects are supported. Formats 9–12 pass
   through retained-goal conversion, formats earlier than 14 convert crash
   credit, and formats earlier than 15 add the cell archive marker. Existing
   crash credit and archive state are not rewritten. Partial installations and
   drift are rejected. Do not reset the marker to get around a rejection.

## Cutover and verification

1. With the fence still in force, run
   `jobdb direct migrate --postgres-dsn "$DSN" --report-file "$STAGED_REPORT"`.
   Programmatic callers use `installer.Installer.MigrateWithReport` for formats
   9–15. `ApplyWithReport` installs fresh or verifies format 16; runtime startup
   only verifies. The migration transaction holds the installation
   advisory lock and uses `ACCESS EXCLUSIVE NOWAIT` in qualified-name order on
   scheduler tables and the present public catalog, configuration, schemas,
   chapter stories, chapter chapters and chapter artifacts tables. A conflict
   rolls back: investigate and reestablish the operational fence before retrying.
2. Keep the staged report and commit confirmation together. Any validation or
   report-write error rolls back definitions, data and marker. If commit is not
   confirmed, keep everyone fenced and reconnect: verify which format is live,
   compare its definitions, original receipt and (when upgrading a project
   installation) source-to-16 transition certificate, and reconcile with the staged report.
   Do not blindly retry. A verified, already-current installation is a no-op and
   does not rewrite its receipt or certificate.
3. Verify the runtime can open on a new connection. Verify both tenant and project
   lists, job cursor continuation, and project catalog read/write and cursor
   continuation where applicable. Check that unknown timestamps are omitted and
   existing expiry and history survived. Only then reopen access, for compatible
   binaries and writers. Never run a mixed-version writer fleet.

After a committed upgrade, reverting requires a coordinated physical restore of
scheduler, catalog, original receipt and shared stores, plus external state at the
same recovery point. Revalidate the restored installation before restarting old
binaries. A logical restore that recreates OIDs, a marker reset, or retaining new
external writes while restoring only PostgreSQL is unsupported.

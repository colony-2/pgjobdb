# Canonical lease history

The native API is `pgjobdb.ListLeaseEvents(ctx, db, jobdb.ListLeaseEventsRequest)`.
It accepts the existing `DB` abstraction, including caller-owned `*sql.Tx` values.
The runtime exposes the same mandatory method through JobDB core and HTTP.

For the last retained acquisition for one job:

```go
page, err := runtime.ListLeaseEvents(ctx, jobdb.ListLeaseEventsRequest{
    TenantID: tenant,
    JobIDs: []string{jobID},
    EventTypes: []jobdb.LeaseEventType{jobdb.LeaseEventAcquired},
    PageSize: 1,
})
```

Events snapshot cell name/repository, primary goal, original job type, leased
route, worker, lease identity and stored expiry. Acquisition and renewal writes
share the lease mutation transaction. Diagnostic tracing can be disabled without
disabling canonical history. Failed insertion fails the mutation; rolling back a
borrowed transaction also rolls back its events. Task-completion acquisitions
remain committed when subsequent independent chapter or reschedule work fails.
There are no fabricated submission, expiry, cancellation or completion events.

Results are newest first by stored microsecond time and numeric event sequence.
Use the opaque `NextPageToken` with identical options to fetch older events.
Both time bounds round upward to microsecond precision. Lower bounds are inclusive;
upper bounds are exclusive. Cell scope is resolved and authorized on every page.
Additional goals use current memberships; returned primary goals remain historical.
Hidden jobs follow the existing include-hidden listing policy.

Traversal is live, not a multi-page snapshot or gap-free changefeed. Concurrent
commits, membership changes and visibility changes can affect later pages. Refresh
the first page to discover newer activity. An empty result means no visible retained
history, not proof that the job never ran. Cursors use persisted tenant catalog keys
and are invalidated by changes to filters or effective permission scope.

Format 10 adds typed `pgjobdb.lease_events` storage and indexes. The installer
transactionally upgrades format 9 without deleting old data or backfilling events.
A pre-upgrade lease may have renewal events without a retained acquisition.
Format 8 and older keep the existing explicit rejection policy. Reinstallation
and reopening preserve retained history and cursor keys.

Operators must stop old writers, upgrade storage, deploy all compatible writers,
and restart them before recording collection activation. Installing the schema
alone does not establish collection completeness. No rollout is performed by
source installation tests. No automatic retention expiry or purge is provided;
capacity planning must include renewal volume and indexes.

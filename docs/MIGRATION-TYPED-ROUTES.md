# Typed route upgrade

Upgrade pgjobdb together with its pinned JobDB dependency and use a fresh
PostgreSQL database (format **4**). The matching JobDB version is
`v0.0.21`. There is no data conversion or compatibility API. Stop old writers before recreating the database and artifact namespace;
re-register schemas, recreate schedules, and submit new jobs.

Public runtime callers now use `jobdb.Route{JobType: ..., TaskType: ...}` for
polling, handoff, leases, and completion. An empty task type means job work.
Keep application task names, including colons, unchanged. See JobDB's
[consumer guide](https://github.com/colony-2/jobdb/blob/main/docs/MIGRATION-TYPED-ROUTES.md)
for API mappings and examples.

Native Go callers keep their existing separate job/task fields. Identifier
validation now matches JobDB: nonempty UTF-8 without U+0000; colons are allowed.

Custom SQL consumers must stop referencing `next_need`, `alternate_next_need`,
and `effective_next_need`. Use typed route/task/alternate fields and `final_*`
archive fields. Dependency wake-ups now use the fixed `pgjobdb.work` channel;
its JSON payload contains `tenantId`, `jobId`, and a `route` object. Notifications
are hints; determine eligibility through the typed work selector.

Client-payload updates and task coordinates retain the semantics described in
the [client-payload guide](MIGRATION-CLIENT-PAYLOAD.md).

External task completion now claims the waiting slot with `ClaimTask`, then
appends its output independently and releases the claim with the normal leased
reschedule. The old native `CompleteTaskWork` and `complete_native_task_work`
APIs are removed. See the [task completion recovery guide](../GUIDE-PGJOBDB-TASK-COMPLETION-RECOVERY.md)
for the protocol and its deferred late-append limitation.

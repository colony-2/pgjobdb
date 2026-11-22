# PostgreSQL submission availability

Submissions accept original instants from `0001-01-01T00:00:00Z` through
`9999-12-31T23:59:59.999999Z`, inclusive. Omission uses database time; an explicit
Go zero timestamp is the valid lower endpoint.

The scheduler rounds an original instant upward to the next whole microsecond.
For example, `2300-01-01T00:00:00.123456789Z` becomes
`2300-01-01T00:00:00.123457Z`. Both the original and any carry are checked before
publication. This conversion does not use `UnixNano`.

Preparation and selected-submission receipts retain the original nanoseconds.
Equivalent offsets identify the same selected submission. Two different originals
that round to the same scheduler time remain different requests and conflict on
replay, including after the job becomes overdue, is deleted, or the runtime
reopens. Replaying an accepted selected submission returns its original handle;
it does not recreate deleted work.

Availability and submission dependencies are both required before acquisition.
Completion waits can pass when a prerequisite fails. Failed success waits admit
settlement by the workflow without calling the consumer application.

Native job detail, status and full lists serialize timestamps inside functions
with a local UTC time zone. Compact lists format timestamps explicitly in UTC.
Neither changes the caller's pool or persistent session settings.

Applications need this pgjobdb repair, JobDB
`v0.0.91` (`9de45717f1f4c218b8e2d4fc0de171af4e5b43dd`), and the updated embedded SQL applied
through the existing installer. All readers and writers using this behavior need
the repaired application code; the SQL update alone does not validate or round
Go submission arguments. Updating Go code alone leaves old SQL timestamp readers
vulnerable to non-UTC session settings. `runtime.NewSQLDB` applies the embedded SQL;
native callers can use `installer.Installer.Apply` and `Verify`.

The availability repair originated in format 12. The retained-goal installer now
converts supported formats 9–12 to 13; it preserves these timestamps without
reconstructing original nanoseconds. See [retained goals and conversion](retained-goals.md). General long polling and unrelated
expiration/reschedule conversions are outside this repair.

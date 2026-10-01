# pgjobdb requirements for task alternates

## Implemented behavior

This checkout pins JobDB `v0.0.23` (commit `6c68db48af6d`). Scheduler format
**6** requires a fresh database under the existing hard-break policy.

Rescheduling records one fixed `alternate_at` timestamp from the configured
relative delay. Fractional-second delays are preserved to PostgreSQL timestamp
precision; availability and dependencies remain separate acquisition gates.
Primary claims and renewals preserve the timestamp. Polling and direct acquisition
promote a due alternate only as part of a successful claim, updating the stored
route and execution snapshot together and clearing all alternate fields. A crash
after promotion leaves the alternate route available for recovery.

Task alternates retain the original input/output ordinals, input hash, and resume
job type. A job-only alternate clears task coordinates to maintain the JOB row
constraint. Acquisition, validation, renewal, and supplied-lease import agree on
the resulting route. Listings continue to show the stored pending task until a
successful claim changes it. Conditional external completion remains valid after
eligibility if no competing lease or route change has taken ownership.

Native `JobDetail.AlternateAfterSeconds` now uses `*float64` for fractional delay
inspection, and `AlternateAt` exposes the fixed timestamp. Archived snapshots
retain both values. This supersedes the earlier supplied-lease behavior that left
an acquired alternate unconsumed.

`TestNativeTaskAlternateLifecycle` and `TestNativeTaskAlternateOwnershipRace`
cover polling/direct recovery, listings, and ownership races. The PostgreSQL
`TestPostgresTaskAlternates` suite runs the upstream lifecycle, hard timeout,
late-claim, supplied-lease, invocation-timeout, and completed-answer replay cases
against direct and remote runtimes, using isolated databases on a shared server.
The crash case requests a whole-second lease and waits for the scheduler expiry;
the signed remote capability intentionally expires earlier.

The original requirements follow.

Update the JobDB dependency to include `workflow.TaskOptions` and delayed task
alternates. The shared runner implements dispatch and timeout classification;
there is no new scheduler API or chapter/scheduler transaction to implement.
No pgjobdb source changes are included in this JobDB change.

Verify these scheduler semantics, correcting pgjobdb where necessary:

- Job and schedule-run listings return the stored pending route. An alternate
  becoming due must not hide the original waiting task from external completion.
- Worker acquisition matches the effective route, promoting a due alternate only
  after a successful claim. A poll with no matching worker must not promote it.
- Claiming the primary route before the alternate is due preserves that alternate
  so a crashed primary can recover after lease expiry. A successful alternate
  claim persists that route and consumes the alternate.
- Acquisition, validation, renewal, and supplied-lease import agree on the claimed
  route and task-wait coordinates. See the existing
  [supplied-lease guide](GUIDE-PGJOBDB-SUPPLIED-LEASE.md) for the route persistence
  issue identified in the previously reviewed pgjobdb implementation.
- External completion and alternate acquisition compete through conditional
  scheduler ownership. Passing the alternate time alone does not reject an answer;
  an active competing lease does. Chapter insertion remains an independent atomic
  append with ordinal uniqueness and sequence guarantees.

The primary job worker is required for timeout wakeups, and a declared alternate
requires a worker serving that route. Keep the single alternate mechanism and
normal availability/dependency restrictions. No additional timer is requested.

Run `TestTaskAlternateLifecycle`, `TestTaskAlternateHardTimeout`,
`TestTaskAlternateClaimAfterHardDeadline`, and `TestTaskAlternateSuppliedLease`
from JobDB's workflow tests against PostgreSQL. Also exercise both polling and
direct acquisition: claim the primary before the alternate is due, let its lease
expire, and claim the now-due alternate. Check listings and conditional completion
both before eligibility and after eligibility but before acquisition.

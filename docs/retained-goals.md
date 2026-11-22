# Retained-job goals and conversion

A job has one immutable primary goal. An unsuccessful or cancelled execution
leaves that goal open for recovery. A fresh shared restart or explicitly selected
submission is admitted only when the goal is open and every retained member is
terminal. Visibility, delay, blocking dependencies, route, cancellation requests
and lease expiry do not release admission capacity. Preparation reserves identity,
not capacity. An equivalent accepted request returns its complete original handle
after checking current authority, even if the goal has since closed.

`FailGoal` closes an open goal without cancelling its executions. It atomically
publishes one protected archived `GOAL_FAILURE` record, captures sorted membership
before inserting that record, and settles durable goal waiters. The first closure
wins: a later successful execution cannot undo explicit failure, and failure after
success is rejected. Equal-message replay returns the original identity, message,
time and snapshot, including after hiding or reopening; another message conflicts.
Failure uses the owner's existing `jobs.submit` authority. No new permission is
introduced.

Protected records have type `jobdb.goal_failure`, completion status `failed`, nil
data and typed job/goal failure. They have no chapters, execution, attempts,
artifacts, lease events or runnable route. Restart and replay reject them before
history cloning or callbacks. An ordinary job with that type, metadata or error
remains an ordinary execution and cannot close a goal. `OtherGoalIDs` is empty;
job and lease-event goal filters use primary membership.

Origin and first closure live on retained `job_facts`; active and archive rows
mirror primary membership. The goal projection is a derived view. There are no
`goals` or `goal_candidates` tables, candidate mutation function, lifecycle
compatibility payload, or runtime conversion-report store. Retained indexes serve
discovery, including hidden jobs and open recovery gaps. Cell retirement considers
those gaps and unpublished preparations.

## Supported installations

Fresh Apply creates format 13. Apply, ApplyWithReport and `runtime.NewSQLDB`
transactionally convert authentic formats 9, 10, 11 and 12 to 13; reapplication
preserves identities. Verify requires the retained contract and rejects either
removed table, any retired mutation overload, damaged protection and inconsistent
mirrors. Formats 1–8, future formats, old `pgwf`, unmarked, corrupt and incomplete
installations fail without mutation. Use the installer; raw embedded SQL does not
perform historical input validation or establish writer exclusion.

The installer serializes competing installations, acquires the existing tenant
advisory locks in bytewise order, then excludes writers from its input tables with
ACCESS EXCLUSIVE locks using NOWAIT. It rechecks version, tenants, ownership and
references after exclusion, using current inputs. A lock conflict or newly observed
tenant aborts cleanly. Runtime tenant lock keys and ordering are unchanged.

Conversion preserves job IDs, primary goals, owners, results, history, artifacts,
declarations, preparations, accepted receipts, frozen wait settlements and pending
targets. It selects the earliest matching primary member as origin, breaking equal
times by bytewise job ID. It drops secondary links without merging goals or moving
wait targets. Historical success retains its exact producer and closure time,
including a producer with another primary goal. Historical failure gets one marked
`MIGRATED_GOAL_FAILURE` record containing the original message, time and snapshot,
including dropped members and excluding the new record. Conversion does not infer
closure from ordinary failure or choose a new success witness.

Inherited live overlaps survive. Fresh admission stays blocked until every live
member has drained. Conversion never resets or cancels those jobs. Unexpected
dependencies fail transactionally; no blanket CASCADE removes unrelated state.

## Retaining conversion evidence

`Installer.Preflight(ctx)` is read-only and advisory. It allocates no identities.
Changes after preflight are included in the authoritative report captured under
writer exclusion by the actual conversion. The versioned report contains per-tenant
counts, every removed membership and timestamp, affected pending waits and
preparations with original targets, cross-primary success witnesses, origin
metadata derivations, live overlaps and allocated migration failure IDs.

Use `Installer.ApplyWithReport(ctx, destination)` with a caller-owned durable
writer, or set `runtime.Config.ConversionReportWriter` for startup. The destination
must acknowledge retaining the complete staged report before commit is attempted.
Write failure, short write or a database error rolls back definitions, tables,
records and marker. The writer may retain staged bytes from a rolled-back attempt;
those bytes alone do not establish a committed conversion.

A successful `CommitConfirmation` binds the exported report digest and source and
target versions to the committed result. A commit error remains uncertain: retain
the staged report and reconcile its exact migration IDs and retained facts on a
fresh connection before retrying. The installer never retries an uncertain commit.
Plain Apply uses the same conversion engine; without a destination its in-memory
report is not durable operator evidence. Post-conversion preflight explicitly sets
`legacyInputsAvailable` false: removed links and timestamps cannot be reconstructed
from the retained records.

The three capability flags describe enforced new-write guarantees. They do not
prove that old deployed writers are excluded, that inherited overlaps have drained,
or that a production cutover has happened. This change performs no deployment.


## Accepted initial-history unreliability for this delivery

The pinned JobDB publication `05238600e743697cb902630ad9dfc6e1728ba744`
can expose story identity before chapter zero commits. An equivalent concurrent
selected submission can therefore return `chapter not found` while another
submission succeeds. This delivery leaves that shared behavior unchanged. Human
direction recorded on cancelled consultation `Dj83t2CdcL0jrAlhoM8nI8uzyyb` says:
“we should accept this unreliability for today, not try to fix it.”

The verification runner still executes the controlled initial-history regression
and public selection checks. It accepts only their exact diagnosed assertion and
source location against this pinned dependency. Raw failed test outcomes, binary
exit codes and the accepted exception remain in JSON/logs and the aggregate
report. The fatal assertion can end an affected test early; assertions after it
are not established by that failed invocation. Other assertions, missing
outcomes, startup failures, skips, panics and
unexpected binary exits remain blocking. No test is retried or skipped to obtain
acceptance. Remove or reconsider this delivery-specific exception when adopting
a different JobDB publication; it does not establish concurrent history
reliability or authorize broader failures. The fresh publication consumer remains
strict and has no accepted-failure rule.


Verification executes the complete enumerated selection in batches of at most
20 top-level tests per binary invocation. Each batch retains its 25-minute
execution limit; failed/skipped outcomes and exit codes accumulate across every
batch. This prevents a large package from exhausting one cumulative limit before
its final tests run. Individual test assertions and database startup checks are
unchanged.

Installer validation compares owned goal protections with canonical catalog definitions from authentic formats 9–12 and fresh format 13. It checks validated constraints, index keys/collations/predicates, enabled trigger timing/events/columns/conditions/function bindings, and bound function definitions. Missing or altered protections reject Preflight, Apply and Verify without repairing the input. Apply repeats validation after writer exclusion. Catalog deparsing uses a transaction-local search path and restores the caller's path; extra unrelated objects remain permitted. Equivalent manually rewritten definitions are not automatically accepted as authentic installation inputs.

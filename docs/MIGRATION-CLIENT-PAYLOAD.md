# Client payload API change

This release requires a fresh database and matching JobDB/pgjobdb versions.
Stop old servers, workers, and native writers, recreate the dedicated database
and artifact namespace, then re-register schemas, recreate schedules, and submit
new jobs. Format 1 is rejected; there is no conversion or API compatibility.

`LeasePayload` and `ClearLeasePayload` are removed. Use `ClientPayloadUpdate` on
`SubmitJobRequest`, `RescheduleRequest`, `CompleteTaskWorkRequest`, or `Completion`.
Omission preserves the stored state. `Mode: "patch"` applies RFC 7396 Merge Patch;
`Mode: "reset"` replaces it. A reset with no `Value` clears it; JSON `null` remains
a present value. Existing-job updates require `ExpectedRevision`; submission
forbids it. Rescheduling assigns all routing fields; a nil `Alternate` clears
the alternate route. Unheld administrative calls preserve client state; acquire
a lease or use authorized task completion to update it. Read `ClientPayload` and `ClientPayloadRevision` from leases or job
details. Job policy remains submit-only; task coordinates and routing stay typed.

Client JSON lives in `pgjobdb.job_client_state`, survives tasks and archival, and
is returned separately from JSONB job projections to preserve exact numbers.
Updates commit with their scheduler transition. Ordinary chapter completion does
not write scheduler rows. Native SQL signatures have changed with the Go API.
Payload limits are 64 KiB and 128 container levels; duplicate keys, malformed
Unicode, and U+0000 are rejected.

Update the JobDB dependency to the matching release. During development, use a
Go workspace containing both checkouts. See JobDB's
[full migration guide](https://github.com/colony-2/jobdb/blob/main/docs/MIGRATION-CLIENT-PAYLOAD.md)
for workflow and REST examples.

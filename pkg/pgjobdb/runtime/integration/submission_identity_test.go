package integration_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	runtimecore "github.com/colony-2/jobdb/pkg/jobdb/runtime/core"
	"github.com/colony-2/pgjobdb/pkg/pgjobdb"
	"github.com/colony-2/pgjobdb/pkg/pgjobdb/runtime/internal/runtimeadapter"
	"github.com/stretchr/testify/require"
)

func TestSubmissionAvailabilitySelectedIdentity(t *testing.T) {
	newDSN := conformanceDSNFactory(t)
	for _, route := range []string{"selected-direct", "selected-http"} {
		t.Run(route, func(t *testing.T) {
			f := newAvailabilityFixture(t, newDSN(t))
			submit := f.submitter(route)
			// Start in the future, then exercise replay again after DB time passes.
			at := f.databaseTime().Add(time.Second).Truncate(time.Microsecond).Add(123 * time.Nanosecond)
			req := availabilityRequest("selected", &at)
			h, err := submit(f.ctx, req)
			require.NoError(t, err)
			descriptor := f.descriptor(h.JobKey)
			require.Contains(t, descriptor, at.Format(time.RFC3339Nano))
			var receipt string
			require.NoError(t, f.db.QueryRowContext(f.ctx, `SELECT receipt::text FROM pgjobdb.job_preparations WHERE job_id='selected'`).Scan(&receipt))
			replay := func() {
				f.assertReads(h.JobKey, effectiveAvailability(at))
				offset := at.In(time.FixedZone("west", -7*3600))
				same := req
				same.Job.AvailableAt = &offset
				got, err := submit(f.ctx, same)
				require.NoError(t, err)
				require.Equal(t, h, got)
				collision := at.Add(time.Nanosecond)
				changed := req
				changed.Job.AvailableAt = &collision
				require.True(t, effectiveAvailability(collision).Equal(effectiveAvailability(at)))
				_, err = submit(f.ctx, changed)
				require.ErrorIs(t, err, jobdb.ErrConflict)
				invalid := time.Date(9999, 12, 31, 23, 59, 59, 999999001, time.UTC)
				changed.Job.AvailableAt = &invalid
				_, err = submit(f.ctx, changed)
				require.ErrorContains(t, err, "submission availability")
				require.Equal(t, descriptor, f.descriptor(h.JobKey))
				var retained string
				require.NoError(t, f.db.QueryRowContext(f.ctx, `SELECT receipt::text FROM pgjobdb.job_preparations WHERE job_id='selected'`).Scan(&retained))
				require.Equal(t, receipt, retained)
			}
			replay()
			f.waitUntil(effectiveAvailability(at))
			replay()
			// Capture the exact original preparation, then exercise native and adapter
			// published shortcuts with invalid descriptors independently of the facade.
			var goal string
			require.NoError(t, f.db.QueryRowContext(f.ctx, `SELECT goal_id FROM pgjobdb.job_facts WHERE job_id='selected'`).Scan(&goal))
			p := pgjobdb.JobPreparation{Key: h.JobKey, Cell: h.Cell, CellRepo: h.CellRepo, SelectedGoalID: goal, Descriptor: descriptor}
			before, err := pgjobdb.PrepareJob(f.ctx, f.db, p)
			require.NoError(t, err)
			require.True(t, before.Published)
			require.Equal(t, &h, before.Receipt)
			p.Descriptor = strings.Replace(descriptor, at.Format(time.RFC3339Nano), "9999-12-31T23:59:59.999999001Z", 1)
			_, err = pgjobdb.PrepareJob(f.ctx, f.db, p)
			require.ErrorContains(t, err, "submission availability")
			_, err = (runtimeadapter.Scheduler{DB: f.db}).PrepareJob(f.ctx, runtimecore.JobPreparation{Key: p.Key, Cell: p.Cell, CellRepo: p.CellRepo, SelectedGoalID: p.SelectedGoalID, Descriptor: p.Descriptor})
			require.ErrorContains(t, err, "submission availability")
			_, err = f.backend.DeleteJobs(f.ctx, jobdb.DeleteJobsRequest{TenantID: "tenant", JobIDs: []string{h.JobKey.JobId}})
			require.NoError(t, err)
			f.reopen()
			// Rebuild the submit closure against the reopened runtime while retaining
			// the already accepted primary goal (creating another source is forbidden).
			submit = func(ctx context.Context, r jobdb.SubmitJobRequest) (jobdb.JobHandle, error) {
				r.Job.GoalID = goal
				if route == "selected-http" {
					return f.client.SubmitJob(ctx, r)
				}
				return f.backend.SubmitJob(ctx, r)
			}
			offset := at.In(time.FixedZone("east", 14*3600))
			same := req
			same.Job.AvailableAt = &offset
			got, err := submit(f.ctx, same)
			require.NoError(t, err)
			require.Equal(t, h, got)
			collision := at.Add(time.Nanosecond)
			same.Job.AvailableAt = &collision
			_, err = submit(f.ctx, same)
			require.ErrorIs(t, err, jobdb.ErrConflict)
			var executable int
			require.NoError(t, f.db.QueryRowContext(f.ctx, `SELECT count(*) FROM pgjobdb.jobs WHERE job_id='selected'`).Scan(&executable))
			require.Zero(t, executable)
			require.Equal(t, descriptor, f.descriptor(h.JobKey))
			var retained string
			require.NoError(t, f.db.QueryRowContext(f.ctx, `SELECT receipt::text FROM pgjobdb.job_preparations WHERE job_id='selected'`).Scan(&retained))
			require.Equal(t, receipt, retained)
		})
	}
}

func TestSubmissionAvailabilityReconciliationGuards(t *testing.T) {
	newDSN := conformanceDSNFactory(t)
	f := newAvailabilityFixture(t, newDSN(t))
	scheduler := runtimeadapter.Scheduler{DB: f.db}
	bad := time.Date(9999, 12, 31, 23, 59, 59, 999999001, time.UTC)
	_, err := scheduler.CreateJob(f.ctx, runtimecore.CreateJobRequest{JobKey: jobdb.JobKey{TenantId: "tenant", JobId: "typed-reconcile"}, AvailableAt: &bad})
	require.ErrorContains(t, err, "submission availability")
	f.noPublication("typed-reconcile")
	h, err := f.backend.SubmitJob(f.ctx, availabilityRequest("ordinary", nil))
	require.NoError(t, err)
	var descriptor string
	require.NoError(t, f.db.QueryRowContext(f.ctx, `SELECT descriptor FROM pgjobdb.job_preparations WHERE job_id='ordinary'`).Scan(&descriptor))
	var envelope map[string]json.RawMessage
	require.NoError(t, json.Unmarshal([]byte(descriptor), &envelope))
	var job map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(envelope["Job"], &job))
	job["AvailableAt"] = json.RawMessage(`"9999-12-31T23:59:59.999999001Z"`)
	envelope["Job"], err = json.Marshal(job)
	require.NoError(t, err)
	raw, err := json.Marshal(envelope)
	require.NoError(t, err)
	_, err = scheduler.PrepareJob(f.ctx, runtimecore.JobPreparation{Key: h.JobKey, Cell: h.Cell, CellRepo: h.CellRepo, Descriptor: string(raw)})
	require.ErrorContains(t, err, "submission availability")
	// An invalid retry must leave the original publication and descriptor intact.
	req := availabilityRequest("ordinary", &bad)
	_, err = f.backend.SubmitJob(f.ctx, req)
	require.ErrorContains(t, err, "submission availability")
	var retained string
	require.NoError(t, f.db.QueryRowContext(f.ctx, `SELECT descriptor FROM pgjobdb.job_preparations WHERE job_id='ordinary'`).Scan(&retained))
	require.Equal(t, descriptor, retained)
	got, err := f.backend.SubmitJob(f.ctx, availabilityRequest("ordinary", nil))
	require.NoError(t, err)
	require.Equal(t, h, got)
}

func TestSubmissionAvailabilityRuntimeUTCReads(t *testing.T) {
	newDSN := conformanceDSNFactory(t)
	f := newAvailabilityFixture(t, newDSN(t))
	f.db.SetMaxOpenConns(1)
	for _, zone := range []string{"UTC", "Etc/GMT-14", "Etc/GMT+12", "Europe/Paris"} {
		_, err := f.db.ExecContext(f.ctx, `SELECT set_config('TimeZone',$1,false)`, zone)
		require.NoError(t, err)
		for i, at := range []time.Time{time.Time{}, time.Date(9999, 12, 31, 23, 59, 59, 999999000, time.UTC), time.Date(1850, 1, 1, 0, 0, 0, 123456789, time.UTC)} {
			id := strings.ReplaceAll(zone, "/", "-") + string(rune('a'+i))
			req := availabilityRequest(id, &at)
			h, err := f.client.SubmitJob(f.ctx, req)
			require.NoError(t, err)
			f.assertReads(h.JobKey, effectiveAvailability(at))
			_, err = f.db.ExecContext(f.ctx, `UPDATE pgjobdb.jobs SET available_at=clock_timestamp()-interval '1 hour' WHERE job_id=$1`, h.JobKey.JobId)
			require.NoError(t, err)

			require.NoError(t, pgjobdb.CompleteUnheldJob(f.ctx, f.db, "tenant", pgjobdb.JobID(h.JobKey.JobId), "archiver", pgjobdb.Completion{Status: pgjobdb.CompletionSuccess}))
			archiveTime := effectiveAvailability(at)
			_, err = f.db.ExecContext(f.ctx, `UPDATE pgjobdb.jobs_archive SET archived_at=$2,final_available_at=$2 WHERE job_id=$1`, h.JobKey.JobId, archiveTime)
			require.NoError(t, err)
			f.assertReads(h.JobKey, effectiveAvailability(at))
			for _, rt := range []jobdb.WorkflowRuntime{f.backend, f.client} {
				run, err := jobdb.GetJobRun(f.ctx, rt, jobdb.GetJobRunRequest{JobKey: h.JobKey})
				require.NoError(t, err)
				require.NotNil(t, run.Job.ArchivedAt)
				require.Equal(t, archiveTime, *run.Job.ArchivedAt)
			}

			status, err := f.client.GetJob(f.ctx, h.JobKey)
			require.NoError(t, err)
			require.NotEmpty(t, status.CellID)
		}
		var got string
		require.NoError(t, f.db.QueryRowContext(f.ctx, `SHOW TimeZone`).Scan(&got))
		require.Equal(t, zone, got)
	}
}

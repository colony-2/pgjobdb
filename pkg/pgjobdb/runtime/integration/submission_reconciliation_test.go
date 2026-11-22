package integration_test

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	chapterpostgres "github.com/colony-2/jobdb/pkg/jobdb/chapterstore/postgres"
	runtimecore "github.com/colony-2/jobdb/pkg/jobdb/runtime/core"
	schemapostgres "github.com/colony-2/jobdb/pkg/jobdb/schemastore/postgres"
	"github.com/colony-2/pgjobdb/pkg/pgjobdb"
	"github.com/colony-2/pgjobdb/pkg/pgjobdb/runtime/internal/runtimeadapter"
	"github.com/stretchr/testify/require"
)

func TestSubmissionAvailabilityStagedHistoryAndLostResponse(t *testing.T) {
	newDSN := conformanceDSNFactory(t)
	f := newAvailabilityFixture(t, newDSN(t))
	source, err := f.backend.SubmitJob(f.ctx, availabilityRequest("source", nil))
	require.NoError(t, err)
	retireNativeMember(t, f.ctx, f.db, source.JobKey.JobId)
	chapters, err := chapterpostgres.NewSQLDB(f.ctx, f.db, chapterpostgres.Config{BlobStoreURI: f.cfg.BlobStoreURI, MaxInlineArtifactBytes: 1})
	require.NoError(t, err)
	defer chapters.Close(f.ctx)
	schemas, err := schemapostgres.NewSQLDB(f.ctx, f.db)
	require.NoError(t, err)
	scheduler := &selectedFaultScheduler{Scheduler: runtimeadapter.Scheduler{DB: f.db}}
	core := func() *runtimecore.Runtime {
		r, err := runtimecore.NewRuntime(runtimecore.Config{Scheduler: scheduler, Chapters: chapters, Schemas: schemas, Configurations: newConfigurationStore(t, f.ctx, f.db)})
		require.NoError(t, err)
		return r
	}
	at := time.Date(2300, 1, 1, 0, 0, 0, 123456789, time.UTC)
	request := func(id string) jobdb.SubmitJobRequest {
		r := availabilityRequest(id, &at)
		r.Job.GoalID = source.GoalID
		r.Job.Data = jobdb.NewTaskDataOrPanic("retained", jobdb.NewArtifactFromBytes("input.txt", []byte("retained submission artifact")))
		return r
	}
	bad := time.Date(9999, 12, 31, 23, 59, 59, 999999001, time.UTC)
	scheduler.before = func(ctx context.Context, r runtimecore.CreateJobRequest) error {
		r.AvailableAt = &bad
		_, err := scheduler.Scheduler.CreateJob(ctx, r)
		return err
	}
	_, err = core().SubmitJob(f.ctx, request("staged"))
	require.ErrorContains(t, err, "submission availability")
	f.noPublication("staged")
	chapter, err := chapters.Get(f.ctx, runtimecore.ChapterLogKey{JobKey: jobdb.JobKey{TenantId: "tenant", JobId: "staged"}}, 0)
	require.NoError(t, err)
	require.Len(t, chapter.Artifacts, 1)
	reader, err := chapters.OpenArtifact(f.ctx, runtimecore.ArtifactLookup{JobKey: jobdb.JobKey{TenantId: "tenant", JobId: "staged"}, Ordinal: 0, Name: "input.txt", Digest: chapter.Artifacts[0].Digest})
	require.NoError(t, err)
	stream, err := reader.Open()
	require.NoError(t, err)
	content, err := io.ReadAll(stream)
	require.NoError(t, err)
	require.NoError(t, stream.Close())
	require.Equal(t, "retained submission artifact", string(content))
	scheduler.before = nil
	h, err := core().SubmitJob(f.ctx, request("staged"))
	require.NoError(t, err)
	f.assertReads(h.JobKey, effectiveAvailability(at))
	require.Contains(t, f.descriptor(h.JobKey), "2300-01-01T00:00:00.123456789Z")
	// Drain this delayed attempt before publishing another member of its goal.
	require.NoError(t, pgjobdb.CancelJob(f.ctx, f.db, pgjobdb.CancelJobRequest{TenantID: "tenant", JobID: pgjobdb.JobID(h.JobKey.JobId), WorkerID: "fixture-drain"}))
	scheduler.after = func() error { return errors.New("injected committed response loss") }
	_, err = core().SubmitJob(f.ctx, request("lost-response"))
	require.ErrorContains(t, err, "injected committed response loss")
	descriptor := f.descriptor(jobdb.JobKey{TenantId: "tenant", JobId: "lost-response"})
	scheduler.after = nil
	first, err := core().SubmitJob(f.ctx, request("lost-response"))
	require.NoError(t, err)
	f.reopen()
	second, err := f.backend.SubmitJob(f.ctx, request("lost-response"))
	require.NoError(t, err)
	require.Equal(t, first, second)
	require.Equal(t, descriptor, f.descriptor(first.JobKey))
	f.assertReads(first.JobKey, effectiveAvailability(at))
	var n int
	require.NoError(t, f.db.QueryRowContext(f.ctx, `SELECT count(*) FROM pgjobdb.job_facts WHERE job_id='lost-response'`).Scan(&n))
	require.Equal(t, 1, n)
}

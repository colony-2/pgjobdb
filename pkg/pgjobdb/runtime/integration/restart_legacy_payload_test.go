package integration_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/colony-2/jobdb/pkg/jobdb"
	chapterpostgres "github.com/colony-2/jobdb/pkg/jobdb/chapterstore/postgres"
	runtimecore "github.com/colony-2/jobdb/pkg/jobdb/runtime/core"
	schemapostgres "github.com/colony-2/jobdb/pkg/jobdb/schemastore/postgres"
	pgruntime "github.com/colony-2/pgjobdb/pkg/pgjobdb/runtime"
	"github.com/colony-2/pgjobdb/pkg/pgjobdb/runtime/internal/runtimeadapter"
	"github.com/stretchr/testify/require"
)

func TestRestartPreservesOversizedLegacyChapters(t *testing.T) {
	runDatabaseTest(t, func(ctx context.Context, db *sql.DB) {
		blobURI := "blobfs://" + t.TempDir()
		chapters, err := chapterpostgres.NewSQLDB(ctx, db, chapterpostgres.Config{BlobStoreURI: blobURI})
		require.NoError(t, err)
		defer chapters.Close(ctx)
		schemas, err := schemapostgres.NewSQLDB(ctx, db)
		require.NoError(t, err)
		// Seed history admitted before the current 256 KiB write limit.
		legacy, err := runtimecore.NewRuntime(runtimecore.Config{Configurations: newConfigurationStore(t, ctx, db), Scheduler: runtimeadapter.Scheduler{DB: db}, Chapters: chapters, Schemas: schemas, MaxChapterApplicationPayloadBytes: 2 * 1024 * 1024})
		require.NoError(t, err)
		source, err := legacy.SubmitJob(ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", JobID: "legacy", JobType: "recipe", Data: jobdb.NewTaskDataOrPanic(nil)}})
		require.NoError(t, err)
		leases, err := legacy.PollWork(ctx, jobdb.PollWorkRequest{TenantId: "tenant", WorkerID: "writer", Routes: []jobdb.Route{{JobType: "recipe"}}, Limit: 1})
		require.NoError(t, err)
		require.Len(t, leases, 1)
		large, err := json.Marshal(map[string]string{"recipe_yaml": strings.Repeat("x", 1260320)})
		require.NoError(t, err)
		for ordinal := int64(1); ordinal <= 43; ordinal++ {
			data := json.RawMessage(`null`)
			if ordinal == 3 {
				data = large
			}
			record, err := json.Marshal(jobdb.ExecutionRecord{Version: jobdb.ExecutionPatternVersion, Kind: "result", ID: "root-source", Attempt: 1, Output: &jobdb.ExecutionSnapshot{Data: data}})
			require.NoError(t, err)
			require.NoError(t, legacy.PutChapter(ctx, jobdb.PutChapterRequest{Ref: jobdb.ChapterRef{JobKey: source.JobKey, Ordinal: ordinal}, LeaseID: leases[0].LeaseID(), Chapter: jobdb.Chapter{Ordinal: ordinal, TaskType: "__execution__", Body: jobdb.ExecutionChapter{Record: record}}}))
		}
		// Exercise the public pgjobdb facade with its normal write limit.
		rt, err := pgruntime.NewSQLDB(ctx, db, pgruntime.Config{BlobStoreURI: blobURI})
		require.NoError(t, err)
		defer rt.Close(ctx)
		for i, schema := range []json.RawMessage{json.RawMessage(`{"chapterShape":true}`), json.RawMessage(`{"chapterShape":{"properties":{"ordinal":{"minimum":43}}}}`)} {
			req := jobdb.SubmitRestartJobRequest{Job: jobdb.SubmitRestartJob{PriorJobKey: source.JobKey, JobID: fmt.Sprintf("restart-%d", i), LastStepToKeep: 42, Schema: &jobdb.JobSchemaSelector{Schema: schema}}}
			clone, err := rt.SubmitRestartJob(ctx, req)
			require.NoError(t, err)
			inherited, err := rt.GetChapter(ctx, jobdb.ChapterRef{JobKey: clone.JobKey, Ordinal: 3})
			require.NoError(t, err)
			record, err := jobdb.DecodeExecutionRecord(inherited.Body.(jobdb.ExecutionChapter).Record)
			require.NoError(t, err)
			require.JSONEq(t, string(large), string(record.Output.Data))
			var count int
			require.NoError(t, db.QueryRowContext(ctx, `SELECT count(*) FROM jobdb_chapter_chapters WHERE anthology_id=$1 AND story_id=$2`, clone.JobKey.TenantId, clone.JobKey.JobId).Scan(&count))
			require.Zero(t, count, "thin clone must not copy retained chapter rows")
			var base string
			var fork int64
			require.NoError(t, db.QueryRowContext(ctx, `SELECT base_story_id,base_fork_ordinal FROM jobdb_chapter_stories WHERE anthology_id=$1 AND story_id=$2`, clone.JobKey.TenantId, clone.JobKey.JobId).Scan(&base, &fork))
			require.Equal(t, source.JobKey.JobId, base)
			require.EqualValues(t, 42, fork)
			again, err := rt.SubmitRestartJob(ctx, req)
			require.NoError(t, err)
			require.Equal(t, clone, again)
			req.Job.JobID = fmt.Sprintf("oversized-extra-%d", i)
			req.Job.ExtraTaskOutput = jobdb.NewTaskDataOrPanic(large)
			_, err = rt.SubmitRestartJob(ctx, req)
			require.ErrorIs(t, err, jobdb.ErrChapterApplicationPayloadTooLarge, "new restart output still obeys the write limit")
		}
	})
}

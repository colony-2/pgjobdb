package integration_test

import (
	"context"
	"database/sql"
	"testing"

	"github.com/colony-2/jobdb/pkg/jobdb"
	pgruntime "github.com/colony-2/pgjobdb/pkg/pgjobdb/runtime"
	"github.com/stretchr/testify/require"
)

func TestPostgresGoalSettlementSurvivesRuntimeReopen(t *testing.T) {
	runDatabaseTest(t, func(ctx context.Context, db *sql.DB) {
		cfg := pgruntime.Config{BlobStoreURI: "blobfs://" + t.TempDir()}
		r, err := pgruntime.NewSQLDB(ctx, db, cfg)
		require.NoError(t, err)
		producer := goalSubmit(t, r, "producer")
		consumer := goalSubmit(t, r, "consumer", jobdb.Dependency{Target: jobdb.DependencyTarget{GoalID: producer.GoalID}, Condition: jobdb.JobPrereqSuccess})
		goalComplete(t, r, producer, true)
		key := jobdb.DependencyWaitKey{JobKey: consumer.JobKey, WaitID: "submission"}
		before, err := r.GetDependencyWait(ctx, key)
		require.NoError(t, err)
		chapters, err := r.ListChapters(ctx, jobdb.ListChaptersRequest{JobKey: consumer.JobKey})
		require.NoError(t, err)
		require.Len(t, chapters, 1, "settlement precedes consumer history recording")
		require.NoError(t, r.Close(ctx))
		reopened, err := pgruntime.NewSQLDB(ctx, db, cfg)
		require.NoError(t, err)
		defer reopened.Close(ctx)
		after, err := reopened.GetDependencyWait(ctx, key)
		require.NoError(t, err)
		require.Equal(t, before, after)
		require.Equal(t, producer.JobKey.JobId, after.Dependencies[0].SatisfiedByJobID)
		lease := goalLease(t, reopened, consumer)
		require.NotNil(t, lease)
		require.Equal(t, consumer.GoalID, lease.Job().GoalID)
	})
}

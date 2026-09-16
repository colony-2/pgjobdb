package installer_test

import (
	"context"
	"database/sql"
	"testing"

	"github.com/colony-2/jobdb/pkg/jobdb"
	pgjobdbruntime "github.com/colony-2/pgjobdb/runtime"
)

func TestPublicRuntimeInitializesFreshDatabase(t *testing.T) {
	withBareDatabase(t, func(ctx context.Context, db *sql.DB) {
		cfg := pgjobdbruntime.Config{BlobStoreURI: "blobfs://" + t.TempDir()}
		runtime, err := pgjobdbruntime.New(ctx, db, cfg)
		if err != nil {
			t.Fatalf("initialize runtime: %v", err)
		}
		input, err := jobdb.NewTaskData(map[string]any{"item": 1})
		if err != nil {
			t.Fatal(err)
		}
		handle, err := runtime.SubmitJob(ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{
			TenantId: "tenant", JobID: "fresh", JobType: "collect", Data: input,
		}})
		if err != nil || handle.JobKey.JobId != "fresh" {
			t.Fatalf("submit through public runtime = %+v, %v", handle, err)
		}
		if err := runtime.Close(ctx); err != nil {
			t.Fatalf("close runtime: %v", err)
		}
		reopened, err := pgjobdbruntime.New(ctx, db, cfg)
		if err != nil {
			t.Fatalf("reopen runtime: %v", err)
		}
		defer func() { _ = reopened.Close(ctx) }()
		info, err := reopened.GetJob(ctx, handle.JobKey)
		if err != nil || info.Status != jobdb.JobStatusReady {
			t.Fatalf("read reopened job = %+v, %v", info, err)
		}
	})
}

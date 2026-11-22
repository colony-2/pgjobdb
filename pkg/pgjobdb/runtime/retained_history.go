package runtime

import (
	"context"
	"fmt"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/pgjobdb/pkg/pgjobdb/runtime/internal/runtimeadapter"
)

// ListChapters recognizes protected scheduler records before asking the
// PostgreSQL chapter store for a story. Such records intentionally have no
// story. Ordinary missing histories continue returning the store's error.
func (r *Runtime) ListChapters(ctx context.Context, req jobdb.ListChaptersRequest) ([]jobdb.Chapter, error) {
	if req.StartOrdinal < 0 || req.EndOrdinal != nil && *req.EndOrdinal < req.StartOrdinal {
		return nil, fmt.Errorf("invalid chapter range")
	}
	if err := req.JobKey.Validate(); err != nil {
		return nil, err
	}
	row, err := (runtimeadapter.Scheduler{DB: r.db}).GetJob(ctx, req.JobKey)
	if err != nil {
		return nil, err
	}
	if row.RecordKind.IsTerminalRecord() {
		return []jobdb.Chapter{}, nil
	}
	return r.Runtime.ListChapters(ctx, req)
}

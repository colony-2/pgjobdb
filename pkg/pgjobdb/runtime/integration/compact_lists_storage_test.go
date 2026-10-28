package integration_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/pgjobdb/pkg/pgjobdb"
	"github.com/stretchr/testify/require"
)

type listQueryRecorder struct {
	*sql.DB
	queries int
	query   string
	args    []any
}

func (d *listQueryRecorder) QueryContext(ctx context.Context, q string, args ...any) (*sql.Rows, error) {
	d.queries++
	d.query, d.args = q, args
	return d.DB.QueryContext(ctx, q, args...)
}
func (d *listQueryRecorder) QueryRowContext(ctx context.Context, q string, args ...any) *sql.Row {
	d.queries++
	return d.DB.QueryRowContext(ctx, q, args...)
}

// The guards are installed only after fixture construction. Listing must never
// evaluate explanations, even when every selected job has a pending goal wait.
func forbidListDiagnostics(t *testing.T, db *sql.DB) {
	t.Helper()
	for _, signature := range []string{"pgjobdb._evaluate_wait(jsonb,timestamp with time zone)", "pgjobdb._goal_members(text,text)", "pgjobdb.dependency_wait_info(text,text,text)"} {
		var original string
		require.NoError(t, db.QueryRow(`SELECT pg_get_functiondef($1::regprocedure)`, signature).Scan(&original))
		prefix, _, ok := strings.Cut(original, "AS $function$")
		require.True(t, ok)
		prefix = strings.Replace(prefix, "LANGUAGE sql", "LANGUAGE plpgsql", 1)
		_, err := db.Exec(prefix + `AS $function$ BEGIN RAISE EXCEPTION 'list evaluated diagnostics'; END; $function$`)
		require.NoError(t, err)
		t.Cleanup(func() { _, err := db.Exec(original); require.NoError(t, err) })
	}
}

func TestCompactListStoragePagesAndPlans(t *testing.T) {
	runDatabaseTest(t, func(ctx context.Context, db *sql.DB) {
		producer := nativeGoalJob(t, ctx, db, "producer")
		secondary := nativeGoalJob(t, ctx, db, "secondary")
		// More than a page, with unrestricted metadata and large detail-only data.
		metadata := json.RawMessage(`{"team":"blue","nested":{"large":"` + strings.Repeat("m", 4096) + `"}}`)
		for i := 0; i < 240; i++ {
			id := pgjobdb.JobID(fmt.Sprintf("consumer-%03d", i))
			_, err := pgjobdb.SubmitJob(ctx, db, pgjobdb.SubmitJobRequest{TenantID: "tenant", JobID: id, WorkerID: "test", JobType: "consumer", AppMetadata: metadata, Dependencies: []jobdb.Dependency{{Target: jobdb.DependencyTarget{GoalID: producer.GoalID}, Condition: jobdb.JobPrereqSuccess}}, ClientPayloadUpdate: &pgjobdb.ClientPayloadUpdate{Mode: "reset", Value: json.RawMessage(`{"large":"` + strings.Repeat("p", 32768) + `"}`)}})
			require.NoError(t, err)
			if i%20 == 0 {
				_, err = pgjobdb.AddGoalCandidate(ctx, db, jobdb.AddGoalCandidateRequest{GoalKey: nativeGoalKey(secondary), JobID: string(id)})
				require.NoError(t, err)
			}
		}
		// Exercise equal-timestamp cursor ties rather than relying on sleep timing.
		_, err := db.Exec(`UPDATE pgjobdb.job_facts SET created_at='2026-01-01' WHERE job_type='consumer'`)
		require.NoError(t, err)
		_, err = db.Exec(`ANALYZE pgjobdb.jobs; ANALYZE pgjobdb.job_facts; ANALYZE pgjobdb.dependency_waits; ANALYZE pgjobdb.goal_candidates; ANALYZE pgjobdb.job_client_state`)
		require.NoError(t, err)
		forbidListDiagnostics(t, db)
		for _, size := range []int{1, 100, 200} {
			t.Run(fmt.Sprint(size), func(t *testing.T) {
				d := &listQueryRecorder{DB: db}
				opts := pgjobdb.ListJobsOptions{TenantIDs: []pgjobdb.TenantID{"tenant"}, JobTypes: []pgjobdb.JobType{"consumer"}, PageSize: size}
				seen := map[pgjobdb.JobID]bool{}
				var previous pgjobdb.JobID
				for {
					d.queries = 0
					page, err := pgjobdb.ListJobSummaries(ctx, d, opts)
					require.NoError(t, err)
					require.Equal(t, 1, d.queries)
					require.LessOrEqual(t, len(page.Jobs), size)
					for _, j := range page.Jobs {
						require.False(t, seen[j.JobID])
						seen[j.JobID] = true
						if previous != "" {
							require.Less(t, string(j.JobID), string(previous))
						}
						previous = j.JobID
						require.JSONEq(t, string(metadata), string(j.AppMetadata))
						require.Equal(t, pgjobdb.JobStatusPendingJobs, j.Status)
						require.NotNil(t, j.BlockingWaitSummary)
						require.Equal(t, "submission", j.BlockingWaitSummary.WaitID)
						require.EqualValues(t, 1, j.ClientPayloadRevision)
					}
					if opts.PageToken == "" {
						recordCompactListPlan(t, db, d, fmt.Sprintf("page-%d", size))
					}
					if page.NextPageToken == "" {
						break
					}
					opts.PageToken = page.NextPageToken
				}
				require.Len(t, seen, 240)
			})
		}
		d := &listQueryRecorder{DB: db}
		page, err := pgjobdb.ListJobSummaries(ctx, d, pgjobdb.ListJobsOptions{TenantIDs: []pgjobdb.TenantID{"tenant"}, GoalIDs: []string{secondary.GoalID}, JobTypes: []pgjobdb.JobType{"consumer"}, MetadataPredicates: []pgjobdb.MetadataPredicate{{Path: []string{"team"}, Values: []json.RawMessage{json.RawMessage(`"blue"`)}}}, PageSize: 100})
		require.NoError(t, err)
		require.Len(t, page.Jobs, 12)
		require.Equal(t, 1, d.queries)
		recordCompactListPlan(t, db, d, "secondary-goal-metadata")
	})
}

func recordCompactListPlan(t *testing.T, db *sql.DB, d *listQueryRecorder, name string) {
	t.Helper()
	rows, err := db.Query(`EXPLAIN (ANALYZE, BUFFERS, VERBOSE, FORMAT JSON) `+d.query, d.args...)
	require.NoError(t, err)
	defer rows.Close()
	require.True(t, rows.Next())
	var raw []byte
	require.NoError(t, rows.Scan(&raw))
	require.NoError(t, rows.Err())
	// Optional export makes representative plans reviewable without committing
	// machine-specific timings from every test run.
	if dir := os.Getenv("PGJOBDB_EXPLAIN_DIR"); dir != "" {
		require.NoError(t, os.MkdirAll(dir, 0755))
		require.NoError(t, os.WriteFile(filepath.Join(dir, name+".json"), raw, 0644))
		planRows, err := db.Query(`EXPLAIN (ANALYZE, BUFFERS) `+d.query, d.args...)
		require.NoError(t, err)
		defer planRows.Close()
		var plan strings.Builder
		for planRows.Next() {
			var line string
			require.NoError(t, planRows.Scan(&line))
			plan.WriteString(line + "\n")
		}
		require.NoError(t, planRows.Err())
		require.NoError(t, os.WriteFile(filepath.Join(dir, name+".txt"), []byte(plan.String()), 0644))
	}
	require.Contains(t, string(raw), `"Node Type": "Limit"`)
	require.NotContains(t, d.query, "dependency_wait_info")
	require.NotContains(t, d.query, "_evaluate_wait")
	require.NotContains(t, d.query, "other_goal_ids")
	require.NotContains(t, d.query, "c.client_payload")
}

func TestCompactListWaitObservationAndRoutes(t *testing.T) {
	runDatabaseTest(t, func(ctx context.Context, db *sql.DB) {
		producer := nativeGoalJob(t, ctx, db, "producer")
		nativeGoalJob(t, ctx, db, "consumer")
		lease := nativeGoalLease(t, ctx, db, "consumer")
		deadline := time.Now().UTC().Add(time.Hour).Truncate(time.Microsecond)
		wait := nativeGoalWait("wait", producer.GoalID)
		wait.Deadline = &deadline
		require.NoError(t, lease.Reschedule(ctx, db, pgjobdb.RescheduleRequest{RouteJobType: "work", WorkKind: pgjobdb.WorkKindJob, DependencyWait: wait}))
		for _, at := range []time.Time{deadline.Add(-time.Microsecond), deadline, deadline.Add(time.Microsecond)} {
			page, err := pgjobdb.ListJobSummaries(ctx, db, pgjobdb.ListJobsOptions{TenantIDs: []pgjobdb.TenantID{"tenant"}, JobKeys: []pgjobdb.JobKey{{TenantID: "tenant", JobID: "consumer"}}, Now: at})
			require.NoError(t, err)
			require.Len(t, page.Jobs, 1)
			if at.Before(deadline) {
				require.Equal(t, pgjobdb.JobStatusPendingJobs, page.Jobs[0].Status)
				require.Equal(t, &jobdb.DependencyWaitSummary{WaitID: "wait", Deadline: &deadline}, page.Jobs[0].BlockingWaitSummary)
			} else {
				require.Equal(t, pgjobdb.JobStatusReady, page.Jobs[0].Status)
				require.Nil(t, page.Jobs[0].BlockingWaitSummary)
			}
		}
		var state string
		require.NoError(t, db.QueryRow(`SELECT state FROM pgjobdb.dependency_waits WHERE job_id='consumer'`).Scan(&state))
		require.Equal(t, "PENDING", state)
		// Stored terminal waits must not be re-evaluated or presented as blocking.
		_, err := db.Exec(`UPDATE pgjobdb.dependency_waits SET state='SATISFIED' WHERE job_id='consumer'`)
		require.NoError(t, err)
		page, err := pgjobdb.ListJobSummaries(ctx, db, pgjobdb.ListJobsOptions{TenantIDs: []pgjobdb.TenantID{"tenant"}, JobKeys: []pgjobdb.JobKey{{TenantID: "tenant", JobID: "consumer"}}, Now: deadline.Add(-time.Minute)})
		require.NoError(t, err)
		require.Nil(t, page.Jobs[0].BlockingWaitSummary)
		require.Equal(t, pgjobdb.JobStatusReady, page.Jobs[0].Status)
		nativeGoalJob(t, ctx, db, "task")
		l := nativeGoalLease(t, ctx, db, "task")
		require.NoError(t, l.Reschedule(ctx, db, pgjobdb.RescheduleRequest{RouteJobType: "work", WorkKind: pgjobdb.WorkKindTask, Task: &pgjobdb.TaskWork{TaskType: "input", ResumeJobType: "work", InputOrdinal: 1, OutputOrdinal: 2, InputHash: "hash"}, Alternate: &pgjobdb.AlternateRoute{JobType: "alternate", After: time.Hour}}))
		detail, err := pgjobdb.GetJob(ctx, db, "tenant", "task")
		require.NoError(t, err)
		for _, at := range []time.Time{detail.AlternateAt.Add(-time.Microsecond), *detail.AlternateAt} {
			for _, route := range []pgjobdb.Route{{JobType: "work", TaskType: "input"}, {JobType: "alternate"}} {
				page, err := pgjobdb.ListJobSummaries(ctx, db, pgjobdb.ListJobsOptions{TenantIDs: []pgjobdb.TenantID{"tenant"}, AvailableRoutes: []pgjobdb.Route{route}, Now: at})
				require.NoError(t, err)
				if (route.JobType == "work") == at.Before(*detail.AlternateAt) {
					require.Len(t, page.Jobs, 1)
					require.Equal(t, pgjobdb.JobType("work"), page.Jobs[0].RouteJobType)
					require.Equal(t, pgjobdb.TaskType("input"), page.Jobs[0].TaskType)
				} else {
					require.Empty(t, page.Jobs)
				}
			}
		}
	})
}

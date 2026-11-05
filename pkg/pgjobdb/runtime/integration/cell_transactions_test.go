package integration_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/cellstate"
	"github.com/colony-2/pgjobdb/pkg/pgjobdb"
	"github.com/stretchr/testify/require"
)

func TestCellTransactions(t *testing.T) {
	runDatabaseTest(t, func(ctx context.Context, db *sql.DB) {
		db.SetMaxOpenConns(8)
		t.Run("nested metadata no-op and creation replay", func(t *testing.T) {
			metadata := map[string]json.RawMessage{"nested": json.RawMessage(`{"b":2,"a":1}`), "number": json.RawMessage(`1e999999`)}
			req := jobdb.CreateCellRequest{TenantID: "metadata", Name: "work", Repo: "example.test/work", Metadata: metadata, RequestID: "create"}
			c, err := pgjobdb.CreateCell(ctx, db, req)
			require.NoError(t, err)
			again, err := pgjobdb.CreateCell(ctx, db, req)
			require.NoError(t, err)
			require.Equal(t, c, again)
			metadata["nested"] = json.RawMessage(`{ "a":1, "b":2 }`)
			patched, err := pgjobdb.UpdateCell(ctx, db, jobdb.UpdateCellRequest{CellKey: jobdb.CellKey{TenantID: c.TenantID, CellID: c.ID}, IfMatch: `"` + c.Revision + `"`, Metadata: &metadata})
			require.NoError(t, err)
			require.Equal(t, c, patched)
			got, err := pgjobdb.GetCell(ctx, db, jobdb.CellKey{TenantID: c.TenantID, CellID: c.ID})
			require.NoError(t, err)
			require.Equal(t, c, got)
		})
		t.Run("private retry cannot retarget a cell", func(t *testing.T) {
			for _, name := range []string{"first", "second"} {
				_, err := pgjobdb.CreateCell(ctx, db, jobdb.CreateCellRequest{TenantID: "private", Name: name, Repo: "example.test/" + name})
				require.NoError(t, err)
			}
			req := pgjobdb.JobPreparation{Key: jobdb.JobKey{TenantId: "private", JobId: "job"}, Cell: "first", CellRepo: "example.test/first", Descriptor: "same"}
			p, err := pgjobdb.PrepareJob(ctx, db, req)
			require.NoError(t, err)
			req.Cell = "second"
			req.CellRepo = "example.test/second"
			_, err = pgjobdb.PrepareJob(ctx, db, req)
			require.True(t, errors.Is(err, jobdb.ErrExistingJobMismatch))
			_, err = pgjobdb.SubmitJob(ctx, db, pgjobdb.SubmitJobRequest{TenantID: "private", JobID: "job", WorkerID: "worker", JobType: "recipe", Cell: "second", CellRepo: "example.test/second", GoalID: p.GoalID})
			require.True(t, errors.Is(err, jobdb.ErrExistingJobMismatch))
			listed, err := pgjobdb.ListCells(ctx, db, jobdb.ListCellsRequest{TenantID: "private"})
			require.NoError(t, err)
			require.Len(t, listed.Cells, 2)
			require.NoError(t, pgjobdb.DiscardJobPreparation(ctx, db, p.Key))
		})
		t.Run("job ids are tenant wide", func(t *testing.T) {
			for _, name := range []string{"first", "second"} {
				_, err := pgjobdb.CreateCell(ctx, db, jobdb.CreateCellRequest{TenantID: "identity", Name: name, Repo: "example.test/" + name})
				require.NoError(t, err)
			}
			_, err := pgjobdb.SubmitJob(ctx, db, pgjobdb.SubmitJobRequest{TenantID: "identity", JobID: "same", WorkerID: "worker", JobType: "recipe", Cell: "first", CellRepo: "example.test/first"})
			require.NoError(t, err)
			_, err = pgjobdb.SubmitJob(ctx, db, pgjobdb.SubmitJobRequest{TenantID: "identity", JobID: "same", WorkerID: "worker", JobType: "recipe", Cell: "second", CellRepo: "example.test/second"})
			require.True(t, errors.Is(err, jobdb.ErrExistingJobMismatch))
			listed, err := pgjobdb.ListCells(ctx, db, jobdb.ListCellsRequest{TenantID: "identity"})
			require.NoError(t, err)
			require.Len(t, listed.Cells, 2)
		})
		t.Run("borrowed transaction rolls back failed publication", func(t *testing.T) {
			_, err := pgjobdb.CreateCell(ctx, db, jobdb.CreateCellRequest{TenantID: "rollback", Name: "new", Repo: "example.test/new"})
			require.NoError(t, err)
			tx, err := db.BeginTx(ctx, nil)
			require.NoError(t, err)
			defer tx.Rollback()
			_, err = pgjobdb.SubmitJob(ctx, tx, pgjobdb.SubmitJobRequest{TenantID: "rollback", JobID: "job", WorkerID: "worker", Cell: "new", CellRepo: "example.test/new"})
			require.Error(t, err) // empty job type fails after cell resolution
			require.NoError(t, tx.Commit())
			var count int
			require.NoError(t, db.QueryRowContext(ctx, `SELECT count(*) FROM pgjobdb.job_facts WHERE tenant_id='rollback'`).Scan(&count))
			require.Zero(t, count)
		})
		t.Run("lease loss between preparation and publication", func(t *testing.T) {
			_, err := pgjobdb.CreateCell(ctx, db, jobdb.CreateCellRequest{TenantID: "lease", Name: "root", Repo: "example.test/root"})
			require.NoError(t, err)
			_, err = pgjobdb.SubmitJob(ctx, db, pgjobdb.SubmitJobRequest{TenantID: "lease", JobID: "parent", WorkerID: "submitter", JobType: "recipe", Cell: "root"})
			require.NoError(t, err)
			lease, err := pgjobdb.GetJobLease(ctx, db, "lease", "parent", "worker", pgjobdb.WorkSelector{JobTypes: []pgjobdb.JobType{"recipe"}}, pgjobdb.GetJobLeaseOptions{})
			require.NoError(t, err)
			require.NotNil(t, lease)
			proof := cellstate.WithLease(context.Background(), cellstate.LeaseProof{JobKey: jobdb.JobKey{TenantId: "lease", JobId: "parent"}, LeaseID: lease.LeaseID, WorkerID: "worker"})
			prep, err := pgjobdb.PrepareJob(proof, db, pgjobdb.JobPreparation{Key: jobdb.JobKey{TenantId: "lease", JobId: "child"}, Cell: "root", CellRepo: "example.test/root", Descriptor: "child", ParentJobID: "parent"})
			require.NoError(t, err)
			require.NoError(t, pgjobdb.CompleteJob(ctx, db, lease.Identity(), pgjobdb.Completion{Status: pgjobdb.CompletionSuccess}))
			_, err = pgjobdb.SubmitJob(proof, db, pgjobdb.SubmitJobRequest{TenantID: "lease", JobID: "child", WorkerID: "worker", JobType: "recipe", Cell: "root", CellRepo: "example.test/root", GoalID: prep.GoalID, Target: prep.Target})
			require.ErrorIs(t, err, jobdb.ErrExecutionLeaseLost)
			listed, err := pgjobdb.ListCells(ctx, db, jobdb.ListCellsRequest{TenantID: "lease", Name: "child"})
			require.NoError(t, err)
			require.Empty(t, listed.Cells)
			require.NoError(t, pgjobdb.DiscardJobPreparation(ctx, db, prep.Key))
		})
		t.Run("retirement serializes with admission", func(t *testing.T) {
			for i := 0; i < 12; i++ {
				tenant := fmt.Sprintf("retire-%d", i)
				c, err := pgjobdb.CreateCell(ctx, db, jobdb.CreateCellRequest{TenantID: tenant, Name: "work", Repo: "example.test/work"})
				require.NoError(t, err)
				start := make(chan struct{})
				var wg sync.WaitGroup
				wg.Add(2)
				var retireErr, submitErr error
				go func() {
					defer wg.Done()
					<-start
					_, retireErr = pgjobdb.RetireCell(ctx, db, jobdb.RetireCellRequest{CellKey: jobdb.CellKey{TenantID: tenant, CellID: c.ID}, IfMatch: `"` + c.Revision + `"`})
				}()
				go func() {
					defer wg.Done()
					<-start
					_, submitErr = pgjobdb.SubmitJob(ctx, db, pgjobdb.SubmitJobRequest{TenantID: pgjobdb.TenantID(tenant), JobID: "job", WorkerID: "worker", JobType: "recipe", Cell: "work"})
				}()
				close(start)
				wg.Wait()
				if retireErr == nil {
					require.ErrorIs(t, submitErr, jobdb.ErrCellRetired)
				} else {
					require.ErrorIs(t, retireErr, jobdb.ErrCellBusy)
					require.NoError(t, submitErr)
				}
			}
		})
		t.Run("lease creation rollback and identity", func(t *testing.T) {
			const tenant = "lease-create"
			_, err := pgjobdb.CreateCell(ctx, db, jobdb.CreateCellRequest{TenantID: tenant, Name: "ordinary", Repo: "example.test/ordinary"})
			require.NoError(t, err)
			_, err = pgjobdb.SubmitJob(ctx, db, pgjobdb.SubmitJobRequest{TenantID: tenant, JobID: "parent", WorkerID: "submitter", JobType: "recipe", Cell: "ordinary"})
			require.NoError(t, err)
			lease, err := pgjobdb.GetJobLease(ctx, db, tenant, "parent", "worker", pgjobdb.WorkSelector{JobTypes: []pgjobdb.JobType{"recipe"}}, pgjobdb.GetJobLeaseOptions{})
			require.NoError(t, err)
			require.NotNil(t, lease)
			key := jobdb.JobKey{TenantId: tenant, JobId: "parent"}
			req := jobdb.CreateCellRequest{Name: "new", Repo: "example.test/new", RequestID: "once"}
			plain := context.Background()
			_, err = pgjobdb.CreateCellWithLeaseByID(plain, db, key, lease.LeaseID, "wrong-worker", req)
			require.ErrorIs(t, err, jobdb.ErrExecutionLeaseLost)
			tx, err := db.BeginTx(plain, nil)
			require.NoError(t, err)
			defer tx.Rollback()
			uncommitted, err := pgjobdb.CreateCellWithLeaseByID(plain, tx, key, lease.LeaseID, "worker", req)
			require.NoError(t, err)
			require.NoError(t, tx.Rollback())
			list, err := pgjobdb.ListCells(ctx, db, jobdb.ListCellsRequest{TenantID: tenant, Name: req.Name})
			require.NoError(t, err)
			require.Empty(t, list.Cells)
			created, err := pgjobdb.CreateCellWithLeaseByID(plain, db, key, lease.LeaseID, "worker", req)
			require.NoError(t, err)
			require.NotEqual(t, uncommitted.ID, created.ID, "creation replay must roll back with the cell")
			again, err := pgjobdb.CreateCellWithLeaseByID(plain, db, key, lease.LeaseID, "worker", req)
			require.NoError(t, err)
			require.Equal(t, created, again)
			require.NoError(t, pgjobdb.CompleteJob(ctx, db, lease.Identity(), pgjobdb.Completion{Status: pgjobdb.CompletionSuccess}))
			_, err = pgjobdb.CreateCellWithLeaseByID(plain, db, key, lease.LeaseID, "worker", req)
			require.ErrorIs(t, err, jobdb.ErrExecutionLeaseLost)
		})
		t.Run("repository claims are unique across connections", func(t *testing.T) {
			start := make(chan struct{})
			var wg sync.WaitGroup
			wg.Add(2)
			errs := make([]error, 2)
			for i := range errs {
				go func(i int) {
					defer wg.Done()
					<-start
					_, errs[i] = pgjobdb.CreateCell(ctx, db, jobdb.CreateCellRequest{TenantID: "claims", Name: fmt.Sprintf("name-%d", i), Repo: "example.test/shared"})
				}(i)
			}
			close(start)
			wg.Wait()
			success := 0
			for _, err := range errs {
				if err == nil {
					success++
				} else {
					require.ErrorIs(t, err, jobdb.ErrCellConflict)
				}
			}
			require.Equal(t, 1, success)
		})
		t.Run("repository edit serializes with schedule admission", func(t *testing.T) {
			c, err := pgjobdb.CreateCell(ctx, db, jobdb.CreateCellRequest{TenantID: "schedule-race", Name: "work", Repo: "example.test/work"})
			require.NoError(t, err)
			target, _ := json.Marshal(c.Resolved())
			start := make(chan struct{})
			var wg sync.WaitGroup
			wg.Add(2)
			var editErr, scheduleErr error
			go func() {
				defer wg.Done()
				<-start
				repo := "example.test/updated"
				_, editErr = pgjobdb.UpdateCell(ctx, db, jobdb.UpdateCellRequest{CellKey: jobdb.CellKey{TenantID: c.TenantID, CellID: c.ID}, IfMatch: `"` + c.Revision + `"`, Repo: &repo})
			}()
			go func() {
				defer wg.Done()
				<-start
				_, scheduleErr = pgjobdb.UpsertSchedule(ctx, db, pgjobdb.UpsertScheduleRequest{TenantID: "schedule-race", ScheduleID: "daily", State: pgjobdb.ScheduleStateActive, SpecHash: "hash", Trigger: json.RawMessage(`{}`), TargetJobType: "recipe", TargetSnapshot: target, OverlapPolicy: "ALLOW", FailurePolicy: json.RawMessage(`{}`)})
			}()
			close(start)
			wg.Wait()
			require.NoError(t, editErr)
			if scheduleErr != nil {
				require.ErrorIs(t, scheduleErr, jobdb.ErrCellRepoMismatch)
			} else {
				s, err := pgjobdb.GetSchedule(ctx, db, "schedule-race", "daily")
				require.NoError(t, err)
				require.Equal(t, pgjobdb.ScheduleStatePaused, s.State)
				require.Equal(t, "cell_repo_mismatch", s.PauseReason)
			}
		})
		t.Run("preparation remains bound through rename", func(t *testing.T) {
			c, err := pgjobdb.CreateCell(ctx, db, jobdb.CreateCellRequest{TenantID: "pending-name", Name: "work", Repo: "example.test/work"})
			require.NoError(t, err)
			p, err := pgjobdb.PrepareJob(ctx, db, pgjobdb.JobPreparation{Key: jobdb.JobKey{TenantId: "pending-name", JobId: "job"}, Cell: "work", CellRepo: "example.test/work", Descriptor: "job"})
			require.NoError(t, err)
			require.Equal(t, c.ID, p.Target.CellID)
			name := "renamed"
			c, err = pgjobdb.UpdateCell(ctx, db, jobdb.UpdateCellRequest{CellKey: jobdb.CellKey{TenantID: c.TenantID, CellID: c.ID}, IfMatch: fmt.Sprintf("%q", c.Revision), Name: &name})
			require.NoError(t, err)
			req := jobdb.RetireCellRequest{CellKey: jobdb.CellKey{TenantID: c.TenantID, CellID: c.ID}, IfMatch: fmt.Sprintf("%q", c.Revision)}
			_, err = pgjobdb.RetireCell(ctx, db, req)
			require.ErrorIs(t, err, jobdb.ErrCellBusy)
			require.NoError(t, pgjobdb.DiscardJobPreparation(ctx, db, p.Key))
			_, err = pgjobdb.RetireCell(ctx, db, req)
			require.NoError(t, err)
		})
		t.Run("pending preparation prevents retirement", func(t *testing.T) {
			c, err := pgjobdb.CreateCell(ctx, db, jobdb.CreateCellRequest{TenantID: "pending", Name: "work", Repo: "example.test/work"})
			require.NoError(t, err)
			p, err := pgjobdb.PrepareJob(ctx, db, pgjobdb.JobPreparation{Key: jobdb.JobKey{TenantId: "pending", JobId: "job"}, Cell: "work", Descriptor: "job"})
			require.NoError(t, err)
			req := jobdb.RetireCellRequest{CellKey: jobdb.CellKey{TenantID: c.TenantID, CellID: c.ID}, IfMatch: `"` + c.Revision + `"`}
			_, err = pgjobdb.RetireCell(ctx, db, req)
			require.ErrorIs(t, err, jobdb.ErrCellBusy)
			require.NoError(t, pgjobdb.DiscardJobPreparation(ctx, db, p.Key))
			_, err = pgjobdb.RetireCell(ctx, db, req)
			require.NoError(t, err)
		})
	})
}

func TestCellSelectionBeforePaginationAndAcquisition(t *testing.T) {
	runDatabaseTest(t, func(ctx context.Context, db *sql.DB) {
		cells := []jobdb.Cell{}
		for _, name := range []string{"allowed", "other"} {
			c, err := pgjobdb.CreateCell(ctx, db, jobdb.CreateCellRequest{TenantID: "scope", Name: name, Repo: "example.test/" + name})
			require.NoError(t, err)
			cells = append(cells, c)
			for i := 0; i < 3; i++ {
				_, err = pgjobdb.SubmitJob(ctx, db, pgjobdb.SubmitJobRequest{TenantID: "scope", JobID: pgjobdb.JobID(fmt.Sprintf("%s-%d", name, i)), WorkerID: "submitter", JobType: "recipe", Cell: name})
				require.NoError(t, err)
			}
		}
		authority := jobdb.CellAuthority{Principal: "reader", TenantID: "scope", Permissions: map[string]bool{"jobs.list": true, "jobs.acquire": true}, Cells: map[string]bool{cells[0].ID: true}}
		restricted := jobdb.WithCellAuthority(context.Background(), authority)
		opts := pgjobdb.ListJobsOptions{TenantIDs: []pgjobdb.TenantID{"scope"}, PageSize: 1}
		first, err := pgjobdb.ListJobSummaries(restricted, db, opts)
		require.NoError(t, err)
		require.Len(t, first.Jobs, 1)
		require.Equal(t, cells[0].ID, first.Jobs[0].CellID)
		require.NotEmpty(t, first.NextPageToken)
		opts.PageToken = first.NextPageToken
		other := authority
		other.Principal = "other-reader"
		_, err = pgjobdb.ListJobSummaries(jobdb.WithCellAuthority(context.Background(), other), db, opts)
		require.Error(t, err)
		second, err := pgjobdb.ListJobSummaries(restricted, db, opts)
		require.NoError(t, err)
		require.Len(t, second.Jobs, 1)
		require.NotEqual(t, first.Jobs[0].JobID, second.Jobs[0].JobID)
		opts.PageToken = ""
		opts.CellNames = []string{"other"}
		_, err = pgjobdb.ListJobSummaries(restricted, db, opts)
		require.Error(t, err)
		for i := 0; i < 3; i++ {
			l, err := pgjobdb.GetWork(restricted, db, "worker", pgjobdb.WorkSelector{JobTypes: []pgjobdb.JobType{"recipe"}}, pgjobdb.GetWorkOptions{TenantIDs: []pgjobdb.TenantID{"scope"}})
			require.NoError(t, err)
			require.NotNil(t, l)
			require.Equal(t, cells[0].ID, l.Cell.CellID)
		}
		l, err := pgjobdb.GetWork(restricted, db, "worker", pgjobdb.WorkSelector{JobTypes: []pgjobdb.JobType{"recipe"}}, pgjobdb.GetWorkOptions{TenantIDs: []pgjobdb.TenantID{"scope"}})
		require.NoError(t, err)
		require.Nil(t, l)
		_, err = pgjobdb.GetJobLease(restricted, db, "scope", "other-0", "worker", pgjobdb.WorkSelector{JobTypes: []pgjobdb.JobType{"recipe"}}, pgjobdb.GetJobLeaseOptions{})
		require.NoError(t, err)
		var leased bool
		require.NoError(t, db.QueryRowContext(ctx, `SELECT lease_id IS NOT NULL FROM pgjobdb.jobs WHERE tenant_id='scope' AND job_id='other-0'`).Scan(&leased))
		require.False(t, leased)
	})
}

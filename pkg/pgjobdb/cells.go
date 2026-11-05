package pgjobdb

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/cellstate"
)

// Cell operations take the tenant goal/admission lock before any row lock.
// Existing transactions must use the same order. Jobs reference explicitly
// registered cells; publication never changes the registry.
func cellTx(ctx context.Context, db DB, tenant string, fn func(*sql.Tx, *cellstate.State) error) (err error) {
	if err := validateScheduleDB(ctx, db); err != nil {
		return err
	}
	if tenant == "" {
		return jobdb.ErrInvalidCell
	}
	var tx *sql.Tx
	owned := false
	if existing, ok := db.(*sql.Tx); ok {
		tx = existing
	} else if starter, ok := db.(interface {
		BeginTx(context.Context, *sql.TxOptions) (*sql.Tx, error)
	}); ok {
		tx, err = starter.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		owned = true
		defer tx.Rollback()
	} else {
		return fmt.Errorf("pgjobdb: cell operations require a transaction-capable DB")
	}
	if !owned {
		if _, err = tx.ExecContext(ctx, "SAVEPOINT cell_operation"); err != nil {
			return err
		}
		defer func() {
			if err != nil {
				_, rollbackErr := tx.ExecContext(context.WithoutCancel(ctx), "ROLLBACK TO SAVEPOINT cell_operation")
				err = errors.Join(err, rollbackErr)
			}
			_, releaseErr := tx.ExecContext(context.WithoutCancel(ctx), "RELEASE SAVEPOINT cell_operation")
			err = errors.Join(err, releaseErr)
		}()
	}
	if _, err = tx.ExecContext(ctx, `SELECT pgjobdb._goal_lock($1)`, tenant); err != nil {
		return err
	}
	state, err := loadCells(ctx, tx, tenant)
	if err != nil {
		return err
	}
	before := cellstate.Clone(*state)
	if err = fn(tx, state); err != nil {
		return err
	}
	if err = saveCells(ctx, tx, tenant, &before, state); err != nil {
		return err
	}
	if owned {
		return tx.Commit()
	}
	return nil
}

// A single statement reads the normalized registry at one MVCC snapshot.
// JSON is used only for arbitrary metadata supplied by the caller.
func loadCells(ctx context.Context, db DB, tenant string) (*cellstate.State, error) {
	if err := validateScheduleDB(ctx, db); err != nil {
		return nil, err
	}
	s := cellstate.New()
	rows, err := db.QueryContext(ctx, `SELECT 0 AS kind,cell_id,name,repo,description,metadata::text,state,revision,created_at_ns,updated_at_ns,retired_at_ns,'' AS principal,'' AS request_id,'' AS descriptor,NULL::bytea AS cursor_key FROM pgjobdb.cells WHERE tenant_id=$1
 UNION ALL SELECT 1,cell_id,name,'','','{}','','',0,0,NULL,'','','',NULL FROM pgjobdb.cell_names WHERE tenant_id=$1
 UNION ALL SELECT 2,cell_id,name,repo,description,metadata::text,'ACTIVE',revision,created_at_ns,created_at_ns,NULL,principal,request_id,descriptor,NULL FROM pgjobdb.cell_requests WHERE tenant_id=$1
 UNION ALL SELECT 3,'','','','','{}','','',0,0,NULL,'','','',cursor_key FROM pgjobdb.cell_catalog WHERE tenant_id=$1
 ORDER BY kind,cell_id,name,principal,request_id`, tenant)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var kind int
		c := jobdb.Cell{TenantID: tenant, Aliases: []string{}}
		var raw, key []byte
		var created, updated int64
		var retired sql.NullInt64
		var principal, request, descriptor string
		if err = rows.Scan(&kind, &c.ID, &c.Name, &c.Repo, &c.Description, &raw, &c.State, &c.Revision, &created, &updated, &retired, &principal, &request, &descriptor, &key); err != nil {
			return nil, err
		}
		switch kind {
		case 0, 2:
			if err = json.Unmarshal(raw, &c.Metadata); err != nil {
				return nil, err
			}
			c.CreatedAt = time.Unix(0, created).UTC()
			c.UpdatedAt = time.Unix(0, updated).UTC()
			if retired.Valid {
				t := time.Unix(0, retired.Int64).UTC()
				c.RetiredAt = &t
			}
			if kind == 0 {
				s.Cells[c.ID] = c
			} else {
				s.Creations = append(s.Creations, cellstate.Creation{Principal: principal, RequestID: request, Descriptor: descriptor, Result: c})
			}
		case 1:
			stored := s.Cells[c.ID]
			if stored.Name != c.Name {
				stored.Aliases = append(stored.Aliases, c.Name)
				s.Cells[c.ID] = stored
			}
		case 3:
			s.CursorKey = key
		}
	}
	return s, rows.Err()
}
func saveCells(ctx context.Context, tx *sql.Tx, tenant string, before, after *cellstate.State) error {
	for id, c := range after.Cells {
		if reflect.DeepEqual(before.Cells[id], c) {
			continue
		}
		raw, err := json.Marshal(c.Metadata)
		if err != nil {
			return err
		}
		var retired any
		if c.RetiredAt != nil {
			retired = c.RetiredAt.UnixNano()
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO pgjobdb.cells(tenant_id,cell_id,name,repo,description,metadata,state,revision,created_at_ns,updated_at_ns,retired_at_ns) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11) ON CONFLICT(tenant_id,cell_id) DO UPDATE SET name=excluded.name,repo=excluded.repo,description=excluded.description,metadata=excluded.metadata,state=excluded.state,revision=excluded.revision,updated_at_ns=excluded.updated_at_ns,retired_at_ns=excluded.retired_at_ns`, tenant, id, c.Name, c.Repo, c.Description, string(raw), c.State, c.Revision, c.CreatedAt.UnixNano(), c.UpdatedAt.UnixNano(), retired)
		if err != nil {
			return err
		}
		for _, name := range append([]string{c.Name}, c.Aliases...) {
			if _, err = tx.ExecContext(ctx, `INSERT INTO pgjobdb.cell_names VALUES($1,$2,$3) ON CONFLICT(tenant_id,name) DO NOTHING`, tenant, name, id); err != nil {
				return err
			}
		}
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO pgjobdb.cell_catalog VALUES($1,$2) ON CONFLICT(tenant_id) DO NOTHING`, tenant, after.CursorKey); err != nil {
		return err
	}
	for _, r := range after.Creations {
		c := r.Result
		raw, err := json.Marshal(c.Metadata)
		if err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, `INSERT INTO pgjobdb.cell_requests(tenant_id,principal,request_id,descriptor,cell_id,name,repo,description,metadata,revision,created_at_ns) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11) ON CONFLICT(tenant_id,principal,request_id) DO NOTHING`, tenant, r.Principal, r.RequestID, r.Descriptor, c.ID, c.Name, c.Repo, c.Description, string(raw), c.Revision, c.CreatedAt.UnixNano()); err != nil {
			return err
		}
	}
	return nil
}
func CreateCell(ctx context.Context, db DB, req jobdb.CreateCellRequest) (out jobdb.Cell, err error) {
	err = cellTx(ctx, db, req.TenantID, func(_ *sql.Tx, s *cellstate.State) error { var e error; out, e = s.Create(ctx, req); return e })
	return
}

func CreateCellWithLeaseByID(ctx context.Context, db DB, key jobdb.JobKey, leaseID, workerID string, req jobdb.CreateCellRequest) (out jobdb.Cell, err error) {
	p := cellstate.LeaseProof{JobKey: key, LeaseID: leaseID, WorkerID: workerID}
	ctx = cellstate.WithLease(ctx, p)
	err = cellTx(ctx, db, key.TenantId, func(tx *sql.Tx, s *cellstate.State) error {
		if _, e := cellParent(ctx, tx, s, key.TenantId); e != nil {
			return e
		}
		var e error
		out, e = s.CreateWithLease(ctx, p, req)
		return e
	})
	return
}
func GetCell(ctx context.Context, db DB, key jobdb.CellKey) (out jobdb.Cell, err error) {
	s, err := loadCells(ctx, db, key.TenantID)
	if err != nil {
		return out, err
	}
	return s.Get(ctx, key)
}
func ListCells(ctx context.Context, db DB, req jobdb.ListCellsRequest) (out jobdb.ListCellsResponse, err error) {
	err = cellTx(ctx, db, req.TenantID, func(_ *sql.Tx, s *cellstate.State) error { var e error; out, e = s.List(ctx, req); return e })
	return
}
func UpdateCell(ctx context.Context, db DB, req jobdb.UpdateCellRequest) (out jobdb.Cell, err error) {
	err = cellTx(ctx, db, req.TenantID, func(tx *sql.Tx, s *cellstate.State) error {
		old := s.Cells[req.CellID]
		var e error
		out, e = s.Update(ctx, req)
		if e == nil && old.Repo != out.Repo {
			_, e = tx.ExecContext(ctx, `UPDATE pgjobdb.schedules SET state='PAUSED',pause_reason='cell_repo_mismatch',next_fire_at=NULL,next_job_id=NULL,updated_at=clock_timestamp() WHERE tenant_id=$1 AND state='ACTIVE' AND cell_id=$2`, req.TenantID, req.CellID)
		}
		return e
	})
	return
}
func RetireCell(ctx context.Context, db DB, req jobdb.RetireCellRequest) (out jobdb.Cell, err error) {
	err = cellTx(ctx, db, req.TenantID, func(tx *sql.Tx, s *cellstate.State) error {
		var e error
		out, e = s.Retire(ctx, req, func(id string) (bool, error) {
			var busy bool
			e := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM pgjobdb.jobs j JOIN pgjobdb.job_facts f USING(tenant_id,job_id) WHERE f.tenant_id=$1 AND f.cell_id=$2)
    OR EXISTS(SELECT 1 FROM pgjobdb.goals WHERE tenant_id=$1 AND cell_id=$2 AND state='OPEN')
    OR EXISTS(SELECT 1 FROM pgjobdb.job_preparations p WHERE tenant_id=$1 AND NOT published AND cell_id=$2)
    OR EXISTS(SELECT 1 FROM pgjobdb.schedules WHERE tenant_id=$1 AND cell_id=$2 AND state='ACTIVE')`, req.TenantID, id).Scan(&busy)
			return busy, e
		})
		return e
	})
	return
}
func cellParent(ctx context.Context, tx *sql.Tx, s *cellstate.State, tenant string) (*jobdb.Cell, error) {
	proof, ok := cellstate.Lease(ctx)
	if !ok {
		return nil, nil
	}
	if proof.JobKey.TenantId != tenant {
		return nil, jobdb.ErrCellForbidden
	}
	var id string
	err := tx.QueryRowContext(ctx, `SELECT f.cell_id FROM pgjobdb.jobs j JOIN pgjobdb.job_facts f USING(tenant_id,job_id) WHERE j.tenant_id=$1 AND j.job_id=$2 AND j.lease_id=$3 AND j.lease_worker_id=$4 AND j.lease_expires_at>clock_timestamp() AND NOT j.cancel_requested AND f.hidden_at IS NULL FOR UPDATE OF j`, tenant, proof.JobKey.JobId, proof.LeaseID, proof.WorkerID).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, jobdb.ErrExecutionLeaseLost
	}
	if err != nil {
		return nil, err
	}
	c, ok := s.Cells[id]
	if !ok || c.State != jobdb.CellActive {
		return nil, jobdb.ErrExecutionLeaseLost
	}
	return &c, nil
}
func resolvePreparation(ctx context.Context, tx *sql.Tx, s *cellstate.State, req JobPreparation) (jobdb.ResolvedCell, error) {
	tenant := req.Key.TenantId
	parent, err := cellParent(ctx, tx, s, tenant)
	if err != nil {
		return jobdb.ResolvedCell{}, err
	}
	if key, ok := cellstate.Schedule(ctx); ok {
		if key.TenantId != tenant {
			return jobdb.ResolvedCell{}, jobdb.ErrCellForbidden
		}
		var target jobdb.ResolvedCell
		var state string
		err = tx.QueryRowContext(ctx, `SELECT cell_id,cell_repo,state FROM pgjobdb.schedules WHERE tenant_id=$1 AND schedule_id=$2 FOR UPDATE`, tenant, key.ScheduleId).Scan(&target.CellID, &target.CellRepo, &state)
		if errors.Is(err, sql.ErrNoRows) || state == "ARCHIVED" {
			return jobdb.ResolvedCell{}, jobdb.ErrCellNotFound
		}
		if err != nil {
			return jobdb.ResolvedCell{}, err
		}
		c, ok := s.Cells[target.CellID]
		if !ok {
			return target, jobdb.ErrCellNotFound
		}
		if c.Repo != target.CellRepo || req.CellRepo != target.CellRepo {
			return target, jobdb.ErrCellRepoMismatch
		}
		parent = &c
	}
	var existing *jobdb.ResolvedCell
	confirmation := false
	read := func(id string) error {
		var c jobdb.ResolvedCell
		e := tx.QueryRowContext(ctx, `SELECT cell_name,cell_id,cell_repo FROM pgjobdb.job_facts WHERE tenant_id=$1 AND job_id=$2`, tenant, id).Scan(&c.Cell, &c.CellID, &c.CellRepo)
		if e == nil {
			existing = &c
		}
		return e
	}
	if req.Key.JobId != "" {
		err = read(req.Key.JobId)
		confirmation = err == nil
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return jobdb.ResolvedCell{}, err
		}
	}
	if existing == nil && req.PriorJobID != "" {
		if err = read(req.PriorJobID); err != nil {
			return jobdb.ResolvedCell{}, jobdb.ErrJobNotFound
		}
	}
	return s.Resolve(ctx, tenant, req.Cell, req.CellRepo, parent, existing, confirmation)
}
func ResolveCellWithLeaseByID(ctx context.Context, db DB, key jobdb.JobKey, leaseID, workerID, name string) (out jobdb.ResolvedCell, err error) {
	ctx = cellstate.WithLease(ctx, cellstate.LeaseProof{JobKey: key, LeaseID: leaseID, WorkerID: workerID})
	err = cellTx(ctx, db, key.TenantId, func(tx *sql.Tx, s *cellstate.State) error {
		parent, e := cellParent(ctx, tx, s, key.TenantId)
		if e != nil {
			return e
		}
		if _, ok := s.ByName(name); !ok {
			return jobdb.ErrCellNotFound
		}
		out, e = s.Resolve(ctx, key.TenantId, name, "", parent, nil, false)
		return e
	})
	return
}
func ResolveScheduleCell(ctx context.Context, db DB, tenant, name, repo string) (out jobdb.ResolvedCell, err error) {
	if err = jobdb.AuthorizeCell(ctx, tenant, "", "jobs.submit"); err != nil {
		return out, err
	}
	err = cellTx(ctx, db, tenant, func(_ *sql.Tx, s *cellstate.State) error {
		if _, ok := s.ByName(name); !ok {
			return jobdb.ErrCellNotFound
		}
		var e error
		out, e = s.Resolve(ctx, tenant, name, repo, nil, nil, false)
		return e
	})
	return
}
func selectCells(ctx context.Context, db DB, tenants []TenantID, names []string, permission string) ([]string, error) {
	ids := []string{}
	for _, tenant := range tenants {
		s, err := loadCells(ctx, db, string(tenant))
		if err != nil {
			return nil, err
		}
		selected, err := s.Select(ctx, string(tenant), names, permission)
		if err != nil {
			return nil, err
		}
		for id := range selected {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	return ids, nil
}

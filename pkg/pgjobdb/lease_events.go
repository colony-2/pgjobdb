package pgjobdb

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/cellstate"
	"github.com/lib/pq"
)

// ListLeaseEvents reads canonical history, including when db is a borrowed transaction.
// All scope/key data is read in one statement, followed by one bounded event query.
func ListLeaseEvents(ctx context.Context, db DB, req jobdb.ListLeaseEventsRequest) (jobdb.ListLeaseEventsResponse, error) {
	empty := jobdb.ListLeaseEventsResponse{Events: []jobdb.LeaseEvent{}}
	req, err := jobdb.NormalizeLeaseEventRequest(req)
	if err != nil {
		return empty, err
	}
	if err = validateDB(ctx, db); err != nil {
		return empty, err
	}
	if err = jobdb.AuthorizeCell(ctx, req.TenantID, "", "jobs.list"); err != nil {
		return empty, err
	}
	state := &cellstate.State{Cells: map[string]jobdb.Cell{}}
	rows, err := db.QueryContext(ctx, `SELECT 0,cell_id,name,NULL::bytea FROM pgjobdb.cells WHERE tenant_id=$1
 UNION ALL SELECT 1,cell_id,name,NULL::bytea FROM pgjobdb.cell_names WHERE tenant_id=$1
 UNION ALL SELECT 2,'','',cursor_key FROM pgjobdb.cell_catalog WHERE tenant_id=$1 ORDER BY 1`, req.TenantID)
	if err != nil {
		return empty, err
	}
	for rows.Next() {
		var kind int
		var id, name string
		var key []byte
		if err = rows.Scan(&kind, &id, &name, &key); err != nil {
			rows.Close()
			return empty, err
		}
		switch kind {
		case 0:
			state.Cells[id] = jobdb.Cell{TenantID: req.TenantID, ID: id, Name: name}
		case 1:
			c := state.Cells[id]
			if c.Name != name {
				c.Aliases = append(c.Aliases, name)
				state.Cells[id] = c
			}
		case 2:
			state.CursorKey = key
		}
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return empty, err
	}
	ids, err := state.LeaseEventScope(ctx, req)
	if err != nil {
		return empty, err
	}
	binding := cellstate.LeaseEventBinding(ctx, req, ids)
	pos, err := cellstate.DecodeLeaseEventCursor(state.CursorKey, binding, req.PageToken)
	if err != nil {
		return empty, err
	}
	if len(ids) == 0 {
		return empty, nil
	}
	query, args := leaseEventsQuery(req, ids, pos)
	rows, err = db.QueryContext(ctx, query, args...)
	if err != nil {
		return empty, err
	}
	defer rows.Close()
	events := []jobdb.LeaseEvent{}
	for rows.Next() {
		var e jobdb.LeaseEvent
		var task, source, previous sql.NullString
		var expiry sql.NullTime
		if err = rows.Scan(&e.EventID, &e.EventType, &e.OccurredAt, &e.JobKey.TenantId, &e.JobKey.JobId, &e.CellID, &e.Cell, &e.CellRepo, &e.GoalID, &e.JobType, &e.Route.JobType, &task, &e.LeaseID, &e.WorkerID, &e.LeaseExpiresAt, &expiry, &source, &previous); err != nil {
			return empty, err
		}
		e.Route.TaskType = task.String
		e.AcquisitionSource = jobdb.LeaseAcquisitionSource(source.String)
		e.PreviousLeaseID = previous.String
		e.OccurredAt = e.OccurredAt.UTC()
		e.LeaseExpiresAt = e.LeaseExpiresAt.UTC()
		if expiry.Valid {
			t := expiry.Time.UTC()
			e.PreviousLeaseExpiresAt = &t
		}
		events = append(events, e)
	}
	if err = rows.Err(); err != nil {
		return empty, err
	}
	return cellstate.LeaseEventPage(events, req.PageSize, state.CursorKey, binding), nil
}

func ceilLeaseEventTime(t time.Time) time.Time {
	t = t.UTC()
	rounded := t.Truncate(time.Microsecond)
	if rounded.Before(t) {
		rounded = rounded.Add(time.Microsecond)
	}
	return rounded
}

func leaseEventsQuery(req jobdb.ListLeaseEventsRequest, ids []string, pos *cellstate.LeaseEventPosition) (string, []any) {
	args := []any{req.TenantID, pq.Array(ids)}
	bind := func(v any) string { args = append(args, v); return fmt.Sprintf("$%d", len(args)) }
	prefix := ""
	joins := ` LEFT JOIN pgjobdb.job_facts f ON f.tenant_id=e.tenant_id AND f.job_id=e.job_id `
	predicates := []string{"e.tenant_id=$1", "e.cell_id=ANY($2)"}
	if !req.IncludeHidden {
		predicates = append(predicates, "f.hidden_at IS NULL")
	}
	for _, filter := range []struct {
		column string
		values []string
	}{
		{"job_id", req.JobIDs}, {"worker_id", req.WorkerIDs}, {"lease_id", req.LeaseIDs}, {"job_type", req.JobTypes},
	} {
		if filter.values != nil {
			predicates = append(predicates, "e."+filter.column+"=ANY("+bind(pq.Array(filter.values))+")")
		}
	}
	if req.EventTypes != nil {
		values := []string{}
		for _, v := range req.EventTypes {
			values = append(values, string(v))
		}
		predicates = append(predicates, "e.event_type=ANY("+bind(pq.Array(values))+")")
	}
	if req.GoalIDs != nil {
		p := bind(pq.Array(req.GoalIDs))
		prefix = `WITH matching_candidate_jobs AS (SELECT DISTINCT tenant_id,job_id FROM pgjobdb.goal_candidates WHERE tenant_id=$1 AND goal_id=ANY(` + p + `)) `
		joins += ` LEFT JOIN matching_candidate_jobs g ON g.tenant_id=e.tenant_id AND g.job_id=e.job_id `
		predicates = append(predicates, "(e.goal_id=ANY("+p+") OR g.job_id IS NOT NULL)")
	}
	if req.Routes != nil {
		routes := []string{}
		for _, r := range req.Routes {
			task := "e.route_task_type IS NULL"
			if r.TaskType != "" {
				task = "e.route_task_type=" + bind(r.TaskType)
			}
			routes = append(routes, "(e.route_job_type="+bind(r.JobType)+" AND "+task+")")
		}
		predicates = append(predicates, "("+strings.Join(routes, " OR ")+")")
	}
	if req.OccurredAfter != nil {
		predicates = append(predicates, "e.occurred_at >= "+bind(ceilLeaseEventTime(*req.OccurredAfter)))
	}
	if req.OccurredBefore != nil {
		predicates = append(predicates, "e.occurred_at < "+bind(ceilLeaseEventTime(*req.OccurredBefore)))
	}
	if pos != nil {
		predicates = append(predicates, "(e.occurred_at,e.event_id) < ("+bind(pos.Time)+","+bind(pos.ID)+")")
	}
	query := prefix + `SELECT e.event_id::text,e.event_type,e.occurred_at,e.tenant_id,e.job_id,e.cell_id,e.cell_name,e.cell_repo,e.goal_id,e.job_type,e.route_job_type,e.route_task_type,e.lease_id,e.worker_id,e.lease_expires_at,e.previous_lease_expires_at,e.acquisition_source,e.previous_lease_id FROM pgjobdb.lease_events e` + joins + " WHERE " + strings.Join(predicates, " AND ") + " ORDER BY e.occurred_at DESC,e.event_id DESC LIMIT " + bind(req.PageSize+1)
	return query, args
}

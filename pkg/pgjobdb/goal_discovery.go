package pgjobdb

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/lib/pq"
)

func ListGoals(ctx context.Context, db DB, req jobdb.ListGoalsRequest) (jobdb.ListGoalsResponse, error) {
	out := jobdb.ListGoalsResponse{Goals: []jobdb.GoalSummary{}}
	if err := req.Validate(); err != nil {
		return out, err
	}
	if err := validateScheduleDB(ctx, db); err != nil {
		return out, err
	}
	size := jobdb.DiscoveryPageSize(req.PageSize)
	filter := req
	filter.PageToken = ""
	filter.PageSize = 0
	scope, err := jobdb.DiscoveryPageScope(filter)
	if err != nil {
		return out, err
	}
	after, err := jobdb.DecodeDiscoveryPage(req.PageToken, scope, 1)
	if err != nil {
		return out, err
	}
	args := []any{req.TenantId, after[0]}
	where := []string{"g.tenant_id=$1", `g.goal_id COLLATE "C">$2`}
	bind := func(v any) string { args = append(args, v); return fmt.Sprintf("$%d", len(args)) }
	if len(req.GoalIDs) > 0 {
		where = append(where, "g.goal_id=ANY("+bind(pq.Array(req.GoalIDs))+"::text[])")
	}
	if len(req.States) > 0 {
		states := []string{}
		for _, s := range req.States {
			states = append(states, string(s))
		}
		where = append(where, "g.state=ANY("+bind(pq.Array(states))+"::text[])")
	}
	if req.CreatedAfter != nil {
		where = append(where, "g.created_at>="+bind(*req.CreatedAfter))
	}
	if req.CreatedBefore != nil {
		where = append(where, "g.created_at<="+bind(*req.CreatedBefore))
	}
	if req.UpdatedAfter != nil {
		where = append(where, "g.updated_at>="+bind(*req.UpdatedAfter))
	}
	if req.UpdatedBefore != nil {
		where = append(where, "g.updated_at<="+bind(*req.UpdatedBefore))
	}
	if f := req.Candidate; f != nil {
		sub := []string{"f.tenant_id=g.tenant_id", "f.job_id=m.job_id"}
		if len(f.JobIDs) > 0 {
			sub = append(sub, "f.job_id=ANY("+bind(pq.Array(f.JobIDs))+"::text[])")
		}
		if len(f.JobTypes) > 0 {
			sub = append(sub, "f.job_type=ANY("+bind(pq.Array(f.JobTypes))+"::text[])")
		}
		for _, p := range f.MetadataEquals {
			vals := []string{}
			path := bind(pq.Array(p.Path))
			for _, v := range p.Values {
				raw, e := json.Marshal(v)
				if e != nil {
					return out, fmt.Errorf("%w: invalid metadata value", jobdb.ErrInvalidDependency)
				}
				vals = append(vals, "f.app_metadata #> "+path+"::text[] = "+bind(string(raw))+"::jsonb")
			}
			sub = append(sub, "("+strings.Join(vals, " OR ")+")")
		}
		where = append(where, "EXISTS (SELECT 1 FROM pgjobdb._goal_members(g.tenant_id,g.goal_id) m JOIN pgjobdb.job_facts f ON "+strings.Join(sub, " AND ")+")")
	}
	limit := bind(size + 1)
	query := `WITH page AS MATERIALIZED (SELECT g.tenant_id,g.goal_id FROM pgjobdb.goals g WHERE ` + strings.Join(where, " AND ") + ` ORDER BY g.goal_id COLLATE "C" LIMIT ` + limit + `)
 SELECT pgjobdb.goal_info(p.tenant_id,p.goal_id) || jsonb_build_object(
 'candidateCount',n.total,'nonTerminalCandidateCount',n.nonterminal,
 'terminalCandidateCount',n.terminal,'unavailableCandidateCount',n.unavailable)
 FROM page p CROSS JOIN LATERAL (
 SELECT count(*) total,count(*) FILTER(WHERE a.job_id IS NULL AND f.job_id IS NOT NULL) nonterminal,
 count(*) FILTER(WHERE a.job_id IS NOT NULL) terminal,count(*) FILTER(WHERE f.job_id IS NULL) unavailable
 FROM pgjobdb._goal_members(p.tenant_id,p.goal_id) m
 LEFT JOIN pgjobdb.job_facts f ON f.tenant_id=p.tenant_id AND f.job_id=m.job_id
 LEFT JOIN pgjobdb.jobs_archive a ON a.tenant_id=p.tenant_id AND a.job_id=m.job_id) n ORDER BY p.goal_id COLLATE "C"`
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return out, goalError(err)
	}
	defer rows.Close()
	for rows.Next() {
		var raw []byte
		var g jobdb.GoalSummary
		if err = rows.Scan(&raw); err != nil {
			return out, err
		}
		if err = json.Unmarshal(raw, &g); err != nil {
			return out, err
		}
		normalizeDiscoveryGoal(&g.GoalInfo)
		out.Goals = append(out.Goals, g)
	}
	if err = rows.Err(); err != nil {
		return out, err
	}
	if len(out.Goals) > size {
		out.Goals = out.Goals[:size]
		out.NextPageToken = jobdb.EncodeDiscoveryPage(scope, out.Goals[size-1].GoalKey.GoalID)
	}
	return out, nil
}

func ListDependencyWaits(ctx context.Context, db DB, req jobdb.ListDependencyWaitsRequest) (jobdb.ListDependencyWaitsResponse, error) {
	out := jobdb.ListDependencyWaitsResponse{Waits: []jobdb.DependencyWaitInfo{}}
	if err := req.Validate(); err != nil {
		return out, err
	}
	if err := validateScheduleDB(ctx, db); err != nil {
		return out, err
	}
	size := jobdb.DiscoveryPageSize(req.PageSize)
	filter := req
	filter.PageToken = ""
	filter.PageSize = 0
	scope, err := jobdb.DiscoveryPageScope(filter)
	if err != nil {
		return out, err
	}
	after, err := jobdb.DecodeDiscoveryPage(req.PageToken, scope, 2)
	if err != nil {
		return out, err
	}
	args := []any{req.TenantId, after[0], after[1]}
	where := []string{"w.tenant_id=$1", `(w.job_id COLLATE "C",w.wait_id COLLATE "C")>($2,$3)`}
	bind := func(v any) string { args = append(args, v); return fmt.Sprintf("$%d", len(args)) }
	from := "pgjobdb.dependency_waits w"
	if req.ConsumerJobID != "" {
		where = append(where, "w.job_id="+bind(req.ConsumerJobID))
	}
	projected := []string{"true"}
	if req.Target != nil {
		from += " JOIN pgjobdb.dependency_targets t USING(tenant_id,job_id,wait_id)"
		kind, id := "job", req.Target.JobID
		if req.Target.GoalID != "" {
			kind, id = "goal", req.Target.GoalID
		}
		where = append(where, "w.state='PENDING'", "t.target_kind="+bind(kind), "t.target_id="+bind(id))
		target, _ := json.Marshal(req.Target)
		projected = append(projected, "outcome->>'state'='PENDING'", "EXISTS(SELECT 1 FROM jsonb_array_elements(outcome->'dependencies') d WHERE d->>'state'='PENDING' AND d->'target'="+bind(string(target))+"::jsonb)")
	}
	if len(req.States) > 0 {
		states := []string{}
		for _, s := range req.States {
			states = append(states, string(s))
		}
		projected = append(projected, "outcome->>'state'=ANY("+bind(pq.Array(states))+"::text[])")
	}
	limit := bind(size + 1)
	query := "WITH candidates AS NOT MATERIALIZED (SELECT w.job_id,w.wait_id,pgjobdb._evaluate_wait(w.outcome,statement_timestamp()) outcome FROM " + from + " WHERE " + strings.Join(where, " AND ") + ") SELECT outcome FROM candidates WHERE " + strings.Join(projected, " AND ") + ` ORDER BY job_id COLLATE "C",wait_id COLLATE "C" LIMIT ` + limit
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return out, goalError(err)
	}
	defer rows.Close()
	for rows.Next() {
		var raw []byte
		var w jobdb.DependencyWaitInfo
		if err = rows.Scan(&raw); err != nil {
			return out, err
		}
		if err = json.Unmarshal(raw, &w); err != nil {
			return out, err
		}
		normalizeDiscoveryWait(&w)
		out.Waits = append(out.Waits, w)
	}
	if err = rows.Err(); err != nil {
		return out, err
	}
	if len(out.Waits) > size {
		out.Waits = out.Waits[:size]
		last := out.Waits[size-1]
		out.NextPageToken = jobdb.EncodeDiscoveryPage(scope, last.JobKey.JobId, last.WaitID)
	}
	return out, nil
}

// BlockingDependencyWait uses the scheduler-owned current wait pointer.
func BlockingDependencyWait(ctx context.Context, db DB, key jobdb.JobKey) (*jobdb.DependencyWaitInfo, error) {
	rows, err := db.QueryContext(ctx, `SELECT pgjobdb.dependency_wait_info(tenant_id,job_id,dependency_wait_id) FROM pgjobdb.jobs WHERE tenant_id=$1 AND job_id=$2 AND dependency_wait_id IS NOT NULL`, key.TenantId, key.JobId)
	if err != nil {
		return nil, goalError(err)
	}
	defer rows.Close()
	if !rows.Next() {
		return nil, rows.Err()
	}
	var raw []byte
	var w jobdb.DependencyWaitInfo
	if err = rows.Scan(&raw); err != nil {
		return nil, err
	}
	if err = json.Unmarshal(raw, &w); err != nil {
		return nil, err
	}
	if w.State != jobdb.DependencyPending {
		return nil, nil
	}
	normalizeDiscoveryWait(&w)
	return &w, nil
}

func normalizeDiscoveryGoal(g *jobdb.GoalInfo) {
	g.CreatedAt = g.CreatedAt.UTC()
	g.UpdatedAt = g.UpdatedAt.UTC()
	if g.ClosedAt != nil {
		v := g.ClosedAt.UTC()
		g.ClosedAt = &v
	}
	if g.Failure != nil {
		g.Failure.FailedAt = g.Failure.FailedAt.UTC()
	}
}
func normalizeDiscoveryWait(w *jobdb.DependencyWaitInfo) {
	if w.Deadline != nil {
		v := w.Deadline.UTC()
		w.Deadline = &v
	}
	if w.Failure != nil {
		w.Failure.FailedAt = w.Failure.FailedAt.UTC()
	}
	for i := range w.Dependencies {
		if f := w.Dependencies[i].Failure; f != nil {
			f.FailedAt = f.FailedAt.UTC()
		}
	}
}

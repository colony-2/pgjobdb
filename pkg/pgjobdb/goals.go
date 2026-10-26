package pgjobdb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/lib/pq"
)

// JobPreparation durably identifies a submission before history is prepared.
type JobPreparation struct {
	Key          jobdb.JobKey
	RequestID    string
	PriorJobID   string
	ShareGoal    bool
	Descriptor   string
	Dependencies []jobdb.Dependency
}

// PreparedJob identifies a reserved destination and whether it has been published.
type PreparedJob struct {
	Key       jobdb.JobKey
	GoalID    string
	Published bool
}

func goalError(err error) error {
	var pg *pq.Error
	if !errors.As(err, &pg) {
		return err
	}
	var cause error
	switch pg.Code {
	case "JGN01":
		cause = jobdb.ErrGoalNotFound
	case "JJN01":
		cause = jobdb.ErrJobNotFound
	case "JGF01":
		cause = jobdb.ErrGoalConflict
	case "JGI01":
		cause = jobdb.ErrInvalidDependency
	case "JGW01":
		cause = jobdb.ErrDependencyWaitNotFound
	case "JGW02":
		cause = jobdb.ErrDependencyWaitConflict
	case "JGC01":
		cause = jobdb.ErrDependencyCycle
	case "JGM01":
		return jobdb.NewExistingJobMismatchError(pg.Message)
	default:
		return err
	}
	return fmt.Errorf("%w: %s", cause, pg.Message)
}
func goalQuery[T any](ctx context.Context, db DB, query string, args ...any) (out T, err error) {
	if err = validateScheduleDB(ctx, db); err != nil {
		return
	}
	var raw []byte
	if err = db.QueryRowContext(ctx, query, args...).Scan(&raw); err != nil {
		return out, goalError(err)
	}
	err = json.Unmarshal(raw, &out)
	// PostgreSQL emits +00:00, which Go may decode as time.Local. Normalize
	// public timestamps so snapshots remain identical through history/HTTP codecs.
	normalizeFailure := func(f *jobdb.GoalFailure) {
		if f != nil {
			f.FailedAt = f.FailedAt.UTC()
		}
	}
	switch v := any(&out).(type) {
	case *jobdb.GoalInfo:
		v.CreatedAt = v.CreatedAt.UTC()
		v.UpdatedAt = v.UpdatedAt.UTC()
		if v.ClosedAt != nil {
			u := v.ClosedAt.UTC()
			v.ClosedAt = &u
		}
		normalizeFailure(v.Failure)
	case *jobdb.GoalCandidateInfo:
		v.AddedAt = v.AddedAt.UTC()
	case *jobdb.DependencyWaitInfo:
		if v.Deadline != nil {
			u := v.Deadline.UTC()
			v.Deadline = &u
		}
		normalizeFailure(v.Failure)
		for i := range v.Dependencies {
			normalizeFailure(v.Dependencies[i].Failure)
		}
	}
	return
}

// PrepareJob reserves stable identities before chapter preparation.
func PrepareJob(ctx context.Context, db DB, req JobPreparation) (PreparedJob, error) {
	if strings.TrimSpace(req.Key.TenantId) == "" || req.Descriptor == "" || (req.RequestID != "" && strings.TrimSpace(req.RequestID) == "") {
		return PreparedJob{}, jobdb.ErrInvalidDependency
	}
	if err := jobdb.ValidateDependencies(req.Dependencies); err != nil {
		return PreparedJob{}, err
	}
	deps := req.Dependencies
	if deps == nil {
		deps = []jobdb.Dependency{}
	}
	raw, err := json.Marshal(deps)
	if err != nil {
		return PreparedJob{}, err
	}
	return goalQuery[PreparedJob](ctx, db, `SELECT pgjobdb.prepare_job($1,$2,$3,$4,$5,$6,$7)`, req.Key.TenantId, nilIfBlank(req.Key.JobId), nilIfBlank(req.RequestID), nilIfBlank(req.PriorJobID), req.ShareGoal, req.Descriptor, string(raw))
}

// GetGoal returns the durable goal outcome.
func GetGoal(ctx context.Context, db DB, key jobdb.GoalKey) (jobdb.GoalInfo, error) {
	if err := key.Validate(); err != nil {
		return jobdb.GoalInfo{}, err
	}
	return goalQuery[jobdb.GoalInfo](ctx, db, `SELECT pgjobdb.goal_info($1,$2)`, key.TenantId, key.GoalID)
}

// AddGoalCandidate attaches a job and resolves the goal if it already succeeded.
func AddGoalCandidate(ctx context.Context, db DB, req jobdb.AddGoalCandidateRequest) (jobdb.GoalCandidateInfo, error) {
	if err := req.GoalKey.Validate(); err != nil {
		return jobdb.GoalCandidateInfo{}, err
	}
	if strings.TrimSpace(req.JobID) == "" {
		return jobdb.GoalCandidateInfo{}, jobdb.ErrInvalidDependency
	}
	return goalQuery[jobdb.GoalCandidateInfo](ctx, db, `SELECT pgjobdb.add_goal_candidate($1,$2,$3)`, req.GoalKey.TenantId, req.GoalKey.GoalID, req.JobID)
}

// FailGoal freezes a failure snapshot without cancelling candidates.
func FailGoal(ctx context.Context, db DB, req jobdb.FailGoalRequest) (jobdb.GoalInfo, error) {
	if err := req.Validate(); err != nil {
		return jobdb.GoalInfo{}, err
	}
	return goalQuery[jobdb.GoalInfo](ctx, db, `SELECT pgjobdb.fail_goal($1,$2,$3)`, req.GoalKey.TenantId, req.GoalKey.GoalID, req.Message)
}

// GetDependencyWait reads retained bindings and projects expired deadlines without writes.
func GetDependencyWait(ctx context.Context, db DB, key jobdb.DependencyWaitKey) (jobdb.DependencyWaitInfo, error) {
	if err := key.JobKey.Validate(); err != nil {
		return jobdb.DependencyWaitInfo{}, fmt.Errorf("%w: %v", jobdb.ErrInvalidDependency, err)
	}
	if key.WaitID == "" {
		return jobdb.DependencyWaitInfo{}, jobdb.ErrInvalidDependency
	}
	return goalQuery[jobdb.DependencyWaitInfo](ctx, db, `SELECT pgjobdb.dependency_wait_info($1,$2,$3)`, key.JobKey.TenantId, key.JobKey.JobId, key.WaitID)
}

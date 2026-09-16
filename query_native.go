package pgjobdb

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
)

var ErrJobNotFound = errors.New("pgjobdb: job not found")

func GetJob(ctx context.Context, db DB, tenant TenantID, job JobID) (*JobDetail, error) {
	if err := validateScheduleDB(ctx, db); err != nil {
		return nil, err
	}
	if tenant == "" || job == "" {
		return nil, fmt.Errorf("pgjobdb: tenant id and job id are required")
	}
	var raw []byte
	err := db.QueryRowContext(ctx, `SELECT * FROM pgjobdb.get_native_job($1, $2)`,
		string(tenant), string(job)).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrJobNotFound
	}
	if err != nil {
		return nil, err
	}
	var detail JobDetail
	if err := json.Unmarshal(raw, &detail); err != nil {
		return nil, fmt.Errorf("pgjobdb: decode native job: %w", err)
	}
	return &detail, nil
}

func GetJobStatus(ctx context.Context, db DB, tenant TenantID, job JobID) (*JobStatusInfo, error) {
	if err := validateScheduleDB(ctx, db); err != nil {
		return nil, err
	}
	if tenant == "" || job == "" {
		return nil, fmt.Errorf("pgjobdb: tenant id and job id are required")
	}
	var raw []byte
	err := db.QueryRowContext(ctx, `SELECT * FROM pgjobdb.get_native_job_status($1, $2)`,
		string(tenant), string(job)).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrJobNotFound
	}
	if err != nil {
		return nil, err
	}
	var status JobStatusInfo
	if err := json.Unmarshal(raw, &status); err != nil {
		return nil, fmt.Errorf("pgjobdb: decode native job status: %w", err)
	}
	return &status, nil
}

BEGIN;

CREATE SCHEMA IF NOT EXISTS pgjobdb;

SET LOCAL search_path = pgjobdb, public;

DO $$ BEGIN
 IF to_regclass('pgjobdb.installation') IS NOT NULL THEN
  IF (SELECT format_version FROM pgjobdb.installation WHERE name='pgjobdb') IS DISTINCT FROM 7 THEN
   RAISE EXCEPTION 'unsupported pgjobdb format; create a fresh database';
  END IF;
 END IF;
END $$;

CREATE TABLE IF NOT EXISTS pgjobdb.installation (
    name TEXT PRIMARY KEY CHECK (name = 'pgjobdb'),
    format_version INTEGER NOT NULL CHECK (format_version = 7),
    installed_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

INSERT INTO pgjobdb.installation (name, format_version)
VALUES ('pgjobdb', 7)
ON CONFLICT (name) DO NOTHING;

CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE SEQUENCE IF NOT EXISTS pgjobdb.jobs_trace_id_seq;

-- Goal state shares the scheduler transaction domain. Format 7 is a fresh-data cutover.
CREATE TABLE IF NOT EXISTS pgjobdb.goals (
 tenant_id TEXT NOT NULL, goal_id TEXT NOT NULL,
 state TEXT NOT NULL DEFAULT 'OPEN' CHECK(state IN ('OPEN','SUCCEEDED','FAILED')),
 revision BIGINT NOT NULL DEFAULT 1 CHECK(revision IN (1,2)),
 successful_job_id TEXT, failure JSONB,
 created_at TIMESTAMPTZ NOT NULL DEFAULT statement_timestamp(),
 updated_at TIMESTAMPTZ NOT NULL DEFAULT statement_timestamp(), closed_at TIMESTAMPTZ,
 PRIMARY KEY(tenant_id,goal_id),
 CHECK ((state='OPEN' AND revision=1 AND closed_at IS NULL AND successful_job_id IS NULL AND failure IS NULL)
 OR (state='SUCCEEDED' AND revision=2 AND closed_at IS NOT NULL AND successful_job_id IS NOT NULL AND failure IS NULL)
 OR (state='FAILED' AND revision=2 AND closed_at IS NOT NULL AND successful_job_id IS NULL AND failure IS NOT NULL))
);
CREATE TABLE IF NOT EXISTS pgjobdb.job_preparations (
 tenant_id TEXT NOT NULL, job_id TEXT NOT NULL, goal_id TEXT NOT NULL,
 request_id TEXT, prior_job_id TEXT, share_goal BOOLEAN NOT NULL,
 descriptor TEXT NOT NULL, dependencies JSONB NOT NULL,
 published BOOLEAN NOT NULL DEFAULT FALSE,
 PRIMARY KEY(tenant_id,job_id)
);
CREATE UNIQUE INDEX IF NOT EXISTS preparation_request_idx ON pgjobdb.job_preparations(tenant_id,request_id) WHERE request_id IS NOT NULL;
CREATE TABLE IF NOT EXISTS pgjobdb.goal_candidates (
 tenant_id TEXT NOT NULL, goal_id TEXT NOT NULL, job_id TEXT NOT NULL,
 added_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(tenant_id,goal_id,job_id),
 FOREIGN KEY(tenant_id,goal_id) REFERENCES pgjobdb.goals(tenant_id,goal_id)
);
CREATE INDEX IF NOT EXISTS candidate_job_idx ON pgjobdb.goal_candidates(tenant_id,job_id,goal_id);
CREATE TABLE IF NOT EXISTS pgjobdb.dependency_waits (
 tenant_id TEXT NOT NULL, job_id TEXT NOT NULL, wait_id TEXT NOT NULL,
 descriptor JSONB NOT NULL, deadline TIMESTAMPTZ,
 state TEXT NOT NULL CHECK(state IN ('PENDING','SATISFIED','FAILED','TIMED_OUT','CANCELLED')),
 outcome JSONB NOT NULL, PRIMARY KEY(tenant_id,job_id,wait_id)
);
CREATE INDEX IF NOT EXISTS wait_deadline_idx ON pgjobdb.dependency_waits(tenant_id,deadline) WHERE state='PENDING';
CREATE INDEX IF NOT EXISTS wait_due_idx ON pgjobdb.dependency_waits(deadline,tenant_id) WHERE state='PENDING' AND deadline IS NOT NULL;
CREATE TABLE IF NOT EXISTS pgjobdb.dependency_targets (
 tenant_id TEXT NOT NULL, job_id TEXT NOT NULL, wait_id TEXT NOT NULL,
 target_kind TEXT NOT NULL CHECK(target_kind IN ('job','goal')), target_id TEXT NOT NULL,
 PRIMARY KEY(tenant_id,job_id,wait_id,target_kind,target_id),
 FOREIGN KEY(tenant_id,job_id,wait_id) REFERENCES pgjobdb.dependency_waits(tenant_id,job_id,wait_id)
);
CREATE INDEX IF NOT EXISTS dependency_target_idx ON pgjobdb.dependency_targets(tenant_id,target_kind,target_id);
CREATE OR REPLACE FUNCTION pgjobdb._goal_lock(p_tenant TEXT) RETURNS VOID LANGUAGE sql AS $$
 SELECT pg_advisory_xact_lock(hashtextextended('pgjobdb.goals:' || p_tenant, 0));
$$;

CREATE TABLE IF NOT EXISTS pgjobdb.jobs (
    goal_id TEXT NOT NULL,
    dependency_wait_id TEXT,
    tenant_id TEXT NOT NULL,
    job_id TEXT NOT NULL,
    alternate_after_seconds DOUBLE PRECISION,
    alternate_at TIMESTAMPTZ,
    wait_for TEXT[] NOT NULL DEFAULT '{}'::TEXT[],
    payload JSONB NOT NULL DEFAULT '{}'::JSONB,
    metadata JSONB NOT NULL DEFAULT '{}'::JSONB,
    available_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    expires_at TIMESTAMPTZ NOT NULL DEFAULT 'infinity',
    lease_id TEXT,
    lease_expires_at TIMESTAMPTZ NOT NULL DEFAULT '-infinity',
    lease_expiration_count BIGINT NOT NULL DEFAULT 0,
    consecutive_expirations BIGINT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    cancel_requested BOOLEAN NOT NULL DEFAULT FALSE,
    cancel_requested_by TEXT,
    cancel_requested_at TIMESTAMPTZ,
    PRIMARY KEY (tenant_id, job_id),
    CONSTRAINT jobs_payload_is_object CHECK (jsonb_typeof(payload) = 'object'),
    CONSTRAINT jobs_payload_size_limit CHECK (pg_column_size(payload) <= 512),
    CONSTRAINT jobs_metadata_is_object CHECK (jsonb_typeof(metadata) = 'object'),
    CONSTRAINT jobs_metadata_size_limit CHECK (pg_column_size(metadata) <= 8192),
    CONSTRAINT jobs_alternate_after_seconds_nonnegative CHECK (alternate_after_seconds IS NULL OR (alternate_after_seconds >= 0 AND alternate_after_seconds < 'Infinity'::DOUBLE PRECISION)),
    route_job_type TEXT,
    work_kind TEXT,
    task_type TEXT,
    resume_job_type TEXT,
    task_input_ordinal BIGINT,
    task_output_ordinal BIGINT,
    task_input_hash TEXT,
    alternate_job_type TEXT,
    alternate_task_type TEXT,
    lease_worker_id TEXT,
    -- Execution route captured with lease_id, including a promoted alternate.
    lease_route_job_type TEXT,
    lease_work_kind TEXT,
    lease_task_type TEXT,
    CONSTRAINT jobs_lease_route_valid CHECK (
        (lease_route_job_type IS NULL AND lease_work_kind IS NULL AND lease_task_type IS NULL)
        OR (lease_id IS NOT NULL AND lease_route_job_type IS NOT NULL
            AND lease_route_job_type <> '' AND lease_work_kind IS NOT NULL
            AND (lease_work_kind = 'JOB'
                OR (lease_work_kind = 'TASK' AND lease_task_type IS NOT NULL AND lease_task_type <> '')))
    ),
    CONSTRAINT jobs_native_route_valid CHECK (
                (route_job_type IS NULL AND work_kind IS NULL
                    AND task_type IS NULL AND resume_job_type IS NULL
                    AND task_input_ordinal IS NULL AND task_output_ordinal IS NULL
                    AND task_input_hash IS NULL)
                OR (route_job_type IS NOT NULL AND route_job_type <> ''
                    AND work_kind IS NOT NULL AND (
                    (work_kind = 'JOB' AND task_type IS NULL
                        AND resume_job_type IS NULL AND task_input_ordinal IS NULL
                        AND task_output_ordinal IS NULL AND task_input_hash IS NULL)
                    OR (work_kind = 'TASK' AND task_type IS NOT NULL
                        AND task_type <> '' AND resume_job_type IS NOT NULL
                        AND resume_job_type <> '' AND task_input_ordinal IS NOT NULL
                        AND task_input_ordinal >= 0 AND task_output_ordinal IS NOT NULL
                        AND task_output_ordinal >= 0 AND task_input_hash IS NOT NULL
                        AND task_input_hash <> '')
                ))
            )
);

-- Rescheduled dependency waits may wake on completion OR a deadline. Keep
-- ordinary submission availability and delay-only reschedules as not-before times.
ALTER TABLE pgjobdb.jobs ADD COLUMN IF NOT EXISTS wait_until_is_deadline BOOLEAN NOT NULL DEFAULT FALSE;

CREATE TABLE IF NOT EXISTS pgjobdb.jobs_archive (
    goal_id TEXT NOT NULL,
    archived_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    tenant_id TEXT NOT NULL,
    job_id TEXT NOT NULL,
    alternate_after_seconds DOUBLE PRECISION,
    alternate_at TIMESTAMPTZ,
    wait_for TEXT[] NOT NULL DEFAULT '{}'::TEXT[],
    payload JSONB NOT NULL DEFAULT '{}'::JSONB,
    metadata JSONB NOT NULL DEFAULT '{}'::JSONB,
    created_at TIMESTAMPTZ NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL DEFAULT 'infinity',
    lease_id TEXT,
    lease_expiration_count BIGINT NOT NULL DEFAULT 0,
    consecutive_expirations BIGINT NOT NULL DEFAULT 0,
    cancel_requested BOOLEAN NOT NULL DEFAULT FALSE,
    cancel_requested_by TEXT,
    cancel_requested_at TIMESTAMPTZ,
    completion_status TEXT NOT NULL DEFAULT 'succeeded',
    completion_detail TEXT,
    PRIMARY KEY (tenant_id, job_id),
    CONSTRAINT jobs_archive_payload_is_object CHECK (jsonb_typeof(payload) = 'object'),
    CONSTRAINT jobs_archive_payload_size_limit CHECK (pg_column_size(payload) <= 512),
    CONSTRAINT jobs_archive_metadata_is_object CHECK (jsonb_typeof(metadata) = 'object'),
    CONSTRAINT jobs_archive_metadata_size_limit CHECK (pg_column_size(metadata) <= 8192),
    CONSTRAINT jobs_archive_alternate_after_seconds_nonnegative CHECK (alternate_after_seconds IS NULL OR (alternate_after_seconds >= 0 AND alternate_after_seconds < 'Infinity'::DOUBLE PRECISION)),
    completion_error_kind TEXT,
    completion_retryable BOOLEAN,
    final_route_job_type TEXT,
    final_work_kind TEXT,
    final_task_type TEXT,
    final_resume_job_type TEXT,
    final_task_input_ordinal BIGINT,
    final_task_output_ordinal BIGINT,
    final_task_input_hash TEXT,
    final_alternate_job_type TEXT,
    final_alternate_task_type TEXT,
    final_alternate_after_seconds DOUBLE PRECISION,
    final_alternate_at TIMESTAMPTZ,
    final_wait_for TEXT[],
    final_available_at TIMESTAMPTZ,
    final_lease_expires_at TIMESTAMPTZ,
    final_lease_worker_id TEXT,
    final_cancel_requested BOOLEAN,
    CONSTRAINT jobs_archive_native_snapshot_valid CHECK (
                (final_route_job_type IS NULL AND final_work_kind IS NULL
                    AND final_task_type IS NULL AND final_resume_job_type IS NULL
                    AND final_task_input_ordinal IS NULL
                    AND final_task_output_ordinal IS NULL
                    AND final_task_input_hash IS NULL AND final_wait_for IS NULL
                    AND final_available_at IS NULL AND final_cancel_requested IS NULL
)
                OR (final_route_job_type IS NOT NULL AND final_route_job_type <> ''
                    AND final_work_kind IS NOT NULL
                    AND final_wait_for IS NOT NULL AND final_available_at IS NOT NULL
                    AND final_cancel_requested IS NOT NULL
                    AND (
                        (final_work_kind = 'JOB' AND final_task_type IS NULL
                            AND final_resume_job_type IS NULL
                            AND final_task_input_ordinal IS NULL
                            AND final_task_output_ordinal IS NULL
                            AND final_task_input_hash IS NULL)
                        OR (final_work_kind = 'TASK' AND final_task_type IS NOT NULL
                            AND final_task_type <> '' AND final_resume_job_type IS NOT NULL
                            AND final_resume_job_type <> ''
                            AND final_task_input_ordinal IS NOT NULL
                            AND final_task_input_ordinal >= 0
                            AND final_task_output_ordinal IS NOT NULL
                            AND final_task_output_ordinal >= 0
                            AND final_task_input_hash IS NOT NULL
                            AND final_task_input_hash <> '')
                    ))
            )
);

CREATE TABLE IF NOT EXISTS pgjobdb.jobs_trace (
    trace_id BIGINT PRIMARY KEY DEFAULT nextval('pgjobdb.jobs_trace_id_seq'),
    tenant_id TEXT NOT NULL,
    job_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    worker_id TEXT NOT NULL,
    event_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    input_data JSONB NOT NULL,
    output_data JSONB
);

-- Performance indexes for multi-tenant operations
CREATE INDEX IF NOT EXISTS idx_jobs_tenant_ready_work
ON pgjobdb.jobs(tenant_id, route_job_type, task_type, created_at)
WHERE NOT cancel_requested;

CREATE INDEX IF NOT EXISTS idx_jobs_tenant_waitfor
ON pgjobdb.jobs(tenant_id, job_id)
INCLUDE (wait_for);

CREATE INDEX IF NOT EXISTS idx_jobs_metadata_gin
ON pgjobdb.jobs USING GIN (metadata);

CREATE INDEX IF NOT EXISTS idx_jobs_tenant_cancelled
ON pgjobdb.jobs(tenant_id, created_at)
WHERE cancel_requested = TRUE;

CREATE INDEX IF NOT EXISTS idx_jobs_archive_metadata_gin
ON pgjobdb.jobs_archive USING GIN (metadata);

CREATE INDEX IF NOT EXISTS idx_trace_tenant_job_event
ON pgjobdb.jobs_trace(tenant_id, job_id, event_at DESC);


CREATE OR REPLACE FUNCTION pgjobdb.crash_concern_threshold()
RETURNS INTEGER
LANGUAGE sql
AS $$
    SELECT 5;
$$;

CREATE OR REPLACE FUNCTION pgjobdb.set_crash_concern_threshold(p_threshold INTEGER)
RETURNS INTEGER
LANGUAGE plpgsql
AS $$
BEGIN
    IF p_threshold IS NULL OR p_threshold <= 0 THEN
        RAISE EXCEPTION 'threshold must be positive';
    END IF;

    EXECUTE format(
        'CREATE OR REPLACE FUNCTION pgjobdb.crash_concern_threshold() RETURNS INTEGER LANGUAGE plpgsql AS %L',
        format('BEGIN RETURN %s; END;', p_threshold)
    );

    RETURN p_threshold;
END;
$$;

-- Shared eligibility for live acquisition and read-only discovery at a supplied time.
CREATE OR REPLACE FUNCTION pgjobdb.job_status_at(j pgjobdb.jobs, p_now TIMESTAMPTZ)
RETURNS TEXT LANGUAGE sql STABLE AS $$
    SELECT CASE
            WHEN j.lease_expires_at > p_now THEN 'ACTIVE'
            WHEN j.cancel_requested THEN 'CANCELLED'
            WHEN EXISTS(SELECT 1 FROM pgjobdb.dependency_waits w WHERE w.tenant_id=j.tenant_id AND w.job_id=j.job_id
                AND w.wait_id=j.dependency_wait_id AND w.state='PENDING' AND (w.deadline IS NULL OR w.deadline>p_now)) THEN 'PENDING_JOBS'
            WHEN NOT j.wait_until_is_deadline AND j.available_at > p_now THEN 'AWAITING_FUTURE'
            WHEN EXISTS (
                SELECT 1
                FROM unnest(j.wait_for) AS dep_job_id
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM pgjobdb.jobs_archive ja
                    WHERE ja.tenant_id = j.tenant_id
                      AND ja.job_id = dep_job_id
                )
            ) AND NOT (j.wait_until_is_deadline AND j.available_at <= p_now) THEN 'PENDING_JOBS'
            WHEN j.consecutive_expirations >= pgjobdb.crash_concern_threshold() THEN 'CRASH_CONCERN'
            WHEN j.expires_at <= p_now THEN 'EXPIRED'
            ELSE 'READY'
        END;
$$;

CREATE OR REPLACE FUNCTION pgjobdb.effective_native_route(j pgjobdb.jobs, p_now TIMESTAMPTZ)
RETURNS TABLE(effective_job_type TEXT, effective_work_kind TEXT, effective_task_type TEXT, alternate_due BOOLEAN)
LANGUAGE sql STABLE AS $$
    SELECT CASE WHEN due THEN j.alternate_job_type ELSE j.route_job_type END,
        CASE WHEN due THEN CASE WHEN j.alternate_task_type IS NULL THEN 'JOB' ELSE 'TASK' END ELSE j.work_kind END,
        CASE WHEN due THEN j.alternate_task_type ELSE j.task_type END, due
    FROM (SELECT COALESCE(j.alternate_job_type IS NOT NULL AND j.alternate_at IS NOT NULL
        AND p_now >= j.alternate_at, FALSE) AS due) alt;
$$;

CREATE OR REPLACE VIEW pgjobdb.jobs_with_status AS
WITH status_calc AS (
    SELECT
        j.tenant_id,
        j.job_id,
        j.alternate_after_seconds,
        j.alternate_at,
        j.wait_for,
        j.payload,
        j.metadata,
        j.available_at,
        j.expires_at,
        j.lease_id,
        j.lease_expires_at,
        j.lease_expiration_count,
        j.consecutive_expirations,
        j.created_at,
        j.cancel_requested,
        j.cancel_requested_by,
        j.cancel_requested_at,
        j.route_job_type,
        j.work_kind,
        j.task_type,
        j.resume_job_type,
        j.task_input_ordinal,
        j.task_output_ordinal,
        j.task_input_hash,
        j.alternate_job_type,
        j.alternate_task_type,
        j.lease_worker_id,
        j.lease_route_job_type,
        j.lease_work_kind,
        j.lease_task_type,
        pgjobdb.job_status_at(j, clock_timestamp()) AS status
    FROM pgjobdb.jobs j
),
ready_calc AS (
    SELECT
        sc.*,
        CASE
            WHEN sc.status = 'READY' THEN GREATEST(
                sc.available_at,
                COALESCE(NULLIF(sc.lease_expires_at, '-infinity'), sc.created_at),
                CASE WHEN COALESCE(array_length(sc.wait_for, 1), 0) = 0 THEN sc.created_at ELSE '-infinity' END
            )
        END AS ready_since
    FROM status_calc sc
)
SELECT rc.*
FROM ready_calc rc;

CREATE OR REPLACE FUNCTION pgjobdb.is_trace_enabled()
RETURNS BOOLEAN
LANGUAGE sql
AS $$
    SELECT TRUE;
$$;

CREATE OR REPLACE FUNCTION pgjobdb.set_trace(enabled BOOLEAN)
RETURNS BOOLEAN
LANGUAGE plpgsql
AS $$
BEGIN
    IF enabled IS NULL THEN
        RAISE EXCEPTION 'enabled flag cannot be NULL';
    END IF;

    EXECUTE format(
        'CREATE OR REPLACE FUNCTION pgjobdb.is_trace_enabled() RETURNS BOOLEAN LANGUAGE plpgsql AS %L',
        CASE WHEN enabled THEN 'BEGIN RETURN TRUE; END;' ELSE 'BEGIN RETURN FALSE; END;' END
    );

    RETURN enabled;
END;
$$;

CREATE OR REPLACE FUNCTION pgjobdb.is_notify_enabled()
RETURNS BOOLEAN
LANGUAGE sql
AS $$
    SELECT FALSE;
$$;

CREATE OR REPLACE FUNCTION pgjobdb.set_notify(enabled BOOLEAN)
RETURNS BOOLEAN
LANGUAGE plpgsql
AS $$
BEGIN
    IF enabled IS NULL THEN
        RAISE EXCEPTION 'enabled flag cannot be NULL';
    END IF;

    EXECUTE format(
        'CREATE OR REPLACE FUNCTION pgjobdb.is_notify_enabled() RETURNS BOOLEAN LANGUAGE plpgsql AS %L',
        CASE WHEN enabled THEN 'BEGIN RETURN TRUE; END;' ELSE 'BEGIN RETURN FALSE; END;' END
    );

    RETURN enabled;
END;
$$;

CREATE OR REPLACE FUNCTION pgjobdb._lock_job_for_status(
    p_tenant_id TEXT,
    p_job_id TEXT,
    p_status TEXT,
    p_expected_lease_id TEXT DEFAULT NULL,
    p_missing_message TEXT DEFAULT NULL
)
RETURNS pgjobdb.jobs_with_status
LANGUAGE plpgsql
AS $$
DECLARE
    v_row pgjobdb.jobs_with_status%ROWTYPE;
BEGIN
    IF p_tenant_id IS NULL THEN
        RAISE EXCEPTION 'tenant_id cannot be NULL';
    END IF;
    IF p_job_id IS NULL THEN
        RAISE EXCEPTION 'job_id cannot be NULL';
    END IF;

    SELECT *
    INTO v_row
    FROM pgjobdb.jobs_with_status
    WHERE tenant_id = p_tenant_id
      AND job_id = p_job_id
      AND (
          status = p_status
          OR (p_status = 'READY' AND status IN ('CRASH_CONCERN', 'EXPIRED'))
      )
      AND (p_expected_lease_id IS NULL OR lease_id = p_expected_lease_id)
    FOR UPDATE;

    IF NOT FOUND THEN
        RAISE EXCEPTION '%', COALESCE(p_missing_message, format('job %s/%s is not in a valid status (%s requested)', p_tenant_id, p_job_id, p_status));
    END IF;

    RETURN v_row;
END;
$$;

-- A fixed channel avoids encoding identifiers in PostgreSQL channel names.
CREATE OR REPLACE FUNCTION pgjobdb._notify_work(
    p_tenant_id TEXT, p_job_id TEXT, p_job_type TEXT, p_task_type TEXT
)
RETURNS VOID LANGUAGE plpgsql AS $$
BEGIN
    IF pgjobdb.is_notify_enabled() THEN
        PERFORM pg_notify('pgjobdb.work', json_build_object(
            'tenantId', p_tenant_id, 'jobId', p_job_id,
            'route', json_build_object('jobType', p_job_type, 'taskType', COALESCE(p_task_type, ''))
        )::TEXT);
    END IF;
END;
$$;

CREATE OR REPLACE FUNCTION pgjobdb._emit_trace_event(
    p_tenant_id TEXT,
    p_job_id TEXT,
    p_event_type TEXT,
    p_worker_id TEXT,
    p_input JSONB,
    p_output JSONB DEFAULT NULL
)
RETURNS VOID
LANGUAGE plpgsql
AS $$
BEGIN
    IF NOT pgjobdb.is_trace_enabled() THEN
        RETURN;
    END IF;

    INSERT INTO pgjobdb.jobs_trace (tenant_id, job_id, event_type, worker_id, input_data, output_data)
    VALUES (
        p_tenant_id,
        p_job_id,
        p_event_type,
        p_worker_id,
        COALESCE(p_input, '{}'::JSONB),
        p_output
    );
END;
$$;

CREATE OR REPLACE FUNCTION pgjobdb._archive_and_delete_job(
    p_locked_job pgjobdb.jobs_with_status,
    p_completion_status TEXT,
    p_completion_detail TEXT
)
RETURNS pgjobdb.jobs_archive
LANGUAGE plpgsql
AS $$
DECLARE
    v_archive pgjobdb.jobs_archive%ROWTYPE;
BEGIN
    INSERT INTO pgjobdb.jobs_archive (
        tenant_id,
        job_id,
        alternate_after_seconds,
        alternate_at,
        wait_for,
        payload,
        metadata,
        created_at,
        expires_at,
        lease_id,
        lease_expiration_count,
        consecutive_expirations,
        cancel_requested,
        cancel_requested_by,
        cancel_requested_at,
        completion_status,
        completion_detail
    )
    VALUES (
        p_locked_job.tenant_id,
        p_locked_job.job_id,
        p_locked_job.alternate_after_seconds,
        p_locked_job.alternate_at,
        p_locked_job.wait_for,
        p_locked_job.payload,
        p_locked_job.metadata,
        p_locked_job.created_at,
        p_locked_job.expires_at,
        p_locked_job.lease_id,
        p_locked_job.lease_expiration_count,
        p_locked_job.consecutive_expirations,
        p_locked_job.cancel_requested,
        p_locked_job.cancel_requested_by,
        p_locked_job.cancel_requested_at,
        p_completion_status,
        p_completion_detail
    )
    RETURNING * INTO v_archive;

    DELETE FROM pgjobdb.jobs
    WHERE tenant_id = p_locked_job.tenant_id
      AND job_id = p_locked_job.job_id;

    RETURN v_archive;
END;
$$;

CREATE OR REPLACE FUNCTION pgjobdb._update_waiters_for_completion_bulk(
    p_tenant_id TEXT,
    p_completed_jobs TEXT[]
)
RETURNS TABLE(job_id TEXT, route_job_type TEXT, task_type TEXT, became_unblocked BOOLEAN)
LANGUAGE plpgsql
AS $$
DECLARE
    v_row RECORD;
    v_now TIMESTAMPTZ := clock_timestamp();
BEGIN
    IF p_completed_jobs IS NULL OR array_length(p_completed_jobs, 1) IS NULL OR array_length(p_completed_jobs, 1) = 0 THEN
        RETURN;
    END IF;

    FOR v_row IN
        WITH targets AS (
            SELECT j.*, ARRAY(
                SELECT pending.val
                FROM unnest(j.wait_for) WITH ORDINALITY AS pending(val, ord)
                WHERE pending.val IS NOT NULL
                  AND NOT (pending.val = ANY(p_completed_jobs))
                ORDER BY pending.ord
            ) AS remaining_wait_for
            FROM pgjobdb.jobs j
            WHERE j.tenant_id = p_tenant_id
              AND EXISTS (
                  SELECT 1
                  FROM unnest(j.wait_for) pending(job_id)
                  WHERE pending.job_id = ANY(p_completed_jobs)
              )
            FOR UPDATE
        ),
        updated AS (
            UPDATE pgjobdb.jobs j
            SET wait_for = t.remaining_wait_for,
                available_at = CASE WHEN j.wait_until_is_deadline
                    AND cardinality(t.remaining_wait_for) = 0 THEN v_now ELSE j.available_at END,
                wait_until_is_deadline = j.wait_until_is_deadline AND cardinality(t.remaining_wait_for) > 0
            FROM targets t
            WHERE j.tenant_id = t.tenant_id
              AND j.job_id = t.job_id
            RETURNING
                j.job_id,
                j.route_job_type, j.task_type,
                j.available_at,
                (COALESCE(array_length(j.wait_for, 1), 0) = 0) AS now_unblocked,
                j.cancel_requested,
                j.expires_at
        )
        SELECT *
        FROM updated
    LOOP
        IF v_row.now_unblocked
           AND v_row.available_at <= v_now
           AND NOT v_row.cancel_requested
           AND v_row.expires_at > v_now THEN
            PERFORM pgjobdb._notify_work(p_tenant_id, v_row.job_id, v_row.route_job_type, v_row.task_type);
        END IF;

        job_id := v_row.job_id;
        route_job_type := v_row.route_job_type;
        task_type := v_row.task_type;
        became_unblocked := v_row.now_unblocked;
        RETURN NEXT;
    END LOOP;
END;
$$;

CREATE OR REPLACE FUNCTION pgjobdb._update_waiters_for_completion(
    p_tenant_id TEXT,
    p_completed_job_id TEXT
)
RETURNS TABLE(job_id TEXT, route_job_type TEXT, task_type TEXT, became_unblocked BOOLEAN)
LANGUAGE plpgsql
AS $$
BEGIN
    IF p_completed_job_id IS NULL THEN
        RETURN;
    END IF;

    RETURN QUERY
    SELECT *
    FROM pgjobdb._update_waiters_for_completion_bulk(p_tenant_id, ARRAY[p_completed_job_id]);
END;
$$;

CREATE OR REPLACE FUNCTION pgjobdb._complete_locked_job(
    p_locked_job pgjobdb.jobs_with_status,
    p_worker_id TEXT,
    p_completion_status TEXT,
    p_completion_detail TEXT,
    p_trace_context JSONB DEFAULT '{}'::JSONB
)
RETURNS BOOLEAN
LANGUAGE plpgsql
AS $$
DECLARE
    v_archive pgjobdb.jobs_archive%ROWTYPE;
BEGIN
    p_locked_job.consecutive_expirations := 0;
    v_archive := pgjobdb._archive_and_delete_job(
        p_locked_job,
        COALESCE(p_completion_status, 'succeeded'),
        p_completion_detail
    );

    PERFORM pgjobdb._update_waiters_for_completion(p_locked_job.tenant_id, p_locked_job.job_id);

    PERFORM pgjobdb._emit_trace_event(
        p_locked_job.tenant_id,
        p_locked_job.job_id,
        'job_finished',
        p_worker_id,
        jsonb_build_object(
            'tenant_id', p_locked_job.tenant_id,
            'job_id', p_locked_job.job_id,
            'worker_id', p_worker_id
        ) || COALESCE(p_trace_context, '{}'::JSONB),
        jsonb_build_object('archived_row', to_jsonb(v_archive) - 'payload' - 'metadata')
    );

    RETURN TRUE;
END;
$$;

CREATE OR REPLACE FUNCTION pgjobdb.normalize_wait_for(p_tenant_id TEXT, p_wait_for TEXT[])
RETURNS TEXT[]
LANGUAGE plpgsql
AS $$
DECLARE
    v_input TEXT[] := COALESCE(p_wait_for, ARRAY[]::TEXT[]);
    v_clean TEXT[];
    v_missing TEXT[];
BEGIN
    IF array_length(v_input, 1) IS NULL THEN
        RETURN ARRAY[]::TEXT[];
    END IF;

    WITH ordered AS (
        SELECT value AS job_id, ord
        FROM unnest(v_input) WITH ORDINALITY AS w(value, ord)
        WHERE value IS NOT NULL
    )
    SELECT
        COALESCE(array_agg(o.job_id ORDER BY ord)
                 FILTER (WHERE j.job_id IS NOT NULL), ARRAY[]::TEXT[]),
        array_agg(o.job_id ORDER BY ord)
            FILTER (WHERE j.job_id IS NULL AND ja.job_id IS NULL)
    INTO v_clean, v_missing
    FROM ordered o
    LEFT JOIN pgjobdb.jobs j ON j.tenant_id = p_tenant_id AND j.job_id = o.job_id
    LEFT JOIN pgjobdb.jobs_archive ja ON ja.tenant_id = p_tenant_id AND ja.job_id = o.job_id;

    IF v_missing IS NOT NULL THEN
        RAISE EXCEPTION 'wait_for references unknown jobs in tenant %: %', p_tenant_id, v_missing;
    END IF;

    RETURN v_clean;
END;
$$;

CREATE OR REPLACE FUNCTION pgjobdb._cancel_job(
    p_tenant_id TEXT,
    p_job_id TEXT,
    p_worker_id TEXT,
    p_reason TEXT DEFAULT NULL
)
RETURNS pgjobdb.jobs
LANGUAGE plpgsql
AS $$
DECLARE
    v_job pgjobdb.jobs%ROWTYPE;
    v_now TIMESTAMPTZ := clock_timestamp();
BEGIN
    PERFORM pgjobdb._goal_lock(p_tenant_id);
    IF p_tenant_id IS NULL THEN
        RAISE EXCEPTION 'tenant_id cannot be NULL';
    END IF;
    IF p_job_id IS NULL THEN
        RAISE EXCEPTION 'job_id cannot be NULL';
    END IF;
    IF p_worker_id IS NULL THEN
        RAISE EXCEPTION 'worker_id cannot be NULL';
    END IF;

    SELECT *
    INTO v_job
    FROM pgjobdb.jobs
    WHERE tenant_id = p_tenant_id
      AND job_id = p_job_id
    FOR UPDATE;

    IF NOT FOUND THEN
        IF EXISTS (SELECT 1 FROM pgjobdb.jobs_archive WHERE tenant_id = p_tenant_id AND job_id = p_job_id) THEN
            RAISE EXCEPTION 'job_id %/% has already completed and cannot be cancelled', p_tenant_id, p_job_id;
        END IF;
        RAISE EXCEPTION 'job_id %/% does not exist', p_tenant_id, p_job_id;
    END IF;

    IF v_job.cancel_requested THEN
        PERFORM pgjobdb._emit_trace_event(
            p_tenant_id,
            p_job_id,
            'job_cancel_requested',
            p_worker_id,
            jsonb_build_object(
                'tenant_id', p_tenant_id,
                'job_id', p_job_id,
                'worker_id', p_worker_id,
                'reason', p_reason,
                'already_cancelled', TRUE,
                'was_active', v_job.lease_expires_at > v_now
            )
        );
        RETURN v_job;
    END IF;

    UPDATE pgjobdb.jobs
    SET cancel_requested = TRUE,
        cancel_requested_by = p_worker_id,
        cancel_requested_at = v_now
    WHERE tenant_id = p_tenant_id
      AND job_id = p_job_id
    RETURNING * INTO v_job;

    PERFORM pgjobdb._emit_trace_event(
        p_tenant_id,
        p_job_id,
        'job_cancel_requested',
        p_worker_id,
        jsonb_build_object(
            'tenant_id', p_tenant_id,
            'job_id', p_job_id,
            'worker_id', p_worker_id,
            'reason', p_reason,
            'was_active', v_job.lease_expires_at > v_now
        )
    );

    RETURN v_job;
END;
$$;

CREATE OR REPLACE FUNCTION pgjobdb._extend_lease(
    p_tenant_id TEXT,
    p_job_id TEXT,
    p_lease_id TEXT,
    p_worker_id TEXT,
    p_additional_seconds INTEGER DEFAULT 60
)
RETURNS TIMESTAMPTZ
LANGUAGE plpgsql
AS $$
DECLARE
    v_job pgjobdb.jobs%ROWTYPE;
    v_new TIMESTAMPTZ;
BEGIN
    IF p_additional_seconds IS NULL OR p_additional_seconds <= 0 THEN
        RAISE EXCEPTION 'additional_seconds must be positive';
    END IF;

    -- Lock the base row, not the computed status view. Authority and expiry
    -- must be checked after any lock wait, immediately before the update.
    SELECT * INTO v_job FROM pgjobdb.jobs
    WHERE tenant_id = p_tenant_id AND job_id = p_job_id FOR UPDATE;
    IF NOT FOUND OR v_job.lease_id IS DISTINCT FROM p_lease_id THEN
        RAISE EXCEPTION 'native lease not found';
    END IF;
    IF v_job.lease_worker_id IS DISTINCT FROM p_worker_id THEN
        RAISE EXCEPTION 'native lease owner mismatch';
    END IF;
    IF v_job.lease_expires_at <= clock_timestamp() THEN
        RAISE EXCEPTION 'native lease not active';
    END IF;
    IF v_job.cancel_requested THEN
        RAISE EXCEPTION 'job %s/%s is cancelled and cannot extend the lease', p_tenant_id, p_job_id;
    END IF;

    v_new := clock_timestamp() + make_interval(secs => p_additional_seconds);

    UPDATE pgjobdb.jobs
    SET lease_expires_at = v_new
    WHERE tenant_id = v_job.tenant_id
      AND job_id = v_job.job_id
      AND lease_id = v_job.lease_id;

    PERFORM pgjobdb._emit_trace_event(
        p_tenant_id,
        p_job_id,
        'lease_extended',
        p_worker_id,
        jsonb_build_object(
            'tenant_id', p_tenant_id,
            'job_id', p_job_id,
            'lease_id', p_lease_id,
            'additional_seconds', p_additional_seconds
        ),
        jsonb_build_object(
            'previous_expires_at', v_job.lease_expires_at,
            'new_expires_at', v_new
        )
    );

    RETURN v_new;
END;
$$;

CREATE OR REPLACE FUNCTION pgjobdb.clear_crash_concern(
    p_tenant_id TEXT,
    p_job_id TEXT,
    p_worker_id TEXT,
    p_reason TEXT DEFAULT NULL
)
RETURNS BOOLEAN
LANGUAGE plpgsql
AS $$
DECLARE
    v_job pgjobdb.jobs%ROWTYPE;
    v_previous_consecutive BIGINT;
BEGIN
    IF p_tenant_id IS NULL THEN
        RAISE EXCEPTION 'tenant_id cannot be NULL';
    END IF;
    IF p_job_id IS NULL THEN
        RAISE EXCEPTION 'job_id cannot be NULL';
    END IF;
    IF p_worker_id IS NULL THEN
        RAISE EXCEPTION 'worker_id cannot be NULL';
    END IF;

    SELECT *
    INTO v_job
    FROM pgjobdb.jobs
    WHERE tenant_id = p_tenant_id
      AND job_id = p_job_id
    FOR UPDATE;

    IF NOT FOUND THEN
        IF EXISTS (SELECT 1 FROM pgjobdb.jobs_archive WHERE tenant_id = p_tenant_id AND job_id = p_job_id) THEN
            RAISE EXCEPTION 'job_id %/% has already been archived and cannot clear crash concern', p_tenant_id, p_job_id;
        END IF;
        RAISE EXCEPTION 'job_id %/% does not exist', p_tenant_id, p_job_id;
    END IF;

    IF v_job.cancel_requested THEN
        RAISE EXCEPTION 'job %/%  is cancelled and cannot clear crash concern', p_tenant_id, p_job_id;
    END IF;

    v_previous_consecutive := v_job.consecutive_expirations;

    UPDATE pgjobdb.jobs
    SET consecutive_expirations = 0
    WHERE tenant_id = v_job.tenant_id
      AND job_id = v_job.job_id;

    PERFORM pgjobdb._emit_trace_event(
        p_tenant_id,
        p_job_id,
        'crash_concern_cleared',
        p_worker_id,
        jsonb_build_object(
            'tenant_id', p_tenant_id,
            'job_id', p_job_id,
            'worker_id', p_worker_id,
            'previous_consecutive_expirations', v_previous_consecutive,
            'reason', p_reason
        ),
        jsonb_build_object('consecutive_expirations', 0)
    );

    RETURN TRUE;
END;
$$;

CREATE OR REPLACE FUNCTION pgjobdb.archive_cancelled_jobs(
    p_worker_id TEXT,
    p_tenant_ids TEXT[] DEFAULT NULL,
    p_limit INTEGER DEFAULT 100
)
RETURNS INTEGER
LANGUAGE plpgsql
AS $$
DECLARE
    v_archived_count INTEGER := 0;
    v_trace_enabled BOOLEAN := pgjobdb.is_trace_enabled();
    v_effective_limit INTEGER := COALESCE(p_limit, 0);
    v_tenant_id TEXT;
    v_job_ids TEXT[];
BEGIN
    FOR v_tenant_id IN SELECT DISTINCT tenant_id FROM pgjobdb.jobs WHERE cancel_requested
        AND (p_tenant_ids IS NULL OR cardinality(p_tenant_ids)=0 OR tenant_id=ANY(p_tenant_ids)) ORDER BY tenant_id LOOP
        PERFORM pgjobdb._goal_lock(v_tenant_id);
    END LOOP;
    IF p_worker_id IS NULL THEN
        RAISE EXCEPTION 'worker_id cannot be NULL';
    END IF;
    IF v_effective_limit <= 0 THEN
        RAISE EXCEPTION 'limit must be positive';
    END IF;

    WITH candidates AS (
        SELECT tenant_id, job_id
        FROM pgjobdb.jobs
        WHERE cancel_requested
          AND lease_expires_at <= clock_timestamp()
          AND (p_tenant_ids IS NULL OR array_length(p_tenant_ids, 1) IS NULL OR tenant_id = ANY(p_tenant_ids))
        ORDER BY created_at
        LIMIT v_effective_limit
        FOR UPDATE SKIP LOCKED
    ),
    archived AS (
        INSERT INTO pgjobdb.jobs_archive (
            tenant_id,
            job_id,
            alternate_after_seconds,
            alternate_at,
            wait_for,
            payload,
            metadata,
            created_at,
            expires_at,
            lease_id,
            lease_expiration_count,
            consecutive_expirations,
            cancel_requested,
            cancel_requested_by,
            cancel_requested_at,
            completion_status,
            completion_detail
        )
        SELECT
            j.tenant_id,
            j.job_id,
            j.alternate_after_seconds,
            j.alternate_at,
            j.wait_for,
            j.payload,
            j.metadata,
            j.created_at,
            j.expires_at,
            j.lease_id,
            j.lease_expiration_count,
            j.consecutive_expirations,
            j.cancel_requested,
            j.cancel_requested_by,
            j.cancel_requested_at,
            'cancelled',
            NULL
        FROM pgjobdb.jobs j
        INNER JOIN candidates c ON j.tenant_id = c.tenant_id AND j.job_id = c.job_id
        RETURNING *
    ),
    deleted AS (
        DELETE FROM pgjobdb.jobs j
        USING archived a
        WHERE j.tenant_id = a.tenant_id
          AND j.job_id = a.job_id
        RETURNING j.tenant_id, j.job_id
    ),
    per_job_trace AS (
        INSERT INTO pgjobdb.jobs_trace (tenant_id, job_id, event_type, worker_id, input_data, output_data)
        SELECT
            a.tenant_id,
            a.job_id,
            'job_cancel_archived',
            p_worker_id,
            jsonb_build_object(
                'tenant_id', a.tenant_id,
                'job_id', a.job_id,
                'worker_id', p_worker_id,
                'cancel_requested_at', a.cancel_requested_at
            ),
            jsonb_build_object('archived_row', to_jsonb(a) - 'payload' - 'metadata')
        FROM archived a
        WHERE v_trace_enabled
    ),
    tenant_groups AS (
        SELECT d.tenant_id, array_agg(d.job_id) AS job_ids
        FROM deleted d
        GROUP BY d.tenant_id
    )
    SELECT COUNT(*)
    INTO v_archived_count
    FROM deleted;

    IF v_archived_count = 0 THEN
        RETURN 0;
    END IF;

    -- Process each tenant's completed jobs to update waiters
    -- Use a recursive approach: iterate through recently archived cancelled jobs
    FOR v_tenant_id, v_job_ids IN
        WITH recent_archived AS (
            SELECT tenant_id, job_id
            FROM pgjobdb.jobs_archive
            WHERE cancel_requested
              AND NOT EXISTS (
                  SELECT 1 FROM pgjobdb.jobs j
                  WHERE j.tenant_id = jobs_archive.tenant_id
                    AND j.job_id = jobs_archive.job_id
              )
            ORDER BY created_at DESC
            LIMIT v_effective_limit
        )
        SELECT tenant_id, array_agg(job_id) AS job_ids
        FROM recent_archived
        GROUP BY tenant_id
    LOOP
        PERFORM pgjobdb._update_waiters_for_completion_bulk(v_tenant_id, v_job_ids);
    END LOOP;

    IF v_trace_enabled THEN
        PERFORM pgjobdb._emit_trace_event(
            'pgjobdb',
            'archive_cancelled_jobs',
            'job_cancel_archived_run',
            p_worker_id,
            jsonb_build_object(
                'worker_id', p_worker_id,
                'tenant_ids', p_tenant_ids,
                'limit', v_effective_limit,
                'archived_jobs', v_archived_count
            )
        );
    END IF;

    RETURN v_archived_count;
END;
$$;

-- Immutable JobDB facts and schedule state.
CREATE TABLE IF NOT EXISTS pgjobdb.schedules (
    tenant_id TEXT NOT NULL,
    schedule_id TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('ACTIVE', 'PAUSED', 'ARCHIVED')),
    generation BIGINT NOT NULL CHECK (generation > 0),
    spec_hash TEXT NOT NULL CHECK (spec_hash <> ''),
    trigger JSONB NOT NULL CHECK (jsonb_typeof(trigger) = 'object'),
    target_job_type TEXT NOT NULL CHECK (target_job_type <> ''),
    target_snapshot JSONB NOT NULL CHECK (jsonb_typeof(target_snapshot) = 'object'),
    overlap_policy TEXT NOT NULL CHECK (overlap_policy <> ''),
    failure_policy JSONB NOT NULL CHECK (jsonb_typeof(failure_policy) = 'object'),
    next_fire_at TIMESTAMPTZ,
    next_job_id TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id, schedule_id)
);

CREATE INDEX IF NOT EXISTS schedules_due_idx
    ON pgjobdb.schedules (next_fire_at, tenant_id, schedule_id)
    WHERE state = 'ACTIVE' AND next_fire_at IS NOT NULL;

CREATE TABLE IF NOT EXISTS pgjobdb.job_facts (
    goal_id TEXT NOT NULL,
    initial_dependencies JSONB NOT NULL DEFAULT '[]',
    tenant_id TEXT NOT NULL,
    job_id TEXT NOT NULL,
    job_type TEXT NOT NULL CHECK (job_type <> ''),
    run_policy JSONB NOT NULL DEFAULT '{}'::JSONB
        CHECK (jsonb_typeof(run_policy) = 'object'),
    app_metadata JSONB NOT NULL DEFAULT '{}'::JSONB
        CHECK (jsonb_typeof(app_metadata) = 'object'),
    schema_hash TEXT,
    parent_job_id TEXT,
    schedule_id TEXT,
    schedule_generation BIGINT,
    schedule_spec_hash TEXT,
    scheduled_at TIMESTAMPTZ,
    schedule_run_id TEXT,
    schedule_reason TEXT,
    schedule_manual BOOLEAN NOT NULL DEFAULT FALSE,
    schedule_backfill_id TEXT,
    schedule_previous_job_id TEXT,
    schedule_failure_history JSONB,
    created_by_worker_id TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    expires_at TIMESTAMPTZ NOT NULL DEFAULT 'infinity',
    PRIMARY KEY (tenant_id, job_id),
    FOREIGN KEY (tenant_id, goal_id) REFERENCES pgjobdb.goals(tenant_id, goal_id),
    FOREIGN KEY (tenant_id, schedule_id)
        REFERENCES pgjobdb.schedules (tenant_id, schedule_id),
    CONSTRAINT job_facts_schedule_complete CHECK (
        (schedule_id IS NULL AND schedule_generation IS NULL
            AND schedule_spec_hash IS NULL AND scheduled_at IS NULL
            AND schedule_run_id IS NULL)
        OR
        (schedule_id IS NOT NULL AND schedule_generation IS NOT NULL
            AND schedule_generation > 0 AND schedule_spec_hash IS NOT NULL
            AND schedule_spec_hash <> '' AND scheduled_at IS NOT NULL
            AND schedule_run_id IS NOT NULL AND schedule_run_id <> '')
    ),
    CONSTRAINT job_facts_failure_history_object CHECK (
        schedule_failure_history IS NULL
        OR jsonb_typeof(schedule_failure_history) = 'object'
    )
);

-- Visibility belongs to the retained identity, across active and archived state.
ALTER TABLE pgjobdb.job_facts ADD COLUMN IF NOT EXISTS hidden_at TIMESTAMPTZ;


CREATE UNIQUE INDEX IF NOT EXISTS job_facts_schedule_run_idx
    ON pgjobdb.job_facts (tenant_id, schedule_id, schedule_generation, schedule_run_id)
    WHERE schedule_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS job_facts_parent_idx
    ON pgjobdb.job_facts (tenant_id, parent_job_id)
    WHERE parent_job_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS job_facts_metadata_idx
    ON pgjobdb.job_facts USING GIN (app_metadata jsonb_path_ops);
CREATE INDEX IF NOT EXISTS job_facts_created_idx
    ON pgjobdb.job_facts (created_at DESC, tenant_id DESC, job_id DESC);





CREATE INDEX IF NOT EXISTS jobs_native_route_idx
    ON pgjobdb.jobs (route_job_type, work_kind, task_type, available_at)
    WHERE route_job_type IS NOT NULL;





CREATE OR REPLACE FUNCTION pgjobdb.upsert_schedule(
    p_tenant_id TEXT,
    p_schedule_id TEXT,
    p_state TEXT,
    p_spec_hash TEXT,
    p_trigger JSONB,
    p_target_job_type TEXT,
    p_target_snapshot JSONB,
    p_overlap_policy TEXT,
    p_failure_policy JSONB,
    p_next_fire_at TIMESTAMPTZ,
    p_next_job_id TEXT,
    p_expected_generation BIGINT DEFAULT NULL
)
RETURNS pgjobdb.schedules
LANGUAGE plpgsql
AS $$
DECLARE
    v_existing pgjobdb.schedules%ROWTYPE;
    v_result pgjobdb.schedules%ROWTYPE;
BEGIN
    IF p_state NOT IN ('ACTIVE', 'PAUSED') THEN
        RAISE EXCEPTION 'schedule upsert requires ACTIVE or PAUSED state';
    END IF;
    IF p_state = 'PAUSED' AND (p_next_fire_at IS NOT NULL OR p_next_job_id IS NOT NULL) THEN
        RAISE EXCEPTION 'paused schedule cannot have next fire state';
    END IF;
    IF (p_next_fire_at IS NULL) <> (p_next_job_id IS NULL) THEN
        RAISE EXCEPTION 'next fire and next job id must be provided together';
    END IF;

    SELECT * INTO v_existing FROM pgjobdb.schedules
    WHERE tenant_id = p_tenant_id AND schedule_id = p_schedule_id FOR UPDATE;
    IF FOUND THEN
        IF v_existing.state = 'ARCHIVED' THEN
            RAISE EXCEPTION 'archived schedule cannot be updated';
        END IF;
        IF p_expected_generation IS NOT NULL
            AND v_existing.generation <> p_expected_generation THEN
            RAISE EXCEPTION 'schedule generation mismatch';
        END IF;
        UPDATE pgjobdb.schedules SET
            state = p_state,
            generation = v_existing.generation + 1,
            spec_hash = p_spec_hash,
            trigger = p_trigger,
            target_job_type = p_target_job_type,
            target_snapshot = p_target_snapshot,
            overlap_policy = p_overlap_policy,
            failure_policy = p_failure_policy,
            next_fire_at = p_next_fire_at,
            next_job_id = p_next_job_id,
            updated_at = clock_timestamp()
        WHERE tenant_id = p_tenant_id AND schedule_id = p_schedule_id
        RETURNING * INTO v_result;
    ELSE
        IF p_expected_generation IS NOT NULL THEN
            RAISE EXCEPTION 'schedule generation mismatch';
        END IF;
        INSERT INTO pgjobdb.schedules (
            tenant_id, schedule_id, state, generation, spec_hash, trigger,
            target_job_type, target_snapshot, overlap_policy, failure_policy,
            next_fire_at, next_job_id
        ) VALUES (
            p_tenant_id, p_schedule_id, p_state, 1, p_spec_hash, p_trigger,
            p_target_job_type, p_target_snapshot, p_overlap_policy,
            p_failure_policy, p_next_fire_at, p_next_job_id
        ) RETURNING * INTO v_result;
    END IF;
    RETURN v_result;
END;
$$;

CREATE OR REPLACE FUNCTION pgjobdb.mutate_schedule(
    p_tenant_id TEXT,
    p_schedule_id TEXT,
    p_state TEXT,
    p_next_fire_at TIMESTAMPTZ DEFAULT NULL,
    p_next_job_id TEXT DEFAULT NULL,
    p_expected_generation BIGINT DEFAULT NULL
)
RETURNS pgjobdb.schedules
LANGUAGE plpgsql
AS $$
DECLARE
    v_existing pgjobdb.schedules%ROWTYPE;
    v_result pgjobdb.schedules%ROWTYPE;
BEGIN
    IF p_state NOT IN ('ACTIVE', 'PAUSED', 'ARCHIVED') THEN
        RAISE EXCEPTION 'invalid schedule state';
    END IF;
    IF p_state <> 'ACTIVE' AND (p_next_fire_at IS NOT NULL OR p_next_job_id IS NOT NULL) THEN
        RAISE EXCEPTION 'inactive schedule cannot have next fire state';
    END IF;
    IF (p_next_fire_at IS NULL) <> (p_next_job_id IS NULL) THEN
        RAISE EXCEPTION 'next fire and next job id must be provided together';
    END IF;

    SELECT * INTO v_existing FROM pgjobdb.schedules
    WHERE tenant_id = p_tenant_id AND schedule_id = p_schedule_id FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'schedule %/% does not exist', p_tenant_id, p_schedule_id;
    END IF;
    IF p_expected_generation IS NOT NULL
        AND v_existing.generation <> p_expected_generation THEN
        RAISE EXCEPTION 'schedule generation mismatch';
    END IF;
    IF v_existing.state = 'ARCHIVED' AND p_state <> 'ARCHIVED' THEN
        RAISE EXCEPTION 'archived schedule cannot change state';
    END IF;
    UPDATE pgjobdb.schedules SET
        state = p_state,
        generation = v_existing.generation + 1,
        next_fire_at = p_next_fire_at,
        next_job_id = p_next_job_id,
        updated_at = clock_timestamp()
    WHERE tenant_id = p_tenant_id AND schedule_id = p_schedule_id
    RETURNING * INTO v_result;
    RETURN v_result;
END;
$$;

CREATE OR REPLACE FUNCTION pgjobdb.pause_schedule(
    p_tenant_id TEXT, p_schedule_id TEXT, p_expected_generation BIGINT DEFAULT NULL
)
RETURNS pgjobdb.schedules
LANGUAGE sql
AS $$
    SELECT pgjobdb.mutate_schedule(p_tenant_id, p_schedule_id, 'PAUSED',
        NULL, NULL, p_expected_generation);
$$;

CREATE OR REPLACE FUNCTION pgjobdb.resume_schedule(
    p_tenant_id TEXT, p_schedule_id TEXT,
    p_next_fire_at TIMESTAMPTZ DEFAULT NULL,
    p_next_job_id TEXT DEFAULT NULL,
    p_expected_generation BIGINT DEFAULT NULL
)
RETURNS pgjobdb.schedules
LANGUAGE sql
AS $$
    SELECT pgjobdb.mutate_schedule(p_tenant_id, p_schedule_id, 'ACTIVE',
        p_next_fire_at, p_next_job_id, p_expected_generation);
$$;

CREATE OR REPLACE FUNCTION pgjobdb.archive_schedule(
    p_tenant_id TEXT, p_schedule_id TEXT, p_expected_generation BIGINT DEFAULT NULL
)
RETURNS pgjobdb.schedules
LANGUAGE sql
AS $$
    SELECT pgjobdb.mutate_schedule(p_tenant_id, p_schedule_id, 'ARCHIVED',
        NULL, NULL, p_expected_generation);
$$;

CREATE OR REPLACE FUNCTION pgjobdb.get_schedule(
    p_tenant_id TEXT, p_schedule_id TEXT
)
RETURNS SETOF pgjobdb.schedules
LANGUAGE sql STABLE
AS $$
    SELECT * FROM pgjobdb.schedules
    WHERE tenant_id = p_tenant_id AND schedule_id = p_schedule_id;
$$;

DROP FUNCTION IF EXISTS pgjobdb.list_schedules(
    TEXT, TEXT[], TEXT[], TIMESTAMPTZ, TEXT, INTEGER
);

CREATE OR REPLACE FUNCTION pgjobdb.list_schedules(
    p_tenant_id TEXT,
    p_states TEXT[] DEFAULT NULL,
    p_target_job_types TEXT[] DEFAULT NULL,
    p_before_updated_at TIMESTAMPTZ DEFAULT NULL,
    p_before_schedule_id TEXT DEFAULT NULL,
    p_limit INTEGER DEFAULT 100,
    p_schedule_ids TEXT[] DEFAULT NULL
)
RETURNS SETOF pgjobdb.schedules
LANGUAGE plpgsql STABLE
AS $$
BEGIN
    IF p_limit IS NULL OR p_limit < 1 OR p_limit > 1000 THEN
        RAISE EXCEPTION 'schedule page limit must be between 1 and 1000';
    END IF;
    IF (p_before_updated_at IS NULL) <> (p_before_schedule_id IS NULL) THEN
        RAISE EXCEPTION 'schedule cursor fields must be provided together';
    END IF;
    RETURN QUERY SELECT s.* FROM pgjobdb.schedules s
    WHERE s.tenant_id = p_tenant_id
      AND (p_states IS NULL OR s.state = ANY(p_states))
      AND (p_target_job_types IS NULL OR s.target_job_type = ANY(p_target_job_types))
      AND (p_schedule_ids IS NULL OR s.schedule_id = ANY(p_schedule_ids))
      AND (p_before_updated_at IS NULL
        OR (s.updated_at, s.schedule_id) < (p_before_updated_at, p_before_schedule_id))
    ORDER BY s.updated_at DESC, s.schedule_id DESC
    LIMIT p_limit;
END;
$$;

DO $$ BEGIN
 IF NOT EXISTS(SELECT 1 FROM pg_constraint WHERE conrelid='pgjobdb.jobs'::regclass AND conname='active_goal_fk') THEN
  ALTER TABLE pgjobdb.jobs ADD CONSTRAINT active_goal_fk FOREIGN KEY(tenant_id,goal_id) REFERENCES pgjobdb.goals(tenant_id,goal_id);
  ALTER TABLE pgjobdb.jobs ADD CONSTRAINT native_goal_required CHECK(route_job_type IS NULL OR goal_id IS NOT NULL);
  ALTER TABLE pgjobdb.jobs_archive ADD CONSTRAINT archive_goal_fk FOREIGN KEY(tenant_id,goal_id) REFERENCES pgjobdb.goals(tenant_id,goal_id);
 END IF;
END $$;
CREATE OR REPLACE FUNCTION pgjobdb._immutable_goal_id() RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
 IF NEW.goal_id IS DISTINCT FROM OLD.goal_id THEN RAISE EXCEPTION 'primary goal is immutable' USING ERRCODE='JGF01'; END IF;
 RETURN NEW;
END $$;
CREATE OR REPLACE FUNCTION pgjobdb._check_active_goal() RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
 IF NOT EXISTS(SELECT 1 FROM pgjobdb.job_facts f WHERE f.tenant_id=NEW.tenant_id AND f.job_id=NEW.job_id AND f.goal_id=NEW.goal_id) THEN
  RAISE EXCEPTION 'active goal differs from retained identity' USING ERRCODE='JGM01';
 END IF;
 RETURN NEW;
END $$;
DROP TRIGGER IF EXISTS check_active_goal ON pgjobdb.jobs;
CREATE TRIGGER check_active_goal BEFORE INSERT ON pgjobdb.jobs FOR EACH ROW EXECUTE FUNCTION pgjobdb._check_active_goal();
DROP TRIGGER IF EXISTS immutable_goal_id ON pgjobdb.jobs;
CREATE TRIGGER immutable_goal_id BEFORE UPDATE OF goal_id ON pgjobdb.jobs FOR EACH ROW EXECUTE FUNCTION pgjobdb._immutable_goal_id();
DROP TRIGGER IF EXISTS immutable_goal_id ON pgjobdb.job_facts;
CREATE TRIGGER immutable_goal_id BEFORE UPDATE OF goal_id ON pgjobdb.job_facts FOR EACH ROW EXECUTE FUNCTION pgjobdb._immutable_goal_id();
DROP TRIGGER IF EXISTS immutable_goal_id ON pgjobdb.jobs_archive;
CREATE TRIGGER immutable_goal_id BEFORE UPDATE OF goal_id ON pgjobdb.jobs_archive FOR EACH ROW EXECUTE FUNCTION pgjobdb._immutable_goal_id();

CREATE INDEX IF NOT EXISTS facts_goal_idx ON pgjobdb.job_facts(tenant_id,goal_id,job_id);
DO $$ BEGIN
 IF NOT EXISTS(SELECT 1 FROM pg_constraint WHERE conrelid='pgjobdb.goal_candidates'::regclass AND conname='candidate_job_fk') THEN
  ALTER TABLE pgjobdb.goal_candidates ADD CONSTRAINT candidate_job_fk FOREIGN KEY(tenant_id,job_id) REFERENCES pgjobdb.job_facts(tenant_id,job_id);
  ALTER TABLE pgjobdb.dependency_waits ADD CONSTRAINT wait_job_fk FOREIGN KEY(tenant_id,job_id) REFERENCES pgjobdb.job_facts(tenant_id,job_id);
 END IF;
END $$;

CREATE OR REPLACE FUNCTION pgjobdb.goal_info(p_tenant TEXT,p_goal TEXT) RETURNS JSONB LANGUAGE plpgsql STABLE AS $$
DECLARE g pgjobdb.goals%ROWTYPE;
BEGIN
 SELECT * INTO g FROM pgjobdb.goals WHERE tenant_id=p_tenant AND goal_id=p_goal;
 IF NOT FOUND THEN RAISE EXCEPTION 'goal not found' USING ERRCODE='JGN01'; END IF;
 RETURN jsonb_strip_nulls(jsonb_build_object('goalKey',jsonb_build_object('tenantId',p_tenant,'goalId',p_goal),
 'state',g.state,'revision',g.revision::TEXT,'successfulJobId',g.successful_job_id,'failure',g.failure,
 'createdAt',g.created_at,'updatedAt',g.updated_at,'closedAt',g.closed_at));
END $$;

-- Retained identity supplies membership; terminal outcomes come from the archive.
CREATE OR REPLACE FUNCTION pgjobdb._goal_members(p_tenant TEXT,p_goal TEXT) RETURNS TABLE(job_id TEXT) LANGUAGE sql STABLE AS $$
 SELECT f.job_id FROM pgjobdb.job_facts f WHERE f.tenant_id=p_tenant AND f.goal_id=p_goal
 UNION SELECT c.job_id FROM pgjobdb.goal_candidates c WHERE c.tenant_id=p_tenant AND c.goal_id=p_goal;
$$;
CREATE OR REPLACE FUNCTION pgjobdb._close_wait(p_outcome JSONB,p_state TEXT) RETURNS JSONB LANGUAGE sql IMMUTABLE AS $$
 SELECT jsonb_set(jsonb_set(p_outcome,'{state}',to_jsonb(p_state)),'{dependencies}',
 COALESCE((SELECT jsonb_agg(CASE WHEN d->>'state'='PENDING' THEN (d-'blockedReason') || '{"state":"CANCELLED"}'::JSONB ELSE d END ORDER BY n)
 FROM jsonb_array_elements(p_outcome->'dependencies') WITH ORDINALITY x(d,n)), '[]'::JSONB));
$$;
CREATE OR REPLACE FUNCTION pgjobdb._evaluate_wait(p_outcome JSONB,p_now TIMESTAMPTZ) RETURNS JSONB LANGUAGE plpgsql STABLE AS $$
DECLARE w JSONB:=p_outcome; ds JSONB:=w->'dependencies'; d JSONB; g pgjobdb.goals%ROWTYPE;
 v_tenant TEXT:=w#>>'{jobKey,tenantId}'; v_job TEXT; v_status TEXT; i INTEGER; pending BOOLEAN:=FALSE; failure JSONB;
BEGIN
 IF w->>'state'<>'PENDING' THEN RETURN w; END IF;
 IF (w->>'deadline')::TIMESTAMPTZ<=p_now THEN RETURN pgjobdb._close_wait(w,'TIMED_OUT'); END IF;
 FOR i IN 0..jsonb_array_length(ds)-1 LOOP
  d:=ds->i;
  IF d->>'state'<>'PENDING' THEN CONTINUE; END IF;
  IF d#>>'{target,goalId}' IS NOT NULL THEN
   SELECT * INTO g FROM pgjobdb.goals WHERE tenant_id=v_tenant AND goal_id=d#>>'{target,goalId}';
   IF NOT FOUND THEN RAISE EXCEPTION 'goal not found' USING ERRCODE='JGN01'; END IF;
   IF g.state='SUCCEEDED' THEN d:=(d-'blockedReason') || jsonb_build_object('state','SATISFIED','satisfiedByJobId',g.successful_job_id);
   ELSIF g.state='FAILED' THEN d:=(d-'blockedReason') || jsonb_build_object('state','FAILED','failure',g.failure);
   ELSE
    d:=d || jsonb_build_object('blockedReason',CASE WHEN EXISTS(SELECT 1 FROM pgjobdb._goal_members(v_tenant,g.goal_id) m
     WHERE NOT EXISTS(SELECT 1 FROM pgjobdb.jobs_archive a WHERE a.tenant_id=v_tenant AND a.job_id=m.job_id))
     THEN 'waiting_for_execution' ELSE 'awaiting_candidate' END);
   END IF;
  ELSE
   v_job:=d#>>'{target,jobId}';
   IF NOT EXISTS(SELECT 1 FROM pgjobdb.job_facts WHERE tenant_id=v_tenant AND job_id=v_job) THEN RAISE EXCEPTION 'job not found' USING ERRCODE='JJN01'; END IF;
   SELECT completion_status INTO v_status FROM pgjobdb.jobs_archive WHERE tenant_id=v_tenant AND job_id=v_job;
   IF FOUND THEN
    IF v_status='success' OR d->>'condition'='complete' THEN d:=d || jsonb_build_object('state','SATISFIED','satisfiedByJobId',v_job);
    ELSE d:=d || jsonb_build_object('state','FAILED','failure',jsonb_build_object('code','job_dependency_failed','jobId',v_job,'message','prerequisite job '||v_job||' did not succeed'));
    END IF;
   END IF;
  END IF;
  ds:=jsonb_set(ds,ARRAY[i::TEXT],d);
  IF d->>'state'='FAILED' THEN
   RETURN pgjobdb._close_wait(w || jsonb_build_object('dependencies',ds,'failedDependencyIndex',i,'failure',d->'failure'),'FAILED');
  END IF;
  pending:=pending OR d->>'state'='PENDING';
 END LOOP;
 RETURN w || jsonb_build_object('dependencies',ds,'state',CASE WHEN pending THEN 'PENDING' ELSE 'SATISFIED' END);
END $$;
CREATE OR REPLACE FUNCTION pgjobdb._save_wait(p_outcome JSONB) RETURNS VOID LANGUAGE plpgsql AS $$
DECLARE t TEXT:=p_outcome#>>'{jobKey,tenantId}'; j TEXT:=p_outcome#>>'{jobKey,jobId}'; w TEXT:=p_outcome->>'waitId'; d JSONB;
BEGIN
 UPDATE pgjobdb.dependency_waits SET outcome=p_outcome,state=p_outcome->>'state' WHERE tenant_id=t AND job_id=j AND wait_id=w;
 DELETE FROM pgjobdb.dependency_targets WHERE tenant_id=t AND job_id=j AND wait_id=w;
 IF p_outcome->>'state'='PENDING' THEN
  FOR d IN SELECT value FROM jsonb_array_elements(p_outcome->'dependencies') LOOP
   IF d->>'state'='PENDING' THEN
    INSERT INTO pgjobdb.dependency_targets VALUES(t,j,w,CASE WHEN d#>>'{target,goalId}' IS NULL THEN 'job' ELSE 'goal' END,COALESCE(d#>>'{target,goalId}',d#>>'{target,jobId}'));
   END IF;
  END LOOP;
 ELSE
  -- NOTIFY is delivered only at commit; durable state remains authoritative.
  PERFORM pg_notify('pgjobdb_work',t);
 END IF;
END $$;
CREATE OR REPLACE FUNCTION pgjobdb._settle_target(t TEXT,k TEXT,id TEXT) RETURNS VOID LANGUAGE plpgsql AS $$
DECLARE w RECORD;
BEGIN
 FOR w IN SELECT dw.outcome FROM pgjobdb.dependency_targets dt JOIN pgjobdb.dependency_waits dw USING(tenant_id,job_id,wait_id)
  WHERE dt.tenant_id=t AND dt.target_kind=k AND dt.target_id=id ORDER BY dw.job_id,dw.wait_id LOOP
  PERFORM pgjobdb._save_wait(pgjobdb._evaluate_wait(w.outcome,clock_timestamp()));
 END LOOP;
END $$;
CREATE OR REPLACE FUNCTION pgjobdb._cancel_waits(t TEXT,j TEXT,keep TEXT DEFAULT NULL) RETURNS VOID LANGUAGE plpgsql AS $$
DECLARE w RECORD;
BEGIN
 FOR w IN SELECT outcome FROM pgjobdb.dependency_waits WHERE tenant_id=t AND job_id=j AND state='PENDING' AND wait_id IS DISTINCT FROM keep LOOP
  PERFORM pgjobdb._save_wait(pgjobdb._close_wait(w.outcome,'CANCELLED'));
 END LOOP;
END $$;
CREATE OR REPLACE FUNCTION pgjobdb._check_goal_cycle(t TEXT,start_kind TEXT,start_id TEXT) RETURNS VOID LANGUAGE plpgsql AS $$
DECLARE cyclic BOOLEAN;
BEGIN
 WITH RECURSIVE walk(kind,id,path,cycle) AS (
  SELECT start_kind,start_id,ARRAY[start_kind||':'||start_id],FALSE
  UNION ALL
  SELECT e.kind,e.id,w.path||(e.kind||':'||e.id),(e.kind||':'||e.id)=ANY(w.path)
  FROM walk w CROSS JOIN LATERAL (
   SELECT dt.target_kind AS kind,dt.target_id AS id FROM pgjobdb.dependency_targets dt
   JOIN pgjobdb.dependency_waits dw USING(tenant_id,job_id,wait_id)
   WHERE w.kind='job' AND dt.tenant_id=t AND dt.job_id=w.id
    AND (dw.deadline IS NULL OR dw.deadline>clock_timestamp())
    AND NOT EXISTS(SELECT 1 FROM pgjobdb.jobs_archive a WHERE a.tenant_id=t AND a.job_id=w.id)
   UNION ALL
   SELECT 'job',m.job_id FROM pgjobdb.goals g
   CROSS JOIN LATERAL pgjobdb._goal_members(t,g.goal_id) m
   WHERE w.kind='goal' AND g.tenant_id=t AND g.goal_id=w.id AND g.state='OPEN'
    AND NOT EXISTS(SELECT 1 FROM pgjobdb.jobs_archive a WHERE a.tenant_id=t AND a.job_id=m.job_id)
  ) e WHERE NOT w.cycle
 ) SELECT EXISTS(SELECT 1 FROM walk WHERE cycle) INTO cyclic;
 IF cyclic THEN RAISE EXCEPTION 'dependency cycle' USING ERRCODE='JGC01'; END IF;
END $$;
CREATE OR REPLACE FUNCTION pgjobdb._validate_dependencies(deps JSONB) RETURNS VOID LANGUAGE plpgsql AS $$
DECLARE d JSONB; target JSONB; seen JSONB:='[]';
BEGIN
 IF jsonb_typeof(deps) IS DISTINCT FROM 'array' THEN RAISE EXCEPTION 'invalid dependencies' USING ERRCODE='JGI01'; END IF;
 FOR d IN SELECT value FROM jsonb_array_elements(deps) LOOP
  target:=d->'target';
  IF (COALESCE(btrim(target->>'jobId'),'')='')=(COALESCE(btrim(target->>'goalId'),'')='')
   OR COALESCE(d->>'condition','') NOT IN ('success','complete')
   OR (target->>'goalId' IS NOT NULL AND d->>'condition'<>'success')
   OR seen @> jsonb_build_array(target) THEN RAISE EXCEPTION 'invalid or duplicate dependency' USING ERRCODE='JGI01'; END IF;
  seen:=seen||jsonb_build_array(target);
 END LOOP;
END $$;
CREATE OR REPLACE FUNCTION pgjobdb._register_wait(t TEXT,j TEXT,req JSONB,submission BOOLEAN DEFAULT FALSE) RETURNS VOID LANGUAGE plpgsql AS $$
DECLARE old pgjobdb.dependency_waits%ROWTYPE; d JSONB; w JSONB; ds JSONB:='[]'; id TEXT:=req->>'waitId'; deadline TIMESTAMPTZ:=(req->>'deadline')::TIMESTAMPTZ;
BEGIN
 IF COALESCE(id,'')='' OR id ~ '[/\\?#% ]' OR (id='submission' AND NOT submission) THEN RAISE EXCEPTION 'invalid wait id' USING ERRCODE='JGI01'; END IF;
 PERFORM pgjobdb._validate_dependencies(req->'dependencies');
 IF jsonb_array_length(req->'dependencies')=0 THEN RAISE EXCEPTION 'empty dependency wait' USING ERRCODE='JGI01'; END IF;
 SELECT * INTO old FROM pgjobdb.dependency_waits WHERE tenant_id=t AND job_id=j AND wait_id=id;
 IF FOUND THEN
  IF old.descriptor IS DISTINCT FROM req->'dependencies' OR old.deadline IS DISTINCT FROM deadline THEN RAISE EXCEPTION 'wait descriptor changed' USING ERRCODE='JGW02'; END IF;
  PERFORM pgjobdb._save_wait(pgjobdb._evaluate_wait(old.outcome,clock_timestamp()));
  RETURN;
 END IF;
 FOR d IN SELECT value FROM jsonb_array_elements(req->'dependencies') LOOP
  IF d#>>'{target,jobId}'=j THEN RAISE EXCEPTION 'self dependency' USING ERRCODE='JGC01'; END IF;
  IF d#>>'{target,jobId}' IS NOT NULL THEN
   IF NOT EXISTS(SELECT 1 FROM pgjobdb.job_facts WHERE tenant_id=t AND job_id=d#>>'{target,jobId}') THEN RAISE EXCEPTION 'job not found' USING ERRCODE='JJN01'; END IF;
  ELSE PERFORM pgjobdb.goal_info(t,d#>>'{target,goalId}'); END IF;
  ds:=ds||jsonb_build_array(d||'{"state":"PENDING"}'::JSONB);
 END LOOP;
 w:=jsonb_strip_nulls(jsonb_build_object('jobKey',jsonb_build_object('tenantId',t,'jobId',j),'waitId',id,'deadline',deadline,'state','PENDING','dependencies',ds));
 INSERT INTO pgjobdb.dependency_waits VALUES(t,j,id,req->'dependencies',deadline,'PENDING',w);
 PERFORM pgjobdb._save_wait(pgjobdb._evaluate_wait(w,clock_timestamp()));
 PERFORM pgjobdb._check_goal_cycle(t,'job',j);
END $$;
CREATE OR REPLACE FUNCTION pgjobdb.dependency_wait_info(t TEXT,j TEXT,id TEXT) RETURNS JSONB LANGUAGE plpgsql STABLE AS $$
DECLARE w JSONB;
BEGIN
 SELECT outcome INTO w FROM pgjobdb.dependency_waits WHERE tenant_id=t AND job_id=j AND wait_id=id;
 IF NOT FOUND THEN RAISE EXCEPTION 'wait not found' USING ERRCODE='JGW01'; END IF;
 RETURN pgjobdb._evaluate_wait(w,statement_timestamp());
END $$;
CREATE OR REPLACE FUNCTION pgjobdb._expire_goal_waits(tenants TEXT[]) RETURNS VOID LANGUAGE plpgsql AS $$
DECLARE t TEXT; w RECORD;
BEGIN
 -- Lock only tenants with due waits, in stable order, before any job lock.
 FOR t IN SELECT DISTINCT tenant_id FROM pgjobdb.dependency_waits WHERE state='PENDING' AND deadline<=clock_timestamp()
  AND (tenants IS NULL OR tenant_id=ANY(tenants)) ORDER BY tenant_id LOOP
  PERFORM pgjobdb._goal_lock(t);
  FOR w IN SELECT outcome FROM pgjobdb.dependency_waits WHERE tenant_id=t AND state='PENDING' AND deadline<=clock_timestamp() LOOP
   PERFORM pgjobdb._save_wait(pgjobdb._evaluate_wait(w.outcome,clock_timestamp()));
  END LOOP;
 END LOOP;
END $$;
-- Match public conflict diagnostics without disclosing input or metadata values.
-- Descriptor comparison itself remains exact; parsing is only for diagnostics.
CREATE OR REPLACE FUNCTION pgjobdb._preparation_mismatch(old TEXT,new TEXT) RETURNS TEXT LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE a JSONB; b JSONB; f RECORD;
BEGIN
 BEGIN a:=old::JSONB; b:=new::JSONB; EXCEPTION WHEN invalid_text_representation THEN RETURN 'different job preparation'; END;
 IF a->'Data' IS DISTINCT FROM b->'Data' THEN RETURN 'different input'; END IF;
 FOR f IN SELECT * FROM (VALUES ('JobType','job type'),('Metadata','metadata'),('RunPolicy','run policy'),
 ('Prerequisites','prerequisites'),('Dependencies','dependencies'),('ShareGoal','goal sharing'),
 ('ClientPayloadUpdate','initial client payload'),('Schema','schema')) labels(key,label) LOOP
  IF a#>ARRAY['Job',f.key] IS DISTINCT FROM b#>ARRAY['Job',f.key] THEN RETURN 'different '||f.label; END IF;
 END LOOP;
 RETURN 'different job preparation';
END $$;
CREATE OR REPLACE FUNCTION pgjobdb.prepare_job(t TEXT,j TEXT,r TEXT,prior TEXT,sharing BOOLEAN,descr TEXT,deps JSONB) RETURNS JSONB LANGUAGE plpgsql AS $$
DECLARE p pgjobdb.job_preparations%ROWTYPE; g TEXT;
BEGIN
 IF COALESCE(btrim(t),'')='' OR COALESCE(descr,'')='' OR (sharing AND j IS NULL AND r IS NULL) THEN RAISE EXCEPTION 'invalid preparation' USING ERRCODE='JGI01'; END IF;
 PERFORM pgjobdb._validate_dependencies(deps);
 PERFORM pgjobdb._goal_lock(t);
 SELECT * INTO p FROM pgjobdb.job_preparations WHERE tenant_id=t AND ((j IS NOT NULL AND job_id=j) OR (r IS NOT NULL AND request_id=r)) ORDER BY job_id LIMIT 1;
 IF FOUND THEN
  IF p.descriptor<>descr OR p.prior_job_id IS DISTINCT FROM prior OR p.share_goal<>sharing OR p.dependencies<>deps
   OR (j IS NOT NULL AND p.job_id<>j) OR (r IS NOT NULL AND p.request_id IS NOT NULL AND p.request_id<>r)
   OR EXISTS(SELECT 1 FROM pgjobdb.job_preparations x WHERE x.tenant_id=t AND x.request_id=r AND x.job_id<>p.job_id)
  THEN RAISE EXCEPTION '%',pgjobdb._preparation_mismatch(p.descriptor,descr) USING ERRCODE='JGM01'; END IF;
  IF r IS NOT NULL AND p.request_id IS NULL THEN UPDATE pgjobdb.job_preparations SET request_id=r WHERE tenant_id=t AND job_id=p.job_id; END IF;
  RETURN jsonb_build_object('Key',jsonb_build_object('tenantId',t,'jobId',p.job_id),'GoalID',p.goal_id,'Published',p.published);
 END IF;
 IF j IS NOT NULL AND EXISTS(SELECT 1 FROM pgjobdb.job_facts WHERE tenant_id=t AND job_id=j) THEN RAISE EXCEPTION 'destination already exists without preparation' USING ERRCODE='JGM01'; END IF;
 IF prior IS NOT NULL THEN
  SELECT goal_id INTO g FROM pgjobdb.job_facts WHERE tenant_id=t AND job_id=prior;
  IF NOT FOUND THEN RAISE EXCEPTION 'source job not found' USING ERRCODE='JJN01'; END IF;
 END IF;
 IF sharing THEN
  IF g IS NULL THEN RAISE EXCEPTION 'shared restart requires a source' USING ERRCODE='JGI01'; END IF;
  IF (pgjobdb.goal_info(t,g)->>'state')<>'OPEN' THEN RAISE EXCEPTION 'goal is closed' USING ERRCODE='JGF01'; END IF;
 ELSE g:=gen_random_uuid()::TEXT; END IF;
 j:=COALESCE(j,gen_random_uuid()::TEXT);
 INSERT INTO pgjobdb.job_preparations VALUES(t,j,g,r,prior,sharing,descr,deps,FALSE);
 RETURN jsonb_build_object('Key',jsonb_build_object('tenantId',t,'jobId',j),'GoalID',g,'Published',FALSE);
END $$;
CREATE OR REPLACE FUNCTION pgjobdb._resolve_goal(t TEXT,g TEXT,j TEXT) RETURNS VOID LANGUAGE plpgsql AS $$
DECLARE at TIMESTAMPTZ:=clock_timestamp();
BEGIN
 UPDATE pgjobdb.goals SET state='SUCCEEDED',revision=2,successful_job_id=j,updated_at=at,closed_at=at
 WHERE tenant_id=t AND goal_id=g AND state='OPEN';
 IF FOUND THEN PERFORM pgjobdb._settle_target(t,'goal',g); END IF;
END $$;
CREATE OR REPLACE FUNCTION pgjobdb.add_goal_candidate(t TEXT,g TEXT,j TEXT) RETURNS JSONB LANGUAGE plpgsql AS $$
DECLARE info JSONB; primary_goal TEXT; added TIMESTAMPTZ;
BEGIN
 PERFORM pgjobdb._goal_lock(t);
 info:=pgjobdb.goal_info(t,g);
 SELECT goal_id,created_at INTO primary_goal,added FROM pgjobdb.job_facts WHERE tenant_id=t AND job_id=j;
 IF NOT FOUND THEN RAISE EXCEPTION 'job not found' USING ERRCODE='JJN01'; END IF;
 IF primary_goal<>g THEN
  SELECT added_at INTO added FROM pgjobdb.goal_candidates WHERE tenant_id=t AND goal_id=g AND job_id=j;
  IF NOT FOUND THEN
   IF info->>'state'<>'OPEN' THEN RAISE EXCEPTION 'goal is closed' USING ERRCODE='JGF01'; END IF;
   INSERT INTO pgjobdb.goal_candidates VALUES(t,g,j,clock_timestamp()) RETURNING added_at INTO added;
   IF EXISTS(SELECT 1 FROM pgjobdb.jobs_archive WHERE tenant_id=t AND job_id=j AND completion_status='success') THEN PERFORM pgjobdb._resolve_goal(t,g,j); END IF;
   PERFORM pgjobdb._check_goal_cycle(t,'goal',g);
  END IF;
 END IF;
 info:=pgjobdb.goal_info(t,g);
 RETURN jsonb_strip_nulls(jsonb_build_object('goalKey',info->'goalKey','jobId',j,'addedAt',added,'goalState',info->>'state','successfulJobId',info->'successfulJobId'));
END $$;
CREATE OR REPLACE FUNCTION pgjobdb.fail_goal(t TEXT,g TEXT,message TEXT) RETURNS JSONB LANGUAGE plpgsql AS $$
DECLARE info JSONB; v_failure JSONB; terminal JSONB; nonterminal JSONB; at TIMESTAMPTZ;
BEGIN
 IF COALESCE(btrim(message),'')='' OR octet_length(message)>16384 THEN RAISE EXCEPTION 'invalid failure message' USING ERRCODE='JGI01'; END IF;
 PERFORM pgjobdb._goal_lock(t);
 info:=pgjobdb.goal_info(t,g);
 IF info->>'state'='FAILED' AND info#>>'{failure,message}'=message THEN RETURN info; END IF;
 IF info->>'state'<>'OPEN' THEN RAISE EXCEPTION 'goal is closed' USING ERRCODE='JGF01'; END IF;
 SELECT COALESCE(jsonb_agg(m.job_id ORDER BY m.job_id) FILTER(WHERE a.job_id IS NOT NULL),'[]'),
 COALESCE(jsonb_agg(m.job_id ORDER BY m.job_id) FILTER(WHERE a.job_id IS NULL),'[]') INTO terminal,nonterminal
 FROM pgjobdb._goal_members(t,g) m LEFT JOIN pgjobdb.jobs_archive a ON a.tenant_id=t AND a.job_id=m.job_id;
 at:=clock_timestamp();
 v_failure:=jsonb_build_object('code','goal_failed','goalId',g,'message',message,'terminalJobIds',terminal,'nonTerminalJobIds',nonterminal,'failedAt',at);
 UPDATE pgjobdb.goals SET state='FAILED',revision=2,failure=v_failure,updated_at=at,closed_at=at WHERE tenant_id=t AND goal_id=g;
 PERFORM pgjobdb._settle_target(t,'goal',g);
 RETURN pgjobdb.goal_info(t,g);
END $$;
-- Covers every archive path, including deletion and cancellation maintenance.
CREATE OR REPLACE FUNCTION pgjobdb._archive_goal_identity() RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
 SELECT goal_id INTO NEW.goal_id FROM pgjobdb.jobs WHERE tenant_id=NEW.tenant_id AND job_id=NEW.job_id;
 RETURN NEW;
END $$;
CREATE OR REPLACE FUNCTION pgjobdb._terminal_goals() RETURNS TRIGGER LANGUAGE plpgsql AS $$
DECLARE g TEXT;
BEGIN
 IF NEW.goal_id IS NULL THEN RETURN NEW; END IF;
 PERFORM pgjobdb._cancel_waits(NEW.tenant_id,NEW.job_id);
 PERFORM pgjobdb._settle_target(NEW.tenant_id,'job',NEW.job_id);
 IF NEW.completion_status='success' THEN
  FOR g IN SELECT NEW.goal_id UNION SELECT goal_id FROM pgjobdb.goal_candidates WHERE tenant_id=NEW.tenant_id AND job_id=NEW.job_id LOOP
   PERFORM pgjobdb._resolve_goal(NEW.tenant_id,g,NEW.job_id);
  END LOOP;
 END IF;
 RETURN NEW;
END $$;
DROP TRIGGER IF EXISTS archive_goal_identity ON pgjobdb.jobs_archive;
CREATE TRIGGER archive_goal_identity BEFORE INSERT ON pgjobdb.jobs_archive FOR EACH ROW EXECUTE FUNCTION pgjobdb._archive_goal_identity();
DROP TRIGGER IF EXISTS terminal_goals ON pgjobdb.jobs_archive;
CREATE TRIGGER terminal_goals AFTER INSERT ON pgjobdb.jobs_archive FOR EACH ROW EXECUTE FUNCTION pgjobdb._terminal_goals();

CREATE TABLE IF NOT EXISTS pgjobdb.job_client_state (
 tenant_id TEXT NOT NULL,
 job_id TEXT NOT NULL,
 client_payload JSON,
 revision BIGINT NOT NULL CHECK (revision >= 0),
 initial_payload_digest TEXT NOT NULL,
 PRIMARY KEY (tenant_id,job_id),
 FOREIGN KEY (tenant_id,job_id) REFERENCES pgjobdb.job_facts(tenant_id,job_id) ON DELETE CASCADE,
 CHECK (client_payload IS NULL OR octet_length(client_payload::text)<=65536)
);

-- Canonical value identity, also validating duplicates, depth and Unicode.
-- Numeric coefficients/exponents are normalized as text: the JSON number itself
-- is never converted to PostgreSQL numeric (which has a smaller range).
CREATE OR REPLACE FUNCTION pgjobdb._client_json_string(p_text TEXT)
RETURNS TEXT LANGUAGE sql IMMUTABLE STRICT AS $$
 SELECT replace(replace(replace(replace(replace(to_json(p_text)::text,
 '<', E'\\u003c'), '>', E'\\u003e'), '&', E'\\u0026'), chr(8232), E'\\u2028'), chr(8233), E'\\u2029');
$$;
CREATE OR REPLACE FUNCTION pgjobdb._canonical_client_json(p_value JSON,p_depth INTEGER DEFAULT 0,p_normalize_numbers BOOLEAN DEFAULT TRUE)
RETURNS TEXT LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE kind TEXT; result TEXT; item RECORD; s TEXT; coefficient TEXT; exponent NUMERIC:=0; negative BOOLEAN; pos INTEGER; n INTEGER;
BEGIN
 IF p_value IS NULL THEN RETURN 'absent'; END IF;
 IF octet_length(p_value::text)>65536 THEN RAISE EXCEPTION 'client payload exceeds size limit' USING ERRCODE='JCP03'; END IF;
 kind:=json_typeof(p_value);
 IF kind IN ('object','array') AND p_depth>=128 THEN RAISE EXCEPTION 'client payload nesting limit' USING ERRCODE='JCP01'; END IF;
 IF kind='object' THEN
  IF EXISTS(SELECT key FROM json_each(p_value) GROUP BY key HAVING count(*)>1) THEN RAISE EXCEPTION 'duplicate client payload object name' USING ERRCODE='JCP01'; END IF;
  result:='';
  FOR item IN SELECT key,value FROM json_each(p_value) ORDER BY key COLLATE "C" LOOP
   IF result<>'' THEN result:=result||','; END IF;
   result:=result||pgjobdb._client_json_string(item.key)||':'||pgjobdb._canonical_client_json(item.value,p_depth+1,p_normalize_numbers);
  END LOOP;
  RETURN '{'||result||'}';
 ELSIF kind='array' THEN
  result:='';
  FOR item IN SELECT value FROM json_array_elements(p_value) LOOP
   IF result<>'' THEN result:=result||','; END IF;
   result:=result||pgjobdb._canonical_client_json(item.value,p_depth+1,p_normalize_numbers);
  END LOOP;
  RETURN '['||result||']';
 ELSIF kind='string' THEN
  RETURN pgjobdb._client_json_string(p_value#>>'{}');
 ELSIF kind='number' THEN
  IF NOT p_normalize_numbers THEN RETURN btrim(p_value::text);END IF;
  s:=lower(btrim(p_value::text));negative:=left(s,1)='-';IF negative THEN s:=substr(s,2);END IF;
  pos:=strpos(s,'e');IF pos>0 THEN exponent:=substr(s,pos+1)::numeric;s:=substr(s,1,pos-1);END IF;
  pos:=strpos(s,'.');IF pos>0 THEN exponent:=exponent-(length(s)-pos);s:=replace(s,'.','');END IF;
  s:=ltrim(s,'0');IF s='' THEN RETURN '0';END IF;
  n:=length(s);s:=rtrim(s,'0');exponent:=exponent+n-length(s);
  RETURN CASE WHEN negative THEN '-' ELSE '' END||s||'e'||exponent::text;
 ELSE RETURN btrim(p_value::text);
 END IF;
END;
$$;
CREATE OR REPLACE FUNCTION pgjobdb._merge_client_json(p_target JSON,p_patch JSON)
RETURNS JSON LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE result TEXT:=''; item RECORD; merged_value JSON;
BEGIN
 IF json_typeof(p_patch) IS DISTINCT FROM 'object' THEN RETURN p_patch; END IF;
 IF json_typeof(p_target) IS DISTINCT FROM 'object' THEN p_target:='{}'::JSON; END IF;
 FOR item IN SELECT t.key,t.value FROM json_each(p_target) t WHERE NOT EXISTS(SELECT 1 FROM json_each(p_patch) p WHERE p.key=t.key) LOOP
  IF result<>'' THEN result:=result||',';END IF;
  result:=result||to_json(item.key)::text||':'||item.value::text;
 END LOOP;
 FOR item IN SELECT key,value FROM json_each(p_patch) LOOP
  IF json_typeof(item.value)='null' THEN CONTINUE;END IF;
  merged_value:=pgjobdb._merge_client_json(p_target->item.key,item.value);
  IF result<>'' THEN result:=result||',';END IF;
  result:=result||to_json(item.key)::text||':'||merged_value::text;
 END LOOP;
 RETURN ('{'||result||'}')::JSON;
END;
$$;
CREATE OR REPLACE FUNCTION pgjobdb._client_update_value(p_current JSON,p_mode TEXT,p_value JSON)
RETURNS JSON LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE result JSON;
BEGIN
 IF p_mode IS NULL THEN
  IF p_value IS NOT NULL THEN RAISE EXCEPTION 'client update mode required' USING ERRCODE='JCP01';END IF;
  RETURN p_current;
 END IF;
 PERFORM pgjobdb._canonical_client_json(p_value);
 IF p_mode='reset' THEN result:=p_value;
 ELSIF p_mode='patch' AND p_value IS NOT NULL THEN
  result:=pgjobdb._canonical_client_json(pgjobdb._merge_client_json(p_current,p_value),0,FALSE)::JSON;
 ELSE RAISE EXCEPTION 'invalid client payload update' USING ERRCODE='JCP01';END IF;
 PERFORM pgjobdb._canonical_client_json(result);
 RETURN result;
END;
$$;
-- Caller holds and authorizes the scheduler job row first.
CREATE OR REPLACE FUNCTION pgjobdb._update_client_payload(p_tenant TEXT,p_job TEXT,p_mode TEXT,p_value JSON,p_expected BIGINT)
RETURNS VOID LANGUAGE plpgsql AS $$
DECLARE state pgjobdb.job_client_state%ROWTYPE; value JSON;
BEGIN
 IF p_mode IS NULL THEN
  IF p_value IS NOT NULL OR p_expected IS NOT NULL THEN RAISE EXCEPTION 'invalid preserve update' USING ERRCODE='JCP01';END IF;
  RETURN;
 END IF;
 SELECT * INTO STRICT state FROM pgjobdb.job_client_state WHERE tenant_id=p_tenant AND job_id=p_job FOR UPDATE;
 IF p_expected IS NULL OR p_expected<0 THEN RAISE EXCEPTION 'expected revision required' USING ERRCODE='JCP01';END IF;
 IF state.revision<>p_expected OR state.revision=9223372036854775807 THEN RAISE EXCEPTION 'client payload revision conflict' USING ERRCODE='JCP02';END IF;
 value:=pgjobdb._client_update_value(state.client_payload,p_mode,p_value);
 UPDATE pgjobdb.job_client_state SET client_payload=value,revision=state.revision+1 WHERE tenant_id=p_tenant AND job_id=p_job;
END;
$$;

CREATE OR REPLACE FUNCTION pgjobdb.submit_native_job(
    p_tenant_id TEXT,
    p_job_id TEXT,
    p_worker_id TEXT,
    p_job_type TEXT,
    p_run_policy JSONB,
    p_app_metadata JSONB,
    p_schema_hash TEXT,
    p_parent_job_id TEXT,
    p_schedule JSONB,
    p_wait_for TEXT[],
    p_available_at TIMESTAMPTZ,
    p_expires_at TIMESTAMPTZ,
    p_client_mode TEXT,
    p_client_value JSON,
    p_goal_id TEXT DEFAULT NULL, p_share_goal BOOLEAN DEFAULT FALSE, p_dependencies JSONB DEFAULT '[]'
)
RETURNS TABLE(job_id TEXT, created BOOLEAN)
LANGUAGE plpgsql
AS $$
DECLARE
    v_inserted INTEGER;
    v_goal TEXT;
    v_preparation pgjobdb.job_preparations%ROWTYPE;
    v_parent_hidden TIMESTAMPTZ;
    v_initial JSON;
    v_digest TEXT;
    v_existing pgjobdb.job_facts%ROWTYPE;
    v_schedule_id TEXT := p_schedule->>'schedule_id';
    v_schedule_generation BIGINT := (p_schedule->>'generation')::BIGINT;
    v_schedule_spec_hash TEXT := p_schedule->>'spec_hash';
    v_scheduled_at TIMESTAMPTZ := (p_schedule->>'scheduled_at')::TIMESTAMPTZ;
    v_schedule_run_id TEXT := p_schedule->>'run_id';
    v_expires_at TIMESTAMPTZ := COALESCE(p_expires_at, 'infinity');
BEGIN
    PERFORM pgjobdb._goal_lock(p_tenant_id);
    IF p_tenant_id IS NULL OR p_tenant_id = '' OR p_job_id IS NULL OR p_job_id = ''
        OR p_worker_id IS NULL OR p_worker_id = '' OR p_job_type IS NULL
        OR p_job_type = '' THEN
        RAISE EXCEPTION 'tenant, job, worker, and job type are required';
    END IF;
    IF jsonb_typeof(p_run_policy) IS DISTINCT FROM 'object'
        OR jsonb_typeof(p_app_metadata) IS DISTINCT FROM 'object' THEN
        RAISE EXCEPTION 'run policy and app metadata must be JSON objects';
    END IF;
    IF p_schedule IS NOT NULL AND jsonb_typeof(p_schedule) IS DISTINCT FROM 'object' THEN
        RAISE EXCEPTION 'schedule occurrence must be a JSON object';
    END IF;
    PERFORM pgjobdb._validate_dependencies(p_dependencies);
    IF cardinality(p_wait_for)>0 AND jsonb_array_length(p_dependencies)>0 THEN RAISE EXCEPTION 'mixed dependencies' USING ERRCODE='JGI01'; END IF;
    SELECT * INTO v_preparation FROM pgjobdb.job_preparations prep WHERE prep.tenant_id=p_tenant_id AND prep.job_id=p_job_id;
    IF FOUND AND (v_preparation.goal_id IS DISTINCT FROM p_goal_id OR v_preparation.share_goal<>p_share_goal OR v_preparation.dependencies<>p_dependencies) THEN
        RAISE EXCEPTION 'publication differs from preparation' USING ERRCODE='JGM01';
    END IF;
    SELECT f.goal_id INTO v_goal FROM pgjobdb.job_facts f WHERE f.tenant_id=p_tenant_id AND f.job_id=p_job_id;
    IF FOUND THEN
        IF p_goal_id IS NOT NULL AND v_goal<>p_goal_id THEN RAISE EXCEPTION 'primary goal differs' USING ERRCODE='JGM01'; END IF;
    ELSE
        v_goal:=COALESCE(p_goal_id,gen_random_uuid()::TEXT);
        IF p_share_goal THEN
            IF v_preparation.job_id IS NULL OR (pgjobdb.goal_info(p_tenant_id,v_goal)->>'state')<>'OPEN' THEN RAISE EXCEPTION 'shared goal is closed or unprepared' USING ERRCODE='JGF01'; END IF;
        ELSE
            IF EXISTS(SELECT 1 FROM pgjobdb.goals WHERE tenant_id=p_tenant_id AND goal_id=v_goal) THEN RAISE EXCEPTION 'goal already exists' USING ERRCODE='JGF01'; END IF;
            INSERT INTO pgjobdb.goals(tenant_id,goal_id) VALUES(p_tenant_id,v_goal);
        END IF;
    END IF;
    -- Hold a shared identity lock through child creation. Deletion takes the
    -- conflicting lock, closing the gap after core's initial lease validation.
    IF p_parent_job_id IS NOT NULL THEN
        SELECT parent.hidden_at INTO v_parent_hidden FROM pgjobdb.job_facts parent
        WHERE parent.tenant_id=p_tenant_id AND parent.job_id=p_parent_job_id FOR SHARE;
        IF FOUND AND v_parent_hidden IS NOT NULL THEN
            RAISE EXCEPTION 'parent job was deleted' USING ERRCODE='JDL01';
        END IF;
    END IF;
    v_initial:=pgjobdb._client_update_value(NULL,p_client_mode,p_client_value);
    v_digest:=encode(sha256(convert_to(pgjobdb._canonical_client_json(v_initial),'UTF8')),'hex');

    INSERT INTO pgjobdb.job_facts (
        goal_id, initial_dependencies, tenant_id, job_id, job_type, run_policy, app_metadata, schema_hash,
        parent_job_id, schedule_id, schedule_generation, schedule_spec_hash,
        scheduled_at, schedule_run_id, schedule_reason, schedule_manual,
        schedule_backfill_id, schedule_previous_job_id,
        schedule_failure_history, created_by_worker_id, expires_at
    ) VALUES (
        v_goal, p_dependencies, p_tenant_id, p_job_id, p_job_type, p_run_policy, p_app_metadata,
        p_schema_hash, p_parent_job_id, v_schedule_id, v_schedule_generation,
        v_schedule_spec_hash, v_scheduled_at, v_schedule_run_id,
        p_schedule->>'reason', COALESCE((p_schedule->>'manual')::BOOLEAN, FALSE),
        p_schedule->>'backfill_id', p_schedule->>'previous_job_id',
        p_schedule->'failure_history', p_worker_id, v_expires_at
    ) ON CONFLICT ON CONSTRAINT job_facts_pkey DO NOTHING;
    GET DIAGNOSTICS v_inserted = ROW_COUNT;

    IF v_inserted = 0 THEN
        SELECT * INTO v_existing FROM pgjobdb.job_facts f
        WHERE f.tenant_id = p_tenant_id AND f.job_id = p_job_id;
        IF v_existing.initial_dependencies IS DISTINCT FROM p_dependencies THEN RAISE EXCEPTION 'different dependencies' USING ERRCODE='JGM01'; END IF;
        IF v_existing.job_type IS DISTINCT FROM p_job_type
            OR v_existing.run_policy IS DISTINCT FROM p_run_policy
            OR v_existing.app_metadata IS DISTINCT FROM p_app_metadata
            OR v_existing.schema_hash IS DISTINCT FROM p_schema_hash
            OR v_existing.parent_job_id IS DISTINCT FROM p_parent_job_id
            OR v_existing.schedule_id IS DISTINCT FROM v_schedule_id
            OR v_existing.schedule_generation IS DISTINCT FROM v_schedule_generation
            OR v_existing.schedule_spec_hash IS DISTINCT FROM v_schedule_spec_hash
            OR v_existing.scheduled_at IS DISTINCT FROM v_scheduled_at
            OR v_existing.schedule_run_id IS DISTINCT FROM v_schedule_run_id
            OR v_existing.schedule_reason IS DISTINCT FROM p_schedule->>'reason'
            OR v_existing.schedule_manual IS DISTINCT FROM
                COALESCE((p_schedule->>'manual')::BOOLEAN, FALSE)
            OR v_existing.schedule_backfill_id IS DISTINCT FROM
                p_schedule->>'backfill_id'
            OR v_existing.schedule_previous_job_id IS DISTINCT FROM
                p_schedule->>'previous_job_id'
            OR v_existing.schedule_failure_history IS DISTINCT FROM
                p_schedule->'failure_history'
            OR v_existing.expires_at IS DISTINCT FROM v_expires_at THEN
            RAISE EXCEPTION 'job id already exists with different immutable facts';
        END IF;
        IF NOT EXISTS (SELECT 1 FROM pgjobdb.job_client_state c
            WHERE c.tenant_id = p_tenant_id AND c.job_id = p_job_id AND c.initial_payload_digest=v_digest) THEN
            RAISE EXCEPTION 'initial client payload differs or is missing';
        END IF;
        RETURN QUERY SELECT p_job_id, FALSE;
        RETURN;
    END IF;

    INSERT INTO pgjobdb.jobs (
        goal_id, tenant_id, job_id, wait_for, available_at, expires_at,
        route_job_type, work_kind
    ) VALUES (
        v_goal, p_tenant_id, p_job_id,
        pgjobdb.normalize_wait_for(p_tenant_id, p_wait_for),
        COALESCE(p_available_at, clock_timestamp()), v_expires_at,
        p_job_type, 'JOB'
    );
    INSERT INTO pgjobdb.job_client_state VALUES(p_tenant_id,p_job_id,v_initial,CASE WHEN v_initial IS NULL THEN 0 ELSE 1 END,v_digest);
    IF jsonb_array_length(p_dependencies)>0 THEN
        PERFORM pgjobdb._register_wait(p_tenant_id,p_job_id,jsonb_build_object('waitId','submission','dependencies',p_dependencies),TRUE);
        UPDATE pgjobdb.jobs j SET dependency_wait_id='submission' WHERE j.tenant_id=p_tenant_id AND j.job_id=p_job_id;
    END IF;
    IF p_share_goal THEN PERFORM pgjobdb._check_goal_cycle(p_tenant_id,'goal',v_goal); END IF;
    UPDATE pgjobdb.job_preparations prep SET published=TRUE WHERE prep.tenant_id=p_tenant_id AND prep.job_id=p_job_id;
    RETURN QUERY SELECT p_job_id, TRUE;
END;
$$;

DROP FUNCTION IF EXISTS pgjobdb.get_native_work(
    TEXT, TEXT[], TEXT[], JSONB, JSONB, INTEGER, TEXT, TEXT
);

CREATE OR REPLACE FUNCTION pgjobdb.get_native_work(
    p_worker_id TEXT,
    p_tenant_ids TEXT[],
    p_job_types TEXT[],
    p_task_selectors JSONB,
    p_app_metadata_contains JSONB,
    p_lease_seconds INTEGER,
    p_target_tenant_id TEXT DEFAULT NULL,
    p_target_job_id TEXT DEFAULT NULL,
    p_metadata_predicates JSONB DEFAULT '[]'::JSONB
)
RETURNS TABLE(
    tenant_id TEXT, job_id TEXT, lease_id TEXT, lease_expires_at TIMESTAMPTZ,
    job_type TEXT, route_job_type TEXT, work_kind TEXT, task_type TEXT,
    resume_job_type TEXT, task_input_ordinal BIGINT,
    task_output_ordinal BIGINT, task_input_hash TEXT,
    run_policy JSONB, client_payload JSON, client_payload_revision BIGINT,
    schema_hash TEXT, goal_id TEXT
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_now TIMESTAMPTZ := clock_timestamp();
BEGIN
    IF p_worker_id IS NULL OR p_worker_id = '' THEN
        RAISE EXCEPTION 'worker id is required';
    END IF;
    IF p_lease_seconds IS NULL OR p_lease_seconds < 1 OR p_lease_seconds > 86400 THEN
        RAISE EXCEPTION 'lease seconds must be between 1 and 86400';
    END IF;
    IF jsonb_typeof(p_task_selectors) IS DISTINCT FROM 'array'
        OR jsonb_typeof(p_app_metadata_contains) IS DISTINCT FROM 'object'
        OR jsonb_typeof(p_metadata_predicates) IS DISTINCT FROM 'array' THEN
        RAISE EXCEPTION 'task selectors and metadata predicates must be arrays; metadata containment must be an object';
    END IF;
    IF (p_target_tenant_id IS NULL) <> (p_target_job_id IS NULL) THEN
        RAISE EXCEPTION 'target tenant and job id must be provided together';
    END IF;

    PERFORM pgjobdb._expire_goal_waits(p_tenant_ids);
    v_now:=clock_timestamp();
    RETURN QUERY
    WITH candidate AS (
        SELECT j.tenant_id, j.job_id,
            route.effective_job_type,
            route.effective_work_kind,
            route.effective_task_type, route.alternate_due
        FROM pgjobdb.jobs j
        -- The status view is authoritative for readiness, including crash
        -- concern. Lock the base row below to preserve SKIP LOCKED acquisition.
        JOIN pgjobdb.jobs_with_status eligibility USING (tenant_id, job_id)
        JOIN pgjobdb.job_facts f USING (tenant_id, job_id) JOIN pgjobdb.job_client_state c USING (tenant_id,job_id)
        CROSS JOIN LATERAL pgjobdb.effective_native_route(j, v_now) route
        WHERE eligibility.status = 'READY'
          AND j.route_job_type IS NOT NULL
          AND (p_tenant_ids IS NULL OR j.tenant_id = ANY(p_tenant_ids))
          AND (p_target_tenant_id IS NULL
            OR (j.tenant_id = p_target_tenant_id AND j.job_id = p_target_job_id))
          AND j.cancel_requested = FALSE
          AND j.expires_at > v_now
          AND j.lease_expires_at <= v_now
          AND f.app_metadata @> p_app_metadata_contains
          AND NOT EXISTS (
              SELECT 1 FROM jsonb_array_elements(p_metadata_predicates) AS predicate(item)
              WHERE NOT EXISTS (
                  SELECT 1 FROM jsonb_array_elements(predicate.item->'values') AS value(item)
                  WHERE f.app_metadata #> ARRAY(
                      SELECT jsonb_array_elements_text(predicate.item->'path')
                  ) = value.item
              )
          )
          AND (
              (route.effective_work_kind = 'JOB'
                  AND route.effective_job_type = ANY(COALESCE(p_job_types, ARRAY[]::TEXT[])))
              OR (route.effective_work_kind = 'TASK' AND EXISTS (
                  SELECT 1 FROM jsonb_array_elements(p_task_selectors) AS selector(item)
                  WHERE selector.item->>'job_type' = route.effective_job_type
                    AND selector.item->>'task_type' = route.effective_task_type
              ))
          )
        ORDER BY j.available_at, j.created_at, j.tenant_id, j.job_id
        FOR UPDATE OF j SKIP LOCKED
        LIMIT 1
    ), leased AS (
        UPDATE pgjobdb.jobs j SET
            lease_id = gen_random_uuid()::TEXT,
            lease_worker_id = p_worker_id,
            lease_route_job_type = c.effective_job_type,
            lease_work_kind = c.effective_work_kind,
            lease_task_type = c.effective_task_type,
            -- Promote only the matched, locked candidate. Primary claims leave
            -- the alternate and its original eligibility timestamp intact.
            route_job_type = c.effective_job_type,
            work_kind = c.effective_work_kind,
            task_type = c.effective_task_type,
            resume_job_type = CASE WHEN c.effective_work_kind = 'TASK' THEN j.resume_job_type END,
            task_input_ordinal = CASE WHEN c.effective_work_kind = 'TASK' THEN j.task_input_ordinal END,
            task_output_ordinal = CASE WHEN c.effective_work_kind = 'TASK' THEN j.task_output_ordinal END,
            task_input_hash = CASE WHEN c.effective_work_kind = 'TASK' THEN j.task_input_hash END,
            alternate_job_type = CASE WHEN NOT c.alternate_due THEN j.alternate_job_type END,
            alternate_task_type = CASE WHEN NOT c.alternate_due THEN j.alternate_task_type END,
            alternate_after_seconds = CASE WHEN NOT c.alternate_due THEN j.alternate_after_seconds END,
            alternate_at = CASE WHEN NOT c.alternate_due THEN j.alternate_at END,
            lease_expires_at = v_now + make_interval(secs => p_lease_seconds),
            lease_expiration_count = j.lease_expiration_count +
                CASE WHEN j.lease_id IS NOT NULL THEN 1 ELSE 0 END,
            consecutive_expirations = CASE WHEN j.lease_id IS NOT NULL
                THEN j.consecutive_expirations + 1 ELSE 0 END
        FROM candidate c
        WHERE j.tenant_id = c.tenant_id AND j.job_id = c.job_id
        RETURNING j.*
    )
    SELECT l.tenant_id, l.job_id, l.lease_id, l.lease_expires_at,
        f.job_type, l.lease_route_job_type, l.lease_work_kind, l.lease_task_type,
        l.resume_job_type, l.task_input_ordinal, l.task_output_ordinal,
        l.task_input_hash, f.run_policy, c.client_payload, c.revision, f.schema_hash, l.goal_id
    FROM leased l JOIN pgjobdb.job_facts f USING (tenant_id, job_id) JOIN pgjobdb.job_client_state c USING (tenant_id,job_id);
END;
$$;

CREATE OR REPLACE FUNCTION pgjobdb._complete_native_locked_job(
    p_locked_job pgjobdb.jobs_with_status,
    p_worker_id TEXT,
    p_completion_status TEXT,
    p_completion_detail TEXT,
    p_error_kind TEXT,
    p_retryable BOOLEAN
)
RETURNS BOOLEAN
LANGUAGE plpgsql
AS $$
DECLARE
    v_active pgjobdb.jobs%ROWTYPE;
BEGIN
    IF p_completion_status NOT IN
        ('success', 'failed_app', 'failed_system', 'failed_timeout', 'cancelled') THEN
        RAISE EXCEPTION 'invalid JobDB completion status';
    END IF;
    SELECT * INTO v_active FROM pgjobdb.jobs j
    WHERE j.tenant_id = p_locked_job.tenant_id AND j.job_id = p_locked_job.job_id
    FOR UPDATE;
    IF NOT FOUND OR v_active.route_job_type IS NULL THEN
        RAISE EXCEPTION 'native job state is missing';
    END IF;

    PERFORM pgjobdb._complete_locked_job(
        p_locked_job, p_worker_id, p_completion_status, p_completion_detail
    );
    UPDATE pgjobdb.jobs_archive a SET
        completion_error_kind = p_error_kind,
        completion_retryable = p_retryable,
        final_route_job_type = v_active.route_job_type,
        final_work_kind = v_active.work_kind,
        final_task_type = v_active.task_type,
        final_resume_job_type = v_active.resume_job_type,
        final_task_input_ordinal = v_active.task_input_ordinal,
        final_task_output_ordinal = v_active.task_output_ordinal,
        final_task_input_hash = v_active.task_input_hash,
        final_alternate_job_type = v_active.alternate_job_type,
        final_alternate_task_type = v_active.alternate_task_type,
        final_alternate_after_seconds = v_active.alternate_after_seconds,
        final_alternate_at = v_active.alternate_at,
        final_wait_for = v_active.wait_for,
        final_available_at = v_active.available_at,
        final_lease_expires_at = v_active.lease_expires_at,
        final_lease_worker_id = v_active.lease_worker_id,
        final_cancel_requested = v_active.cancel_requested
    WHERE a.tenant_id = v_active.tenant_id AND a.job_id = v_active.job_id;
    RETURN TRUE;
END;
$$;

CREATE OR REPLACE FUNCTION pgjobdb.complete_native_job(
    p_tenant_id TEXT,
    p_job_id TEXT,
    p_lease_id TEXT,
    p_worker_id TEXT,
    p_completion_status TEXT,
    p_completion_detail TEXT,
    p_error_kind TEXT,
    p_retryable BOOLEAN,
 p_client_mode TEXT, p_client_value JSON, p_client_expected BIGINT
)
RETURNS BOOLEAN
LANGUAGE plpgsql
AS $$
DECLARE
    v_job pgjobdb.jobs_with_status%ROWTYPE;
BEGIN
    PERFORM pgjobdb._goal_lock(p_tenant_id);
    IF p_worker_id IS NULL OR p_worker_id = '' THEN
        RAISE EXCEPTION 'worker id is required';
    END IF;
    IF p_lease_id IS NULL OR p_lease_id = '' THEN
        RAISE EXCEPTION 'lease id is required';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pgjobdb.jobs j
        WHERE j.tenant_id = p_tenant_id AND j.job_id = p_job_id
          AND j.lease_id = p_lease_id AND j.lease_worker_id = p_worker_id) THEN
        RAISE EXCEPTION 'native lease owner mismatch';
    END IF;
    v_job := pgjobdb._lock_job_for_status(
        p_tenant_id, p_job_id, 'ACTIVE', p_lease_id,
        format('native lease not active for job %s/%s', p_tenant_id, p_job_id)
    );
    IF v_job.lease_expires_at<=clock_timestamp() OR v_job.cancel_requested THEN RAISE EXCEPTION 'native lease not active';END IF;
    PERFORM pgjobdb._update_client_payload(p_tenant_id,p_job_id,p_client_mode,p_client_value,p_client_expected);
    RETURN pgjobdb._complete_native_locked_job(
        v_job, p_worker_id, p_completion_status,
        p_completion_detail, p_error_kind, p_retryable
    );
END;
$$;

CREATE OR REPLACE FUNCTION pgjobdb.complete_native_unheld_job(
    p_tenant_id TEXT,
    p_job_id TEXT,
    p_worker_id TEXT,
    p_completion_status TEXT,
    p_completion_detail TEXT DEFAULT NULL,
    p_error_kind TEXT DEFAULT NULL,
    p_retryable BOOLEAN DEFAULT NULL
)
RETURNS BOOLEAN
LANGUAGE plpgsql
AS $$
DECLARE
    v_job pgjobdb.jobs_with_status%ROWTYPE;
BEGIN
    PERFORM pgjobdb._goal_lock(p_tenant_id);
    IF p_worker_id IS NULL OR p_worker_id = '' THEN
        RAISE EXCEPTION 'worker id is required';
    END IF;
    v_job := pgjobdb._lock_job_for_status(
        p_tenant_id, p_job_id, 'READY', NULL,
        format('native job %s/%s is not unheld', p_tenant_id, p_job_id)
    );
    RETURN pgjobdb._complete_native_locked_job(
        v_job, p_worker_id, p_completion_status,
        p_completion_detail, p_error_kind, p_retryable
    );
END;
$$;

CREATE OR REPLACE FUNCTION pgjobdb.validate_native_lease(
    p_tenant_id TEXT,
    p_job_id TEXT,
    p_lease_id TEXT,
    p_worker_id TEXT
)
RETURNS TABLE(
    tenant_id TEXT, job_id TEXT, lease_id TEXT, lease_expires_at TIMESTAMPTZ,
    job_type TEXT, route_job_type TEXT, work_kind TEXT, task_type TEXT,
    resume_job_type TEXT, task_input_ordinal BIGINT,
    task_output_ordinal BIGINT, task_input_hash TEXT,
    run_policy JSONB, client_payload JSON, client_payload_revision BIGINT,
    schema_hash TEXT, goal_id TEXT
)
LANGUAGE plpgsql
AS $$
BEGIN
    IF p_worker_id IS NULL OR p_worker_id = '' OR p_lease_id IS NULL OR p_lease_id = '' THEN
        RAISE EXCEPTION 'worker id and lease id are required';
    END IF;
    RETURN QUERY
    SELECT j.tenant_id, j.job_id, j.lease_id, j.lease_expires_at,
        f.job_type, j.lease_route_job_type, j.lease_work_kind, j.lease_task_type,
        j.resume_job_type, j.task_input_ordinal, j.task_output_ordinal,
        j.task_input_hash, f.run_policy, c.client_payload, c.revision, f.schema_hash, j.goal_id
    FROM pgjobdb.jobs j JOIN pgjobdb.job_facts f USING (tenant_id, job_id) JOIN pgjobdb.job_client_state c USING (tenant_id,job_id)
    WHERE j.tenant_id = p_tenant_id AND j.job_id = p_job_id
      AND j.lease_id = p_lease_id AND j.lease_expires_at > clock_timestamp()
      AND j.lease_worker_id = p_worker_id
      AND j.route_job_type IS NOT NULL AND NOT j.cancel_requested;
END;
$$;

CREATE OR REPLACE FUNCTION pgjobdb.renew_native_lease(
    p_tenant_id TEXT,
    p_job_id TEXT,
    p_lease_id TEXT,
    p_worker_id TEXT,
    p_additional_seconds INTEGER
)
RETURNS TIMESTAMPTZ
LANGUAGE plpgsql
AS $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pgjobdb.jobs j
        WHERE j.tenant_id = p_tenant_id AND j.job_id = p_job_id
          AND j.lease_id = p_lease_id AND j.lease_worker_id = p_worker_id
          AND j.route_job_type IS NOT NULL) THEN
        RAISE EXCEPTION 'native lease not found';
    END IF;
    RETURN pgjobdb._extend_lease(
        p_tenant_id, p_job_id, p_lease_id, p_worker_id, p_additional_seconds
    );
END;
$$;

CREATE OR REPLACE FUNCTION pgjobdb.cancel_native_job(
    p_tenant_id TEXT,
    p_job_id TEXT,
    p_worker_id TEXT,
    p_reason TEXT DEFAULT NULL
)
RETURNS BOOLEAN
LANGUAGE plpgsql
AS $$
DECLARE v_locked pgjobdb.jobs_with_status%ROWTYPE;
BEGIN
    PERFORM pgjobdb._goal_lock(p_tenant_id);
    IF NOT EXISTS (SELECT 1 FROM pgjobdb.jobs j
        WHERE j.tenant_id = p_tenant_id AND j.job_id = p_job_id
          AND j.route_job_type IS NOT NULL) THEN
        RAISE EXCEPTION 'native job not found';
    END IF;
    PERFORM pgjobdb._cancel_job(p_tenant_id, p_job_id, p_worker_id, p_reason);
    SELECT * INTO v_locked FROM pgjobdb.jobs_with_status WHERE tenant_id=p_tenant_id AND job_id=p_job_id;
    PERFORM pgjobdb._complete_native_locked_job(v_locked,p_worker_id,'cancelled',p_reason,NULL,FALSE);
    RETURN TRUE;
END;
$$;

CREATE OR REPLACE FUNCTION pgjobdb.reschedule_native_job(
    p_tenant_id TEXT,
    p_job_id TEXT,
    p_lease_id TEXT,
    p_worker_id TEXT,
    p_route_job_type TEXT,
    p_work_kind TEXT,
    p_task_type TEXT,
    p_resume_job_type TEXT,
    p_task_input_ordinal BIGINT,
    p_task_output_ordinal BIGINT,
    p_task_input_hash TEXT,
    p_wait_for TEXT[],
    p_available_at TIMESTAMPTZ,
    p_alternate_job_type TEXT,
    p_alternate_task_type TEXT,
    p_alternate_after_seconds DOUBLE PRECISION,
    p_client_mode TEXT, p_client_value JSON, p_client_expected BIGINT,
    p_wait_until_is_deadline BOOLEAN, p_dependency_wait JSONB DEFAULT NULL
)
RETURNS BOOLEAN
LANGUAGE plpgsql
AS $$
DECLARE
    v_locked pgjobdb.jobs_with_status%ROWTYPE;
    v_active pgjobdb.jobs%ROWTYPE;
    v_wait_for TEXT[];
    v_now TIMESTAMPTZ := clock_timestamp();
BEGIN
    PERFORM pgjobdb._goal_lock(p_tenant_id);
    IF p_wait_until_is_deadline IS NULL OR (p_wait_until_is_deadline
        AND (COALESCE(cardinality(p_wait_for), 0) = 0 OR p_available_at IS NULL)) THEN
        RAISE EXCEPTION 'dependency deadline requires wait-for jobs and available-at';
    END IF;
    IF p_worker_id IS NULL OR p_worker_id = '' OR p_route_job_type IS NULL
        OR p_route_job_type = '' THEN
        RAISE EXCEPTION 'worker id and route job type are required';
    END IF;
    IF p_work_kind = 'JOB' THEN
        IF p_task_type IS NOT NULL OR p_resume_job_type IS NOT NULL
            OR p_task_input_ordinal IS NOT NULL OR p_task_output_ordinal IS NOT NULL
            OR p_task_input_hash IS NOT NULL THEN
            RAISE EXCEPTION 'job route cannot carry task coordinates';
        END IF;
    ELSIF p_work_kind = 'TASK' THEN
        IF p_task_type IS NULL OR p_task_type = ''
            OR p_resume_job_type IS NULL OR p_resume_job_type = ''
            OR p_task_input_ordinal IS NULL OR p_task_input_ordinal < 0
            OR p_task_output_ordinal IS NULL OR p_task_output_ordinal < 0
            OR p_task_input_hash IS NULL OR p_task_input_hash = '' THEN
            RAISE EXCEPTION 'task route requires complete coordinates';
        END IF;
    ELSE
        RAISE EXCEPTION 'work kind must be JOB or TASK';
    END IF;
    BEGIN
        IF p_alternate_job_type IS NULL THEN
            IF p_alternate_task_type IS NOT NULL OR p_alternate_after_seconds IS NOT NULL THEN
                RAISE EXCEPTION 'cleared alternate route cannot have task or delay';
            END IF;
        ELSIF p_alternate_job_type = '' OR p_alternate_after_seconds IS NULL
            OR p_alternate_after_seconds < 0 OR p_alternate_after_seconds >= 'Infinity'::DOUBLE PRECISION
            OR (p_alternate_task_type IS NOT NULL AND p_alternate_task_type = '') THEN
            RAISE EXCEPTION 'invalid alternate route';
        END IF;
        IF p_alternate_task_type IS NOT NULL AND p_work_kind <> 'TASK' THEN
            RAISE EXCEPTION 'alternate task route requires task coordinates';
        END IF;
    END;
    IF p_job_id = ANY(COALESCE(p_wait_for, ARRAY[]::TEXT[])) THEN
        RAISE EXCEPTION 'job cannot wait for itself';
    END IF;

    IF p_lease_id IS NULL THEN
        IF p_client_mode IS NOT NULL OR p_client_value IS NOT NULL OR p_client_expected IS NOT NULL THEN RAISE EXCEPTION 'client payload updates require a lease' USING ERRCODE='JCP01';END IF;
        v_locked := pgjobdb._lock_job_for_status(
            p_tenant_id, p_job_id, 'READY', NULL,
            format('native job %s/%s is not unheld', p_tenant_id, p_job_id)
        );
    ELSE
        v_locked := pgjobdb._lock_job_for_status(
            p_tenant_id, p_job_id, 'ACTIVE', p_lease_id,
            format('native lease not active for job %s/%s', p_tenant_id, p_job_id)
        );
    END IF;
    SELECT * INTO v_active FROM pgjobdb.jobs j
    WHERE j.tenant_id = p_tenant_id AND j.job_id = p_job_id FOR UPDATE;
    IF v_active.route_job_type IS NULL THEN
        RAISE EXCEPTION 'native job state is missing';
    END IF;
    IF p_lease_id IS NOT NULL AND v_active.lease_worker_id IS DISTINCT FROM p_worker_id THEN
        RAISE EXCEPTION 'native lease owner mismatch';
    END IF;
    IF v_active.cancel_requested THEN
        RAISE EXCEPTION 'cancelled job cannot be rescheduled';
    END IF;
    IF p_lease_id IS NOT NULL AND v_active.lease_expires_at<=clock_timestamp() THEN RAISE EXCEPTION 'native lease not active';END IF;
    PERFORM pgjobdb._update_client_payload(p_tenant_id,p_job_id,p_client_mode,p_client_value,p_client_expected);
    IF p_dependency_wait IS NOT NULL AND (cardinality(p_wait_for)>0 OR p_available_at IS NOT NULL OR p_alternate_job_type IS NOT NULL) THEN
        RAISE EXCEPTION 'mixed structured and legacy wait' USING ERRCODE='JGI01';
    END IF;
    PERFORM pgjobdb._cancel_waits(p_tenant_id,p_job_id,p_dependency_wait->>'waitId');
    IF p_dependency_wait IS NOT NULL THEN PERFORM pgjobdb._register_wait(p_tenant_id,p_job_id,p_dependency_wait); END IF;
    v_wait_for := pgjobdb.normalize_wait_for(p_tenant_id, p_wait_for);

    UPDATE pgjobdb.jobs j SET
        dependency_wait_id = p_dependency_wait->>'waitId',
        route_job_type = p_route_job_type,
        work_kind = p_work_kind,
        task_type = p_task_type,
        resume_job_type = p_resume_job_type,
        task_input_ordinal = p_task_input_ordinal,
        task_output_ordinal = p_task_output_ordinal,
        task_input_hash = p_task_input_hash,
        wait_for = v_wait_for,
        available_at = CASE WHEN p_wait_until_is_deadline AND cardinality(v_wait_for) = 0 THEN v_now ELSE COALESCE(p_available_at, v_now) END,
        wait_until_is_deadline = p_wait_until_is_deadline AND cardinality(v_wait_for) > 0,
        alternate_job_type = p_alternate_job_type,
        alternate_task_type = p_alternate_task_type,
        alternate_after_seconds = p_alternate_after_seconds,
        alternate_at = CASE WHEN p_alternate_job_type IS NOT NULL
            THEN v_now + make_interval(secs => p_alternate_after_seconds) END,
        lease_id = NULL,
        lease_worker_id = NULL,
        lease_route_job_type = NULL, lease_work_kind = NULL, lease_task_type = NULL,
        lease_expires_at = '-infinity',
        consecutive_expirations = 0
    WHERE j.tenant_id = p_tenant_id AND j.job_id = p_job_id;
    RETURN TRUE;
END;
$$;

-- Preserve the original native API's not-before semantics for existing callers.
CREATE OR REPLACE FUNCTION pgjobdb.reschedule_native_job(
    p_tenant_id TEXT,
    p_job_id TEXT,
    p_lease_id TEXT,
    p_worker_id TEXT,
    p_route_job_type TEXT,
    p_work_kind TEXT,
    p_task_type TEXT,
    p_resume_job_type TEXT,
    p_task_input_ordinal BIGINT,
    p_task_output_ordinal BIGINT,
    p_task_input_hash TEXT,
    p_wait_for TEXT[],
    p_available_at TIMESTAMPTZ,
    p_alternate_job_type TEXT,
    p_alternate_task_type TEXT,
    p_alternate_after_seconds DOUBLE PRECISION,
    p_client_mode TEXT, p_client_value JSON, p_client_expected BIGINT
)
RETURNS BOOLEAN
LANGUAGE sql
AS $$
    SELECT pgjobdb.reschedule_native_job(
        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
        $11, $12, $13, $14, $15, $16, $17, $18, $19, FALSE);
$$;

-- External task completion claims only scheduler state. Chapter insertion is
-- independent and recovery replays history after this lease expires.
CREATE OR REPLACE FUNCTION pgjobdb.claim_native_task(
    p_tenant_id TEXT,
    p_job_id TEXT,
    p_worker_id TEXT,
    p_job_type TEXT,
    p_task_type TEXT,
    p_resume_job_type TEXT,
    p_input_ordinal BIGINT,
    p_output_ordinal BIGINT,
    p_input_hash TEXT,
    p_lease_seconds INTEGER
)
RETURNS TABLE(lease_id TEXT, lease_expires_at TIMESTAMPTZ)
LANGUAGE plpgsql
AS $$
DECLARE
    v_job pgjobdb.jobs%ROWTYPE;
    v_now TIMESTAMPTZ;
BEGIN
    PERFORM pgjobdb._expire_goal_waits(ARRAY[p_tenant_id]);
    v_now:=clock_timestamp();
    IF p_worker_id IS NULL OR p_worker_id = '' THEN
        RAISE EXCEPTION 'worker id is required';
    END IF;
    IF p_lease_seconds IS NULL OR p_lease_seconds < 1 OR p_lease_seconds > 86400 THEN
        RAISE EXCEPTION 'lease seconds must be between 1 and 86400';
    END IF;
    SELECT * INTO v_job FROM pgjobdb.jobs j
    WHERE j.tenant_id = p_tenant_id AND j.job_id = p_job_id FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'native waiting task not found' USING ERRCODE = 'JTC01';
    END IF;
    -- Take the clock reading after any wait for the row lock.
    v_now := clock_timestamp();
    IF NOT EXISTS (SELECT 1 FROM pgjobdb.jobs_with_status s
        WHERE s.tenant_id = p_tenant_id AND s.job_id = p_job_id
          AND s.status = 'READY') THEN
        RAISE EXCEPTION 'waiting task is not ready' USING ERRCODE = 'JTC01';
    END IF;
    IF v_job.cancel_requested OR v_job.lease_expires_at > v_now
        OR v_job.expires_at <= v_now THEN
        RAISE EXCEPTION 'waiting task is not eligible' USING ERRCODE = 'JTC01';
    END IF;
    IF v_job.work_kind IS DISTINCT FROM 'TASK'
        OR v_job.route_job_type IS DISTINCT FROM p_job_type
        OR v_job.task_type IS DISTINCT FROM p_task_type
        OR v_job.resume_job_type IS DISTINCT FROM p_resume_job_type
        OR v_job.task_input_ordinal IS DISTINCT FROM p_input_ordinal
        OR v_job.task_output_ordinal IS DISTINCT FROM p_output_ordinal
        OR v_job.task_input_hash IS DISTINCT FROM p_input_hash THEN
        RAISE EXCEPTION 'waiting task coordinates do not match' USING ERRCODE = 'JTC01';
    END IF;
    RETURN QUERY
    UPDATE pgjobdb.jobs j SET
        lease_id = gen_random_uuid()::TEXT,
        lease_worker_id = p_worker_id,
        lease_route_job_type = v_job.resume_job_type,
        lease_work_kind = 'JOB', lease_task_type = NULL,
        lease_expires_at = v_now + make_interval(secs => p_lease_seconds),
        lease_expiration_count = j.lease_expiration_count +
            CASE WHEN j.lease_id IS NOT NULL THEN 1 ELSE 0 END,
        consecutive_expirations = CASE WHEN j.lease_id IS NOT NULL
            THEN j.consecutive_expirations + 1 ELSE 0 END,
        work_kind = 'JOB',
        route_job_type = v_job.resume_job_type,
        task_type = NULL, resume_job_type = NULL,
        task_input_ordinal = NULL, task_output_ordinal = NULL, task_input_hash = NULL,
        alternate_job_type = NULL, alternate_task_type = NULL,
        alternate_after_seconds = NULL, alternate_at = NULL
    WHERE j.tenant_id = p_tenant_id AND j.job_id = p_job_id
    RETURNING j.lease_id, j.lease_expires_at;
END;
$$;

CREATE OR REPLACE VIEW pgjobdb.native_jobs AS
SELECT
    f.tenant_id, f.job_id, 'ACTIVE'::TEXT AS store, s.status,
    f.job_type, j.route_job_type, j.work_kind, j.task_type,
    j.resume_job_type, j.task_input_ordinal, j.task_output_ordinal,
    j.task_input_hash, j.alternate_job_type, j.alternate_task_type,
    j.alternate_after_seconds, j.alternate_at, j.wait_for, j.available_at,
    NULLIF(j.lease_expires_at, '-infinity'::TIMESTAMPTZ) AS lease_expires_at,
    j.lease_worker_id, j.cancel_requested,
    f.run_policy, f.app_metadata, f.schema_hash, f.parent_job_id,
    f.created_at,
    NULLIF(f.expires_at, 'infinity'::TIMESTAMPTZ) AS expires_at,
    NULL::TIMESTAMPTZ AS archived_at,
    NULL::TEXT AS completion_status, NULL::TEXT AS completion_detail,
    NULL::TEXT AS completion_error_kind, NULL::BOOLEAN AS completion_retryable,
    f.schedule_id, f.schedule_generation, f.schedule_spec_hash,
    f.scheduled_at, f.schedule_run_id, f.schedule_reason,
    f.schedule_manual, f.schedule_backfill_id,
    f.schedule_previous_job_id, f.schedule_failure_history, f.goal_id,
    ARRAY(SELECT gc.goal_id FROM pgjobdb.goal_candidates gc WHERE gc.tenant_id=f.tenant_id AND gc.job_id=f.job_id ORDER BY gc.goal_id) AS other_goal_ids
FROM pgjobdb.job_facts f
JOIN pgjobdb.jobs j USING (tenant_id, job_id)
JOIN pgjobdb.jobs_with_status s USING (tenant_id, job_id)
WHERE j.route_job_type IS NOT NULL
UNION ALL
SELECT
    f.tenant_id, f.job_id, 'ARCHIVED'::TEXT AS store,
    CASE WHEN a.final_cancel_requested OR a.completion_status = 'cancelled'
        THEN 'CANCELLED' ELSE 'COMPLETED' END AS status,
    f.job_type, a.final_route_job_type, a.final_work_kind,
    a.final_task_type, a.final_resume_job_type,
    a.final_task_input_ordinal, a.final_task_output_ordinal,
    a.final_task_input_hash, a.final_alternate_job_type,
    a.final_alternate_task_type, a.final_alternate_after_seconds,
    a.final_alternate_at, a.final_wait_for, a.final_available_at,
    NULLIF(a.final_lease_expires_at, '-infinity'::TIMESTAMPTZ),
    a.final_lease_worker_id, a.final_cancel_requested,
    f.run_policy, f.app_metadata,
    f.schema_hash, f.parent_job_id, f.created_at,
    NULLIF(f.expires_at, 'infinity'::TIMESTAMPTZ), a.archived_at,
    a.completion_status, a.completion_detail,
    a.completion_error_kind, a.completion_retryable,
    f.schedule_id, f.schedule_generation, f.schedule_spec_hash,
    f.scheduled_at, f.schedule_run_id, f.schedule_reason,
    f.schedule_manual, f.schedule_backfill_id,
    f.schedule_previous_job_id, f.schedule_failure_history, f.goal_id,
    ARRAY(SELECT gc.goal_id FROM pgjobdb.goal_candidates gc WHERE gc.tenant_id=f.tenant_id AND gc.job_id=f.job_id ORDER BY gc.goal_id) AS other_goal_ids
FROM pgjobdb.job_facts f
JOIN pgjobdb.jobs_archive a USING (tenant_id, job_id)
WHERE a.final_route_job_type IS NOT NULL;

CREATE OR REPLACE FUNCTION pgjobdb.get_native_job(
    p_tenant_id TEXT, p_job_id TEXT
)
RETURNS TABLE(job_details JSONB,client_payload JSON,client_payload_revision BIGINT,initial_payload_digest TEXT)
LANGUAGE sql STABLE
AS $$
    SELECT to_jsonb(n),c.client_payload,c.revision,c.initial_payload_digest FROM pgjobdb.native_jobs n JOIN pgjobdb.job_client_state c USING(tenant_id,job_id)
    WHERE n.tenant_id = p_tenant_id AND n.job_id = p_job_id;
$$;

CREATE OR REPLACE FUNCTION pgjobdb.get_native_job_status(
    p_tenant_id TEXT, p_job_id TEXT
)
RETURNS SETOF JSONB
LANGUAGE sql STABLE
AS $$
    SELECT jsonb_build_object(
        'tenant_id', n.tenant_id, 'job_id', n.job_id,
        'store', n.store, 'status', n.status,
        'job_type', n.job_type, 'created_at', n.created_at,
        'archived_at', n.archived_at,
        'completion_status', n.completion_status,
        'completion_detail', n.completion_detail,
        'cancel_requested', n.cancel_requested
    ) FROM pgjobdb.native_jobs n
    WHERE n.tenant_id = p_tenant_id AND n.job_id = p_job_id;
$$;

CREATE OR REPLACE FUNCTION pgjobdb.list_native_jobs(
    p_tenant_ids TEXT[],
    p_statuses TEXT[],
    p_stores TEXT[],
    p_job_types TEXT[],
    p_task_selectors JSONB,
    p_job_keys JSONB,
    p_parent_job_ids TEXT[],
    p_root_only BOOLEAN,
    p_metadata_predicates JSONB,
    p_created_after TIMESTAMPTZ,
    p_created_before TIMESTAMPTZ,
    p_before_created_at TIMESTAMPTZ,
    p_before_tenant_id TEXT,
    p_before_job_id TEXT,
    p_limit INTEGER,
    p_available_routes JSONB,
    p_now TIMESTAMPTZ,
    p_include_hidden BOOLEAN, p_goal_ids TEXT[] DEFAULT NULL
)
RETURNS TABLE(job_details JSONB,client_payload JSON,client_payload_revision BIGINT,initial_payload_digest TEXT)
LANGUAGE plpgsql
AS $$
BEGIN
    IF p_tenant_ids IS NULL OR cardinality(p_tenant_ids) = 0 THEN
        RAISE EXCEPTION 'tenant ids are required';
    END IF;
    IF p_limit IS NULL OR p_limit < 1 OR p_limit > 1000 THEN
        RAISE EXCEPTION 'job page limit must be between 1 and 1000';
    END IF;
    IF p_now IS NULL OR jsonb_typeof(p_available_routes) IS DISTINCT FROM 'array'
        OR jsonb_typeof(p_task_selectors) IS DISTINCT FROM 'array'
        OR jsonb_typeof(p_job_keys) IS DISTINCT FROM 'array'
        OR jsonb_typeof(p_metadata_predicates) IS DISTINCT FROM 'array' THEN
        RAISE EXCEPTION 'task, key, and metadata filters must be arrays';
    END IF;
    IF p_root_only AND p_parent_job_ids IS NOT NULL THEN
        RAISE EXCEPTION 'root-only and parent job filters cannot be combined';
    END IF;
    IF (p_before_created_at IS NULL) <> (p_before_tenant_id IS NULL)
        OR (p_before_created_at IS NULL) <> (p_before_job_id IS NULL) THEN
        RAISE EXCEPTION 'job cursor fields must be provided together';
    END IF;

    RETURN QUERY
    SELECT CASE WHEN jsonb_array_length(p_available_routes) > 0
        THEN to_jsonb(n) || '{"status":"READY"}'::JSONB ELSE to_jsonb(n) END,
        c.client_payload,c.revision,c.initial_payload_digest FROM pgjobdb.native_jobs n JOIN pgjobdb.job_client_state c USING(tenant_id,job_id)
    WHERE n.tenant_id = ANY(p_tenant_ids)
      AND (p_goal_ids IS NULL OR n.goal_id=ANY(p_goal_ids) OR EXISTS(SELECT 1 FROM pgjobdb.goal_candidates gc WHERE gc.tenant_id=n.tenant_id AND gc.job_id=n.job_id AND gc.goal_id=ANY(p_goal_ids)))
      AND (p_include_hidden OR EXISTS (SELECT 1 FROM pgjobdb.job_facts f
          WHERE f.tenant_id=n.tenant_id AND f.job_id=n.job_id AND f.hidden_at IS NULL))
      AND (p_statuses IS NULL OR (CASE WHEN jsonb_array_length(p_available_routes) > 0
          THEN 'READY' ELSE n.status END) = ANY(p_statuses))
      AND (jsonb_array_length(p_available_routes) = 0 OR EXISTS (
          SELECT 1 FROM pgjobdb.jobs j
          CROSS JOIN LATERAL pgjobdb.effective_native_route(j, p_now) route
          WHERE j.tenant_id = n.tenant_id AND j.job_id = n.job_id
            AND pgjobdb.job_status_at(j, p_now) = 'READY'
            AND EXISTS (SELECT 1 FROM jsonb_array_elements(p_available_routes) selector(item)
                WHERE selector.item->>'job_type' = route.effective_job_type
                  AND COALESCE(selector.item->>'task_type', '') = COALESCE(route.effective_task_type, ''))
      ))
      AND (p_stores IS NULL OR n.store = ANY(p_stores))
      AND (
          (p_job_types IS NULL AND jsonb_array_length(p_task_selectors) = 0)
          OR n.job_type = ANY(COALESCE(p_job_types, ARRAY[]::TEXT[]))
          OR (n.work_kind = 'TASK' AND EXISTS (
              SELECT 1 FROM jsonb_array_elements(p_task_selectors) AS selector(item)
              WHERE selector.item->>'job_type' = n.route_job_type
                AND selector.item->>'task_type' = n.task_type
          ))
      )
      AND (jsonb_array_length(p_job_keys) = 0 OR EXISTS (
          SELECT 1 FROM jsonb_array_elements(p_job_keys) AS key(item)
          WHERE key.item->>'tenant_id' = n.tenant_id
            AND key.item->>'job_id' = n.job_id
      ))
      AND (p_parent_job_ids IS NULL OR n.parent_job_id = ANY(p_parent_job_ids))
      AND (NOT p_root_only OR n.parent_job_id IS NULL)
      AND NOT EXISTS (
          SELECT 1 FROM jsonb_array_elements(p_metadata_predicates) AS predicate(item)
          WHERE NOT EXISTS (
              SELECT 1 FROM jsonb_array_elements(predicate.item->'values') AS value(item)
              WHERE n.app_metadata #> ARRAY(
                  SELECT jsonb_array_elements_text(predicate.item->'path')
              ) = value.item
          )
      )
      AND (p_created_after IS NULL OR n.created_at > p_created_after)
      AND (p_created_before IS NULL OR n.created_at < p_created_before)
      AND (p_before_created_at IS NULL
        OR (n.created_at, n.tenant_id, n.job_id)
          < (p_before_created_at, p_before_tenant_id, p_before_job_id))
    ORDER BY n.created_at DESC, n.tenant_id DESC, n.job_id DESC
    LIMIT p_limit;
END;
$$;

-- Original native list signature retains ordinary inspection behavior.
CREATE OR REPLACE FUNCTION pgjobdb.list_native_jobs(
    p_tenant_ids TEXT[],
    p_statuses TEXT[],
    p_stores TEXT[],
    p_job_types TEXT[],
    p_task_selectors JSONB,
    p_job_keys JSONB,
    p_parent_job_ids TEXT[],
    p_root_only BOOLEAN,
    p_metadata_predicates JSONB,
    p_created_after TIMESTAMPTZ,
    p_created_before TIMESTAMPTZ,
    p_before_created_at TIMESTAMPTZ,
    p_before_tenant_id TEXT,
    p_before_job_id TEXT,
    p_limit INTEGER
)
RETURNS TABLE(job_details JSONB,client_payload JSON,client_payload_revision BIGINT,initial_payload_digest TEXT)
LANGUAGE sql AS $$
    SELECT * FROM pgjobdb.list_native_jobs($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,'[]'::JSONB,clock_timestamp(),FALSE);
$$;

CREATE OR REPLACE FUNCTION pgjobdb.list_native_jobs(
    p_tenant_ids TEXT[],
    p_statuses TEXT[],
    p_stores TEXT[],
    p_job_types TEXT[],
    p_task_selectors JSONB,
    p_job_keys JSONB,
    p_parent_job_ids TEXT[],
    p_root_only BOOLEAN,
    p_metadata_predicates JSONB,
    p_created_after TIMESTAMPTZ,
    p_created_before TIMESTAMPTZ,
    p_before_created_at TIMESTAMPTZ,
    p_before_tenant_id TEXT,
    p_before_job_id TEXT,
    p_limit INTEGER,
    p_available_routes JSONB,
    p_now TIMESTAMPTZ
)
RETURNS TABLE(job_details JSONB,client_payload JSON,client_payload_revision BIGINT,initial_payload_digest TEXT)
LANGUAGE sql AS $$
    SELECT * FROM pgjobdb.list_native_jobs($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,FALSE);
$$;

CREATE OR REPLACE FUNCTION pgjobdb.list_native_schedule_runs(
    p_tenant_id TEXT,
    p_schedule_id TEXT,
    p_scheduled_after TIMESTAMPTZ,
    p_scheduled_before TIMESTAMPTZ,
    p_statuses TEXT[],
    p_before_scheduled_at TIMESTAMPTZ,
    p_before_job_id TEXT,
    p_limit INTEGER
)
RETURNS TABLE(job_details JSONB, client_payload JSON, client_payload_revision BIGINT, initial_payload_digest TEXT)
LANGUAGE plpgsql
AS $$
BEGIN
    IF p_tenant_id IS NULL OR p_tenant_id = '' OR
        p_schedule_id IS NULL OR p_schedule_id = '' THEN
        RAISE EXCEPTION 'tenant id and schedule id are required';
    END IF;
    IF p_limit IS NULL OR p_limit < 1 OR p_limit > 1000 THEN
        RAISE EXCEPTION 'schedule run page limit must be between 1 and 1000';
    END IF;
    IF (p_before_scheduled_at IS NULL) <> (p_before_job_id IS NULL) THEN
        RAISE EXCEPTION 'schedule run cursor fields must be provided together';
    END IF;
    RETURN QUERY
    SELECT to_jsonb(n),c.client_payload,c.revision,c.initial_payload_digest FROM pgjobdb.native_jobs n JOIN pgjobdb.job_client_state c USING (tenant_id,job_id)
    WHERE n.tenant_id = p_tenant_id AND n.schedule_id = p_schedule_id
      AND EXISTS (SELECT 1 FROM pgjobdb.job_facts f
          WHERE f.tenant_id=n.tenant_id AND f.job_id=n.job_id AND f.hidden_at IS NULL)
      AND n.scheduled_at IS NOT NULL
      AND (p_scheduled_after IS NULL OR n.scheduled_at >= p_scheduled_after)
      AND (p_scheduled_before IS NULL OR n.scheduled_at <= p_scheduled_before)
      AND (p_statuses IS NULL OR n.status = ANY(p_statuses))
      AND (p_before_scheduled_at IS NULL OR
        (n.scheduled_at, n.job_id) < (p_before_scheduled_at, p_before_job_id))
    ORDER BY n.scheduled_at DESC, n.job_id DESC
    LIMIT p_limit;
END;
$$;

-- One statement/transaction for the full selection; never page or skip locked jobs.
CREATE OR REPLACE FUNCTION pgjobdb.delete_native_jobs(
    p_tenant_id TEXT, p_job_ids TEXT[], p_created_before TIMESTAMPTZ,
    p_metadata_predicates JSONB, p_all BOOLEAN, p_dry_run BOOLEAN, p_now TIMESTAMPTZ
)
RETURNS TABLE(matched_count BIGINT, deleted_count BIGINT)
LANGUAGE plpgsql AS $$
DECLARE
    v_id TEXT;
    v_job pgjobdb.jobs%ROWTYPE;
    v_count BIGINT := 0;
BEGIN
    PERFORM pgjobdb._goal_lock(p_tenant_id);
    IF p_tenant_id IS NULL OR btrim(p_tenant_id) = '' OR p_now IS NULL
        OR p_all IS NULL OR p_dry_run IS NULL
        OR jsonb_typeof(p_metadata_predicates) IS DISTINCT FROM 'array' THEN
        RAISE EXCEPTION 'invalid deletion request';
    END IF;
    IF p_all = (COALESCE(cardinality(p_job_ids),0) > 0 OR p_created_before IS NOT NULL
        OR jsonb_array_length(p_metadata_predicates) > 0) THEN
        RAISE EXCEPTION 'choose deletion selectors or all, exclusively';
    END IF;
    IF p_dry_run THEN
        RETURN QUERY SELECT count(*),0::BIGINT FROM pgjobdb.job_facts f
        WHERE f.tenant_id=p_tenant_id AND f.hidden_at IS NULL
          AND (COALESCE(cardinality(p_job_ids),0)=0 OR f.job_id=ANY(p_job_ids))
          AND (p_created_before IS NULL OR f.created_at<p_created_before)
          AND NOT EXISTS (
              SELECT 1 FROM jsonb_array_elements(p_metadata_predicates) predicate(item)
              WHERE NOT EXISTS (SELECT 1 FROM jsonb_array_elements(predicate.item->'values') value(item)
                  WHERE f.app_metadata #> ARRAY(SELECT jsonb_array_elements_text(predicate.item->'path')) = value.item)
          );
        RETURN;
    END IF;

    -- Identity locks serialize repeated deletion and parent-authorized creation.
    -- Active-row locks serialize acquisition and all lease mutations. A row that
    -- completed while we waited is already archived and retains that outcome.
    FOR v_id IN SELECT f.job_id FROM pgjobdb.job_facts f
        WHERE f.tenant_id=p_tenant_id AND f.hidden_at IS NULL
          AND (COALESCE(cardinality(p_job_ids),0)=0 OR f.job_id=ANY(p_job_ids))
          AND (p_created_before IS NULL OR f.created_at<p_created_before)
          AND NOT EXISTS (
              SELECT 1 FROM jsonb_array_elements(p_metadata_predicates) predicate(item)
              WHERE NOT EXISTS (SELECT 1 FROM jsonb_array_elements(predicate.item->'values') value(item)
                  WHERE f.app_metadata #> ARRAY(SELECT jsonb_array_elements_text(predicate.item->'path')) = value.item)
          )
        ORDER BY f.job_id FOR UPDATE OF f
    LOOP
        SELECT * INTO v_job FROM pgjobdb.jobs
        WHERE tenant_id=p_tenant_id AND job_id=v_id FOR UPDATE;
        IF FOUND THEN
            INSERT INTO pgjobdb.jobs_archive (
                archived_at,tenant_id,job_id,alternate_after_seconds,alternate_at,wait_for,
                payload,metadata,created_at,expires_at,lease_id,lease_expiration_count,consecutive_expirations,
                cancel_requested,cancel_requested_by,cancel_requested_at,completion_status,completion_detail,
                completion_error_kind,completion_retryable,
                final_route_job_type,final_work_kind,final_task_type,final_resume_job_type,
                final_task_input_ordinal,final_task_output_ordinal,final_task_input_hash,
                final_alternate_job_type,final_alternate_task_type,final_alternate_after_seconds,final_alternate_at,
                final_wait_for,final_available_at,final_lease_expires_at,final_lease_worker_id,final_cancel_requested
            ) VALUES (
                p_now,v_job.tenant_id,v_job.job_id,v_job.alternate_after_seconds,v_job.alternate_at,v_job.wait_for,
                v_job.payload,v_job.metadata,v_job.created_at,v_job.expires_at,NULL,v_job.lease_expiration_count,0,
                TRUE,'job-deletion',p_now,'cancelled','job deleted',NULL,FALSE,
                v_job.route_job_type,v_job.work_kind,v_job.task_type,v_job.resume_job_type,
                v_job.task_input_ordinal,v_job.task_output_ordinal,v_job.task_input_hash,
                v_job.alternate_job_type,v_job.alternate_task_type,v_job.alternate_after_seconds,v_job.alternate_at,
                v_job.wait_for,v_job.available_at,'-infinity',NULL,TRUE
            );
            DELETE FROM pgjobdb.jobs WHERE tenant_id=p_tenant_id AND job_id=v_id;
            PERFORM pgjobdb._update_waiters_for_completion(p_tenant_id,v_id);
        END IF;
        UPDATE pgjobdb.job_facts SET hidden_at=p_now WHERE tenant_id=p_tenant_id AND job_id=v_id;
        IF pgjobdb.is_trace_enabled() THEN
            INSERT INTO pgjobdb.jobs_trace (tenant_id,job_id,event_type,worker_id,event_at,input_data,output_data)
            VALUES(p_tenant_id,v_id,'job_deleted','job-deletion',p_now,'{}'::JSONB,'{}'::JSONB);
        END IF;
        v_count:=v_count+1;
    END LOOP;
    RETURN QUERY SELECT v_count,v_count;
END;
$$;

COMMIT;

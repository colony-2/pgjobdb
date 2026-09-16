BEGIN;

CREATE SCHEMA IF NOT EXISTS pgjobdb;

SET search_path = pgjobdb, public;

CREATE TABLE IF NOT EXISTS pgjobdb.installation (
    name TEXT PRIMARY KEY CHECK (name = 'pgjobdb'),
    format_version INTEGER NOT NULL CHECK (format_version = 1),
    installed_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

INSERT INTO pgjobdb.installation (name, format_version)
VALUES ('pgjobdb', 1)
ON CONFLICT (name) DO NOTHING;

CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE SEQUENCE IF NOT EXISTS pgjobdb.jobs_trace_id_seq;

CREATE TABLE IF NOT EXISTS pgjobdb.jobs (
    tenant_id TEXT NOT NULL,
    job_id TEXT NOT NULL,
    next_need TEXT NOT NULL,
    alternate_next_need TEXT,
    alternate_after_seconds INTEGER,
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
    CONSTRAINT jobs_alternate_after_seconds_nonnegative CHECK (alternate_after_seconds IS NULL OR alternate_after_seconds >= 0)
);

ALTER TABLE pgjobdb.jobs
ADD COLUMN IF NOT EXISTS metadata JSONB;

ALTER TABLE pgjobdb.jobs
ALTER COLUMN metadata SET DEFAULT '{}'::JSONB;

UPDATE pgjobdb.jobs
SET metadata = '{}'::JSONB
WHERE metadata IS NULL;

ALTER TABLE pgjobdb.jobs
ALTER COLUMN metadata SET NOT NULL;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'jobs_metadata_is_object'
          AND conrelid = 'pgjobdb.jobs'::regclass
    ) THEN
        ALTER TABLE pgjobdb.jobs
        ADD CONSTRAINT jobs_metadata_is_object CHECK (jsonb_typeof(metadata) = 'object');
    END IF;
END;
$$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'jobs_metadata_size_limit'
          AND conrelid = 'pgjobdb.jobs'::regclass
    ) THEN
        ALTER TABLE pgjobdb.jobs
        ADD CONSTRAINT jobs_metadata_size_limit CHECK (pg_column_size(metadata) <= 8192);
    END IF;
END;
$$;

CREATE TABLE IF NOT EXISTS pgjobdb.jobs_archive (
    archived_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    tenant_id TEXT NOT NULL,
    job_id TEXT NOT NULL,
    next_need TEXT NOT NULL,
    alternate_next_need TEXT,
    alternate_after_seconds INTEGER,
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
    CONSTRAINT jobs_archive_alternate_after_seconds_nonnegative CHECK (alternate_after_seconds IS NULL OR alternate_after_seconds >= 0)
);

ALTER TABLE pgjobdb.jobs_archive
ADD COLUMN IF NOT EXISTS metadata JSONB;

ALTER TABLE pgjobdb.jobs_archive
ALTER COLUMN metadata SET DEFAULT '{}'::JSONB;

UPDATE pgjobdb.jobs_archive
SET metadata = '{}'::JSONB
WHERE metadata IS NULL;

ALTER TABLE pgjobdb.jobs_archive
ALTER COLUMN metadata SET NOT NULL;

ALTER TABLE pgjobdb.jobs_archive
ADD COLUMN IF NOT EXISTS completion_status TEXT;

ALTER TABLE pgjobdb.jobs_archive
ALTER COLUMN completion_status SET DEFAULT 'succeeded';

UPDATE pgjobdb.jobs_archive
SET completion_status = CASE
    WHEN cancel_requested THEN 'cancelled'
    ELSE 'succeeded'
END
WHERE completion_status IS NULL;

ALTER TABLE pgjobdb.jobs_archive
ALTER COLUMN completion_status SET NOT NULL;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'pgjobdb'
          AND table_name = 'jobs_archive'
          AND column_name = 'failure_detail'
    ) THEN
        IF NOT EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = 'pgjobdb'
              AND table_name = 'jobs_archive'
              AND column_name = 'completion_detail'
        ) THEN
            ALTER TABLE pgjobdb.jobs_archive
            RENAME COLUMN failure_detail TO completion_detail;
        ELSE
            UPDATE pgjobdb.jobs_archive
            SET completion_detail = COALESCE(completion_detail, failure_detail)
            WHERE failure_detail IS NOT NULL;

            ALTER TABLE pgjobdb.jobs_archive
            DROP COLUMN failure_detail;
        END IF;
    END IF;
END;
$$;

ALTER TABLE pgjobdb.jobs_archive
ADD COLUMN IF NOT EXISTS completion_detail TEXT;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'jobs_archive_metadata_is_object'
          AND conrelid = 'pgjobdb.jobs_archive'::regclass
    ) THEN
        ALTER TABLE pgjobdb.jobs_archive
        ADD CONSTRAINT jobs_archive_metadata_is_object CHECK (jsonb_typeof(metadata) = 'object');
    END IF;
END;
$$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'jobs_archive_metadata_size_limit'
          AND conrelid = 'pgjobdb.jobs_archive'::regclass
    ) THEN
        ALTER TABLE pgjobdb.jobs_archive
        ADD CONSTRAINT jobs_archive_metadata_size_limit CHECK (pg_column_size(metadata) <= 8192);
    END IF;
END;
$$;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'jobs_archive_completion_status_valid'
          AND conrelid = 'pgjobdb.jobs_archive'::regclass
    ) THEN
        ALTER TABLE pgjobdb.jobs_archive
        DROP CONSTRAINT jobs_archive_completion_status_valid;
    END IF;
END;
$$;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'jobs_archive_failure_detail_requires_failed'
          AND conrelid = 'pgjobdb.jobs_archive'::regclass
    ) THEN
        ALTER TABLE pgjobdb.jobs_archive
        DROP CONSTRAINT jobs_archive_failure_detail_requires_failed;
    END IF;
END;
$$;

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
ON pgjobdb.jobs(tenant_id, next_need, created_at)
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

DROP VIEW IF EXISTS pgjobdb.jobs_friendly_status;
DROP VIEW IF EXISTS pgjobdb.jobs_with_status CASCADE;
DROP FUNCTION IF EXISTS pgjobdb.submit_job(TEXT, TEXT, TEXT, TEXT, TEXT[], JSONB, JSONB, TEXT, TIMESTAMPTZ, TIMESTAMPTZ, TEXT, INTEGER);
DROP FUNCTION IF EXISTS pgjobdb.get_work(TEXT, TEXT[], TEXT[], INTEGER, INTEGER, TEXT[], TEXT[]);
DROP FUNCTION IF EXISTS pgjobdb.get_job_lease(TEXT, TEXT, TEXT, TEXT[], INTEGER);
DROP INDEX IF EXISTS pgjobdb.idx_jobs_tenant_active_singleton;
ALTER TABLE pgjobdb.jobs DROP COLUMN IF EXISTS singleton_key;
ALTER TABLE pgjobdb.jobs_archive DROP COLUMN IF EXISTS singleton_key;

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

CREATE OR REPLACE VIEW pgjobdb.jobs_with_status AS
WITH status_calc AS (
    SELECT
        j.*,
        CASE
            WHEN j.lease_expires_at > clock_timestamp() THEN 'ACTIVE'
            WHEN j.cancel_requested THEN 'CANCELLED'
            WHEN j.available_at > clock_timestamp() THEN 'AWAITING_FUTURE'
            WHEN EXISTS (
                SELECT 1
                FROM unnest(j.wait_for) AS dep_job_id
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM pgjobdb.jobs_archive ja
                    WHERE ja.tenant_id = j.tenant_id
                      AND ja.job_id = dep_job_id
                )
            ) THEN 'PENDING_JOBS'
            WHEN j.consecutive_expirations >= pgjobdb.crash_concern_threshold() THEN 'CRASH_CONCERN'
            WHEN j.expires_at <= clock_timestamp() THEN 'EXPIRED'
            ELSE 'READY'
        END AS status
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
SELECT
    rc.*,
    CASE
        WHEN rc.status = 'READY'
             AND rc.alternate_next_need IS NOT NULL
             AND rc.alternate_after_seconds IS NOT NULL
             AND rc.ready_since IS NOT NULL
             AND clock_timestamp() >= rc.ready_since + make_interval(secs => rc.alternate_after_seconds)
            THEN rc.alternate_next_need
        ELSE rc.next_need
    END AS effective_next_need
FROM ready_calc rc;

CREATE OR REPLACE VIEW pgjobdb.jobs_friendly_status AS
SELECT
    jws.tenant_id,
    jws.job_id,
    jws.status,
    jws.effective_next_need,
    jws.alternate_next_need,
    jws.alternate_after_seconds,
    jws.created_at AS creation_dt,
    jws.ready_since,
    CASE WHEN jws.status = 'PENDING_JOBS' THEN jws.wait_for ELSE NULL END AS pending_jobs,
    CASE WHEN jws.status = 'AWAITING_FUTURE' THEN jws.available_at ELSE NULL END AS sleep_until,
    CASE WHEN jws.status = 'ACTIVE' THEN jws.lease_id ELSE NULL END AS worker_id,
    CASE WHEN jws.status = 'CANCELLED' THEN jws.cancel_requested_at ELSE NULL END AS cancelled_at,
    CASE WHEN jws.status = 'CANCELLED' THEN jws.cancel_requested_by ELSE NULL END AS cancelled_by,
    jws.expires_at,
    jws.payload
FROM pgjobdb.jobs_with_status jws;

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

CREATE OR REPLACE FUNCTION pgjobdb._notify_need(
    p_next_need TEXT,
    p_job_id TEXT
)
RETURNS VOID
LANGUAGE plpgsql
AS $$
BEGIN
    IF p_next_need IS NULL OR p_job_id IS NULL THEN
        RETURN;
    END IF;

    IF pgjobdb.is_notify_enabled() THEN
        PERFORM pg_notify(format('pgjobdb.need.%s', p_next_need), p_job_id);
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
        next_need,
        alternate_next_need,
        alternate_after_seconds,
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
        p_locked_job.next_need,
        p_locked_job.alternate_next_need,
        p_locked_job.alternate_after_seconds,
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
RETURNS TABLE(job_id TEXT, next_need TEXT, became_unblocked BOOLEAN)
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
            SELECT j.*
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
            SET wait_for = (
                SELECT COALESCE(array_agg(val ORDER BY ord), ARRAY[]::TEXT[])
                FROM unnest(j.wait_for) WITH ORDINALITY AS pending(val, ord)
                WHERE pending.val IS NOT NULL
                  AND NOT (pending.val = ANY(p_completed_jobs))
            )
            FROM targets t
            WHERE j.tenant_id = t.tenant_id
              AND j.job_id = t.job_id
            RETURNING
                j.job_id,
                j.next_need,
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
            PERFORM pgjobdb._notify_need(v_row.next_need, v_row.job_id);
        END IF;

        job_id := v_row.job_id;
        next_need := v_row.next_need;
        became_unblocked := v_row.now_unblocked;
        RETURN NEXT;
    END LOOP;
END;
$$;

CREATE OR REPLACE FUNCTION pgjobdb._update_waiters_for_completion(
    p_tenant_id TEXT,
    p_completed_job_id TEXT
)
RETURNS TABLE(job_id TEXT, next_need TEXT, became_unblocked BOOLEAN)
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

CREATE OR REPLACE FUNCTION pgjobdb._reschedule_locked_job(
    p_locked_job pgjobdb.jobs_with_status,
    p_worker_id TEXT,
    p_next_need TEXT,
    p_wait_for TEXT[] DEFAULT '{}'::TEXT[],
    p_available_at TIMESTAMPTZ DEFAULT clock_timestamp(),
    p_payload JSONB DEFAULT NULL,
    p_trace_context JSONB DEFAULT '{}'::JSONB,
    p_alternate_next_need TEXT DEFAULT NULL,
    p_alternate_after_seconds INTEGER DEFAULT NULL,
    p_set_alternate BOOLEAN DEFAULT FALSE
)
RETURNS TABLE(job_id TEXT, next_need TEXT, wait_for TEXT[], available_at TIMESTAMPTZ)
LANGUAGE plpgsql
AS $$
DECLARE
    v_wait_for TEXT[];
    v_available_at TIMESTAMPTZ := COALESCE(p_available_at, clock_timestamp());
    v_expires_at TIMESTAMPTZ;
    v_now TIMESTAMPTZ := clock_timestamp();
    v_payload JSONB := COALESCE(p_payload, p_locked_job.payload);
    v_alternate_next_need TEXT := p_locked_job.alternate_next_need;
    v_alternate_after_seconds INTEGER := p_locked_job.alternate_after_seconds;
BEGIN
    v_wait_for := pgjobdb.normalize_wait_for(p_locked_job.tenant_id, p_wait_for);

    IF p_payload IS NOT NULL THEN
        IF jsonb_typeof(p_payload) IS DISTINCT FROM 'object' THEN
            RAISE EXCEPTION 'payload must be a JSON object';
        END IF;

        IF pg_column_size(p_payload) > 512 THEN
            RAISE EXCEPTION 'payload exceeds 512 bytes';
        END IF;
    END IF;

    IF p_set_alternate OR p_alternate_next_need IS NOT NULL OR p_alternate_after_seconds IS NOT NULL THEN
        IF p_alternate_after_seconds IS NOT NULL AND p_alternate_after_seconds < 0 THEN
            RAISE EXCEPTION 'alternate_after_seconds must be non-negative';
        END IF;
        v_alternate_next_need := p_alternate_next_need;
        v_alternate_after_seconds := p_alternate_after_seconds;
    END IF;

    UPDATE pgjobdb.jobs j
    SET next_need = p_next_need,
        alternate_next_need = v_alternate_next_need,
        alternate_after_seconds = v_alternate_after_seconds,
        wait_for = v_wait_for,
        available_at = v_available_at,
        payload = v_payload,
        consecutive_expirations = 0,
        lease_id = NULL,
        lease_expires_at = '-infinity'
    WHERE j.tenant_id = p_locked_job.tenant_id
      AND j.job_id = p_locked_job.job_id
    RETURNING j.job_id,
              j.next_need,
              j.wait_for,
              j.available_at,
              j.expires_at
    INTO job_id, next_need, wait_for, available_at, v_expires_at;

    IF NOT p_locked_job.cancel_requested AND v_expires_at > v_now THEN
        PERFORM pgjobdb._notify_need(next_need, job_id);
    END IF;

    PERFORM pgjobdb._emit_trace_event(
        p_locked_job.tenant_id,
        job_id,
        'reschedule_job',
        p_worker_id,
        jsonb_build_object(
            'tenant_id', p_locked_job.tenant_id,
            'job_id', p_locked_job.job_id,
            'worker_id', p_worker_id,
            'previous_next_need', p_locked_job.next_need,
            'previous_wait_for', p_locked_job.wait_for,
            'previous_available_at', p_locked_job.available_at,
            'previous_expires_at', p_locked_job.expires_at,
            'next_need', p_next_need,
            'wait_for', v_wait_for,
            'available_at', v_available_at,
            'expires_at', v_expires_at,
            'alternate_next_need', v_alternate_next_need,
            'alternate_after_seconds', v_alternate_after_seconds
        ) || COALESCE(p_trace_context, '{}'::JSONB),
        jsonb_build_object(
            'job_id', job_id,
            'next_need', next_need,
            'wait_for', wait_for,
            'available_at', available_at,
            'expires_at', v_expires_at,
            'alternate_next_need', v_alternate_next_need,
            'alternate_after_seconds', v_alternate_after_seconds
        )
    );

    RETURN NEXT;
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

CREATE OR REPLACE FUNCTION pgjobdb.submit_job(
    p_tenant_id TEXT,
    p_job_id TEXT,
    p_worker_id TEXT,
    p_next_need TEXT,
    p_wait_for TEXT[] DEFAULT '{}'::TEXT[],
    p_payload JSONB DEFAULT '{}'::JSONB,
    p_metadata JSONB DEFAULT '{}'::JSONB,
    p_available_at TIMESTAMPTZ DEFAULT clock_timestamp(),
    p_expires_at TIMESTAMPTZ DEFAULT NULL,
    p_alternate_next_need TEXT DEFAULT NULL,
    p_alternate_after_seconds INTEGER DEFAULT NULL
)
RETURNS TABLE(tenant_id TEXT, job_id TEXT, next_need TEXT, wait_for TEXT[], payload JSONB, metadata JSONB, available_at TIMESTAMPTZ)
LANGUAGE plpgsql
AS $$
DECLARE
    v_wait_for TEXT[];
    v_effective_available TIMESTAMPTZ := COALESCE(p_available_at, clock_timestamp());
    v_expires_at TIMESTAMPTZ := COALESCE(p_expires_at, 'infinity');
    v_cancel_requested BOOLEAN;
    v_now TIMESTAMPTZ := clock_timestamp();
    v_payload JSONB := COALESCE(p_payload, '{}'::JSONB);
    v_metadata JSONB := COALESCE(p_metadata, '{}'::JSONB);
BEGIN
    IF p_tenant_id IS NULL THEN
        RAISE EXCEPTION 'tenant_id cannot be NULL';
    END IF;

    IF EXISTS (SELECT 1 FROM pgjobdb.jobs_archive ja WHERE ja.tenant_id = p_tenant_id AND ja.job_id = p_job_id) THEN
        RAISE EXCEPTION 'job_id %/% has already completed and cannot be resubmitted', p_tenant_id, p_job_id;
    END IF;

    v_wait_for := pgjobdb.normalize_wait_for(p_tenant_id, p_wait_for);

    IF jsonb_typeof(v_payload) IS DISTINCT FROM 'object' THEN
        RAISE EXCEPTION 'payload must be a JSON object';
    END IF;

    IF pg_column_size(v_payload) > 512 THEN
        RAISE EXCEPTION 'payload exceeds 512 bytes';
    END IF;

    IF jsonb_typeof(v_metadata) IS DISTINCT FROM 'object' THEN
        RAISE EXCEPTION 'metadata must be a JSON object';
    END IF;

    IF pg_column_size(v_metadata) > 8192 THEN
        RAISE EXCEPTION 'metadata exceeds 8192 bytes';
    END IF;

    IF p_alternate_after_seconds IS NOT NULL AND p_alternate_after_seconds < 0 THEN
        RAISE EXCEPTION 'alternate_after_seconds must be non-negative';
    END IF;

    INSERT INTO pgjobdb.jobs (
        tenant_id,
        job_id,
        next_need,
        alternate_next_need,
        alternate_after_seconds,
        wait_for,
        payload,
        metadata,
        available_at,
        expires_at
    )
    VALUES (
        p_tenant_id,
        p_job_id,
        p_next_need,
        p_alternate_next_need,
        p_alternate_after_seconds,
        v_wait_for,
        v_payload,
        v_metadata,
        v_effective_available,
        v_expires_at
    )
    RETURNING pgjobdb.jobs.tenant_id,
              pgjobdb.jobs.job_id,
              pgjobdb.jobs.next_need,
              pgjobdb.jobs.wait_for,
              pgjobdb.jobs.payload,
              pgjobdb.jobs.metadata,
              pgjobdb.jobs.available_at,
              pgjobdb.jobs.cancel_requested
    INTO tenant_id, job_id, next_need, wait_for, payload, metadata, available_at, v_cancel_requested;

    IF NOT v_cancel_requested AND v_expires_at > v_now THEN
        PERFORM pgjobdb._notify_need(next_need, job_id);
    END IF;

    PERFORM pgjobdb._emit_trace_event(
        p_tenant_id,
        p_job_id,
        'job_submitted',
        p_worker_id,
        jsonb_build_object(
            'tenant_id', p_tenant_id,
            'job_id', p_job_id,
            'worker_id', p_worker_id,
            'next_need', p_next_need,
            'wait_for', v_wait_for,
            'available_at', v_effective_available,
            'expires_at', v_expires_at,
            'alternate_next_need', p_alternate_next_need,
            'alternate_after_seconds', p_alternate_after_seconds
        ),
        jsonb_build_object(
            'tenant_id', tenant_id,
            'job_id', job_id,
            'next_need', next_need,
            'wait_for', wait_for,
            'available_at', available_at,
            'expires_at', v_expires_at,
            'alternate_next_need', p_alternate_next_need,
            'alternate_after_seconds', p_alternate_after_seconds
        )
    );

    RETURN NEXT;
END;
$$;

CREATE OR REPLACE FUNCTION pgjobdb.cancel_job(
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

CREATE OR REPLACE FUNCTION pgjobdb.validate_metadata_filters(
    p_metadata_filter_paths TEXT[],
    p_metadata_filter_values TEXT[]
)
RETURNS VOID
LANGUAGE plpgsql
AS $$
DECLARE
    v_count INTEGER;
    v_idx INTEGER;
    v_path TEXT[];
    v_values TEXT[];
BEGIN
    IF p_metadata_filter_paths IS NULL AND p_metadata_filter_values IS NULL THEN
        RETURN;
    END IF;

    IF p_metadata_filter_paths IS NULL OR p_metadata_filter_values IS NULL THEN
        RAISE EXCEPTION 'metadata filter paths and values must all be provided together';
    END IF;

    v_count := COALESCE(array_length(p_metadata_filter_paths, 1), 0);
    IF v_count = 0 THEN
        IF COALESCE(array_length(p_metadata_filter_values, 1), 0) <> 0 THEN
            RAISE EXCEPTION 'metadata filter paths and values must have matching lengths';
        END IF;
        RETURN;
    END IF;

    IF COALESCE(array_length(p_metadata_filter_values, 1), 0) <> v_count THEN
        RAISE EXCEPTION 'metadata filter paths and values must have matching lengths';
    END IF;

    FOR v_idx IN 1..v_count LOOP
        BEGIN
            v_path := p_metadata_filter_paths[v_idx]::TEXT[];
        EXCEPTION
            WHEN others THEN
                RAISE EXCEPTION 'metadata filter path must be a valid text[] literal';
        END;

        IF COALESCE(array_length(v_path, 1), 0) = 0 THEN
            RAISE EXCEPTION 'metadata filter path must be a non-empty array';
        END IF;

        IF EXISTS (
            SELECT 1
            FROM unnest(v_path) AS elem(value)
            WHERE elem.value IS NULL OR elem.value = ''
        ) THEN
            RAISE EXCEPTION 'metadata filter path elements must be non-empty strings';
        END IF;

        BEGIN
            v_values := p_metadata_filter_values[v_idx]::TEXT[];
        EXCEPTION
            WHEN others THEN
                RAISE EXCEPTION 'metadata filter values must be a valid text[] literal';
        END;

        IF COALESCE(array_length(v_values, 1), 0) = 0 THEN
            RAISE EXCEPTION 'metadata filter values must be a non-empty array';
        END IF;
    END LOOP;
END;
$$;

CREATE OR REPLACE FUNCTION pgjobdb.get_work(
    p_worker_id TEXT,
    p_worker_caps TEXT[],
    p_tenant_ids TEXT[] DEFAULT NULL,
    p_lease_seconds INTEGER DEFAULT 60,
    p_limit_jobs INTEGER DEFAULT 1,
    p_metadata_filter_paths TEXT[] DEFAULT NULL,
    p_metadata_filter_values TEXT[] DEFAULT NULL
)
RETURNS TABLE(
    tenant_id TEXT,
    job_id TEXT,
    lease_id TEXT,
    next_need TEXT,
    wait_for TEXT[],
    payload JSONB,
    available_at TIMESTAMPTZ,
    lease_expires_at TIMESTAMPTZ
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_caps TEXT[] := p_worker_caps;
    v_now TIMESTAMPTZ := clock_timestamp();
    v_expires TIMESTAMPTZ;
    v_count INTEGER := 0;
    v_cap TEXT;
    v_tenant_id TEXT;
    v_previous_lease_id TEXT;
    v_previous_lease_expires_at TIMESTAMPTZ;
    v_previous_lease_expired BOOLEAN;
    v_total_expirations BIGINT;
    v_consecutive_expirations BIGINT;
    v_stored_next_need TEXT;
    v_pivoted BOOLEAN;
    v_alternate_next_need TEXT;
    v_alternate_after_seconds INTEGER;
    v_ready_since TIMESTAMPTZ;
    v_sql TEXT;
    v_filter_count INTEGER;
    v_filter_idx INTEGER;
    v_filter_path TEXT[];
    v_filter_values TEXT[];
BEGIN
    IF v_caps IS NULL OR array_length(v_caps, 1) = 0 THEN
        RAISE EXCEPTION 'worker_caps cannot be empty';
    END IF;

    IF p_lease_seconds IS NULL OR p_lease_seconds <= 0 THEN
        RAISE EXCEPTION 'lease_seconds must be positive';
    END IF;

    IF p_limit_jobs IS NULL OR p_limit_jobs <= 0 THEN
        RAISE EXCEPTION 'limit_jobs must be positive';
    END IF;

    PERFORM pgjobdb.validate_metadata_filters(
        p_metadata_filter_paths,
        p_metadata_filter_values
    );

    v_expires := v_now + make_interval(secs => p_lease_seconds);

    v_sql := $sql$
        WITH candidates AS (
            SELECT jws.*,
                   (jws.lease_id IS NOT NULL AND jws.lease_expires_at <= $1) AS lease_was_expired
            FROM pgjobdb.jobs_with_status jws
            WHERE jws.status = 'READY'
              AND jws.effective_next_need = ANY($2)
              AND ($3 IS NULL OR array_length($3, 1) IS NULL OR jws.tenant_id = ANY($3))
    $sql$;

    v_filter_count := COALESCE(array_length(p_metadata_filter_paths, 1), 0);
    FOR v_filter_idx IN 1..v_filter_count LOOP
        v_filter_path := p_metadata_filter_paths[v_filter_idx]::TEXT[];
        v_filter_values := p_metadata_filter_values[v_filter_idx]::TEXT[];
        v_sql := v_sql || format(
            ' AND (jws.metadata #>> %L::text[]) = ANY(%L::text[])',
            v_filter_path,
            v_filter_values
        );
    END LOOP;

    v_sql := v_sql || $sql$
            ORDER BY jws.created_at ASC
            LIMIT $4
            FOR UPDATE SKIP LOCKED
        )
        UPDATE pgjobdb.jobs j
        SET
            lease_expiration_count = CASE
                WHEN c.lease_was_expired THEN j.lease_expiration_count + 1
                ELSE j.lease_expiration_count
            END,
            consecutive_expirations = CASE
                WHEN c.lease_was_expired THEN j.consecutive_expirations + 1
                ELSE j.consecutive_expirations
            END,
            lease_id = gen_random_uuid()::TEXT,
            lease_expires_at = $5
        FROM candidates c
        WHERE j.tenant_id = c.tenant_id
          AND j.job_id = c.job_id
        RETURNING j.tenant_id,
                  j.job_id,
                  j.lease_id,
                  c.effective_next_need,
                  j.wait_for,
                  j.payload,
                  j.available_at,
                  j.lease_expires_at,
                  c.lease_id AS previous_lease_id,
                  c.lease_expires_at AS previous_lease_expires_at,
                  c.lease_was_expired AS lease_previously_expired,
                  j.lease_expiration_count,
                  j.consecutive_expirations,
                  c.next_need AS stored_next_need,
                  c.effective_next_need <> c.next_need AS pivoted_to_alternate,
                  c.alternate_next_need,
                  c.alternate_after_seconds,
                  c.ready_since
    $sql$;

    FOR v_tenant_id, job_id, lease_id, next_need, wait_for, payload, available_at, lease_expires_at,
        v_previous_lease_id, v_previous_lease_expires_at, v_previous_lease_expired,
        v_total_expirations, v_consecutive_expirations, v_stored_next_need, v_pivoted,
        v_alternate_next_need, v_alternate_after_seconds, v_ready_since IN
        EXECUTE v_sql
        USING v_now, v_caps, p_tenant_ids, p_limit_jobs, v_expires
    LOOP

        IF v_previous_lease_expired THEN
            PERFORM pgjobdb._emit_trace_event(
                v_tenant_id,
                job_id,
                'lease_expiration_counter_incremented',
                p_worker_id,
                jsonb_build_object(
                    'tenant_id', v_tenant_id,
                    'worker_id', p_worker_id,
                    'worker_caps', v_caps,
                    'previous_lease_id', v_previous_lease_id,
                    'previous_lease_expires_at', v_previous_lease_expires_at,
                    'lease_expiration_count', v_total_expirations,
                    'consecutive_expirations', v_consecutive_expirations
                )
            );
        END IF;

        v_count := v_count + 1;

        PERFORM pgjobdb._emit_trace_event(
            v_tenant_id,
            job_id,
            'job_retrieved',
            p_worker_id,
            jsonb_build_object(
                'tenant_id', v_tenant_id,
                'worker_id', p_worker_id,
                'worker_caps', v_caps,
                'tenant_ids', p_tenant_ids,
                'lease_seconds', p_lease_seconds,
                'limit_jobs', p_limit_jobs,
                'stored_next_need', v_stored_next_need,
                'effective_next_need', next_need,
                'alternate_next_need', v_alternate_next_need,
                'alternate_after_seconds', v_alternate_after_seconds,
                'ready_since', v_ready_since,
                'pivoted_to_alternate', v_pivoted
            ),
            jsonb_build_object(
                'lease_id', lease_id,
                'lease_expires_at', lease_expires_at
            )
        );

        tenant_id := v_tenant_id;
        RETURN NEXT;
    END LOOP;

    IF v_count = 0 AND pgjobdb.is_notify_enabled() THEN
        FOREACH v_cap IN ARRAY v_caps LOOP
            EXIT WHEN v_cap IS NULL;
            EXECUTE 'LISTEN ' || quote_ident('pgjobdb.need.' || v_cap);
        END LOOP;
    END IF;
END;
$$;

CREATE OR REPLACE FUNCTION pgjobdb.get_job_lease(
    p_tenant_id TEXT,
    p_job_id TEXT,
    p_worker_id TEXT,
    p_worker_caps TEXT[],
    p_lease_seconds INTEGER DEFAULT 60
)
RETURNS TABLE(
    tenant_id TEXT,
    job_id TEXT,
    lease_id TEXT,
    next_need TEXT,
    wait_for TEXT[],
    payload JSONB,
    available_at TIMESTAMPTZ,
    lease_expires_at TIMESTAMPTZ
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_caps TEXT[] := p_worker_caps;
    v_now TIMESTAMPTZ := clock_timestamp();
    v_expires TIMESTAMPTZ;
    v_tenant_id TEXT;
    v_previous_lease_id TEXT;
    v_previous_lease_expires_at TIMESTAMPTZ;
    v_previous_lease_expired BOOLEAN;
    v_total_expirations BIGINT;
    v_consecutive_expirations BIGINT;
    v_stored_next_need TEXT;
    v_pivoted BOOLEAN;
    v_alternate_next_need TEXT;
    v_alternate_after_seconds INTEGER;
    v_ready_since TIMESTAMPTZ;
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

    IF v_caps IS NULL OR array_length(v_caps, 1) = 0 THEN
        RAISE EXCEPTION 'worker_caps cannot be empty';
    END IF;

    IF p_lease_seconds IS NULL OR p_lease_seconds <= 0 THEN
        RAISE EXCEPTION 'lease_seconds must be positive';
    END IF;

    v_expires := v_now + make_interval(secs => p_lease_seconds);

    FOR v_tenant_id, job_id, lease_id, next_need, wait_for, payload, available_at, lease_expires_at,
        v_previous_lease_id, v_previous_lease_expires_at, v_previous_lease_expired,
        v_total_expirations, v_consecutive_expirations, v_stored_next_need, v_pivoted,
        v_alternate_next_need, v_alternate_after_seconds, v_ready_since IN
        WITH candidate AS (
            SELECT jws.*,
                   (jws.lease_id IS NOT NULL AND jws.lease_expires_at <= v_now) AS lease_was_expired
            FROM pgjobdb.jobs_with_status jws
            WHERE jws.tenant_id = p_tenant_id
              AND jws.job_id = p_job_id
              AND jws.status = 'READY'
              AND jws.effective_next_need = ANY(v_caps)
            FOR UPDATE SKIP LOCKED
        )
        UPDATE pgjobdb.jobs j
        SET
            lease_expiration_count = CASE
                WHEN c.lease_was_expired THEN j.lease_expiration_count + 1
                ELSE j.lease_expiration_count
            END,
            consecutive_expirations = CASE
                WHEN c.lease_was_expired THEN j.consecutive_expirations + 1
                ELSE j.consecutive_expirations
            END,
            lease_id = gen_random_uuid()::TEXT,
            lease_expires_at = v_expires
        FROM candidate c
        WHERE j.tenant_id = c.tenant_id
          AND j.job_id = c.job_id
        RETURNING j.tenant_id,
                  j.job_id,
                  j.lease_id,
                  c.effective_next_need,
                  j.wait_for,
                  j.payload,
                  j.available_at,
                  j.lease_expires_at,
                  c.lease_id AS previous_lease_id,
                  c.lease_expires_at AS previous_lease_expires_at,
                  c.lease_was_expired AS lease_previously_expired,
                  j.lease_expiration_count,
                  j.consecutive_expirations,
                  c.next_need AS stored_next_need,
                  c.effective_next_need <> c.next_need AS pivoted_to_alternate,
                  c.alternate_next_need,
                  c.alternate_after_seconds,
                  c.ready_since
    LOOP
        IF v_previous_lease_expired THEN
            PERFORM pgjobdb._emit_trace_event(
                v_tenant_id,
                job_id,
                'lease_expiration_counter_incremented',
                p_worker_id,
                jsonb_build_object(
                    'tenant_id', v_tenant_id,
                    'worker_id', p_worker_id,
                    'worker_caps', v_caps,
                    'previous_lease_id', v_previous_lease_id,
                    'previous_lease_expires_at', v_previous_lease_expires_at,
                    'lease_expiration_count', v_total_expirations,
                    'consecutive_expirations', v_consecutive_expirations
                )
            );
        END IF;

        PERFORM pgjobdb._emit_trace_event(
            v_tenant_id,
            job_id,
            'job_retrieved',
            p_worker_id,
            jsonb_build_object(
                'tenant_id', v_tenant_id,
                'job_id', job_id,
                'worker_id', p_worker_id,
                'worker_caps', v_caps,
                'lease_seconds', p_lease_seconds,
                'retrieval_mode', 'direct_job_id',
                'stored_next_need', v_stored_next_need,
                'effective_next_need', next_need,
                'alternate_next_need', v_alternate_next_need,
                'alternate_after_seconds', v_alternate_after_seconds,
                'ready_since', v_ready_since,
                'pivoted_to_alternate', v_pivoted
            ),
            jsonb_build_object(
                'lease_id', lease_id,
                'lease_expires_at', lease_expires_at
            )
        );

        tenant_id := v_tenant_id;
        RETURN NEXT;
    END LOOP;
END;
$$;

CREATE OR REPLACE FUNCTION pgjobdb.extend_lease(
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
    v_job pgjobdb.jobs_with_status%ROWTYPE;
    v_new TIMESTAMPTZ;
BEGIN
    IF p_additional_seconds IS NULL OR p_additional_seconds <= 0 THEN
        RAISE EXCEPTION 'additional_seconds must be positive';
    END IF;

    SELECT *
    INTO v_job
    FROM pgjobdb._lock_job_for_status(
        p_tenant_id,
        p_job_id,
        'ACTIVE',
        p_lease_id,
        format('active lease not found for job %s/%s', p_tenant_id, p_job_id)
    );

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

CREATE OR REPLACE FUNCTION pgjobdb.reschedule_job(
    p_tenant_id TEXT,
    p_job_id TEXT,
    p_lease_id TEXT,
    p_worker_id TEXT,
    p_next_need TEXT,
    p_wait_for TEXT[] DEFAULT '{}'::TEXT[],
    p_available_at TIMESTAMPTZ DEFAULT clock_timestamp(),
    p_payload JSONB DEFAULT NULL,
    p_alternate_next_need TEXT DEFAULT NULL,
    p_alternate_after_seconds INTEGER DEFAULT NULL,
    p_set_alternate BOOLEAN DEFAULT FALSE
)
RETURNS TABLE(job_id TEXT, next_need TEXT, wait_for TEXT[], available_at TIMESTAMPTZ)
LANGUAGE plpgsql
AS $$
DECLARE
    v_job pgjobdb.jobs_with_status%ROWTYPE;
BEGIN
    SELECT * INTO v_job
    FROM pgjobdb._lock_job_for_status(
        p_tenant_id,
        p_job_id,
        'ACTIVE',
        p_lease_id,
        format('job %s/%s is not currently leased with lease %s', p_tenant_id, p_job_id, p_lease_id)
    );

    IF v_job.cancel_requested THEN
        RAISE EXCEPTION 'job %s/%s is cancelled and cannot be rescheduled', p_tenant_id, p_job_id;
    END IF;

    RETURN QUERY
    SELECT *
    FROM pgjobdb._reschedule_locked_job(
        v_job,
        p_worker_id,
        p_next_need,
        p_wait_for,
        p_available_at,
        p_payload,
        jsonb_build_object('lease_id', p_lease_id),
        p_alternate_next_need,
        p_alternate_after_seconds,
        p_set_alternate
    );
END;
$$;

CREATE OR REPLACE FUNCTION pgjobdb.complete_job(
    p_tenant_id TEXT,
    p_job_id TEXT,
    p_lease_id TEXT,
    p_worker_id TEXT,
    p_completion_status TEXT DEFAULT 'succeeded',
    p_completion_detail TEXT DEFAULT NULL
)
RETURNS BOOLEAN
LANGUAGE plpgsql
AS $$
DECLARE
    v_job pgjobdb.jobs_with_status%ROWTYPE;
BEGIN
    v_job := pgjobdb._lock_job_for_status(
        p_tenant_id,
        p_job_id,
        'ACTIVE',
        p_lease_id,
        format('job %s/%s is not actively leased by %s', p_tenant_id, p_job_id, p_lease_id)
    );

    RETURN pgjobdb._complete_locked_job(
        v_job,
        p_worker_id,
        p_completion_status,
        p_completion_detail,
        jsonb_build_object('lease_id', p_lease_id)
    );
END;
$$;

CREATE OR REPLACE FUNCTION pgjobdb.complete_unheld_job(
    p_tenant_id TEXT,
    p_job_id TEXT,
    p_worker_id TEXT,
    p_completion_status TEXT DEFAULT 'succeeded',
    p_completion_detail TEXT DEFAULT NULL
)
RETURNS BOOLEAN
LANGUAGE plpgsql
AS $$
DECLARE
    v_job pgjobdb.jobs_with_status%ROWTYPE;
BEGIN
    SELECT *
    INTO v_job
    FROM pgjobdb._lock_job_for_status(
        p_tenant_id,
        p_job_id,
        'READY',
        NULL,
        format('job %s/%s is not available to complete', p_tenant_id, p_job_id)
    );

    RETURN pgjobdb._complete_locked_job(
        v_job,
        p_worker_id,
        p_completion_status,
        p_completion_detail,
        jsonb_build_object('completed_without_lease', TRUE)
    );
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
            next_need,
            alternate_next_need,
            alternate_after_seconds,
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
            j.next_need,
            j.alternate_next_need,
            j.alternate_after_seconds,
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

CREATE OR REPLACE FUNCTION pgjobdb.reschedule_unheld_job(
    p_tenant_id TEXT,
    p_job_id TEXT,
    p_worker_id TEXT,
    p_next_need TEXT,
    p_wait_for TEXT[] DEFAULT '{}'::TEXT[],
    p_available_at TIMESTAMPTZ DEFAULT clock_timestamp(),
    p_payload JSONB DEFAULT NULL,
    p_alternate_next_need TEXT DEFAULT NULL,
    p_alternate_after_seconds INTEGER DEFAULT NULL,
    p_set_alternate BOOLEAN DEFAULT FALSE
)
RETURNS TABLE(job_id TEXT, next_need TEXT, wait_for TEXT[], available_at TIMESTAMPTZ)
LANGUAGE plpgsql
AS $$
DECLARE
    v_job pgjobdb.jobs_with_status%ROWTYPE;
BEGIN
    SELECT *
    INTO v_job
    FROM pgjobdb._lock_job_for_status(
        p_tenant_id,
        p_job_id,
        'READY',
        NULL,
        format('job %s/%s is not available to reschedule', p_tenant_id, p_job_id)
    );

    IF v_job.cancel_requested THEN
        RAISE EXCEPTION 'job %s/%s is cancelled and cannot be rescheduled', p_tenant_id, p_job_id;
    END IF;

    RETURN QUERY
    SELECT *
    FROM pgjobdb._reschedule_locked_job(
        v_job,
        p_worker_id,
        p_next_need,
        p_wait_for,
        p_available_at,
        p_payload,
        jsonb_build_object('rescheduled_without_lease', TRUE),
        p_alternate_next_need,
        p_alternate_after_seconds,
        p_set_alternate
    );
END;
$$;

-- JobDB-native facts and schedule state. The copied generic procedures remain
-- available while the native procedures are introduced in the next step.
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
    CONSTRAINT job_facts_failure_history_array CHECK (
        schedule_failure_history IS NULL
        OR jsonb_typeof(schedule_failure_history) = 'array'
    )
);

CREATE UNIQUE INDEX IF NOT EXISTS job_facts_schedule_run_idx
    ON pgjobdb.job_facts (tenant_id, schedule_id, schedule_run_id)
    WHERE schedule_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS job_facts_parent_idx
    ON pgjobdb.job_facts (tenant_id, parent_job_id)
    WHERE parent_job_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS job_facts_metadata_idx
    ON pgjobdb.job_facts USING GIN (app_metadata jsonb_path_ops);
CREATE INDEX IF NOT EXISTS job_facts_created_idx
    ON pgjobdb.job_facts (created_at DESC, tenant_id DESC, job_id DESC);

ALTER TABLE pgjobdb.jobs
    ADD COLUMN IF NOT EXISTS route_job_type TEXT,
    ADD COLUMN IF NOT EXISTS work_kind TEXT,
    ADD COLUMN IF NOT EXISTS task_type TEXT,
    ADD COLUMN IF NOT EXISTS resume_job_type TEXT,
    ADD COLUMN IF NOT EXISTS task_input_ordinal BIGINT,
    ADD COLUMN IF NOT EXISTS task_output_ordinal BIGINT,
    ADD COLUMN IF NOT EXISTS task_input_hash TEXT,
    ADD COLUMN IF NOT EXISTS alternate_job_type TEXT,
    ADD COLUMN IF NOT EXISTS alternate_task_type TEXT,
    ADD COLUMN IF NOT EXISTS lease_worker_id TEXT,
    ADD COLUMN IF NOT EXISTS lease_payload JSONB NOT NULL DEFAULT '{}'::JSONB,
    ADD COLUMN IF NOT EXISTS lease_payload_visible BOOLEAN NOT NULL DEFAULT FALSE;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint
        WHERE conrelid = 'pgjobdb.jobs'::regclass
          AND conname = 'jobs_native_route_valid') THEN
        ALTER TABLE pgjobdb.jobs
            ADD CONSTRAINT jobs_native_route_valid CHECK (
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
            );
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint
        WHERE conrelid = 'pgjobdb.jobs'::regclass
          AND conname = 'jobs_native_payload_object') THEN
        ALTER TABLE pgjobdb.jobs
            ADD CONSTRAINT jobs_native_payload_object CHECK
                (jsonb_typeof(lease_payload) = 'object');
    END IF;
END;
$$;

CREATE INDEX IF NOT EXISTS jobs_native_route_idx
    ON pgjobdb.jobs (route_job_type, work_kind, task_type, available_at)
    WHERE route_job_type IS NOT NULL;

ALTER TABLE pgjobdb.jobs_archive
    ADD COLUMN IF NOT EXISTS completion_error_kind TEXT,
    ADD COLUMN IF NOT EXISTS completion_retryable BOOLEAN,
    ADD COLUMN IF NOT EXISTS final_route_job_type TEXT,
    ADD COLUMN IF NOT EXISTS final_work_kind TEXT,
    ADD COLUMN IF NOT EXISTS final_task_type TEXT,
    ADD COLUMN IF NOT EXISTS final_resume_job_type TEXT,
    ADD COLUMN IF NOT EXISTS final_task_input_ordinal BIGINT,
    ADD COLUMN IF NOT EXISTS final_task_output_ordinal BIGINT,
    ADD COLUMN IF NOT EXISTS final_task_input_hash TEXT,
    ADD COLUMN IF NOT EXISTS final_alternate_job_type TEXT,
    ADD COLUMN IF NOT EXISTS final_alternate_task_type TEXT,
    ADD COLUMN IF NOT EXISTS final_alternate_after_seconds INTEGER,
    ADD COLUMN IF NOT EXISTS final_wait_for TEXT[],
    ADD COLUMN IF NOT EXISTS final_available_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS final_lease_expires_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS final_lease_worker_id TEXT,
    ADD COLUMN IF NOT EXISTS final_cancel_requested BOOLEAN,
    ADD COLUMN IF NOT EXISTS final_lease_payload JSONB,
    ADD COLUMN IF NOT EXISTS final_lease_payload_visible BOOLEAN;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint
        WHERE conrelid = 'pgjobdb.jobs_archive'::regclass
          AND conname = 'jobs_archive_native_snapshot_valid') THEN
        ALTER TABLE pgjobdb.jobs_archive
            ADD CONSTRAINT jobs_archive_native_snapshot_valid CHECK (
                (final_route_job_type IS NULL AND final_work_kind IS NULL
                    AND final_task_type IS NULL AND final_resume_job_type IS NULL
                    AND final_task_input_ordinal IS NULL
                    AND final_task_output_ordinal IS NULL
                    AND final_task_input_hash IS NULL AND final_wait_for IS NULL
                    AND final_available_at IS NULL AND final_cancel_requested IS NULL
                    AND final_lease_payload IS NULL
                    AND final_lease_payload_visible IS NULL)
                OR (final_route_job_type IS NOT NULL AND final_route_job_type <> ''
                    AND final_work_kind IS NOT NULL
                    AND final_wait_for IS NOT NULL AND final_available_at IS NOT NULL
                    AND final_cancel_requested IS NOT NULL
                    AND final_lease_payload IS NOT NULL
                    AND final_lease_payload_visible IS NOT NULL
                    AND jsonb_typeof(final_lease_payload) = 'object' AND (
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
            );
    END IF;
END;
$$;

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
    p_lease_payload JSONB,
    p_lease_payload_visible BOOLEAN
)
RETURNS TABLE(job_id TEXT, created BOOLEAN)
LANGUAGE plpgsql
AS $$
DECLARE
    v_inserted INTEGER;
    v_existing pgjobdb.job_facts%ROWTYPE;
    v_schedule_id TEXT := p_schedule->>'schedule_id';
    v_schedule_generation BIGINT := (p_schedule->>'generation')::BIGINT;
    v_schedule_spec_hash TEXT := p_schedule->>'spec_hash';
    v_scheduled_at TIMESTAMPTZ := (p_schedule->>'scheduled_at')::TIMESTAMPTZ;
    v_schedule_run_id TEXT := p_schedule->>'run_id';
    v_expires_at TIMESTAMPTZ := COALESCE(p_expires_at, 'infinity');
BEGIN
    IF p_tenant_id IS NULL OR p_tenant_id = '' OR p_job_id IS NULL OR p_job_id = ''
        OR p_worker_id IS NULL OR p_worker_id = '' OR p_job_type IS NULL
        OR p_job_type = '' THEN
        RAISE EXCEPTION 'tenant, job, worker, and job type are required';
    END IF;
    IF jsonb_typeof(p_run_policy) IS DISTINCT FROM 'object'
        OR jsonb_typeof(p_app_metadata) IS DISTINCT FROM 'object'
        OR jsonb_typeof(p_lease_payload) IS DISTINCT FROM 'object'
        OR p_lease_payload_visible IS NULL THEN
        RAISE EXCEPTION 'run policy, app metadata, and lease payload must be JSON objects';
    END IF;
    IF p_schedule IS NOT NULL AND jsonb_typeof(p_schedule) IS DISTINCT FROM 'object' THEN
        RAISE EXCEPTION 'schedule occurrence must be a JSON object';
    END IF;
    IF EXISTS (SELECT 1 FROM pgjobdb.jobs_archive a
        WHERE a.tenant_id = p_tenant_id AND a.job_id = p_job_id) THEN
        RAISE EXCEPTION 'completed job id cannot be resubmitted';
    END IF;

    INSERT INTO pgjobdb.job_facts (
        tenant_id, job_id, job_type, run_policy, app_metadata, schema_hash,
        parent_job_id, schedule_id, schedule_generation, schedule_spec_hash,
        scheduled_at, schedule_run_id, schedule_reason, schedule_manual,
        schedule_backfill_id, schedule_previous_job_id,
        schedule_failure_history, created_by_worker_id, expires_at
    ) VALUES (
        p_tenant_id, p_job_id, p_job_type, p_run_policy, p_app_metadata,
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
        IF NOT EXISTS (SELECT 1 FROM pgjobdb.jobs j
            WHERE j.tenant_id = p_tenant_id AND j.job_id = p_job_id) THEN
            RAISE EXCEPTION 'job facts exist without active scheduler state';
        END IF;
        RETURN QUERY SELECT p_job_id, FALSE;
        RETURN;
    END IF;

    INSERT INTO pgjobdb.jobs (
        tenant_id, job_id, next_need, wait_for, available_at, expires_at,
        route_job_type, work_kind, lease_payload, lease_payload_visible
    ) VALUES (
        p_tenant_id, p_job_id, '__pgjobdb_native__',
        pgjobdb.normalize_wait_for(p_tenant_id, p_wait_for),
        COALESCE(p_available_at, clock_timestamp()), v_expires_at,
        p_job_type, 'JOB', p_lease_payload, p_lease_payload_visible
    );
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
    run_policy JSONB, lease_payload JSONB, lease_payload_visible BOOLEAN,
    schema_hash TEXT
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

    RETURN QUERY
    WITH candidate AS (
        SELECT j.tenant_id, j.job_id,
            route.effective_job_type,
            route.effective_work_kind,
            route.effective_task_type
        FROM pgjobdb.jobs j
        JOIN pgjobdb.job_facts f USING (tenant_id, job_id)
        CROSS JOIN LATERAL (
            SELECT COALESCE(
                j.alternate_job_type IS NOT NULL
                AND j.alternate_after_seconds IS NOT NULL
                AND v_now >= GREATEST(
                    j.available_at,
                    j.created_at,
                    COALESCE(NULLIF(j.lease_expires_at, '-infinity'::TIMESTAMPTZ),
                        j.created_at)
                ) + make_interval(secs => j.alternate_after_seconds),
                FALSE
            ) AS due
        ) alt
        CROSS JOIN LATERAL (
            SELECT
                CASE WHEN alt.due THEN j.alternate_job_type
                    ELSE j.route_job_type END AS effective_job_type,
                CASE WHEN alt.due THEN
                    CASE WHEN j.alternate_task_type IS NULL THEN 'JOB' ELSE 'TASK' END
                    ELSE j.work_kind END AS effective_work_kind,
                CASE WHEN alt.due THEN j.alternate_task_type
                    ELSE j.task_type END AS effective_task_type
        ) route
        WHERE j.route_job_type IS NOT NULL
          AND (p_tenant_ids IS NULL OR j.tenant_id = ANY(p_tenant_ids))
          AND (p_target_tenant_id IS NULL
            OR (j.tenant_id = p_target_tenant_id AND j.job_id = p_target_job_id))
          AND j.cancel_requested = FALSE
          AND j.available_at <= v_now
          AND j.expires_at > v_now
          AND j.lease_expires_at <= v_now
          AND NOT EXISTS (
              SELECT 1 FROM unnest(j.wait_for) AS pending(id)
              WHERE NOT EXISTS (SELECT 1 FROM pgjobdb.jobs_archive a
                  WHERE a.tenant_id = j.tenant_id AND a.job_id = pending.id)
          )
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
            lease_expires_at = v_now + make_interval(secs => p_lease_seconds),
            lease_expiration_count = j.lease_expiration_count +
                CASE WHEN j.lease_id IS NOT NULL THEN 1 ELSE 0 END,
            consecutive_expirations = CASE WHEN j.lease_id IS NOT NULL
                THEN j.consecutive_expirations + 1 ELSE 0 END
        FROM candidate c
        WHERE j.tenant_id = c.tenant_id AND j.job_id = c.job_id
        RETURNING j.*, c.effective_job_type, c.effective_work_kind,
            c.effective_task_type
    )
    SELECT l.tenant_id, l.job_id, l.lease_id, l.lease_expires_at,
        f.job_type, l.effective_job_type, l.effective_work_kind,
        CASE WHEN l.effective_work_kind = 'TASK' THEN l.effective_task_type
            ELSE l.task_type END,
        l.resume_job_type, l.task_input_ordinal, l.task_output_ordinal,
        l.task_input_hash, f.run_policy, l.lease_payload,
        l.lease_payload_visible, f.schema_hash
    FROM leased l JOIN pgjobdb.job_facts f USING (tenant_id, job_id);
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
        final_wait_for = v_active.wait_for,
        final_available_at = v_active.available_at,
        final_lease_expires_at = v_active.lease_expires_at,
        final_lease_worker_id = v_active.lease_worker_id,
        final_cancel_requested = v_active.cancel_requested,
        final_lease_payload = v_active.lease_payload,
        final_lease_payload_visible = v_active.lease_payload_visible
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
    run_policy JSONB, lease_payload JSONB, lease_payload_visible BOOLEAN,
    schema_hash TEXT
)
LANGUAGE plpgsql
AS $$
BEGIN
    IF p_worker_id IS NULL OR p_worker_id = '' OR p_lease_id IS NULL OR p_lease_id = '' THEN
        RAISE EXCEPTION 'worker id and lease id are required';
    END IF;
    RETURN QUERY
    SELECT j.tenant_id, j.job_id, j.lease_id, j.lease_expires_at,
        f.job_type, j.route_job_type, j.work_kind, j.task_type,
        j.resume_job_type, j.task_input_ordinal, j.task_output_ordinal,
        j.task_input_hash, f.run_policy, j.lease_payload,
        j.lease_payload_visible, f.schema_hash
    FROM pgjobdb.jobs j JOIN pgjobdb.job_facts f USING (tenant_id, job_id)
    WHERE j.tenant_id = p_tenant_id AND j.job_id = p_job_id
      AND j.lease_id = p_lease_id AND j.lease_expires_at > clock_timestamp()
      AND j.lease_worker_id = p_worker_id
      AND j.route_job_type IS NOT NULL;
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
    RETURN pgjobdb.extend_lease(
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
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pgjobdb.jobs j
        WHERE j.tenant_id = p_tenant_id AND j.job_id = p_job_id
          AND j.route_job_type IS NOT NULL) THEN
        RAISE EXCEPTION 'native job not found';
    END IF;
    PERFORM pgjobdb.cancel_job(p_tenant_id, p_job_id, p_worker_id, p_reason);
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
    p_lease_payload JSONB,
    p_set_alternate BOOLEAN,
    p_alternate_job_type TEXT,
    p_alternate_task_type TEXT,
    p_alternate_after_seconds INTEGER,
    p_lease_payload_visible BOOLEAN
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
    IF p_lease_payload IS NOT NULL
        AND jsonb_typeof(p_lease_payload) IS DISTINCT FROM 'object' THEN
        RAISE EXCEPTION 'lease payload must be a JSON object';
    END IF;
    IF (p_lease_payload IS NOT NULL
            AND p_lease_payload_visible IS DISTINCT FROM TRUE)
        OR (p_lease_payload IS NULL AND p_lease_payload_visible IS TRUE) THEN
        RAISE EXCEPTION 'lease payload visibility and value disagree';
    END IF;
    IF p_set_alternate THEN
        IF p_alternate_job_type IS NULL THEN
            IF p_alternate_task_type IS NOT NULL OR p_alternate_after_seconds IS NOT NULL THEN
                RAISE EXCEPTION 'cleared alternate route cannot have task or delay';
            END IF;
        ELSIF p_alternate_job_type = '' OR p_alternate_after_seconds IS NULL
            OR p_alternate_after_seconds < 0
            OR (p_alternate_task_type IS NOT NULL AND p_alternate_task_type = '') THEN
            RAISE EXCEPTION 'invalid alternate route';
        END IF;
        IF p_alternate_task_type IS NOT NULL AND p_work_kind <> 'TASK' THEN
            RAISE EXCEPTION 'alternate task route requires task coordinates';
        END IF;
    END IF;
    IF p_job_id = ANY(COALESCE(p_wait_for, ARRAY[]::TEXT[])) THEN
        RAISE EXCEPTION 'job cannot wait for itself';
    END IF;

    IF p_lease_id IS NULL THEN
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
    v_wait_for := pgjobdb.normalize_wait_for(p_tenant_id, p_wait_for);

    UPDATE pgjobdb.jobs j SET
        route_job_type = p_route_job_type,
        work_kind = p_work_kind,
        task_type = p_task_type,
        resume_job_type = p_resume_job_type,
        task_input_ordinal = p_task_input_ordinal,
        task_output_ordinal = p_task_output_ordinal,
        task_input_hash = p_task_input_hash,
        wait_for = v_wait_for,
        available_at = COALESCE(p_available_at, v_now),
        lease_payload = CASE WHEN p_lease_payload_visible IS FALSE
            THEN '{}'::JSONB ELSE COALESCE(p_lease_payload, j.lease_payload) END,
        lease_payload_visible = COALESCE(p_lease_payload_visible, j.lease_payload_visible),
        alternate_job_type = CASE WHEN p_set_alternate
            THEN p_alternate_job_type ELSE j.alternate_job_type END,
        alternate_task_type = CASE WHEN p_set_alternate
            THEN p_alternate_task_type ELSE j.alternate_task_type END,
        alternate_after_seconds = CASE WHEN p_set_alternate
            THEN p_alternate_after_seconds ELSE j.alternate_after_seconds END,
        lease_id = NULL,
        lease_worker_id = NULL,
        lease_expires_at = '-infinity',
        consecutive_expirations = 0
    WHERE j.tenant_id = p_tenant_id AND j.job_id = p_job_id;
    RETURN TRUE;
END;
$$;

CREATE OR REPLACE FUNCTION pgjobdb.complete_native_task_work(
    p_tenant_id TEXT,
    p_job_id TEXT,
    p_worker_id TEXT,
    p_job_type TEXT,
    p_task_type TEXT,
    p_resume_job_type TEXT,
    p_input_ordinal BIGINT,
    p_output_ordinal BIGINT,
    p_input_hash TEXT,
    p_lease_payload JSONB,
    p_lease_payload_visible BOOLEAN
)
RETURNS BOOLEAN
LANGUAGE plpgsql
AS $$
DECLARE
    v_job pgjobdb.jobs%ROWTYPE;
BEGIN
    IF p_worker_id IS NULL OR p_worker_id = '' THEN
        RAISE EXCEPTION 'worker id is required';
    END IF;
    SELECT * INTO v_job FROM pgjobdb.jobs j
    WHERE j.tenant_id = p_tenant_id AND j.job_id = p_job_id FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'native waiting task not found';
    END IF;
    IF v_job.lease_expires_at > clock_timestamp() THEN
        RAISE EXCEPTION 'waiting task has an active lease';
    END IF;
    IF v_job.cancel_requested THEN
        RAISE EXCEPTION 'cancelled waiting task cannot be completed';
    END IF;
    IF v_job.work_kind IS DISTINCT FROM 'TASK'
        OR v_job.route_job_type IS DISTINCT FROM p_job_type
        OR v_job.task_type IS DISTINCT FROM p_task_type
        OR v_job.resume_job_type IS DISTINCT FROM p_resume_job_type
        OR v_job.task_input_ordinal IS DISTINCT FROM p_input_ordinal
        OR v_job.task_output_ordinal IS DISTINCT FROM p_output_ordinal
        OR v_job.task_input_hash IS DISTINCT FROM p_input_hash THEN
        RAISE EXCEPTION 'waiting task coordinates do not match';
    END IF;
    RETURN pgjobdb.reschedule_native_job(
        p_tenant_id, p_job_id, NULL, p_worker_id,
        p_resume_job_type, 'JOB', NULL, NULL, NULL, NULL, NULL,
        ARRAY[]::TEXT[], clock_timestamp(), p_lease_payload,
        FALSE, NULL, NULL, NULL, p_lease_payload_visible
    );
END;
$$;

CREATE OR REPLACE VIEW pgjobdb.native_jobs AS
SELECT
    f.tenant_id, f.job_id, 'ACTIVE'::TEXT AS store, s.status,
    f.job_type, j.route_job_type, j.work_kind, j.task_type,
    j.resume_job_type, j.task_input_ordinal, j.task_output_ordinal,
    j.task_input_hash, j.alternate_job_type, j.alternate_task_type,
    j.alternate_after_seconds, j.wait_for, j.available_at,
    NULLIF(j.lease_expires_at, '-infinity'::TIMESTAMPTZ) AS lease_expires_at,
    j.lease_worker_id, j.cancel_requested, j.lease_payload,
    j.lease_payload_visible,
    f.run_policy, f.app_metadata, f.schema_hash, f.parent_job_id,
    f.created_at,
    NULLIF(f.expires_at, 'infinity'::TIMESTAMPTZ) AS expires_at,
    NULL::TIMESTAMPTZ AS archived_at,
    NULL::TEXT AS completion_status, NULL::TEXT AS completion_detail,
    NULL::TEXT AS completion_error_kind, NULL::BOOLEAN AS completion_retryable,
    f.schedule_id, f.schedule_generation, f.schedule_spec_hash,
    f.scheduled_at, f.schedule_run_id, f.schedule_reason,
    f.schedule_manual, f.schedule_backfill_id,
    f.schedule_previous_job_id, f.schedule_failure_history
FROM pgjobdb.job_facts f
JOIN pgjobdb.jobs j USING (tenant_id, job_id)
JOIN pgjobdb.jobs_with_status s USING (tenant_id, job_id)
WHERE j.route_job_type IS NOT NULL
UNION ALL
SELECT
    f.tenant_id, f.job_id, 'ARCHIVED'::TEXT AS store,
    CASE WHEN a.final_cancel_requested THEN 'CANCELLED' ELSE 'COMPLETED' END AS status,
    f.job_type, a.final_route_job_type, a.final_work_kind,
    a.final_task_type, a.final_resume_job_type,
    a.final_task_input_ordinal, a.final_task_output_ordinal,
    a.final_task_input_hash, a.final_alternate_job_type,
    a.final_alternate_task_type, a.final_alternate_after_seconds,
    a.final_wait_for, a.final_available_at,
    NULLIF(a.final_lease_expires_at, '-infinity'::TIMESTAMPTZ),
    a.final_lease_worker_id, a.final_cancel_requested,
    a.final_lease_payload, a.final_lease_payload_visible,
    f.run_policy, f.app_metadata,
    f.schema_hash, f.parent_job_id, f.created_at,
    NULLIF(f.expires_at, 'infinity'::TIMESTAMPTZ), a.archived_at,
    a.completion_status, a.completion_detail,
    a.completion_error_kind, a.completion_retryable,
    f.schedule_id, f.schedule_generation, f.schedule_spec_hash,
    f.scheduled_at, f.schedule_run_id, f.schedule_reason,
    f.schedule_manual, f.schedule_backfill_id,
    f.schedule_previous_job_id, f.schedule_failure_history
FROM pgjobdb.job_facts f
JOIN pgjobdb.jobs_archive a USING (tenant_id, job_id)
WHERE a.final_route_job_type IS NOT NULL;

CREATE OR REPLACE FUNCTION pgjobdb.get_native_job(
    p_tenant_id TEXT, p_job_id TEXT
)
RETURNS SETOF JSONB
LANGUAGE sql STABLE
AS $$
    SELECT to_jsonb(n) FROM pgjobdb.native_jobs n
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
    p_limit INTEGER
)
RETURNS SETOF JSONB
LANGUAGE plpgsql
AS $$
BEGIN
    IF p_tenant_ids IS NULL OR cardinality(p_tenant_ids) = 0 THEN
        RAISE EXCEPTION 'tenant ids are required';
    END IF;
    IF p_limit IS NULL OR p_limit < 1 OR p_limit > 1000 THEN
        RAISE EXCEPTION 'job page limit must be between 1 and 1000';
    END IF;
    IF jsonb_typeof(p_task_selectors) IS DISTINCT FROM 'array'
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
    SELECT to_jsonb(n) FROM pgjobdb.native_jobs n
    WHERE n.tenant_id = ANY(p_tenant_ids)
      AND (p_statuses IS NULL OR n.status = ANY(p_statuses))
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
RETURNS SETOF JSONB
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
    SELECT to_jsonb(n) FROM pgjobdb.native_jobs n
    WHERE n.tenant_id = p_tenant_id AND n.schedule_id = p_schedule_id
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

COMMIT;

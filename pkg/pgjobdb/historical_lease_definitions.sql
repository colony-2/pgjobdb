-- Frozen format-14/15 definitions from v0.0.31 (621ed7f01f66).

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
        pgjobdb.job_status_at(j, pgjobdb.scheduler_now()) AS status
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
    v_now TIMESTAMPTZ;
    v_updated pgjobdb.jobs%ROWTYPE;
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
    IF v_job.lease_expires_at <= pgjobdb.scheduler_now() THEN
        RAISE EXCEPTION 'native lease not active';
    END IF;
    IF v_job.cancel_requested THEN
        RAISE EXCEPTION 'job %s/%s is cancelled and cannot extend the lease', p_tenant_id, p_job_id;
    END IF;

    v_now := pgjobdb.scheduler_now();
    v_new := v_now + make_interval(secs => p_additional_seconds);

    UPDATE pgjobdb.jobs
    SET lease_expires_at = v_new
    WHERE tenant_id = v_job.tenant_id
      AND job_id = v_job.job_id
      AND lease_id = v_job.lease_id RETURNING * INTO STRICT v_updated;
    PERFORM pgjobdb._record_lease_event(v_updated,v_now,NULL,NULL,v_job.lease_expires_at);

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
          AND lease_expires_at <= pgjobdb.scheduler_now()
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

CREATE OR REPLACE FUNCTION pgjobdb.get_native_work(
    p_worker_id TEXT,
    p_tenant_ids TEXT[],
    p_job_types TEXT[],
    p_task_selectors JSONB,
    p_app_metadata_contains JSONB,
    p_lease_seconds INTEGER,
    p_target_tenant_id TEXT,
    p_target_job_id TEXT,
    p_metadata_predicates JSONB,
    p_cell_ids TEXT[]
)
RETURNS TABLE(
    tenant_id TEXT, job_id TEXT, lease_id TEXT, lease_expires_at TIMESTAMPTZ,
    job_type TEXT, route_job_type TEXT, work_kind TEXT, task_type TEXT,
    resume_job_type TEXT, task_input_ordinal BIGINT,
    task_output_ordinal BIGINT, task_input_hash TEXT,
    run_policy JSONB, client_payload JSON, client_payload_revision BIGINT,
    schema_hash TEXT, goal_id TEXT, cell JSONB, crash_budget_remaining BIGINT, resource_resume JSON
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_now TIMESTAMPTZ := pgjobdb.scheduler_now();
    v_updated pgjobdb.jobs%ROWTYPE;
    v_previous TEXT;
    v_result RECORD;
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

    PERFORM pgjobdb._goal_lock(t) FROM (SELECT DISTINCT unnest(p_tenant_ids) AS t ORDER BY t) tenants;
    PERFORM pgjobdb._expire_goal_waits(p_tenant_ids);
    v_now:=pgjobdb.scheduler_now();
    WITH candidate AS (
        SELECT j.tenant_id, j.job_id, j.lease_id AS previous_id,
            route.effective_job_type,
            route.effective_work_kind,
            route.effective_task_type, route.alternate_due
        FROM pgjobdb.jobs j
        -- Readiness and route selection share one database-clock observation,
        -- so crossing a deadline mid-statement cannot select a body route.
        -- Lock the base row below to preserve SKIP LOCKED acquisition.
        JOIN pgjobdb.job_facts f USING (tenant_id, job_id) LEFT JOIN pgjobdb.job_client_state c USING (tenant_id,job_id)
        CROSS JOIN LATERAL pgjobdb.effective_native_route(j, v_now) route
        WHERE pgjobdb.job_status_at(j, v_now) = 'READY'
          AND f.cell_id=ANY(p_cell_ids)
          AND EXISTS(SELECT 1 FROM pgjobdb.cells c WHERE c.tenant_id=f.tenant_id AND c.cell_id=f.cell_id AND c.state='ACTIVE')
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
            lease_expires_at = pgjobdb.scheduler_now() + make_interval(secs => p_lease_seconds),
            lease_expiration_count = j.lease_expiration_count +
                CASE WHEN j.lease_id IS NOT NULL THEN 1 ELSE 0 END,
            crash_budget_remaining = CASE WHEN j.lease_id IS NOT NULL AND left(j.lease_worker_id,13)<>'jobdb-append-' THEN greatest(j.crash_budget_remaining-1,0) ELSE j.crash_budget_remaining END
        FROM candidate c
        WHERE j.tenant_id = c.tenant_id AND j.job_id = c.job_id
        RETURNING j AS updated, c.previous_id
    )
    SELECT l.updated, l.previous_id INTO v_result FROM leased l;
    IF NOT FOUND THEN RETURN; END IF;
    v_updated:=v_result.updated; v_previous:=v_result.previous_id;
    PERFORM pgjobdb._record_lease_event(v_updated,v_updated.lease_expires_at - make_interval(secs => p_lease_seconds),
      CASE WHEN p_target_job_id IS NULL THEN 'poll' ELSE 'targeted' END,v_previous,NULL);
    RETURN QUERY
    SELECT l.tenant_id, l.job_id, l.lease_id, l.lease_expires_at,
        f.job_type, l.lease_route_job_type, l.lease_work_kind, l.lease_task_type,
        l.resume_job_type, l.task_input_ordinal, l.task_output_ordinal,
        l.task_input_hash, f.run_policy, c.client_payload, c.revision, f.schema_hash, l.goal_id, jsonb_build_object('cell',f.cell_name,'cellId',f.cell_id,'cellRepo',f.cell_repo), l.crash_budget_remaining, l.resource_resume
    FROM (SELECT (v_updated).*) l JOIN pgjobdb.job_facts f USING (tenant_id,job_id) LEFT JOIN pgjobdb.job_client_state c USING(tenant_id,job_id);
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
    v_updated pgjobdb.jobs%ROWTYPE;
BEGIN
    PERFORM pgjobdb._expire_goal_waits(ARRAY[p_tenant_id]);
    v_now:=pgjobdb.scheduler_now();
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
    v_now := pgjobdb.scheduler_now();
    IF pgjobdb.job_status_at(v_job, v_now) <> 'READY' THEN
        RAISE EXCEPTION 'waiting task is not ready' USING ERRCODE = 'JTC01';
    END IF;
    IF (v_job.resource_not_before IS NOT NULL AND v_job.resource_not_before>v_now) OR v_job.crash_budget_remaining=0
        OR (v_job.resource_deadline IS NOT NULL AND v_job.resource_deadline<=v_now)
        OR v_job.cancel_requested OR v_job.lease_expires_at > v_now
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
    UPDATE pgjobdb.jobs j SET
        lease_id = gen_random_uuid()::TEXT,
        lease_worker_id = p_worker_id,
        lease_route_job_type = v_job.resume_job_type,
        lease_work_kind = 'JOB', lease_task_type = NULL,
        lease_expires_at = v_now + make_interval(secs => p_lease_seconds),
        lease_expiration_count = j.lease_expiration_count +
            CASE WHEN j.lease_id IS NOT NULL THEN 1 ELSE 0 END,
        crash_budget_remaining = CASE WHEN j.lease_id IS NOT NULL AND left(j.lease_worker_id,13)<>'jobdb-append-' THEN greatest(j.crash_budget_remaining-1,0) ELSE j.crash_budget_remaining END,
        work_kind = 'JOB',
        route_job_type = v_job.resume_job_type,
        task_type = NULL, resume_job_type = NULL,
        task_input_ordinal = NULL, task_output_ordinal = NULL, task_input_hash = NULL,
        alternate_job_type = NULL, alternate_task_type = NULL,
        alternate_after_seconds = NULL, alternate_at = NULL
    WHERE j.tenant_id = p_tenant_id AND j.job_id = p_job_id
    RETURNING j.* INTO STRICT v_updated;
    PERFORM pgjobdb._record_lease_event(v_updated,v_now,'task_completion',v_job.lease_id,NULL);
    RETURN QUERY SELECT v_updated.lease_id,v_updated.lease_expires_at;
END;
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
    p_include_hidden BOOLEAN, p_goal_ids TEXT[], p_cell_ids TEXT[], p_append_keys TEXT[] DEFAULT NULL
)
RETURNS TABLE(job_details JSONB,client_payload JSON,client_payload_revision BIGINT,initial_payload_digest TEXT)
LANGUAGE plpgsql
SET TimeZone = 'UTC'
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
        c.client_payload,c.revision,c.initial_payload_digest FROM pgjobdb.native_jobs n LEFT JOIN pgjobdb.job_client_state c USING(tenant_id,job_id)
    WHERE (p_append_keys IS NULL OR cardinality(p_append_keys)=0 OR n.append_key=ANY(p_append_keys)) AND n.tenant_id = ANY(p_tenant_ids) AND n.cell_id=ANY(p_cell_ids)
      AND (p_goal_ids IS NULL OR n.goal_id=ANY(p_goal_ids))
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

CREATE OR REPLACE FUNCTION pgjobdb.claim_append(p_tenant TEXT,p_job TEXT,p_lease TEXT,p_worker TEXT,p_seconds DOUBLE PRECISION)
RETURNS TIMESTAMPTZ LANGUAGE plpgsql AS $$
DECLARE old_id TEXT; updated pgjobdb.jobs%ROWTYPE; t TIMESTAMPTZ:=pgjobdb.scheduler_now();
BEGIN
 SELECT lease_id INTO old_id FROM pgjobdb.jobs WHERE tenant_id=p_tenant AND job_id=p_job FOR UPDATE;
 t:=pgjobdb.scheduler_now();
 UPDATE pgjobdb.jobs j SET lease_id=p_lease,lease_worker_id=p_worker,
 lease_expires_at=t+make_interval(secs=>p_seconds),
 lease_route_job_type=j.route_job_type,lease_work_kind=j.work_kind,lease_task_type=j.task_type,
 crash_budget_remaining=CASE WHEN j.lease_id IS NOT NULL AND left(j.lease_worker_id,13)<>'jobdb-append-' THEN greatest(j.crash_budget_remaining-1,0) ELSE j.crash_budget_remaining END
 WHERE j.tenant_id=p_tenant AND j.job_id=p_job AND NOT j.cancel_requested
 AND j.crash_budget_remaining>0
 AND (j.resource_not_before IS NULL OR j.resource_not_before<=t)
 AND (j.resource_deadline IS NULL OR j.resource_deadline>t)
 AND (j.lease_id IS NULL OR j.lease_expires_at<=t)
 RETURNING j.* INTO updated;
 IF NOT FOUND THEN RETURN NULL; END IF;
 PERFORM pgjobdb._record_lease_event(updated,t,'append',old_id,NULL);
 RETURN updated.lease_expires_at;
END $$;

CREATE OR REPLACE FUNCTION pgjobdb.release_append(p_tenant TEXT,p_job TEXT,p_lease TEXT)
RETURNS VOID LANGUAGE sql AS $$
 UPDATE pgjobdb.jobs SET lease_id=NULL,lease_worker_id=NULL,lease_expires_at='-infinity',lease_route_job_type=NULL,lease_work_kind=NULL,lease_task_type=NULL
 WHERE tenant_id=p_tenant AND job_id=p_job AND lease_id=p_lease;
$$;

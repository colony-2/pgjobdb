-- One observation time, including status, route eligibility, and wait expiry.
-- Keep filters before the page boundary and optional output joins after it.
WITH parameters AS NOT MATERIALIZED (SELECT
    $1::text[] AS p_tenant_ids,
    $2::text[] AS p_statuses,
    $3::text[] AS p_stores,
    $4::text[] AS p_job_types,
    $5::jsonb AS p_task_selectors,
    $6::jsonb AS p_job_keys,
    $7::text[] AS p_parent_job_ids,
    $8::boolean AS p_root_only,
    $9::jsonb AS p_metadata_predicates,
    $10::timestamptz AS p_created_after,
    $11::timestamptz AS p_created_before,
    $12::timestamptz AS p_before_created_at,
    $13::text AS p_before_tenant_id,
    $14::text AS p_before_job_id,
    $15::integer AS p_limit,
    $16::jsonb AS p_available_routes,
    $17::timestamptz AS p_now,
    $18::boolean AS p_include_hidden,
    $19::text[] AS p_goal_ids
), candidates AS NOT MATERIALIZED (
    SELECT f.tenant_id,f.job_id,f.goal_id,'ACTIVE'::text AS store,
        CASE WHEN p_statuses IS NOT NULL OR jsonb_array_length(p_available_routes)>0
            THEN pgjobdb.job_status_at(j,p_now) END AS status,
        f.job_type,j.route_job_type,j.work_kind,j.task_type,
        j.resume_job_type,j.task_input_ordinal,j.task_output_ordinal,j.task_input_hash,
        f.run_policy,j.wait_for,
        f.app_metadata,f.schema_hash,f.parent_job_id,f.created_at,
        j.available_at,NULLIF(f.expires_at,'infinity'::timestamptz) AS expires_at,
        NULLIF(j.lease_expires_at,'-infinity'::timestamptz) AS lease_expires_at,
        NULL::timestamptz AS archived_at,j.cancel_requested,
        NULL::text AS completion_status,NULL::text AS completion_detail,j.dependency_wait_id,f.hidden_at,
        route.effective_job_type,route.effective_task_type
    FROM pgjobdb.jobs j
    JOIN pgjobdb.job_facts f USING(tenant_id,job_id)
    CROSS JOIN parameters
    CROSS JOIN LATERAL pgjobdb.effective_native_route(j,p_now) route
    WHERE j.route_job_type IS NOT NULL
    UNION ALL
    SELECT f.tenant_id,f.job_id,f.goal_id,'ARCHIVED'::text,
        CASE WHEN a.final_cancel_requested OR a.completion_status='cancelled'
            THEN 'CANCELLED' ELSE 'COMPLETED' END,
        f.job_type,a.final_route_job_type,a.final_work_kind,a.final_task_type,
        a.final_resume_job_type,a.final_task_input_ordinal,a.final_task_output_ordinal,a.final_task_input_hash,
        f.run_policy,a.final_wait_for,
        f.app_metadata,f.schema_hash,f.parent_job_id,f.created_at,
        a.final_available_at,NULLIF(f.expires_at,'infinity'::timestamptz),
        NULLIF(a.final_lease_expires_at,'-infinity'::timestamptz),
        a.archived_at,a.final_cancel_requested,a.completion_status,a.completion_detail,
        NULL::text,f.hidden_at,NULL::text,NULL::text
    FROM pgjobdb.jobs_archive a
    JOIN pgjobdb.job_facts f USING(tenant_id,job_id)
    WHERE a.final_route_job_type IS NOT NULL
), page AS MATERIALIZED (
    SELECT n.* FROM candidates n CROSS JOIN parameters
    WHERE n.tenant_id = ANY(p_tenant_ids)
      AND (p_goal_ids IS NULL OR n.goal_id=ANY(p_goal_ids) OR EXISTS(SELECT 1 FROM pgjobdb.goal_candidates gc WHERE gc.tenant_id=n.tenant_id AND gc.job_id=n.job_id AND gc.goal_id=ANY(p_goal_ids)))
      AND (p_include_hidden OR n.hidden_at IS NULL)
      AND (p_statuses IS NULL OR n.status = ANY(p_statuses))
      AND (jsonb_array_length(p_available_routes)=0 OR (
        n.store='ACTIVE' AND n.status='READY' AND EXISTS (
          SELECT 1 FROM jsonb_array_elements(p_available_routes) selector(item)
          WHERE selector.item->>'job_type'=n.effective_job_type
            AND COALESCE(selector.item->>'task_type','')=COALESCE(n.effective_task_type,'')
        )))
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
    LIMIT $15::integer
)
SELECT jsonb_build_object(
    'tenant_id',n.tenant_id,'job_id',n.job_id,'goal_id',n.goal_id,
    'store',n.store,'status',COALESCE(n.status,(SELECT pgjobdb.job_status_at(j,p_now)
        FROM pgjobdb.jobs j WHERE j.tenant_id=n.tenant_id AND j.job_id=n.job_id)),
    'job_type',n.job_type,
    'route_job_type',n.route_job_type,'work_kind',n.work_kind,'task_type',n.task_type,
    'resume_job_type',n.resume_job_type,'task_input_ordinal',n.task_input_ordinal,
    'task_output_ordinal',n.task_output_ordinal,'task_input_hash',n.task_input_hash,
    'run_policy',n.run_policy,'wait_for',n.wait_for,
    'app_metadata',n.app_metadata,'schema_hash',n.schema_hash,'parent_job_id',n.parent_job_id,
    'created_at',n.created_at,'available_at',n.available_at,'expires_at',n.expires_at,
    'lease_expires_at',n.lease_expires_at,'archived_at',n.archived_at,
    'cancel_requested',n.cancel_requested,'completion_status',n.completion_status,
    'completion_detail',n.completion_detail,
    'client_payload_revision',c.revision,
    'blocking_wait_summary',CASE WHEN w.wait_id IS NOT NULL
        THEN jsonb_strip_nulls(jsonb_build_object('waitId',w.wait_id,'deadline',w.deadline_exact)) END
), c.client_payload
FROM page n CROSS JOIN parameters
JOIN pgjobdb.job_client_state c ON c.tenant_id=n.tenant_id AND c.job_id=n.job_id
LEFT JOIN pgjobdb.dependency_waits w ON n.store='ACTIVE' AND NOT n.cancel_requested
    AND w.tenant_id=n.tenant_id AND w.job_id=n.job_id AND w.wait_id=n.dependency_wait_id
    AND w.state='PENDING' AND (w.deadline IS NULL OR w.deadline>p_now)
ORDER BY n.created_at DESC,n.tenant_id DESC,n.job_id DESC

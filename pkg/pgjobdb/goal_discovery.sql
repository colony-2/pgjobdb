-- Origins and closures are retained job facts, including hidden records.
CREATE INDEX IF NOT EXISTS facts_goal_created_idx ON pgjobdb.job_facts(tenant_id,goal_created_at,goal_id COLLATE "C") WHERE goal_origin;
CREATE INDEX IF NOT EXISTS facts_goal_closed_idx ON pgjobdb.job_facts(tenant_id,goal_closed_at,goal_id COLLATE "C") WHERE goal_closed_at IS NOT NULL;
CREATE INDEX IF NOT EXISTS waits_consumer_discovery_idx ON pgjobdb.dependency_waits(tenant_id,job_id COLLATE "C",state,wait_id COLLATE "C");

CREATE INDEX IF NOT EXISTS target_discovery_idx ON pgjobdb.dependency_targets(tenant_id,target_kind,target_id,job_id COLLATE "C",wait_id COLLATE "C");

CREATE INDEX IF NOT EXISTS waits_discovery_idx ON pgjobdb.dependency_waits(tenant_id,job_id COLLATE "C",wait_id COLLATE "C");

-- Discovery uses committed goals and the existing pending-target index.
CREATE INDEX IF NOT EXISTS goals_state_discovery_idx ON pgjobdb.goals(tenant_id,state,goal_id COLLATE "C");
CREATE INDEX IF NOT EXISTS goals_created_discovery_idx ON pgjobdb.goals(tenant_id,created_at,goal_id COLLATE "C");
CREATE INDEX IF NOT EXISTS goals_updated_discovery_idx ON pgjobdb.goals(tenant_id,updated_at,goal_id COLLATE "C");
CREATE INDEX IF NOT EXISTS waits_consumer_discovery_idx ON pgjobdb.dependency_waits(tenant_id,job_id COLLATE "C",state,wait_id COLLATE "C");

CREATE INDEX IF NOT EXISTS target_discovery_idx ON pgjobdb.dependency_targets(tenant_id,target_kind,target_id,job_id COLLATE "C",wait_id COLLATE "C");

CREATE INDEX IF NOT EXISTS goals_discovery_idx ON pgjobdb.goals(tenant_id,goal_id COLLATE "C");
CREATE INDEX IF NOT EXISTS waits_discovery_idx ON pgjobdb.dependency_waits(tenant_id,job_id COLLATE "C",wait_id COLLATE "C");

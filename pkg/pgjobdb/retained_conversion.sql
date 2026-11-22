-- This block runs after the new readers/triggers are installed, under the
-- installer's writer exclusion. Legacy input remains available until every
-- witness and protected failure is retained. Reapply never allocates a record.
DO $$
DECLARE g RECORD; origin TEXT; id TEXT; snapshot JSONB;
BEGIN
 IF to_regclass('pgjobdb.goals') IS NOT NULL THEN
  FOR g IN SELECT * FROM pgjobdb.goals ORDER BY tenant_id COLLATE "C",goal_id COLLATE "C" LOOP
   SELECT f.job_id INTO STRICT origin FROM pgjobdb.job_facts f
    WHERE f.tenant_id=g.tenant_id AND f.goal_id=g.goal_id AND f.cell_id=g.cell_id AND f.cell_repo=g.cell_repo
    ORDER BY f.created_at,f.job_id COLLATE "C" LIMIT 1;
   UPDATE pgjobdb.job_facts SET goal_origin=TRUE,goal_created_at=g.created_at
    WHERE tenant_id=g.tenant_id AND job_id=origin;
   IF g.state='SUCCEEDED' THEN
    -- Preserve the historical producer, including cross-primary success. This
    -- is an explicitly marked witness, never a membership transfer.
    UPDATE pgjobdb.job_facts SET goal_closed_at=g.closed_at,goal_success_job_id=g.successful_job_id,historical_goal_success=TRUE
     WHERE tenant_id=g.tenant_id AND job_id=origin;
   ELSIF g.state='FAILED' THEN
    id:=gen_random_uuid()::TEXT;
    snapshot:=g.failure; -- original time, exact message, arrays and dropped members
    INSERT INTO pgjobdb.job_facts(tenant_id,job_id,goal_id,cell_id,cell_name,cell_repo,job_type,created_at,record_kind,terminal_failure,goal_closed_at)
    SELECT g.tenant_id,id,g.goal_id,g.cell_id,f.cell_name,g.cell_repo,'jobdb.goal_failure',g.closed_at,'MIGRATED_GOAL_FAILURE',snapshot,g.closed_at
    FROM pgjobdb.job_facts f WHERE f.tenant_id=g.tenant_id AND f.job_id=origin;
    INSERT INTO pgjobdb.job_client_state VALUES(g.tenant_id,id,NULL,0,'');
    INSERT INTO pgjobdb.jobs_archive(tenant_id,job_id,goal_id,created_at,archived_at,completion_status,completion_detail)
    VALUES(g.tenant_id,id,g.goal_id,g.closed_at,g.closed_at,'failed',snapshot->>'message');
   END IF;
  END LOOP;
 END IF;
END $$;

-- Remove only known lifecycle dependencies; unknown dependent objects fail the
-- transaction instead of silently deleting unrelated stores or application SQL.
DROP FUNCTION IF EXISTS pgjobdb.submit_native_job(TEXT,TEXT,TEXT,TEXT,JSONB,JSONB,TEXT,TEXT,JSONB,TEXT[],TIMESTAMPTZ,TIMESTAMPTZ,TEXT,JSON,TEXT,BOOLEAN,JSONB,JSONB,TEXT);
DROP FUNCTION IF EXISTS pgjobdb.list_native_jobs(TEXT[],TEXT[],TEXT[],TEXT[],JSONB,JSONB,TEXT[],BOOLEAN,JSONB,TIMESTAMPTZ,TIMESTAMPTZ,TIMESTAMPTZ,TEXT,TEXT,INTEGER);
DROP FUNCTION IF EXISTS pgjobdb.list_native_jobs(TEXT[],TEXT[],TEXT[],TEXT[],JSONB,JSONB,TEXT[],BOOLEAN,JSONB,TIMESTAMPTZ,TIMESTAMPTZ,TIMESTAMPTZ,TEXT,TEXT,INTEGER,JSONB,TIMESTAMPTZ,BOOLEAN,TEXT[]);
DROP FUNCTION IF EXISTS pgjobdb.add_goal_candidate(TEXT,TEXT,TEXT);
ALTER TABLE pgjobdb.job_facts DROP CONSTRAINT IF EXISTS job_facts_tenant_id_goal_id_fkey;
ALTER TABLE pgjobdb.jobs DROP CONSTRAINT IF EXISTS active_goal_fk;
ALTER TABLE pgjobdb.jobs_archive DROP CONSTRAINT IF EXISTS archive_goal_fk;
DROP TABLE IF EXISTS pgjobdb.goal_candidates;
DROP TABLE IF EXISTS pgjobdb.goals;

DO $$ BEGIN
 IF NOT EXISTS(SELECT 1 FROM pg_constraint WHERE conrelid='pgjobdb.job_facts'::regclass AND conname='retained_goal_shape') THEN
 ALTER TABLE pgjobdb.job_facts ADD CONSTRAINT retained_goal_shape CHECK ((
  (goal_origin=(goal_created_at IS NOT NULL)) AND
  ((goal_closed_at IS NULL AND goal_success_job_id IS NULL AND terminal_failure IS NULL AND NOT historical_goal_success)
   OR (goal_closed_at IS NOT NULL AND ((goal_success_job_id IS NOT NULL AND terminal_failure IS NULL AND record_kind='EXECUTION')
    OR (goal_success_job_id IS NULL AND terminal_failure IS NOT NULL AND record_kind IN ('GOAL_FAILURE','MIGRATED_GOAL_FAILURE'))))) AND
  ((record_kind='EXECUTION' AND terminal_failure IS NULL) OR
   (record_kind IN ('GOAL_FAILURE','MIGRATED_GOAL_FAILURE') AND NOT goal_origin AND job_type='jobdb.goal_failure'
    AND terminal_failure->>'code'='goal_failed' AND terminal_failure->>'goalId'=goal_id
    AND terminal_failure->>'message' IS NOT NULL AND (terminal_failure->>'failedAt')::timestamptz=goal_closed_at
    AND jsonb_typeof(terminal_failure->'terminalJobIds')='array' AND jsonb_typeof(terminal_failure->'nonTerminalJobIds')='array'))
 ) IS TRUE);
 END IF;
END $$;
-- Retained origin, membership, protected kind and the first closure cannot be
-- overwritten by metadata updates, later completions or replay.
CREATE OR REPLACE FUNCTION pgjobdb._immutable_retained_goal() RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
 IF (NEW.goal_origin,NEW.goal_created_at,NEW.record_kind,NEW.terminal_failure,NEW.cell_id,NEW.cell_repo)
  IS DISTINCT FROM (OLD.goal_origin,OLD.goal_created_at,OLD.record_kind,OLD.terminal_failure,OLD.cell_id,OLD.cell_repo)
  OR (OLD.goal_closed_at IS NOT NULL AND (NEW.goal_closed_at,NEW.goal_success_job_id,NEW.historical_goal_success)
   IS DISTINCT FROM (OLD.goal_closed_at,OLD.goal_success_job_id,OLD.historical_goal_success)) THEN
  RAISE EXCEPTION 'retained goal identity or first closure is immutable' USING ERRCODE='JGF01';
 END IF;
 RETURN NEW;
END $$;
DROP TRIGGER IF EXISTS immutable_retained_goal ON pgjobdb.job_facts;
CREATE TRIGGER immutable_retained_goal BEFORE UPDATE ON pgjobdb.job_facts FOR EACH ROW EXECUTE FUNCTION pgjobdb._immutable_retained_goal();

-- Retain these records through completion and soft deletion. Identity cleanup
-- alone removes them. Digest indexing permits full 16 KiB invocation keys.
CREATE TABLE IF NOT EXISTS pgjobdb.resource_sequences (
 tenant_id TEXT NOT NULL, job_id TEXT NOT NULL, invocation_digest BYTEA NOT NULL,
 invocation TEXT NOT NULL, attempt INTEGER NOT NULL, reason TEXT NOT NULL,
 count BIGINT NOT NULL CHECK(count>0), policy BYTEA NOT NULL,
 allowance BIGINT NOT NULL CHECK(allowance>0),
 PRIMARY KEY(tenant_id,job_id,invocation_digest,attempt,reason),
 FOREIGN KEY(tenant_id,job_id) REFERENCES pgjobdb.job_facts(tenant_id,job_id) ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS pgjobdb.resource_receipts (
 tenant_id TEXT NOT NULL, job_id TEXT NOT NULL, lease_id TEXT NOT NULL,
 request_id TEXT NOT NULL, worker_id TEXT NOT NULL, request BYTEA NOT NULL, receipt BYTEA NOT NULL,
 PRIMARY KEY(tenant_id,job_id,lease_id,request_id),
 FOREIGN KEY(tenant_id,job_id) REFERENCES pgjobdb.job_facts(tenant_id,job_id) ON DELETE CASCADE
);
-- Archive the authoritative selected state even when completion/deletion uses
-- older narrow projection types. Protected failure identities receive no credit.
CREATE OR REPLACE FUNCTION pgjobdb._archive_resource_state() RETURNS TRIGGER LANGUAGE plpgsql AS $$
DECLARE active pgjobdb.jobs%ROWTYPE;
BEGIN
 SELECT * INTO active FROM pgjobdb.jobs WHERE tenant_id=NEW.tenant_id AND job_id=NEW.job_id;
 IF FOUND THEN
  NEW.crash_budget_remaining:=active.crash_budget_remaining;
  NEW.resource_not_before_ns:=active.resource_not_before_ns;
  NEW.resource_not_before:=active.resource_not_before;
  NEW.resource_deadline:=active.resource_deadline;
  NEW.resource_resume:=active.resource_resume;
 END IF;
 RETURN NEW;
END $$;
DROP TRIGGER IF EXISTS archive_resource_state ON pgjobdb.jobs_archive;
CREATE TRIGGER archive_resource_state BEFORE INSERT ON pgjobdb.jobs_archive FOR EACH ROW EXECUTE FUNCTION pgjobdb._archive_resource_state();

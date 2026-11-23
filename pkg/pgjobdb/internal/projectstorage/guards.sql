-- Protocol 1: guards check seals; they never create them in resource transactions.
CREATE OR REPLACE FUNCTION pgjobdb._project_write_check(project_id text) RETURNS void
LANGUAGE plpgsql SET search_path=pg_catalog AS $guard$
DECLARE s json; r json; completion json; payload_digest text; catalog_xid text; reference_xid numeric; full_xid numeric; catalog_status text;
BEGIN
 BEGIN
  -- Never parse the opaque catalog payload in PostgreSQL: valid metadata and
  -- receipt strings can contain escapes that cannot be converted to SQL text.
  -- The Store atomically writes this minimal projection with the payload.
  SELECT convert_from(admission,'UTF8')::json,encode(sha256(payload),'hex'),xmin::text
    INTO s,payload_digest,catalog_xid FROM public.jobdb_project_catalog WHERE singleton=1;
  SELECT convert_from(payload,'UTF8')::json INTO completion FROM pgjobdb.project_installation WHERE singleton=1;
  -- xmin is 32-bit. A visible committed catalog update can be newer than this
  -- writer's XID, so use the snapshot upper bound as well to reconstruct its
  -- epoch. A frozen tuple, or one older than retained commit status, is committed;
  -- an own top/subtransaction must still be rejected as in progress.
  reference_xid := greatest(pg_current_xact_id()::text::numeric,
                            pg_snapshot_xmax(pg_current_snapshot())::text::numeric);
  full_xid := trunc(reference_xid / 4294967296) * 4294967296 + catalog_xid::numeric;
  IF full_xid > reference_xid THEN full_xid := full_xid - 4294967296; END IF;
  IF catalog_xid IN ('1','2') THEN catalog_status := 'committed';
  ELSE catalog_status := coalesce(pg_xact_status(full_xid::text::xid8),'committed'); END IF;
  IF catalog_status IS DISTINCT FROM 'committed' OR s->>'version' IS DISTINCT FROM '1' OR s->>'id' IS NULL OR
     json_typeof(s->'projects') IS DISTINCT FROM 'object' OR s->>'receiptsReady' IS DISTINCT FROM 'true' OR
     s->>'payloadSHA256' IS DISTINCT FROM payload_digest OR payload_digest IS NULL OR
     length(decode(s->>'cursorKey','base64')) IS DISTINCT FROM 32 OR
     completion->>'protocol' IS DISTINCT FROM '1' OR completion->>'catalogID' IS DISTINCT FROM s->>'id' OR
     completion->>'catalogManifest' IS DISTINCT FROM s->>'manifest' THEN
   RAISE EXCEPTION USING ERRCODE='PJC01',MESSAGE='project catalog unavailable';
  END IF;
 EXCEPTION WHEN OTHERS THEN
  RAISE EXCEPTION USING ERRCODE='PJC01',MESSAGE='project catalog unavailable';
 END;
 r := s->'projects'->project_id;
 IF r IS NULL OR coalesce((r->>'deleted')::boolean,false) THEN
  RAISE EXCEPTION USING ERRCODE='PJC02',MESSAGE='project not found';
 END IF;
 IF r->>'id' IS DISTINCT FROM project_id OR r->>'sealed' IS DISTINCT FROM 'true' THEN
  RAISE EXCEPTION USING ERRCODE='PJC01',MESSAGE='project must be admitted before resource transaction';
 END IF;
EXCEPTION WHEN SQLSTATE 'PJC02' THEN RAISE;
 WHEN OTHERS THEN RAISE EXCEPTION USING ERRCODE='PJC01',MESSAGE='project catalog unavailable';
END $guard$;
CREATE OR REPLACE FUNCTION pgjobdb._project_row_guard() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog AS $guard$
DECLARE old_id text; new_id text; base_id text;
BEGIN
 IF TG_OP<>'INSERT' THEN
  old_id:=row_to_json(OLD)->>TG_ARGV[0];
  PERFORM pgjobdb._project_write_check(old_id);
 END IF;
 IF TG_OP<>'DELETE' THEN
  new_id:=row_to_json(NEW)->>TG_ARGV[0];
  IF TG_OP='UPDATE' AND old_id IS DISTINCT FROM new_id THEN
   RAISE EXCEPTION USING ERRCODE='PJC03',MESSAGE='immutable project identity';
  END IF;
  PERFORM pgjobdb._project_write_check(new_id);
  IF TG_TABLE_NAME='jobdb_chapter_stories' THEN
   base_id:=row_to_json(NEW)->>'base_anthology_id';
   IF base_id IS NOT NULL THEN PERFORM pgjobdb._project_write_check(base_id); END IF;
  END IF;
 END IF;
 IF TG_OP='DELETE' THEN RETURN OLD; ELSE RETURN NEW; END IF;
END $guard$;
CREATE OR REPLACE FUNCTION pgjobdb._project_no_truncate() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog AS $guard$
BEGIN RAISE EXCEPTION USING ERRCODE='PJC01',MESSAGE='scoped resource truncation is forbidden'; END $guard$;

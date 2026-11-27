package pgjobdb

import (
	_ "embed"
	"strings"
)

// SQL contains the pgjobdb schema definition for consumers who need to apply it programmatically.
//
//go:embed pgjobdb.sql
var baseSQL string

//go:embed goal_discovery.sql
var discoverySQL string

//go:embed retained_conversion.sql
var conversionSQL string

//go:embed resource_backoff.sql
var resourceSQL string

// TransactionSQL is the installer-owned DDL without transaction delimiters.
// Callers must validate and exclude writers before applying a historical input.
var TransactionSQL = strings.TrimSuffix(strings.TrimPrefix(baseSQL, "BEGIN;"), "\n")

var SQL string

//go:embed historical_lease_definitions.sql
var HistoricalLeaseSQL string

// UpgradeSQL applies the target definitions without seeding or advancing the marker.
var UpgradeSQL string

func init() {
	end := strings.LastIndex(TransactionSQL, "ALTER TABLE pgjobdb.installation DROP CONSTRAINT")
	start := strings.Index(TransactionSQL, "CREATE EXTENSION IF NOT EXISTS pgcrypto;")
	clock := strings.Index(TransactionSQL, "CREATE OR REPLACE FUNCTION pgjobdb.scheduler_now()")
	guard := strings.Index(TransactionSQL, "DO $$ BEGIN")
	UpgradeSQL = "SET LOCAL search_path = pgjobdb, public;\n" + TransactionSQL[clock:guard] + TransactionSQL[start:end] + conversionSQL + "\n" + discoverySQL + "\n" + resourceSQL
	finish := strings.TrimSuffix(strings.TrimSpace(TransactionSQL[end:]), "COMMIT;")
	TransactionSQL = TransactionSQL[:end] + conversionSQL + "\n" + discoverySQL + "\n" + resourceSQL + "\n" + finish
	SQL = `BEGIN;
 DO $$ BEGIN
  IF to_regclass('pgjobdb.project_installation') IS NOT NULL THEN
   RAISE EXCEPTION 'project installation exists; use the pgjobdb installer to preserve project guards';
  END IF;
 END $$;
  DO $$ BEGIN
   IF to_regclass('pgjobdb.installation') IS NOT NULL AND NOT EXISTS (
      SELECT 1 FROM pg_attribute WHERE attrelid=to_regclass('pgjobdb.jobs')
        AND attname='last_lease_updated_at' AND NOT attisdropped
   ) THEN RAISE EXCEPTION 'existing pgjobdb installation requires explicit verification or offline migration'; END IF;
  END $$;
 ` + TransactionSQL + "\nCOMMIT;"
}

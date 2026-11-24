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

func init() {
	end := strings.LastIndex(TransactionSQL, "ALTER TABLE pgjobdb.installation DROP CONSTRAINT")
	finish := strings.TrimSuffix(strings.TrimSpace(TransactionSQL[end:]), "COMMIT;")
	TransactionSQL = TransactionSQL[:end] + conversionSQL + "\n" + discoverySQL + "\n" + resourceSQL + "\n" + finish
	SQL = `BEGIN;
 DO $$ BEGIN
  IF to_regclass('pgjobdb.project_installation') IS NOT NULL THEN
   RAISE EXCEPTION 'project installation exists; use the pgjobdb installer to preserve project guards';
  END IF;
 END $$;
 ` + TransactionSQL + "\nCOMMIT;"
}

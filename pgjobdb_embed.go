package pgjobdb

import _ "embed"

// SQL contains the pgjobdb schema definition for consumers who need to apply it programmatically.
//
//go:embed pgjobdb.sql
var SQL string

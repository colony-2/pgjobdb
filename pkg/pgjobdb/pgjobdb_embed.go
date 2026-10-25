package pgjobdb

import _ "embed"

// SQL contains the pgjobdb schema definition for consumers who need to apply it programmatically.
//
//go:embed pgjobdb.sql
var baseSQL string

//go:embed goal_discovery.sql
var discoverySQL string

var SQL = baseSQL + "\n" + discoverySQL

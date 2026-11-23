package main

import (
	"github.com/colony-2/pgjobdb/pkg/pgjobdb/projects"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestProjectPostgresInputValidation(t *testing.T) {
	for _, raw := range []string{`{}`, `{"protocol":1} {}`, `{"unknown":true}`, `invalid`, `{"protocol":1,"protocol":2}`, `{"protocol":1,"Protocol":2}`} {
		var in projects.Inventory
		err := decodeInventory([]byte(raw), &in)
		require.Error(t, err)
	}
	for _, values := range [][]string{{"ksuid-without-name"}, {"c2", "c2"}, {"=name"}, {"id="}} {
		_, err := decodeSeeds(values)
		require.Error(t, err)
	}
	seeds, err := decodeSeeds([]string{"c2", "0", "id=name"})
	require.NoError(t, err)
	require.Len(t, seeds, 3)
	out, err := executeRoot(t, "project", "postgres", "create", "--name", "bad_name", "--postgres-dsn", "postgres://never-connect.invalid/db")
	require.Error(t, err)
	require.NotContains(t, out, "never-connect.invalid")
}

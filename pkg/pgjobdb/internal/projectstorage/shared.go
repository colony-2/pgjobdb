package projectstorage

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"slices"
)

// SharedContractSQL describes the public stores at the pinned JobDB version.
// Keep this query and shared-contract.json together when adopting a new contract.
//
//go:embed shared-contract.sql
var SharedContractSQL string

// Captured from PostgreSQL 17 using the published public store constructors.
// No resource content or database-local OIDs are included.
//
//go:embed shared-contract.json
var sharedContract []byte

// VerifyShared refuses incomplete or modified shared schemas before installation
// can certify them. Opening a runtime must never silently repair missing indexes.
func VerifyShared(ctx context.Context, q Queryer) error {
	var expected []string
	if err := json.Unmarshal(sharedContract, &expected); err != nil {
		return err
	}
	rows, err := q.QueryContext(ctx, SharedContractSQL)
	if err != nil {
		return err
	}
	defer rows.Close()
	actual := []string{}
	for rows.Next() {
		var v string
		if err = rows.Scan(&v); err != nil {
			return err
		}
		actual = append(actual, v)
	}
	if err = rows.Err(); err != nil {
		return err
	}
	if !slices.Equal(actual, expected) {
		return fmt.Errorf("shared PostgreSQL schema differs from the published JobDB contract; diagnose explicitly before installing projects")
	}
	return nil
}

// Package projectfixture provides explicit, isolated administrative fixtures.
// It never selects admission for a production runtime. Resource regression tests
// use approved valid IDs, a real installed catalog and permanently sealed imports.
package projectfixture

import (
	"context"
	"database/sql"
	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/projectcatalog"
	"github.com/colony-2/pgjobdb/pkg/pgjobdb/installer"
	"github.com/colony-2/pgjobdb/pkg/pgjobdb/projects"
	"io"
)

const Participants = "1111111111111111111111111111111111111111111111111111111111111111"

func Context(ctx context.Context) context.Context {
	return jobdb.WithProjectAdministration(ctx, func(context.Context) error { return nil })
}

var Seeds = []projectcatalog.Seed{{ID: "c2", Name: "c2"}, {ID: "40F0meyA7Npn0VoSVtJ3cd5gXdD", Name: "fixture-another"}, {ID: "9YCgKaV6wo2tX7ecZqwiyl2XWni", Name: "fixture-authority"}, {ID: "PT1KCgHQTtXRcqRixNSTqkynjIJ", Name: "fixture-claims"}, {ID: "HBDSdEaOWFHDX26m9praNo6jVfc", Name: "fixture-identity"}, {ID: "NaYIYIZyCMz0OyDIb8oqPOnSraI", Name: "fixture-identity-tenant"}, {ID: "J85kFBsryHgvlyQGsZS7USBCJUu", Name: "fixture-lease"}, {ID: "7wPgVS9PQyWMqgmgX2w1Cg7xqCv", Name: "fixture-lease-create"}, {ID: "MYf3iIVoU1sG8DP9dPMk9r88ySq", Name: "fixture-lookup"}, {ID: "ZGywxVlb4u5M6sI7LrxmtVTFSok", Name: "fixture-metadata"}, {ID: "B46WGhqB4vx8uniEy9sGgQAy2vl", Name: "fixture-ns-tenant"}, {ID: "TqtggXyN1eGriOdDqH0R8ApQbbb", Name: "fixture-other"}, {ID: "YN1JXVAhGjedETg58gZU1S1HYdq", Name: "fixture-parallel"}, {ID: "58dLl9YVzx33E4Id6hlwe2gIz5s", Name: "fixture-pending"}, {ID: "PoPOA8ZZnaQWVKQVUCPyQEJW2Ko", Name: "fixture-pending-name"}, {ID: "XP5BIZb2Y8H6je7nERwNsOnRUYI", Name: "fixture-private"}, {ID: "6OEFoOR5M2YPEuYSQ5ly6aRtr2l", Name: "fixture-removed"}, {ID: "AeCaChn8xiv1ud2PdYA6pnqm1cG", Name: "fixture-rollback"}, {ID: "NmEyRmqhY5SZ0ThK0Ev5xwirVGU", Name: "fixture-scope"}, {ID: "29SvvnG4sIXc6sWKacJZXXD9Gha", Name: "fixture-selection-tenant"}, {ID: "KQoE5ZNNjvdjwlaUc25aNvuCYiT", Name: "fixture-storage"}, {ID: "8lPYEAHqlujzxGxkZIIJJU7mygd", Name: "fixture-tenant-1"}, {ID: "AR56al1yl0yzJyAEqN1IteLC5Vi", Name: "fixture-test-tenant"}, {ID: "XkR2hrh4sCknNJ1wPsH6jXZT0Fm", Name: "fixture-transport"}}

// Prepare is called explicitly only on a newly allocated fixture database.
func Prepare(ctx context.Context, db *sql.DB) error {
	ctx = Context(ctx)
	if err := (installer.Installer{DB: db}).Apply(ctx); err != nil {
		return err
	}
	if err := projects.PrepareShared(ctx, db); err != nil {
		return err
	}
	return Adopt(ctx, db)
}

// Adopt retains the resource inventory and explicitly includes fixture-only IDs.
func Adopt(ctx context.Context, db *sql.DB) error {
	ctx = Context(ctx)
	in, err := projects.Inspect(ctx, db, Participants)
	if err != nil {
		return err
	}
	seen := map[string]bool{}
	for _, id := range in.TenantIDs {
		seen[id] = true
	}
	seeds := append([]projectcatalog.Seed{}, Seeds...)
	for _, s := range seeds {
		if !seen[s.ID] {
			in.ExternalIDs = append(in.ExternalIDs, s.ID)
		}
	}
	in, err = projects.Inspect(ctx, db, Participants, in.ExternalIDs...)
	if err != nil {
		return err
	}
	_, err = projects.Install(ctx, db, in, seeds, io.Discard)
	return err
}
func Control(db *sql.DB) jobdb.ProjectControl { return projects.NewControl(db, nil) }

// Package archivefixture provides scoped HTTP identities for disposable tests.
package archivefixture

import (
	"context"
	"fmt"
	"net/http"
	"sort"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/remote"
)

const (
	Writer  = "fixture-writer"
	Reader  = "fixture-reader"
	Wrong   = "fixture-wrong-tenant"
	Outside = "fixture-outside-scope"
)

type Cells struct {
	Project string       `json:"project"`
	Other   string       `json:"otherProject"`
	Seeded  []jobdb.Cell `json:"seeded"`
	Hidden  []string     `json:"hiddenCellIds"`
}

func Seed(ctx context.Context, rt interface {
	jobdb.CellRuntime
	jobdb.CellArchiveRuntime
}, project, other string) (Cells, error) {
	fixture := Cells{Project: project, Other: other}
	ctx = jobdb.WithCellAuthority(ctx, jobdb.CellAuthority{Principal: "fixture-seed", TenantID: project,
		Permissions: map[string]bool{"cells.create": true, "cells.get": true, "cells.update": true, "cells.retire": true}})
	for index := range 9 {
		name := fmt.Sprintf("cell-%02d", index)
		cell, err := rt.CreateCell(ctx, jobdb.CreateCellRequest{TenantID: project, Name: name, Repo: "example.test/archive/" + name})
		if err != nil {
			return Cells{}, err
		}
		if index == 1 {
			newName := "renamed-01"
			cell, err = rt.UpdateCell(ctx, jobdb.UpdateCellRequest{CellKey: Key(cell), IfMatch: Revision(cell), Name: &newName})
			if err != nil {
				return Cells{}, err
			}
		}
		if index%2 == 1 || index == 8 {
			cell, err = rt.ArchiveCell(ctx, ArchiveRequest(cell))
			if err != nil {
				return Cells{}, err
			}
		}
		if index == 8 {
			cell, err = rt.RetireCell(ctx, jobdb.RetireCellRequest{CellKey: Key(cell), IfMatch: Revision(cell)})
			if err != nil {
				return Cells{}, err
			}
		}
		fixture.Seeded = append(fixture.Seeded, cell)
	}
	for _, indices := range [][]int{{0, 2, 4, 6}, {1, 3, 5, 7}} {
		sort.Slice(indices, func(i, j int) bool { return fixture.Seeded[indices[i]].ID < fixture.Seeded[indices[j]].ID })
		index := indices[1]
		if index < 2 {
			index = indices[2]
		}
		fixture.Hidden = append(fixture.Hidden, fixture.Seeded[index].ID)
	}
	return fixture, nil
}

func Key(cell jobdb.Cell) jobdb.CellKey {
	return jobdb.CellKey{TenantID: cell.TenantID, CellID: cell.ID}
}
func Revision(cell jobdb.Cell) string { return fmt.Sprintf("%q", cell.Revision) }
func ArchiveRequest(cell jobdb.Cell) jobdb.ArchiveCellRequest {
	return jobdb.ArchiveCellRequest{CellKey: Key(cell), IfMatch: Revision(cell)}
}

func (fixture Cells) InScope(index int) bool {
	return fixture.Seeded[index].ID != fixture.Hidden[0] && fixture.Seeded[index].ID != fixture.Hidden[1]
}

func (fixture Cells) Handler(runtime jobdb.ConsumerRuntime) http.Handler {
	read := map[string]bool{"cells.get": true, "cells.list": true}
	write := map[string]bool{"cells.get": true, "cells.list": true, "cells.update": true}
	scope := map[string]bool{}
	for index, cell := range fixture.Seeded {
		if fixture.InScope(index) {
			scope[cell.ID] = true
		}
	}
	identities := map[string]jobdb.CellAuthority{
		Writer:  {Principal: Writer, TenantID: fixture.Project, Permissions: write, Cells: scope},
		Reader:  {Principal: Reader, TenantID: fixture.Project, Permissions: read, Cells: scope},
		Wrong:   {Principal: Wrong, TenantID: fixture.Other, Permissions: write, Cells: scope},
		Outside: {Principal: Outside, TenantID: fixture.Project, Permissions: write, Cells: map[string]bool{fixture.Hidden[0]: true}},
	}
	routes := remote.NewConsumerServer(runtime)
	scoped := http.HandlerFunc(func(writer http.ResponseWriter, req *http.Request) {
		for token, authority := range identities {
			if req.Header.Get("Authorization") == "Bearer "+token {
				req = req.WithContext(jobdb.WithCellAuthority(req.Context(), authority))
				break
			}
		}
		routes.ServeHTTP(writer, req)
	})
	mux := http.NewServeMux()
	for _, family := range []string{"tenants", "projects"} {
		collection := "/v1/" + family + "/{id}/cells"
		mux.Handle("GET "+collection, scoped)
		mux.Handle("GET "+collection+"/{cell}", scoped)
		mux.Handle("POST "+collection+"/{cell}/archive", scoped)
		mux.Handle("POST "+collection+"/{cell}/unarchive", scoped)
	}
	return mux
}

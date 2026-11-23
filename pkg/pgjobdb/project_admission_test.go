package pgjobdb

import (
	"errors"
	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/lib/pq"
	"testing"
)

func TestProjectSQLDiagnostics(t *testing.T) {
	for _, tc := range []struct {
		code string
		want error
	}{{"PJC01", jobdb.ErrProjectUnavailable}, {"PJC02", jobdb.ErrProjectNotFound}, {"PJC03", jobdb.ErrInvalidProject}} {
		err := ProjectError(&pq.Error{Code: pq.ErrorCode(tc.code), Message: "private resource bytes"})
		if !errors.Is(err, tc.want) || err.Error() == "private resource bytes" {
			t.Fatal(err)
		}
	}
}

package pgjobdb

import "github.com/colony-2/jobdb/pkg/jobdb"

// Native selectors use the same opaque identifiers as the public runtime.
func validTypeName[T ~string](value T) bool {
	return jobdb.ValidateIdentifier(string(value)) == nil
}

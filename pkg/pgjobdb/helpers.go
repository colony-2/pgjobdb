package pgjobdb

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

func validateDB(ctx context.Context, db DB) error {
	if ctx == nil {
		return fmt.Errorf("pgjobdb: context is nil")
	}
	if db == nil {
		return fmt.Errorf("pgjobdb: DB is nil")
	}
	return nil
}

func isJSONObject(raw json.RawMessage) bool {
	if !json.Valid(raw) {
		return false
	}
	var value any
	if json.Unmarshal(raw, &value) != nil {
		return false
	}
	_, ok := value.(map[string]any)
	return ok
}

func optionalTime(value *time.Time) any {
	if value == nil {
		return nil
	}
	return value.UTC()
}
func nilIfEmpty(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	return values
}

type rowScanner interface{ Scan(...any) error }

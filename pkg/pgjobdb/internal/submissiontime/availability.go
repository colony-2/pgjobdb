// Package submissiontime checks submission availability without changing request identity.
package submissiontime

import (
	"encoding/json"
	"fmt"
	"time"
)

var minimum = time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC)
var maximum = time.Date(9999, 12, 31, 23, 59, 59, 999999000, time.UTC)

// Effective validates the original instant before rounding upward to PostgreSQL
// microseconds. It returns a separate UTC copy; nil retains the database default.
func Effective(original *time.Time) (*time.Time, error) {
	if original == nil {
		return nil, nil
	}
	value := original.UTC()
	if value.Before(minimum) || value.After(maximum) {
		return nil, fmt.Errorf("pgjobdb: submission availability is outside UTC years 1 through 9999 at microsecond precision")
	}
	if remainder := value.Nanosecond() % 1000; remainder != 0 {
		value = value.Add(time.Duration(1000-remainder) * time.Nanosecond)
	}
	if value.Before(*original) || value.Before(minimum) || value.After(maximum) {
		return nil, fmt.Errorf("pgjobdb: submission availability rounding exceeds the supported interval")
	}
	return &value, nil
}

// ValidateDescriptor reads the existing JobDB submit envelope without rewriting
// its original timestamp or other identity fields. Native descriptors are opaque.
func ValidateDescriptor(descriptor string) error {
	var envelope struct {
		Kind string
		Job  json.RawMessage
	}
	if json.Unmarshal([]byte(descriptor), &envelope) != nil || envelope.Kind != "submit" {
		return nil
	}
	var job struct{ AvailableAt *time.Time }
	if err := json.Unmarshal(envelope.Job, &job); err != nil {
		return fmt.Errorf("pgjobdb: invalid submission availability descriptor: %w", err)
	}
	_, err := Effective(job.AvailableAt)
	return err
}

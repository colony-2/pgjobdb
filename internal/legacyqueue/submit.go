package legacyqueue

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/lib/pq"
)

const submitStmt = `
SELECT job_id, next_need, wait_for, payload, available_at
FROM pgjobdb.submit_job($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
`

// SubmitJob inserts workflow metadata using pgjobdb.submit_job.
// metadata is optional; nil defaults to {} and is immutable after creation.
// expiresAt is optional; zero value keeps the job leaseable indefinitely.
func SubmitJob(ctx context.Context, db DB, tenantID TenantID, jobID JobID, deps JobDependencies, payload any, metadata any, worker WorkerID, expiresAt time.Time) error {
	if db == nil {
		return fmt.Errorf("pgjobdb: nil DB")
	}
	if ctx == nil {
		return fmt.Errorf("pgjobdb: nil context")
	}
	if tenantID == "" {
		return fmt.Errorf("pgjobdb: tenant id is required")
	}
	if jobID == "" {
		return fmt.Errorf("pgjobdb: job id is required")
	}
	if worker == "" {
		return fmt.Errorf("pgjobdb: worker id is required")
	}
	if err := deps.validate(); err != nil {
		return err
	}
	payloadArg, err := normalizePayload(payload)
	if err != nil {
		return err
	}
	metadataArg, err := normalizeMetadata(metadata)
	if err != nil {
		return err
	}
	altNeedArg, altAfterArg := deps.alternateArgsForSubmit()

	row := db.QueryRowContext(ctx, submitStmt,
		string(tenantID),
		string(jobID),
		string(worker),
		string(deps.NextNeed),
		pq.Array(deps.waitForStrings()),
		payloadArg,
		metadataArg,
		deps.availableAtArg(),
		expiresAtArg(expiresAt),
		altNeedArg,
		altAfterArg,
	)

	var (
		id          string
		need        string
		waits       pq.StringArray
		payloadJSON json.RawMessage
		available   sql.NullTime
	)

	if err := row.Scan(&id, &need, (*pq.StringArray)(&waits), &payloadJSON, &available); err != nil {
		return annotateError(err)
	}

	return nil
}

func expiresAtArg(t time.Time) any {
	if t.IsZero() {
		return nil
	}
	return t
}

func normalizePayload(payload any) (json.RawMessage, error) {
	if payload == nil {
		return json.RawMessage(`{}`), nil
	}
	return encodePayload(payload)
}

func normalizeMetadata(metadata any) (json.RawMessage, error) {
	if metadata == nil {
		return json.RawMessage(`{}`), nil
	}
	return encodeMetadata(metadata)
}

func normalizePayloadOverride(payload any) (json.RawMessage, error) {
	if payload == nil {
		return nil, nil
	}
	return encodePayload(payload)
}

func encodePayload(payload any) (json.RawMessage, error) {
	switch v := payload.(type) {
	case json.RawMessage:
		return ensurePayloadObject(v)
	case []byte:
		return ensurePayloadObject(json.RawMessage(v))
	default:
		encoded, err := json.Marshal(v)
		if err != nil {
			return nil, fmt.Errorf("pgjobdb: encoding payload: %w", err)
		}
		return ensurePayloadObject(encoded)
	}
}

func encodeMetadata(metadata any) (json.RawMessage, error) {
	switch v := metadata.(type) {
	case json.RawMessage:
		return ensureMetadataObject(v)
	case []byte:
		return ensureMetadataObject(json.RawMessage(v))
	default:
		encoded, err := json.Marshal(v)
		if err != nil {
			return nil, fmt.Errorf("pgjobdb: encoding metadata: %w", err)
		}
		return ensureMetadataObject(encoded)
	}
}

func ensurePayloadObject(raw json.RawMessage) (json.RawMessage, error) {
	return ensureJSONObject(raw, "payload")
}

func ensureMetadataObject(raw json.RawMessage) (json.RawMessage, error) {
	return ensureJSONObject(raw, "metadata")
}

func ensureJSONObject(raw json.RawMessage, field string) (json.RawMessage, error) {
	if len(raw) == 0 {
		return json.RawMessage(`{}`), nil
	}
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(raw, &obj); err != nil {
		return nil, fmt.Errorf("pgjobdb: %s must be a JSON object: %w", field, err)
	}
	if obj == nil {
		return nil, fmt.Errorf("pgjobdb: %s must be a JSON object", field)
	}
	return raw, nil
}

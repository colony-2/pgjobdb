package runtimecodec

import (
	"bytes"
	"encoding/json"
)

const (
	PayloadKindApp                = "App"
	PayloadKindAppError           = "AppError"
	PayloadKindSystemError        = "SystemError"
	PayloadKindTimeout            = "Timeout"
	ChapterTypeJobStart           = "JobStart"
	ChapterTypeJobAttemptOutcome  = "JobAttemptOutcome"
	ChapterTypeTaskAttemptOutcome = "TaskAttemptOutcome"
	ChapterTypeRestartExtra       = "RestartExtra"
)

func cloneJSON(raw json.RawMessage) json.RawMessage {
	return append(json.RawMessage(nil), raw...)
}

func decodeJSONValue(raw []byte) (any, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, err
	}
	return value, nil
}

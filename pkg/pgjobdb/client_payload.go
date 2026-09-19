package pgjobdb

import (
	"errors"
	"fmt"

	"github.com/colony-2/jobdb/pkg/jobdb/clientpayload"
	"github.com/lib/pq"
)

func clientUpdateArgs(u *ClientPayloadUpdate, initial bool) (mode, value, revision any, err error) {
	if err = clientpayload.ValidateUpdate(u, initial); err != nil {
		return
	}
	if u == nil {
		return
	}
	mode = u.Mode
	if u.Value != nil {
		value = string(u.Value)
	}
	if u.ExpectedRevision != nil {
		revision = *u.ExpectedRevision
	}
	return
}
func clientPayloadError(err error) error {
	var pg *pq.Error
	if errors.As(err, &pg) {
		switch pg.Code {
		case "JCP01":
			return fmt.Errorf("%w: %s", clientpayload.ErrInvalid, pg.Message)
		case "JCP02":
			return fmt.Errorf("%w: %s", clientpayload.ErrConflict, pg.Message)
		case "JCP03":
			return clientpayload.ErrTooLarge
		}
	}
	return err
}

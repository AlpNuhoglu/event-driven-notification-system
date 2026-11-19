package domain

import (
	"errors"

	"github.com/google/uuid"
)

// idLen is the length of the canonical text form of an id:
// xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx.
const idLen = 36

// ErrInvalidID is returned by ParseID for a value that is not an id in the
// canonical form. It never contains the value.
var ErrInvalidID = errors.New("domain: not an id in the canonical form")

// ParseID parses an id that arrives from outside the process: a path or
// query parameter, a request body, a WebSocket request, a message body. It
// accepts the canonical 36-character form only, with hexadecimal digits in
// either case, so that one id has one spelling everywhere.
//
// uuid.Parse alone also accepts the urn:uuid: prefix, braces, the 32 digits
// without hyphens and any 38-character string whose middle is a UUID (it
// drops the first and the last character unseen). With the length fixed at
// 36, it requires the hyphens at their places and hexadecimal digits
// elsewhere. This is the only call of the lenient parser outside the tests
// (TestLenientUUIDParserIsCalledOnlyByParseID).
func ParseID(s string) (uuid.UUID, error) {
	if len(s) != idLen {
		return uuid.UUID{}, ErrInvalidID
	}
	id, err := uuid.Parse(s)
	if err != nil {
		return uuid.UUID{}, ErrInvalidID
	}
	return id, nil
}

package domain

import (
	"encoding/base64"
	"encoding/binary"
	"errors"
	"time"

	"github.com/google/uuid"
)

// cursorLen is the size of the decoded cursor: the creation time as Unix
// microseconds (8 bytes, big-endian) followed by the 16 bytes of the id.
const cursorLen = 8 + 16

// ErrInvalidCursor is returned by DecodeCursor for a value that Encode could
// not have produced.
var ErrInvalidCursor = errors.New("domain: invalid cursor")

// Cursor is the position of a notification in the listing order
// (created_at descending, then id descending): a page continues after it.
type Cursor struct {
	CreatedAt time.Time
	ID        uuid.UUID
}

// Encode returns the opaque form of the cursor: base64 (URL alphabet, no
// padding) of the Unix microseconds of CreatedAt followed by the id. The
// time is kept at microsecond precision, the precision of timestamptz.
func (c Cursor) Encode() string {
	var b [cursorLen]byte
	binary.BigEndian.PutUint64(b[:8], uint64(c.CreatedAt.UnixMicro())) //nolint:gosec // two's complement round-trips through int64
	copy(b[8:], c.ID[:])
	return base64.RawURLEncoding.EncodeToString(b[:])
}

// DecodeCursor parses the value Encode returned. It rejects anything else:
// invalid base64 (also with padding) any length
// but 24 bytes, and a time outside the years 1 to 9999. The returned time is in UTC.
func DecodeCursor(s string) (Cursor, error) {
	raw, err := base64.RawURLEncoding.Strict().DecodeString(s)
	if err != nil || len(raw) != cursorLen {
		return Cursor{}, ErrInvalidCursor
	}
	var id uuid.UUID
	copy(id[:], raw[8:])
	micros := int64(binary.BigEndian.Uint64(raw[:8])) //nolint:gosec // two's complement round-trip
	at := time.UnixMicro(micros).UTC()
	if y := at.Year(); y < 1 || y > 9999 {
		return Cursor{}, ErrInvalidCursor // no row has such a time; keep it away from the database
	}
	return Cursor{CreatedAt: at, ID: id}, nil
}

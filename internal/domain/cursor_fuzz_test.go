package domain

import (
	"testing"
	"time"

	"github.com/google/uuid"
)

func FuzzDecodeCursor(f *testing.F) {
	f.Add(Cursor{CreatedAt: time.UnixMicro(1_700_000_000_000_000), ID: uuid.MustParse("0190a5c0-1111-7222-8333-444455556666")}.Encode())
	f.Add("")
	f.Add("AAAA")
	f.Add("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")
	// Found by this target: the base64 decoder skips CR and LF, so this
	// string decoded to a valid cursor with a second spelling.
	f.Add("A000000000000000000\r0000000000000")
	f.Fuzz(func(t *testing.T, s string) {
		c, err := DecodeCursor(s)
		if err != nil {
			return
		}
		enc := c.Encode()
		if enc != s {
			t.Fatalf("DecodeCursor accepted %q but Encode gives %q", s, enc)
		}
		again, err := DecodeCursor(enc)
		if err != nil || !again.CreatedAt.Equal(c.CreatedAt) || again.ID != c.ID {
			t.Fatalf("re-decode of %q: %+v, %v; want %+v", enc, again, err, c)
		}
	})
}

func FuzzCursorRoundTrip(f *testing.F) {
	f.Add(int64(0), make([]byte, 16))
	f.Add(int64(1_700_000_000_123_456), []byte("0123456789abcdef"))
	f.Fuzz(func(t *testing.T, micros int64, id []byte) {
		if len(id) != 16 {
			return
		}
		lo := time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC).UnixMicro()
		hi := time.Date(9999, 12, 31, 23, 59, 59, 999999000, time.UTC).UnixMicro()
		span := hi - lo + 1
		micros = lo + ((micros%span)+span)%span
		var u uuid.UUID
		copy(u[:], id)
		c := Cursor{CreatedAt: time.UnixMicro(micros).UTC(), ID: u}
		got, err := DecodeCursor(c.Encode())
		if err != nil || !got.CreatedAt.Equal(c.CreatedAt) || got.ID != c.ID {
			t.Fatalf("round trip of %+v: %+v, %v", c, got, err)
		}
	})
}

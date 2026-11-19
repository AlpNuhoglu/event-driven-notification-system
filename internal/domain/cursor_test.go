package domain

import (
	"encoding/base64"
	"encoding/binary"
	"errors"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestCursor_RoundTrip(t *testing.T) {
	id := uuid.MustParse("0193f0d2-7c1e-7b3a-9d41-5be0a1c2e3f4")
	for _, at := range []time.Time{
		time.UnixMicro(0).UTC(),
		time.Date(2026, 5, 17, 10, 30, 15, 123456000, time.UTC),
		time.Date(1969, 7, 20, 20, 17, 40, 1000, time.UTC), // before the epoch
		time.Date(2262, 4, 11, 23, 47, 16, 854775000, time.UTC),
	} {
		c := Cursor{CreatedAt: at, ID: id}
		s := c.Encode()
		if len(s) != 32 || strings.ContainsAny(s, "=+/") {
			t.Errorf("Encode = %q: want 32 URL-safe characters without padding", s)
		}
		got, err := DecodeCursor(s)
		if err != nil {
			t.Fatalf("DecodeCursor(%q): %v", s, err)
		}
		if !got.CreatedAt.Equal(c.CreatedAt) || got.ID != c.ID {
			t.Errorf("round trip: got %+v, want %+v", got, c)
		}
	}
}

func TestCursor_EncodeTruncatesToMicroseconds(t *testing.T) {
	id := uuid.New()
	at := time.Date(2026, 1, 2, 3, 4, 5, 123456789, time.UTC)
	got, err := DecodeCursor(Cursor{CreatedAt: at, ID: id}.Encode())
	if err != nil {
		t.Fatal(err)
	}
	if want := at.Truncate(time.Microsecond); !got.CreatedAt.Equal(want) {
		t.Errorf("got %v, want %v", got.CreatedAt, want)
	}
}

func TestDecodeCursor_Rejects(t *testing.T) {
	valid := Cursor{CreatedAt: time.UnixMicro(1_700_000_000_000_000), ID: uuid.New()}.Encode()
	enc := func(n int) string { return base64.RawURLEncoding.EncodeToString(make([]byte, n)) }
	tests := []struct{ name, in string }{
		{"empty", ""},
		{"not base64", "!!!!"},
		{"standard alphabet", strings.NewReplacer("-", "+", "_", "/").Replace(base64.RawURLEncoding.EncodeToString([]byte{0xfb, 0xff, 0xfe, 0xfb, 0xff, 0xfe, 0xfb, 0xff, 0xfe, 0xfb, 0xff, 0xfe, 0xfb, 0xff, 0xfe, 0xfb, 0xff, 0xfe, 0xfb, 0xff, 0xfe, 0xfb, 0xff, 0xfe}))},
		{"padded", valid + "=="},
		{"too short", enc(23)},
		{"too long", enc(25)},
		{"one byte", enc(1)},
		{"truncated text", valid[:len(valid)-1]},
		{"extra text", valid + "A"},
		{"spaces", " " + valid},
	}
	for _, tt := range tests {
		if _, err := DecodeCursor(tt.in); !errors.Is(err, ErrInvalidCursor) {
			t.Errorf("%s: DecodeCursor(%q) error = %v, want ErrInvalidCursor", tt.name, tt.in, err)
		}
	}
	for _, micros := range []int64{math.MinInt64, math.MaxInt64, -62_135_596_800_000_001, 253_402_300_800_000_000} {
		var b [24]byte
		binary.BigEndian.PutUint64(b[:8], uint64(micros)) //nolint:gosec // the bit pattern is the point
		if _, err := DecodeCursor(base64.RawURLEncoding.EncodeToString(b[:])); !errors.Is(err, ErrInvalidCursor) {
			t.Errorf("micros %d: error = %v, want ErrInvalidCursor", micros, err)
		}
	}
	if _, err := DecodeCursor(enc(24)); err != nil {
		t.Errorf("24 zero bytes: %v", err)
	}
}

// A cursor has exactly one accepted spelling. The base64 decoder skips CR and
// LF, so without the check in DecodeCursor these would decode to a cursor.
func TestDecodeCursor_OneSpelling(t *testing.T) {
	valid := Cursor{CreatedAt: time.UnixMicro(1_700_000_000_000_000).UTC(), ID: uuid.MustParse("0190a5c0-1111-7222-8333-444455556666")}.Encode()
	if _, err := DecodeCursor(valid); err != nil {
		t.Fatalf("valid cursor: %v", err)
	}
	for _, s := range []string{
		valid[:10] + "\r" + valid[10:],
		valid[:10] + "\n" + valid[10:],
		valid + "\n",
		"\r\n" + valid,
		"A000000000000000000\r0000000000000",
	} {
		if _, err := DecodeCursor(s); err == nil {
			t.Errorf("DecodeCursor(%q) succeeded", s)
		}
	}
}

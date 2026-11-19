package domain

import (
	"testing"
	"time"
)

func FuzzParseRetryAfter(f *testing.F) {
	for _, s := range []string{
		"", "0", "1", "120", "-1", "+1", "1.5", " 5\t", "99999999999999999999",
		"Wed, 21 Oct 2015 07:28:10 GMT", "Wednesday, 21-Oct-15 07:28:10 GMT",
		"Wed Oct 21 07:28:10 2015", "garbage",
	} {
		f.Add(s)
	}
	now := time.Date(2015, 10, 21, 7, 28, 0, 0, time.UTC)
	f.Fuzz(func(t *testing.T, s string) {
		d, ok := ParseRetryAfter(s, now)
		if ok && d <= 0 {
			t.Fatalf("ParseRetryAfter(%q) = %v, true; want d > 0", s, d)
		}
		if !ok && d != 0 {
			t.Fatalf("ParseRetryAfter(%q) = %v, false; want d == 0", s, d)
		}
	})
}

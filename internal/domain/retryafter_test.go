package domain

import (
	"math"
	"testing"
	"time"
)

func TestParseRetryAfter(t *testing.T) {
	now := time.Date(2015, 10, 21, 7, 28, 0, 0, time.UTC)
	tests := []struct {
		name  string
		value string
		want  time.Duration
		ok    bool
	}{
		{"seconds", "120", 120 * time.Second, true},
		{"one", "1", time.Second, true},
		{"leading zeros", "0005", 5 * time.Second, true},
		{"whitespace", " \t30 \t", 30 * time.Second, true},
		{"zero", "0", 0, false},
		{"zeros", "000", 0, false},
		{"negative", "-5", 0, false},
		{"plus sign", "+5", 0, false},
		{"decimal", "1.5", 0, false},
		{"empty", "", 0, false},
		{"only spaces", "  ", 0, false},
		{"garbage", "soon", 0, false},
		{"digits and letters", "12s", 0, false},
		{"inner space", "1 2", 0, false},
		{"huge", "99999999999999999999999999", math.MaxInt64, true},
		{"just over the limit", "9223372037", math.MaxInt64, true},
		{"largest exact", "9223372036", 9223372036 * time.Second, true},
		{"IMF-fixdate", "Wed, 21 Oct 2015 07:28:10 GMT", 10 * time.Second, true},
		{"IMF-fixdate padded", " Wed, 21 Oct 2015 07:29:00 GMT\t", time.Minute, true},
		{"RFC 850", "Wednesday, 21-Oct-15 07:30:00 GMT", 2 * time.Minute, true},
		{"asctime", "Wed Oct 21 07:28:05 2015", 5 * time.Second, true},
		{"asctime single-digit day", "Fri Nov  6 08:49:37 2015", 16*24*time.Hour + 4897*time.Second, true},
		{"past date", "Wed, 21 Oct 2015 07:27:59 GMT", 0, false},
		{"date equal to now", "Wed, 21 Oct 2015 07:28:00 GMT", 0, false},
		{"bad date", "Wed, 32 Oct 2015 07:28:00 GMT", 0, false},
		{"date without zone", "Wed, 21 Oct 2015 07:28:10", 0, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d, ok := ParseRetryAfter(tt.value, now)
			if ok != tt.ok || d != tt.want {
				t.Errorf("ParseRetryAfter(%q) = %v, %v; want %v, %v", tt.value, d, ok, tt.want, tt.ok)
			}
		})
	}
}

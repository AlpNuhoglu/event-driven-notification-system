package domain

import (
	"math"
	"strings"
	"time"
)

// The three HTTP-date formats of RFC 9110 section 5.6.7.
const (
	httpDateIMF     = "Mon, 02 Jan 2006 15:04:05 GMT"  // IMF-fixdate
	httpDateRFC850  = "Monday, 02-Jan-06 15:04:05 GMT" // obsolete RFC 850
	httpDateAsctime = "Mon Jan _2 15:04:05 2006"       // obsolete asctime
)

// ParseRetryAfter parses a Retry-After header value: delta-seconds or an
// HTTP-date. ok is false when the value is not usable: empty, malformed,
// zero, or a date that is not after now.
func ParseRetryAfter(value string, now time.Time) (d time.Duration, ok bool) {
	value = strings.Trim(value, " \t")
	if value == "" {
		return 0, false
	}
	if isDigits(value) {
		d = deltaSeconds(value)
	} else {
		t, found := parseHTTPDate(value)
		if !found {
			return 0, false
		}
		d = t.Sub(now)
	}
	if d <= 0 {
		return 0, false
	}
	return d, true
}

func isDigits(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return s != ""
}

// deltaSeconds converts a digit string to a duration, saturating at the
// largest duration.
func deltaSeconds(s string) time.Duration {
	const maxSeconds = int64(math.MaxInt64 / int64(time.Second))
	var n int64
	for i := 0; i < len(s); i++ {
		digit := int64(s[i] - '0')
		if n > (maxSeconds-digit)/10 {
			return time.Duration(math.MaxInt64)
		}
		n = n*10 + digit
	}
	return time.Duration(n) * time.Second
}

func parseHTTPDate(s string) (time.Time, bool) {
	for _, layout := range []string{httpDateIMF, httpDateRFC850, httpDateAsctime} {
		if t, err := time.Parse(layout, s); err == nil {
			return t, true
		}
	}
	return time.Time{}, false
}

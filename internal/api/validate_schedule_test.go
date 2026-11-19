package api

import (
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
)

// testNow is the instant the validation tests judge times against.
var testNow = time.Date(2026, 11, 19, 10, 0, 0, 0, time.UTC)

// testRules are the rules of a create with the default limits on the clock
// testNow.
func testRules(phone domain.PhoneValidation) createRules {
	return createRules{phone: phone, schedule: scheduleRules{
		now:       testNow,
		tolerance: DefaultScheduledAtTolerance,
		maxAhead:  DefaultScheduledAtMaxAhead,
		minLead:   DefaultExpiresAtMinLead,
	}}
}

// rfc formats a time as a request sends it, with nanoseconds when it has
// them.
func rfc(t time.Time) string { return t.Format(time.RFC3339Nano) }

// scheduleBody is a valid create with the given extra members (raw JSON).
func scheduleBody(extra string) string {
	return `{"channel":"sms","recipient":"+905551234567","content":"x"` + extra + `}`
}

func TestValidateCreate_Schedule(t *testing.T) {
	const (
		tol   = 30 * time.Second
		ahead = 720 * time.Hour
		lead  = 30 * time.Second
	)
	at := func(d time.Duration) string { return `"` + rfc(testNow.Add(d)) + `"` }
	sched := func(v string) string { return `,"scheduled_at":` + v }
	exp := func(v string) string { return `,"expires_at":` + v }
	ptr := func(d time.Duration) *time.Time { t := testNow.Add(d); return &t }

	tests := []struct {
		name  string
		extra string
		codes []string // nil: valid
		// The parsed values when valid (nil: absent).
		scheduled, expires *time.Time
	}{
		{name: "neither field", extra: ""},
		{name: "scheduled_at null is absent", extra: sched(`null`)},
		{name: "scheduled_at in the future", extra: sched(at(time.Hour)), scheduled: ptr(time.Hour)},
		{name: "scheduled_at one microsecond ahead is scheduled", extra: sched(at(time.Microsecond)), scheduled: ptr(time.Microsecond)},
		{name: "scheduled_at now is now", extra: sched(at(0))},
		{name: "scheduled_at inside the tolerance is now", extra: sched(at(-10 * time.Second))},
		{name: "scheduled_at at now - tolerance is now", extra: sched(at(-tol))},
		{name: "scheduled_at one microsecond older than the tolerance", extra: sched(at(-tol - time.Microsecond)), codes: []string{"scheduled_at:in_the_past"}},
		{name: "scheduled_at long ago", extra: sched(`"2001-01-01T00:00:00Z"`), codes: []string{"scheduled_at:in_the_past"}},
		{name: "scheduled_at at now + horizon", extra: sched(at(ahead)), scheduled: ptr(ahead)},
		{name: "scheduled_at one microsecond beyond the horizon", extra: sched(at(ahead + time.Microsecond)), codes: []string{"scheduled_at:too_far_ahead"}},
		{name: "scheduled_at far beyond the horizon", extra: sched(`"9999-12-31T23:59:59Z"`), codes: []string{"scheduled_at:too_far_ahead"}},
		{name: "scheduled_at sub-microsecond beyond the horizon is cut to it", extra: sched(at(ahead + 999*time.Nanosecond)), scheduled: ptr(ahead)},
		{name: "scheduled_at not a string", extra: sched(`1763546400`), codes: []string{"scheduled_at:invalid_type"}},
		{name: "scheduled_at an object", extra: sched(`{}`), codes: []string{"scheduled_at:invalid_type"}},
		{name: "scheduled_at empty", extra: sched(`""`), codes: []string{"scheduled_at:invalid_value"}},
		{name: "scheduled_at without an offset", extra: sched(`"2026-11-19T11:00:00"`), codes: []string{"scheduled_at:invalid_value"}},
		{name: "scheduled_at a date only", extra: sched(`"2026-11-19"`), codes: []string{"scheduled_at:invalid_value"}},
		{name: "scheduled_at with a space", extra: sched(`"2026-11-19 11:00:00Z"`), codes: []string{"scheduled_at:invalid_value"}},
		{name: "scheduled_at with a compact offset", extra: sched(`"2026-11-19T14:00:00+0300"`), codes: []string{"scheduled_at:invalid_value"}},
		{name: "scheduled_at with a lower-case z", extra: sched(`"2026-11-19T11:00:00z"`), codes: []string{"scheduled_at:invalid_value"}},
		{name: "scheduled_at with an hour 24", extra: sched(`"2026-11-19T24:00:00Z"`), codes: []string{"scheduled_at:invalid_value"}},
		{name: "scheduled_at on a day that does not exist", extra: sched(`"2026-02-30T10:00:00Z"`), codes: []string{"scheduled_at:invalid_value"}},
		{name: "scheduled_at as Unix seconds in a string", extra: sched(`"1763546400"`), codes: []string{"scheduled_at:invalid_value"}},
		{name: "scheduled_at repeated", extra: sched(at(time.Hour)) + sched(at(time.Hour)), codes: []string{"scheduled_at:duplicate_field"}},

		// Every offset form names the same instant.
		{name: "offset Z", extra: sched(`"2026-11-19T11:00:00Z"`), scheduled: ptr(time.Hour)},
		{name: "offset +00:00", extra: sched(`"2026-11-19T11:00:00+00:00"`), scheduled: ptr(time.Hour)},
		{name: "offset -00:00", extra: sched(`"2026-11-19T11:00:00-00:00"`), scheduled: ptr(time.Hour)},
		{name: "offset +03:00", extra: sched(`"2026-11-19T14:00:00+03:00"`), scheduled: ptr(time.Hour)},
		{name: "offset -05:30", extra: sched(`"2026-11-19T05:30:00-05:30"`), scheduled: ptr(time.Hour)},
		{name: "offset +14:00", extra: sched(`"2026-11-20T01:00:00+14:00"`), scheduled: ptr(time.Hour)},
		{name: "fraction of a second", extra: sched(`"2026-11-19T11:00:00.123456Z"`), scheduled: ptr(time.Hour + 123456*time.Microsecond)},
		{name: "nanoseconds are cut to the microsecond", extra: sched(`"2026-11-19T11:00:00.123456789Z"`), scheduled: ptr(time.Hour + 123456*time.Microsecond)},

		// expires_at against now (no scheduled_at).
		{name: "expires_at at now + lead", extra: exp(at(lead)), expires: ptr(lead)},
		{name: "expires_at one microsecond short of now + lead", extra: exp(at(lead - time.Microsecond)), codes: []string{"expires_at:too_soon"}},
		{name: "expires_at in the past", extra: exp(at(-time.Hour)), codes: []string{"expires_at:too_soon"}},
		{name: "expires_at far ahead", extra: exp(at(90 * 24 * time.Hour)), expires: ptr(90 * 24 * time.Hour)},
		{name: "expires_at against now when scheduled_at is null", extra: sched(`null`) + exp(at(lead-time.Microsecond)), codes: []string{"expires_at:too_soon"}},

		// expires_at against a future scheduled_at.
		{name: "expires_at at scheduled_at + lead", extra: sched(at(time.Hour)) + exp(at(time.Hour+lead)), scheduled: ptr(time.Hour), expires: ptr(time.Hour + lead)},
		{name: "expires_at one microsecond short of scheduled_at + lead", extra: sched(at(time.Hour)) + exp(at(time.Hour+lead-time.Microsecond)), codes: []string{"expires_at:too_soon"}},
		{name: "expires_at after now + lead but before scheduled_at + lead", extra: sched(at(time.Hour)) + exp(at(10*time.Minute)), codes: []string{"expires_at:too_soon"}},
		{name: "expires_at before scheduled_at, field order reversed", extra: exp(at(10*time.Minute)) + sched(at(time.Hour)), codes: []string{"expires_at:too_soon"}},

		// expires_at against a scheduled_at within the tolerance: the start is now.
		{name: "expires_at at now + lead with scheduled_at in the tolerance", extra: sched(at(-20*time.Second)) + exp(at(lead)), expires: ptr(lead)},
		{name: "expires_at at scheduled_at + lead with scheduled_at in the tolerance", extra: sched(at(-20*time.Second)) + exp(at(lead-20*time.Second)), codes: []string{"expires_at:too_soon"}},

		// expires_at type and format.
		{name: "expires_at null", extra: exp(`null`), codes: []string{"expires_at:invalid_type"}},
		{name: "expires_at a number", extra: exp(`1`), codes: []string{"expires_at:invalid_type"}},
		{name: "expires_at without an offset", extra: exp(`"2026-11-20T10:00:00"`), codes: []string{"expires_at:invalid_value"}},
		{name: "expires_at with an offset", extra: exp(`"2026-11-19T13:01:00+03:00"`), expires: ptr(time.Minute)},
		{name: "expires_at repeated", extra: exp(at(time.Hour)) + exp(at(time.Hour)), codes: []string{"expires_at:duplicate_field"}},

		// When scheduled_at is invalid, expires_at is only checked for its
		// type and format.
		{name: "scheduled_at in the past, expires_at that would be too soon", extra: sched(at(-time.Hour)) + exp(at(time.Second)), codes: []string{"scheduled_at:in_the_past"}},
		{name: "scheduled_at too far, expires_at before it", extra: sched(at(ahead+time.Hour)) + exp(at(time.Hour)), codes: []string{"scheduled_at:too_far_ahead"}},
		{name: "scheduled_at repeated, expires_at that would be too soon", extra: sched(at(time.Hour)) + sched(at(time.Hour)) + exp(at(time.Second)), codes: []string{"scheduled_at:duplicate_field"}},
		{name: "scheduled_at not a string, expires_at malformed", extra: sched(`true`) + exp(`"tomorrow"`), codes: []string{"scheduled_at:invalid_type", "expires_at:invalid_value"}},
		{name: "both invalid", extra: sched(at(-time.Hour)) + exp(`null`), codes: []string{"scheduled_at:in_the_past", "expires_at:invalid_type"}},
		{name: "both repeated", extra: sched(at(time.Hour)) + exp(at(2*time.Hour)) + sched(at(time.Hour)) + exp(at(2*time.Hour)), codes: []string{"scheduled_at:duplicate_field", "expires_at:duplicate_field"}},

		// Order among the other fields.
		{
			name:  "errors in field order",
			extra: `,"dedupe_key":""` + exp(`1`) + sched(`1`) + `,"priority":"x"`,
			codes: []string{"priority:invalid_value", "scheduled_at:invalid_type", "expires_at:invalid_type", "dedupe_key:empty"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req, p := validateCreate([]byte(scheduleBody(tt.extra)), testRules(domain.PhonePossible))
			if tt.codes != nil {
				if p == nil || p.kind.status != 422 || !slices.Equal(codes(p), tt.codes) {
					t.Fatalf("got %v, want 422 %v", codes(p), tt.codes)
				}
				return
			}
			if p != nil {
				t.Fatalf("got %d %v, want valid", p.kind.status, codes(p))
			}
			sameTime(t, "scheduled_at", req.request.ScheduledAt, tt.scheduled)
			sameTime(t, "expires_at", req.request.ExpiresAt, tt.expires)
		})
	}
}

func sameTime(t *testing.T, name string, got, want *time.Time) {
	t.Helper()
	switch {
	case got == nil && want == nil:
	case got == nil || want == nil:
		t.Errorf("%s = %v, want %v", name, got, want)
	case !got.Equal(*want) || got.Location() != time.UTC:
		t.Errorf("%s = %v, want %v in UTC", name, *got, *want)
	}
}

// The limits are those of the rules, not constants: a shorter tolerance,
// horizon and lead move every boundary.
func TestValidateCreate_ScheduleLimitsAreConfiguration(t *testing.T) {
	rules := createRules{phone: domain.PhonePossible, schedule: scheduleRules{
		now: testNow, tolerance: 2 * time.Second, maxAhead: time.Minute, minLead: 500 * time.Millisecond,
	}}
	tests := []struct {
		extra string
		codes []string
	}{
		{`,"scheduled_at":"` + rfc(testNow.Add(-2*time.Second)) + `"`, nil},
		{`,"scheduled_at":"` + rfc(testNow.Add(-3*time.Second)) + `"`, []string{"scheduled_at:in_the_past"}},
		{`,"scheduled_at":"` + rfc(testNow.Add(time.Minute)) + `"`, nil},
		{`,"scheduled_at":"` + rfc(testNow.Add(2*time.Minute)) + `"`, []string{"scheduled_at:too_far_ahead"}},
		{`,"expires_at":"` + rfc(testNow.Add(500*time.Millisecond)) + `"`, nil},
		{`,"expires_at":"` + rfc(testNow.Add(400*time.Millisecond)) + `"`, []string{"expires_at:too_soon"}},
	}
	for _, tt := range tests {
		_, p := validateCreate([]byte(scheduleBody(tt.extra)), rules)
		if got := codes(p); !slices.Equal(got, tt.codes) {
			t.Errorf("%s: got %v, want %v", tt.extra, got, tt.codes)
		}
	}
}

// The messages name the limits and never the submitted value.
func TestValidateCreate_ScheduleMessages(t *testing.T) {
	const past, far = "2001-02-03T04:05:06Z", "2099-02-03T04:05:06Z"
	body := scheduleBody(`,"scheduled_at":"` + past + `"`)
	p := checkCreate([]byte(body), domain.PhonePossible)
	far1 := checkCreate([]byte(scheduleBody(`,"scheduled_at":"`+far+`"`)), domain.PhonePossible)
	soon := checkCreate([]byte(scheduleBody(`,"expires_at":"`+rfc(testNow)+`"`)), domain.PhonePossible)
	want := map[string]string{
		"scheduled_at must not be more than 30s in the past":                        msg(p),
		"scheduled_at must not be more than 720h ahead":                             msg(far1),
		"expires_at must be at least 30s after the due time (scheduled_at, or now)": msg(soon),
	}
	for w, got := range want {
		if got != w {
			t.Errorf("message %q, want %q", got, w)
		}
		if strings.Contains(got, "2001") || strings.Contains(got, "2099") || strings.Contains(got, "2026") {
			t.Errorf("message leaks a value: %q", got)
		}
	}
}

func msg(p *problem) string {
	if p == nil || len(p.errors) != 1 {
		return ""
	}
	return p.errors[0].Message
}

// Every item of a batch is judged at the same instant, by the rules of the
// request; an item with a time in the past is rejected and the others stay.
func TestValidateBatch_Schedule(t *testing.T) {
	item := func(extra string) string { return scheduleBody(extra) }
	body := batchOf(
		item(`,"scheduled_at":"`+rfc(testNow.Add(time.Hour))+`"`),
		item(`,"scheduled_at":"`+rfc(testNow.Add(-time.Hour))+`"`),
		item(`,"scheduled_at":"`+rfc(testNow.Add(-DefaultScheduledAtTolerance))+`"`),
		item(`,"scheduled_at":null,"expires_at":"`+rfc(testNow.Add(time.Second))+`"`),
		item(`,"expires_at":"`+rfc(testNow.Add(time.Hour))+`"`),
	)
	in, p := validateBatch([]byte(body), testRules(domain.PhonePossible))
	if p != nil {
		t.Fatalf("problem %v", codes(p))
	}
	if got := validIndexes(in); !slices.Equal(got, []int{0, 2, 4}) {
		t.Errorf("valid = %v", got)
	}
	if got := rejectedCodes(in); !slices.Equal(got, []string{"1/scheduled_at:in_the_past", "3/expires_at:too_soon"}) {
		t.Errorf("rejected = %v", got)
	}
	if len(in.valid) == 3 {
		sameTime(t, "item 0 scheduled_at", in.valid[0].request.ScheduledAt, new(testNow.Add(time.Hour)))
		sameTime(t, "item 2 scheduled_at", in.valid[1].request.ScheduledAt, nil)
		sameTime(t, "item 4 expires_at", in.valid[2].request.ExpiresAt, new(testNow.Add(time.Hour)))
	}
}

// The redrive keeps reading its expires_at with the same parser.
func TestParseDateTime(t *testing.T) {
	for s, ok := range map[string]bool{
		"2026-11-19T10:00:00Z":                       true,
		"2026-11-19T10:00:00.5+03:00":                true,
		"2026-11-19T10:00:00-00:00":                  true,
		"2026-11-19T10:00:00":                        false,
		"2026-11-19T10:00:00+0300":                   false,
		"":                                           false,
		"2026-11-19T10:00:00Z trailing":              false,
		"  2026-11-19T10:00:00Z":                     false,
		"2026-11-19T10:00:00.123456789Z":             true,
		"10000-01-01T00:00:00Z":                      false,
		"2026-13-01T00:00:00Z":                       false,
		"2026-11-19T10:00:00+03:00[Europe/Istanbul]": false,
	} {
		if _, got := parseDateTime(s); got != ok {
			t.Errorf("parseDateTime(%q) ok = %v, want %v", s, got, ok)
		}
	}
}

func TestShortDuration(t *testing.T) {
	for d, want := range map[time.Duration]string{
		30 * time.Second:          "30s",
		720 * time.Hour:           "720h",
		time.Minute:               "1m",
		90 * time.Second:          "1m30s",
		500 * time.Millisecond:    "500ms",
		time.Hour + time.Second:   "1h0m1s",
		2*time.Hour + time.Minute: "2h1m",
	} {
		if got := shortDuration(d); got != want {
			t.Errorf("shortDuration(%s) = %q, want %q", d, got, want)
		}
	}
}

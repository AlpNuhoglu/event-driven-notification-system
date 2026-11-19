package api

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/api/apigen"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
)

// The codes of scheduled_at (docs/errors.md). An expires_at of a create that
// is too close to the due time is too_soon, the code of redrive.
const (
	codeInThePast   = "in_the_past"
	codeTooFarAhead = "too_far_ahead"
)

// fieldScheduledAt is the field of the create request that schedules a
// notification; fieldExpiresAt (shared with redrive) is its expiry.
const fieldScheduledAt = "scheduled_at"

// createRules is what a create request is validated by: the level of the
// phone check and the rules of scheduled_at and expires_at.
type createRules struct {
	phone    domain.PhoneValidation
	schedule scheduleRules
}

// scheduleRules are the rules of scheduled_at and expires_at of a create
// (ADR-0008 "Scheduling", ADR-0006 section 3), judged against now: the API's
// clock, read once per request.
//
//   - A scheduled_at from now - tolerance to now (both included) means "now":
//     the notification is due at once and stored with scheduled_at NULL.
//     An older one is in_the_past.
//   - A scheduled_at up to now + maxAhead (included) is accepted; a later one
//     is too_far_ahead.
//   - An explicit expires_at must be at least minLead after the effective
//     start: the scheduled_at when it is in the future, otherwise now
//     (too_soon). When scheduled_at is invalid, expires_at is checked only
//     for its type and format.
//
// A value is judged as it will be stored: in UTC, cut to the microsecond
// that Postgres keeps.
type scheduleRules struct {
	now       time.Time
	tolerance time.Duration
	maxAhead  time.Duration
	minLead   time.Duration
}

// parseDateTime reads an RFC 3339 date-time with an explicit offset, the
// format of every time the API accepts (redrive reads its expires_at so
// too).
func parseDateTime(s string) (time.Time, bool) {
	t, err := time.Parse(time.RFC3339, s)
	return t, err == nil
}

// asStored is a time as Postgres stores it: UTC, microseconds (pgx cuts the
// nanoseconds off).
func asStored(t time.Time) time.Time { return t.UTC().Truncate(time.Microsecond) }

// scheduleCheck carries what the check of scheduled_at leaves for the check
// of expires_at.
type scheduleCheck struct {
	// invalid is true when scheduled_at was given but rejected (repeated
	// included); expires_at is then checked only for its type and format.
	invalid bool
	// start is the effective start of the expires_at rule.
	start time.Time
}

// checkScheduledAt checks scheduled_at. It returns the time to store (nil
// when absent, null, invalid or within the tolerance) and what the check of
// expires_at needs. An absent member and null are the same.
func (r scheduleRules) checkScheduledAt(add addFunc, raw json.RawMessage, present bool) (*time.Time, scheduleCheck) {
	sc := scheduleCheck{start: r.now}
	if !present || isJSONNull(raw) {
		return nil, sc
	}
	sc.invalid = true
	s, ok := stringValue(raw)
	if !ok {
		add(fieldScheduledAt, codeInvalidType, fieldScheduledAt+" must be a string")
		return nil, sc
	}
	t, ok := parseDateTime(s)
	if !ok {
		add(fieldScheduledAt, codeInvalidValue, fieldScheduledAt+" must be an RFC 3339 date-time with an offset")
		return nil, sc
	}
	t = asStored(t)
	switch {
	case t.Before(r.now.Add(-r.tolerance)):
		add(fieldScheduledAt, codeInThePast,
			fmt.Sprintf("%s must not be more than %s in the past", fieldScheduledAt, shortDuration(r.tolerance)))
		return nil, sc
	case t.After(r.now.Add(r.maxAhead)):
		add(fieldScheduledAt, codeTooFarAhead,
			fmt.Sprintf("%s must not be more than %s ahead", fieldScheduledAt, shortDuration(r.maxAhead)))
		return nil, sc
	}
	sc.invalid = false
	if !t.After(r.now) {
		// Within the tolerance: "now", stored as NULL (not a scheduled
		// notification).
		return nil, sc
	}
	sc.start = t
	return &t, sc
}

// checkExpiresAt checks an explicit expires_at against the effective start
// that checkScheduledAt left. null is invalid_type, as for every member but
// template and scheduled_at.
func (r scheduleRules) checkExpiresAt(add addFunc, raw json.RawMessage, present bool, sc scheduleCheck) *time.Time {
	if !present {
		return nil
	}
	s, ok := stringValue(raw)
	if !ok {
		add(fieldExpiresAt, codeInvalidType, fieldExpiresAt+" must be a string")
		return nil
	}
	t, ok := parseDateTime(s)
	if !ok {
		add(fieldExpiresAt, codeInvalidValue, fieldExpiresAt+" must be an RFC 3339 date-time with an offset")
		return nil
	}
	t = asStored(t)
	if !sc.invalid && t.Before(sc.start.Add(r.minLead)) {
		add(fieldExpiresAt, codeTooSoon,
			fmt.Sprintf("%s must be at least %s after the due time (scheduled_at, or now)", fieldExpiresAt, shortDuration(r.minLead)))
		return nil
	}
	return &t
}

// shortDuration formats a duration without trailing zero units: 30s, 720h,
// 1m30s.
func shortDuration(d time.Duration) string {
	s := d.String()
	if strings.HasSuffix(s, "m0s") {
		s = strings.TrimSuffix(s, "0s")
	}
	if strings.HasSuffix(s, "h0m") {
		s = strings.TrimSuffix(s, "0m")
	}
	return s
}

// clockDependentOnly reports whether errs is not empty and every error in it
// comes from a rule that is judged against the clock (in_the_past,
// too_far_ahead, too_soon). Such a request can have been valid when it was
// first sent: a create that was accepted with a scheduled_at inside the
// tolerance, or with an expires_at just far enough ahead, fails these rules
// when it is retried later. Every other error is the same for a request and
// its retry, so a request with one was never accepted.
func clockDependentOnly(errs []apigen.FieldError) bool {
	if len(errs) == 0 {
		return false
	}
	for _, e := range errs {
		switch e.Code {
		case codeInThePast, codeTooFarAhead, codeTooSoon:
		default:
			return false
		}
	}
	return true
}

// rejectedByClockOnly reports whether an item of the batch was rejected only
// by the rules judged against the clock.
func (in batchInput) rejectedByClockOnly() bool {
	for _, r := range in.rejected {
		if clockDependentOnly(r.Errors) {
			return true
		}
	}
	return false
}

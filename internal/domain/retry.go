package domain

import (
	"math"
	"time"
)

// Action is what to do with a failed attempt.
type Action int

const (
	// ActionRetry schedules another attempt after Decision.Delay.
	ActionRetry Action = iota
	// ActionFail moves the notification to failed.
	ActionFail
)

// String returns the action name.
func (a Action) String() string {
	switch a {
	case ActionRetry:
		return "retry"
	case ActionFail:
		return "fail"
	}
	return "unknown"
}

// Profile is one priority's back-off profile.
type Profile struct {
	Base, Cap, MinDelay time.Duration
	RetryAfterCap       time.Duration // upper bound of the retryAfter hint (RETRY_AFTER_CAP, 10 min)
	ThrottleDefault     time.Duration // hint for a throttled outcome without one (THROTTLE_DEFAULT_DELAY, 5 s)
}

// Decision is the result of DecideRetry.
type Decision struct {
	Action         Action
	Delay          time.Duration // set when Action is ActionRetry
	Reason         FailureReason // set when Action is ActionFail
	ConsumesBudget bool
}

// DecideRetry is the single retry decision used by transitions 5, 6 and 7
// (decision D17, ADR-0007 sections 2 to 4, ADR-0004 section 5).
//
// failures is the number of budget-consuming failures recorded before this
// outcome. The rules, in order:
//
//  1. A permanent outcome fails at once with ReasonProviderRejected and does
//     not consume budget.
//  2. Whether the outcome consumes budget is Outcome.ConsumesBudget. An
//     outcome for which the function is not defined (success, in_flight,
//     aborted, an empty or unknown value) is handled like internal_error and
//     consumes budget.
//  3. A consuming outcome fails when failures+1 reaches maxFailures, with
//     ReasonMaxFailuresUnknownOutcome for unknown, ReasonMaxFailures for
//     retryable and ReasonInternalError for internal_error and the outcomes
//     handled like it.
//  4. The back-off is jitter x min(Cap, Base x 2^failures). It saturates at
//     Cap, a negative failures counts as 0 and jitter is clamped into [0, 1).
//  5. The hint is retryAfter; a throttled outcome without one (hint <= 0)
//     gets ThrottleDefault. A hint above RetryAfterCap (when positive) is cut
//     to it.
//  6. The delay is max(backoff, hint, MinDelay).
//  7. If now+delay is at or after expiresAt the call fails with ReasonExpired.
//     Budget exhaustion (3) is reported in preference to expiry.
//  8. Otherwise the call retries after the delay.
//
// The function is deterministic and performs no I/O.
func DecideRetry(outcome Outcome, failures, maxFailures int, expiresAt time.Time,
	retryAfter time.Duration, profile Profile, now time.Time, jitter float64,
) Decision {
	if outcome == OutcomePermanent {
		return Decision{Action: ActionFail, Reason: ReasonProviderRejected}
	}

	consumes := outcome.ConsumesBudget()
	switch outcome {
	case OutcomeRetryable, OutcomeUnknown, OutcomeInternalError,
		OutcomeUnreachable, OutcomeThrottled, OutcomeProviderFatal:
	default:
		consumes = true // handled like internal_error
	}

	if consumes && failures+1 >= maxFailures {
		reason := ReasonInternalError
		switch outcome {
		case OutcomeUnknown:
			reason = ReasonMaxFailuresUnknownOutcome
		case OutcomeRetryable:
			reason = ReasonMaxFailures
		}
		return Decision{Action: ActionFail, Reason: reason, ConsumesBudget: true}
	}

	backoff := backoffDelay(profile, failures, jitter)

	hint := retryAfter
	if hint <= 0 && outcome == OutcomeThrottled {
		hint = profile.ThrottleDefault
	}
	if profile.RetryAfterCap > 0 && hint > profile.RetryAfterCap {
		hint = profile.RetryAfterCap
	}

	delay := max(backoff, hint, profile.MinDelay)

	if !now.Add(delay).Before(expiresAt) {
		return Decision{Action: ActionFail, Reason: ReasonExpired, ConsumesBudget: consumes}
	}
	return Decision{Action: ActionRetry, Delay: delay, ConsumesBudget: consumes}
}

// backoffDelay is jitter x min(Cap, Base x 2^failures) without overflow.
func backoffDelay(p Profile, failures int, jitter float64) time.Duration {
	if !(jitter > 0) { // also catches NaN
		return 0
	}
	if jitter >= 1 {
		jitter = math.Nextafter(1, 0)
	}
	b := p.Base
	for i := 0; i < failures && b > 0 && b < p.Cap; i++ {
		if b > p.Cap/2 {
			b = p.Cap
			break
		}
		b *= 2
	}
	b = min(b, p.Cap)
	if b <= 0 {
		return 0
	}
	return time.Duration(float64(b) * jitter)
}

package domain

import "time"

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
}

// Decision is the result of DecideRetry.
type Decision struct {
	Action         Action
	Delay          time.Duration // set when Action is ActionRetry
	Reason         FailureReason // set when Action is ActionFail
	ConsumesBudget bool
}

// DecideRetry is the single retry decision used by transitions 5, 6 and 7
// (decision D17). The signature is final; the body is deliberately minimal in
// this phase and is replaced in phase P3.
//
// failures is the number of budget-consuming failures recorded before this
// outcome. In this phase every outcome consumes budget, the delay is fixed at
// max(Base, MinDelay), and the call fails once failures+1 reaches maxFailures.
// The function is deterministic.
func DecideRetry(outcome Outcome, failures, maxFailures int, expiresAt time.Time,
	retryAfter time.Duration, profile Profile, now time.Time, jitter float64,
) Decision {
	// P3 uses these: expiry shortcut, Retry-After handling and full jitter.
	_, _, _, _ = expiresAt, retryAfter, now, jitter

	if failures+1 >= maxFailures {
		reason := ReasonMaxFailures
		if outcome == OutcomeUnknown {
			reason = ReasonMaxFailuresUnknownOutcome
		}
		return Decision{Action: ActionFail, Reason: reason, ConsumesBudget: true}
	}
	return Decision{
		Action:         ActionRetry,
		Delay:          max(profile.Base, profile.MinDelay),
		ConsumesBudget: true,
	}
}

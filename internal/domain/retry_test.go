package domain

import (
	"testing"
	"time"
)

func TestDecideRetry(t *testing.T) {
	prof := Profile{Base: 10 * time.Second, Cap: time.Minute, MinDelay: 2 * time.Second}
	tests := []struct {
		name     string
		outcome  Outcome
		failures int
		max      int
		profile  Profile
		want     Decision
	}{
		{
			"budget remaining", OutcomeRetryable, 0, 5, prof,
			Decision{Action: ActionRetry, Delay: 10 * time.Second, ConsumesBudget: true},
		},
		{
			"min delay wins", OutcomeThrottled, 1, 5,
			Profile{Base: time.Second, MinDelay: 5 * time.Second},
			Decision{Action: ActionRetry, Delay: 5 * time.Second, ConsumesBudget: true},
		},
		{
			"last failure", OutcomeRetryable, 4, 5, prof,
			Decision{Action: ActionFail, Reason: ReasonMaxFailures, ConsumesBudget: true},
		},
		{
			"last failure unknown", OutcomeUnknown, 4, 5, prof,
			Decision{Action: ActionFail, Reason: ReasonMaxFailuresUnknownOutcome, ConsumesBudget: true},
		},
		{
			"max one fails first", OutcomeRetryable, 0, 1, prof,
			Decision{Action: ActionFail, Reason: ReasonMaxFailures, ConsumesBudget: true},
		},
	}
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := DecideRetry(tt.outcome, tt.failures, tt.max, now.Add(time.Hour), 0, tt.profile, now, 0.5)
			if got != tt.want {
				t.Errorf("got %+v, want %+v", got, tt.want)
			}
			if again := DecideRetry(tt.outcome, tt.failures, tt.max, now.Add(time.Hour), 0, tt.profile, now, 0.5); again != got {
				t.Error("not deterministic")
			}
		})
	}
}

func TestActionString(t *testing.T) {
	if ActionRetry.String() != "retry" || ActionFail.String() != "fail" {
		t.Error("bad Action.String")
	}
}

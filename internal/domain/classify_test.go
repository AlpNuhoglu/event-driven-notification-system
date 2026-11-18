package domain

import "testing"

func TestClassifyResponse(t *testing.T) {
	tests := []struct {
		status int
		want   Outcome
	}{
		{0, OutcomeUnknown},
		{200, OutcomeSuccess},
		{202, OutcomeSuccess},
		{299, OutcomeSuccess},
		{199, OutcomeRetryable},
		{300, OutcomeRetryable},
		{400, OutcomeRetryable},
		{429, OutcomeRetryable},
		{500, OutcomeRetryable},
		{503, OutcomeRetryable},
	}
	for _, tt := range tests {
		if got := ClassifyResponse(tt.status); got != tt.want {
			t.Errorf("ClassifyResponse(%d) = %q, want %q", tt.status, got, tt.want)
		}
	}
}

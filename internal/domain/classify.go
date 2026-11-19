package domain

// ProviderResult is what one provider request produced.
type ProviderResult struct {
	StatusCode   int  // HTTP status of the response; 0 when no response arrived
	WroteRequest bool // the request was written completely and without error
}

// Classify maps a provider result to the outcome of the attempt, following
// the classification table of ADR-0007 section 2. When a response arrived
// (StatusCode != 0), WroteRequest is ignored.
func Classify(r ProviderResult) Outcome {
	s := r.StatusCode
	switch {
	case s == 0 && !r.WroteRequest:
		return OutcomeUnreachable
	case s == 0:
		return OutcomeUnknown
	case s >= 200 && s <= 299:
		return OutcomeSuccess
	case s >= 300 && s <= 399:
		return OutcomeProviderFatal
	case s == 401 || s == 403 || s == 404 || s == 410:
		return OutcomeProviderFatal
	case s == 408 || s == 409 || s == 425:
		return OutcomeRetryable
	case s == 429:
		return OutcomeThrottled
	case s >= 400 && s <= 499:
		return OutcomePermanent
	case s == 500 || s == 502 || s == 503 || s == 504:
		return OutcomeRetryable
	default:
		// Not in the ADR's table: open issue 9 in docs/progress.md.
		return OutcomeRetryable
	}
}

// Scope tells whose fault an outcome is.
type Scope string

// The scopes of an outcome.
const (
	// ScopeNone is for outcomes that blame nobody.
	ScopeNone Scope = ""
	// ScopeMessage means the problem is this message.
	ScopeMessage Scope = "message"
	// ScopeProvider means the problem is the provider.
	ScopeProvider Scope = "provider"
)

// BreakerSignal is what an outcome tells the circuit breaker.
type BreakerSignal int

// The breaker signals.
const (
	// BreakerIgnore is not counted at all.
	BreakerIgnore BreakerSignal = iota
	// BreakerSuccess counts as a success.
	BreakerSuccess
	// BreakerFailure counts as a failure.
	BreakerFailure
	// BreakerOpen opens the circuit directly.
	BreakerOpen
)

// Scope returns the scope of the outcome (ADR-0007 section 2).
func (o Outcome) Scope() Scope {
	switch o {
	case OutcomeRetryable, OutcomeUnknown, OutcomeInternalError, OutcomePermanent:
		return ScopeMessage
	case OutcomeProviderFatal, OutcomeThrottled, OutcomeUnreachable:
		return ScopeProvider
	default:
		return ScopeNone
	}
}

// ConsumesBudget reports whether the outcome counts against the message's
// retry budget.
func (o Outcome) ConsumesBudget() bool {
	switch o {
	case OutcomeRetryable, OutcomeUnknown, OutcomeInternalError:
		return true
	default:
		return false
	}
}

// Breaker returns what the outcome tells the circuit breaker.
func (o Outcome) Breaker() BreakerSignal {
	switch o {
	case OutcomeSuccess:
		return BreakerSuccess
	case OutcomeRetryable, OutcomeUnknown, OutcomeUnreachable:
		return BreakerFailure
	case OutcomeProviderFatal, OutcomeThrottled:
		return BreakerOpen
	default:
		return BreakerIgnore
	}
}

// HonoursRetryAfter reports whether a Retry-After header of a response with
// this status is used: 429 and 503 only.
func HonoursRetryAfter(statusCode int) bool {
	return statusCode == 429 || statusCode == 503
}

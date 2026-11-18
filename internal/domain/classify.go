package domain

// ClassifyResponse maps a provider result to the outcome of the attempt.
// statusCode is the HTTP status of the response, or 0 when no response
// arrived.
//
// This phase distinguishes three cases: a 2xx is a success, any other
// response is retryable, and no response is unknown. Phase P3 replaces the
// body with the full classification table of ADR-0007 section 2 (and may add
// the inputs that table needs, such as whether the request was written).
func ClassifyResponse(statusCode int) Outcome {
	switch {
	case statusCode == 0:
		return OutcomeUnknown
	case statusCode >= 200 && statusCode <= 299:
		return OutcomeSuccess
	default:
		return OutcomeRetryable
	}
}

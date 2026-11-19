package api

import (
	"context"
	"net/http"
	"strings"
	"sync"

	"github.com/google/uuid"
)

// Header names.
const (
	headerRequestID   = "X-Request-ID"
	headerTraceParent = "Traceparent"
)

// maxRequestIDLen is the longest X-Request-ID that is accepted (ADR-0006 §2).
const maxRequestIDLen = 128

// validRequestID reports whether a client's X-Request-ID is accepted: 1 to
// 128 characters of [A-Za-z0-9._-]. Anything else would let a client write
// arbitrary text into every log line (ADR-0009 §5).
func validRequestID(s string) bool {
	if s == "" || len(s) > maxRequestIDLen {
		return false
	}
	for i := range len(s) {
		c := s[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9',
			c == '.', c == '_', c == '-':
		default:
			return false
		}
	}
	return true
}

// correlationID returns the request's correlation id: the client's
// X-Request-ID when exactly one valid value was sent, otherwise a new UUIDv7.
func correlationID(values []string) string {
	if len(values) == 1 && validRequestID(values[0]) {
		return values[0]
	}
	if id, err := uuid.NewV7(); err == nil {
		return id.String()
	}
	return uuid.NewString()
}

// traceParentLen is the length of a version 00 traceparent value.
const traceParentLen = 55

// traceParent returns the W3C traceparent value to store, or "" when the
// request has none, more than one, or a malformed one. A value of a later
// version is accepted when its first four fields are well formed, as the W3C
// Trace Context specification asks, and stored as those four fields.
func traceParent(values []string) string {
	if len(values) != 1 {
		return ""
	}
	v := values[0]
	if len(v) < traceParentLen {
		return ""
	}
	version := v[0:2]
	if !isLowerHex(version) || version == "ff" {
		return ""
	}
	if version == "00" && len(v) != traceParentLen {
		return ""
	}
	if len(v) > traceParentLen && v[traceParentLen] != '-' {
		return ""
	}
	if v[2] != '-' || v[35] != '-' || v[52] != '-' {
		return ""
	}
	traceID, parentID, flags := v[3:35], v[36:52], v[53:55]
	if !isLowerHex(traceID) || !isLowerHex(parentID) || !isLowerHex(flags) {
		return ""
	}
	if allZero(traceID) || allZero(parentID) {
		return ""
	}
	return v[:traceParentLen]
}

func isLowerHex(s string) bool {
	for i := range len(s) {
		c := s[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

func allZero(s string) bool {
	return strings.Trim(s, "0") == ""
}

// requestState is what the middleware learns about a request and what the
// access log line reports. It is shared by pointer through the context, so
// inner layers can record the route and the cause of an error.
type requestState struct {
	correlationID string
	traceParent   string

	mu    sync.Mutex
	route string
	cause error
	// problem is the last problem written (writeProblem), for the metrics.
	problem *problem
}

func (s *requestState) setRoute(route string) {
	s.mu.Lock()
	s.route = route
	s.mu.Unlock()
}

func (s *requestState) setProblem(p *problem) {
	s.mu.Lock()
	s.problem = p
	s.mu.Unlock()
}

func (s *requestState) lastProblem() *problem {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.problem
}

func (s *requestState) setCause(err error) {
	s.mu.Lock()
	s.cause = err
	s.mu.Unlock()
}

func (s *requestState) snapshot() (route string, cause error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.route, s.cause
}

type stateKey struct{}

// stateFrom returns the request state. Outside the middleware (in unit
// tests of a single layer) it returns a fresh state with a new id.
func stateFrom(ctx context.Context) *requestState {
	if st, ok := ctx.Value(stateKey{}).(*requestState); ok {
		return st
	}
	return &requestState{correlationID: correlationID(nil)}
}

// withRequestHeaders derives the correlation id and the trace parent from the
// request headers, normalises both headers to the accepted values (so the
// generated parameter binding never sees a duplicate or invalid value), sets
// X-Request-ID on the response and returns the request with its state.
func withRequestHeaders(w http.ResponseWriter, r *http.Request) (*http.Request, *requestState) {
	st := &requestState{
		correlationID: correlationID(r.Header.Values(headerRequestID)),
		traceParent:   traceParent(r.Header.Values(headerTraceParent)),
	}
	r.Header.Set(headerRequestID, st.correlationID)
	if st.traceParent == "" {
		r.Header.Del(headerTraceParent)
	} else {
		r.Header.Set(headerTraceParent, st.traceParent)
	}
	w.Header().Set(headerRequestID, st.correlationID)
	return r.WithContext(context.WithValue(r.Context(), stateKey{}, st)), st
}

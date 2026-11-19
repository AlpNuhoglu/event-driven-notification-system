package domain

import (
	"go/ast"
	"go/parser"
	"go/token"
	"slices"
	"strings"
	"testing"
)

func TestClassify(t *testing.T) {
	type row struct {
		name   string
		status []int
		want   Outcome
	}
	rows := []row{
		{"2xx", []int{200, 201, 202, 204, 299}, OutcomeSuccess},
		{"3xx", []int{300, 301, 302, 304, 308, 399}, OutcomeProviderFatal},
		{"credentials and endpoint", []int{401, 403, 404, 410}, OutcomeProviderFatal},
		{"retryable 4xx", []int{408, 409, 425}, OutcomeRetryable},
		{"throttled", []int{429}, OutcomeThrottled},
		{"other 4xx", []int{400, 402, 405, 418, 422, 451, 499}, OutcomePermanent},
		{"listed 5xx", []int{500, 502, 503, 504}, OutcomeRetryable},
		{"unlisted", []int{100, 199, 501, 505, 599, 600, 999, -1}, OutcomeRetryable},
	}
	for _, r := range rows {
		for _, s := range r.status {
			for _, wrote := range []bool{false, true} {
				got := Classify(ProviderResult{StatusCode: s, WroteRequest: wrote})
				if got != r.want {
					t.Errorf("%s: Classify(status %d, wrote %v) = %q, want %q", r.name, s, wrote, got, r.want)
				}
			}
		}
	}
	if got := Classify(ProviderResult{}); got != OutcomeUnreachable {
		t.Errorf("no response, not written = %q, want unreachable", got)
	}
	if got := Classify(ProviderResult{WroteRequest: true}); got != OutcomeUnknown {
		t.Errorf("no response, written = %q, want unknown", got)
	}
}

// declaredOutcomes returns the values of all Outcome constants in types.go.
func declaredOutcomes(t *testing.T) []Outcome {
	t.Helper()
	f, err := parser.ParseFile(token.NewFileSet(), "types.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	var out []Outcome
	for _, d := range f.Decls {
		gd, ok := d.(*ast.GenDecl)
		if !ok || gd.Tok != token.CONST {
			continue
		}
		for _, sp := range gd.Specs {
			vs := sp.(*ast.ValueSpec)
			for i, n := range vs.Names {
				if !strings.HasPrefix(n.Name, "Outcome") || vs.Type == nil || i >= len(vs.Values) {
					continue
				}
				if id, ok := vs.Type.(*ast.Ident); !ok || id.Name != "Outcome" {
					continue
				}
				lit := vs.Values[i].(*ast.BasicLit)
				out = append(out, Outcome(strings.Trim(lit.Value, `"`)))
			}
		}
	}
	return out
}

func TestOutcomeProperties(t *testing.T) {
	tests := []struct {
		o        Outcome
		scope    Scope
		consumes bool
		breaker  BreakerSignal
	}{
		{OutcomeSuccess, ScopeNone, false, BreakerSuccess},
		{OutcomeRetryable, ScopeMessage, true, BreakerFailure},
		{OutcomeUnknown, ScopeMessage, true, BreakerFailure},
		{OutcomeInternalError, ScopeMessage, true, BreakerIgnore},
		{OutcomePermanent, ScopeMessage, false, BreakerIgnore},
		{OutcomeProviderFatal, ScopeProvider, false, BreakerOpen},
		{OutcomeThrottled, ScopeProvider, false, BreakerOpen},
		{OutcomeUnreachable, ScopeProvider, false, BreakerFailure},
		{OutcomeAborted, ScopeNone, false, BreakerIgnore},
		{OutcomeInFlight, ScopeNone, false, BreakerIgnore},
		{Outcome(""), ScopeNone, false, BreakerIgnore},
		{Outcome("bogus"), ScopeNone, false, BreakerIgnore},
	}
	var listed []Outcome
	for _, tt := range tests {
		listed = append(listed, tt.o)
		if got := tt.o.Scope(); got != tt.scope {
			t.Errorf("%q.Scope() = %q, want %q", tt.o, got, tt.scope)
		}
		if got := tt.o.ConsumesBudget(); got != tt.consumes {
			t.Errorf("%q.ConsumesBudget() = %v, want %v", tt.o, got, tt.consumes)
		}
		if got := tt.o.Breaker(); got != tt.breaker {
			t.Errorf("%q.Breaker() = %v, want %v", tt.o, got, tt.breaker)
		}
	}
	declared := declaredOutcomes(t)
	if len(declared) == 0 {
		t.Fatal("found no Outcome constants in types.go")
	}
	for _, o := range declared {
		if !slices.Contains(listed, o) {
			t.Errorf("outcome %q is declared in types.go but missing from this test", o)
		}
	}
}

func TestHonoursRetryAfter(t *testing.T) {
	for _, s := range []int{0, 200, 202, 301, 400, 401, 428, 430, 500, 502, 504, 599} {
		if HonoursRetryAfter(s) {
			t.Errorf("HonoursRetryAfter(%d) = true", s)
		}
	}
	for _, s := range []int{429, 503} {
		if !HonoursRetryAfter(s) {
			t.Errorf("HonoursRetryAfter(%d) = false", s)
		}
	}
}

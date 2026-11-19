package health

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func readyz(t *testing.T, r *Readiness) (int, Report) {
	t.Helper()
	rec := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/readyz", http.NoBody)
	healthMux(r).ServeHTTP(rec, req)
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("content type %q", ct)
	}
	var rep Report
	if err := json.Unmarshal(rec.Body.Bytes(), &rep); err != nil {
		t.Fatalf("body %s: %v", rec.Body, err)
	}
	return rec.Code, rep
}

// A role without checks is ready while it serves, and says so with an empty
// list.
func TestReadiness_NoChecksIsReady(t *testing.T) {
	code, rep := readyz(t, NewReadiness(time.Second))
	if code != http.StatusOK || rep.Status != StatusOK || rep.Checks == nil || len(rep.Checks) != 0 {
		t.Errorf("status %d, report %+v; want 200, ok, an empty list", code, rep)
	}
}

// Every check is reported by name, in the order it was added, with its own
// status; one failure makes the answer a 503. The error's text is not in
// the body.
func TestReadiness_ReportsEveryCheck(t *testing.T) {
	r := NewReadiness(time.Second)
	r.Add("postgres", func(context.Context) error { return nil })
	r.Add("rabbitmq", func(context.Context) error { return errors.New("dial tcp 10.1.2.3:5672: refused") })
	r.Add("consumer", func(context.Context) error { return nil })

	rec := httptest.NewRecorder()
	healthMux(r).ServeHTTP(rec, httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/readyz", http.NoBody))
	body := rec.Body.String()
	want := `{"status":"fail","checks":[{"name":"postgres","status":"ok","duration_ms":`
	if rec.Code != http.StatusServiceUnavailable || len(body) < len(want) || body[:len(want)] != want {
		t.Fatalf("status %d, body %s", rec.Code, body)
	}
	var rep Report
	if err := json.Unmarshal(rec.Body.Bytes(), &rep); err != nil {
		t.Fatal(err)
	}
	got := map[string]string{}
	for i, c := range rep.Checks {
		got[c.Name] = c.Status
		if want := []string{"postgres", "rabbitmq", "consumer"}[i]; c.Name != want {
			t.Errorf("check %d is %q, want %q", i, c.Name, want)
		}
		if c.DurationMS < 0 {
			t.Errorf("check %s: duration %v", c.Name, c.DurationMS)
		}
	}
	if got["postgres"] != StatusOK || got["rabbitmq"] != StatusFail || got["consumer"] != StatusOK {
		t.Errorf("checks %v", got)
	}
	for _, leaked := range []string{"10.1.2.3", "refused", "dial"} {
		if contains(body, leaked) {
			t.Errorf("the body contains %q of the check's error: %s", leaked, body)
		}
	}

	// With the failing dependency back, the same registry answers 200.
	ok := NewReadiness(time.Second)
	ok.Add("postgres", func(context.Context) error { return nil })
	if code, rep := readyz(t, ok); code != http.StatusOK || rep.Status != StatusOK || len(rep.Checks) != 1 {
		t.Errorf("status %d, report %+v", code, rep)
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

// A check that has not answered when its time is up has failed, and it does
// not hold the answer: here it never returns on its own, and the other check
// is still reported as ok.
func TestReadiness_CheckThatDoesNotAnswerFails(t *testing.T) {
	const timeout = 20 * time.Millisecond
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	r := NewReadiness(timeout)
	r.Add("stuck", func(context.Context) error { <-release; return nil })
	r.Add("fine", func(context.Context) error { return nil })

	start := time.Now()
	code, rep := readyz(t, r)
	if took := time.Since(start); took < timeout {
		t.Errorf("answered after %s, before the timeout of %s", took, timeout)
	}
	if code != http.StatusServiceUnavailable || rep.Checks[0].Status != StatusFail || rep.Checks[1].Status != StatusOK {
		t.Errorf("status %d, report %+v; want 503 with stuck failed and fine ok", code, rep)
	}
}

// The checks see the request's context with the timeout: one that honours it
// is told to stop.
func TestReadiness_CheckGetsADeadline(t *testing.T) {
	r := NewReadiness(20 * time.Millisecond)
	r.Add("slow", func(ctx context.Context) error {
		if _, ok := ctx.Deadline(); !ok {
			return nil // would pass, and fail the test below
		}
		<-ctx.Done()
		return ctx.Err()
	})
	if code, rep := readyz(t, r); code != http.StatusServiceUnavailable || rep.Checks[0].Status != StatusFail {
		t.Errorf("status %d, report %+v", code, rep)
	}
}

// The registry travels to a role's service in its context; without one, Add
// does nothing.
func TestReadiness_Context(t *testing.T) {
	r := NewReadiness(0)
	if got := ReadinessFrom(WithReadiness(context.Background(), r)); got != r {
		t.Errorf("ReadinessFrom = %p, want %p", got, r)
	}
	none := ReadinessFrom(context.Background())
	if none != nil {
		t.Fatalf("ReadinessFrom without a registry = %v", none)
	}
	none.Add("x", func(context.Context) error { return nil }) // must not panic
}

// Liveness checks no dependency: /healthz answers ok while a readiness
// check fails.
func TestHealthz_IgnoresTheChecks(t *testing.T) {
	r := NewReadiness(time.Second)
	r.Add("down", func(context.Context) error { return errors.New("down") })
	rec := httptest.NewRecorder()
	healthMux(r).ServeHTTP(rec, httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/healthz", http.NoBody))
	if rec.Code != http.StatusOK || rec.Body.String() != `{"status":"ok"}` {
		t.Errorf("healthz: %d %s", rec.Code, rec.Body)
	}
}

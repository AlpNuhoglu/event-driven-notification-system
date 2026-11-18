//go:build e2e

// Package e2e holds the tests that run against a running Compose stack with
// the mock override (make test-e2e). They reach the API and mockprovider on
// their published host ports, configured with E2E_API_URL and E2E_MOCK_URL,
// and never contact the default provider URL.
package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/mockprovider"
)

const (
	defaultAPIURL  = "http://localhost:8080"
	defaultMockURL = "http://localhost:8090"

	// requestTimeout bounds one HTTP request of the tests.
	requestTimeout = 10 * time.Second
	// sendTimeout is how long a single notification may take to reach sent.
	sendTimeout = 30 * time.Second
	// statusPollInterval is how often a notification's status is read.
	statusPollInterval = 50 * time.Millisecond
)

// Reserved fake recipients (D71); nothing here leaves the machine anyway,
// because the mock override points the worker at mockprovider.
const (
	fakePhone  = "+905551234567"
	fakeEmail  = "e2e@example.com"
	fakeDevice = "e2e-fake-push-device"
)

// stack is the running stack as the tests see it.
type stack struct {
	apiURL  string
	mockURL string
	client  *http.Client
}

func newStack(t *testing.T) *stack {
	t.Helper()
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.MaxIdleConnsPerHost = 32
	s := &stack{
		apiURL:  strings.TrimRight(envOr("E2E_API_URL", defaultAPIURL), "/"),
		mockURL: strings.TrimRight(envOr("E2E_MOCK_URL", defaultMockURL), "/"),
		client:  &http.Client{Timeout: requestTimeout, Transport: tr},
	}
	t.Cleanup(tr.CloseIdleConnections)
	return s
}

func envOr(name, def string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return def
}

// do sends a request and returns the status, the headers and the body.
func (s *stack) do(method, url string, body any) (int, http.Header, []byte, error) {
	var rd io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return 0, nil, nil, err
		}
		rd = bytes.NewReader(b)
	}
	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, method, url, rd)
	if err != nil {
		return 0, nil, nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return 0, nil, nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, nil, nil, err
	}
	return resp.StatusCode, resp.Header, b, nil
}

// resetMock clears mockprovider's records and its configured status, so a
// test does not see the requests of another. E2E_MOCK_STATUS, when set, is
// the status mockprovider answers with after the reset; it exists only to
// check by hand that the tests fail clearly when the provider fails (for
// example E2E_MOCK_STATUS=500), and is never set by make test-e2e.
func (s *stack) resetMock(t *testing.T) {
	t.Helper()
	code, _, body, err := s.do(http.MethodPost, s.mockURL+"/admin/reset", nil)
	if err != nil {
		t.Fatalf("reset mockprovider at %s: %v (is the stack running with the mock override? make up-mock)", s.mockURL, err)
	}
	if code != http.StatusOK {
		t.Fatalf("reset mockprovider: status %d: %s", code, body)
	}
	v := os.Getenv("E2E_MOCK_STATUS")
	if v == "" {
		return
	}
	status, err := strconv.Atoi(v)
	if err != nil {
		t.Fatalf("E2E_MOCK_STATUS=%q: want an HTTP status code", v)
	}
	code, _, body, err = s.do(http.MethodPut, s.mockURL+"/admin/config", map[string]int{"status": status})
	if err != nil || code != http.StatusOK {
		t.Fatalf("configure mockprovider status %d: status %d, error %v: %s", status, code, err, body)
	}
	t.Logf("mockprovider answers %d (E2E_MOCK_STATUS)", status)
}

// report reads mockprovider's report.
func (s *stack) report() (mockprovider.Report, error) {
	var rep mockprovider.Report
	code, _, body, err := s.do(http.MethodGet, s.mockURL+"/admin/report", nil)
	if err != nil {
		return rep, err
	}
	if code != http.StatusOK {
		return rep, fmt.Errorf("mockprovider report: status %d", code)
	}
	if err := json.Unmarshal(body, &rep); err != nil {
		return rep, fmt.Errorf("decode mockprovider report: %w", err)
	}
	return rep, nil
}

// createRequest is the body of POST /v1/notifications.
type createRequest struct {
	Channel   string `json:"channel"`
	Recipient string `json:"recipient"`
	Content   string `json:"content"`
	Priority  string `json:"priority,omitempty"`
}

// notification holds the fields of the Notification schema the tests read.
type notification struct {
	ID                string  `json:"id"`
	Channel           string  `json:"channel"`
	Recipient         string  `json:"recipient"`
	Priority          string  `json:"priority"`
	Status            string  `json:"status"`
	AttemptCount      int     `json:"attempt_count"`
	FailureReason     *string `json:"failure_reason"`
	ProviderMessageID *string `json:"provider_message_id"`
}

// create posts a notification and returns the decoded 202 body and the
// Location header. Any other answer is an error.
func (s *stack) create(req createRequest) (notification, string, error) {
	var n notification
	code, hdr, body, err := s.do(http.MethodPost, s.apiURL+"/v1/notifications", req)
	if err != nil {
		return n, "", err
	}
	if code != http.StatusAccepted {
		return n, "", fmt.Errorf("create: status %d, want 202: %s", code, body)
	}
	if err := json.Unmarshal(body, &n); err != nil {
		return n, "", fmt.Errorf("decode create response: %w", err)
	}
	return n, hdr.Get("Location"), nil
}

// get reads a notification by id.
func (s *stack) get(id string) (notification, error) {
	var n notification
	code, _, body, err := s.do(http.MethodGet, s.apiURL+"/v1/notifications/"+id, nil)
	if err != nil {
		return n, err
	}
	if code != http.StatusOK {
		return n, fmt.Errorf("get %s: status %d, want 200: %s", id, code, body)
	}
	if err := json.Unmarshal(body, &n); err != nil {
		return n, fmt.Errorf("decode get response: %w", err)
	}
	return n, nil
}

// waitSent polls the notification until it is sent and returns it. It fails
// the test with the last observed state when the notification reaches a
// final state other than sent, or is not sent within timeout.
func (s *stack) waitSent(t *testing.T, id string, timeout time.Duration) notification {
	t.Helper()
	var last notification
	var lastErr error
	terminal := false
	ok := poll(timeout, statusPollInterval, func() bool {
		last, lastErr = s.get(id)
		if lastErr != nil {
			return false
		}
		switch last.Status {
		case "sent":
			return true
		case "failed", "cancelled":
			terminal = true
			return true
		}
		return false
	})
	if ok && !terminal {
		return last
	}
	received := "unknown"
	if rep, err := s.report(); err == nil {
		received = fmt.Sprint(rep.Total)
	}
	reason := "none"
	if last.FailureReason != nil {
		reason = *last.FailureReason
	}
	if terminal {
		t.Fatalf("notification %s (%s) ended in status %q (failure_reason %s, attempt_count %d) instead of sent; mockprovider received %s request(s)",
			id, last.Channel, last.Status, reason, last.AttemptCount, received)
	}
	t.Fatalf("notification %s (%s) not sent within %s: last status %q, attempt_count %d, failure_reason %s, last read error %v; mockprovider received %s request(s)",
		id, last.Channel, timeout, last.Status, last.AttemptCount, reason, lastErr, received)
	return last
}

// poll evaluates condition every interval until it returns true or timeout
// passes, and reports whether it returned true. It is the waiting primitive
// of these tests: like testutil.Eventually it polls with a deadline and never
// sleeps, but with an interval that suits requests to a running stack.
func poll(timeout, interval time.Duration, condition func() bool) bool {
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	tick := time.NewTicker(interval)
	defer tick.Stop()
	for {
		if condition() {
			return true
		}
		select {
		case <-deadline.C:
			return condition()
		case <-tick.C:
		}
	}
}

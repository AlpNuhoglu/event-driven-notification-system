//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/mockprovider"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/store"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/testutil"
)

// The classification of provider results through the wired roles (ADR-0007
// sections 2 to 4): the relay and the worker role against real Postgres,
// RabbitMQ and Redis, and the real mockprovider handler behind httptest,
// configured through its admin API.

// Durations of the fault scenarios, in milliseconds, through the
// configuration. The back-off of the normal profile is exactly 50 ms (base,
// cap and floor are equal), so the throttle default of 300 ms and a
// Retry-After of one second are both clearly larger than it.
const (
	faultBackoff         = 50 * time.Millisecond
	faultThrottleDefault = 300 * time.Millisecond
	// faultFatalOpen is BREAKER_FATAL_OPEN: how long a provider_fatal
	// answer keeps the circuit open.
	faultFatalOpen = 100 * time.Millisecond
)

// millis formats a duration as a configuration value in milliseconds.
func millis(d time.Duration) string { return fmt.Sprintf("%dms", d.Milliseconds()) }

func faultVars(providerURL string) map[string]string {
	return map[string]string{
		"PROVIDER_URL":           providerURL,
		"PROVIDER_TIMEOUT":       "2000ms",
		"RETRY_NORMAL_BASE":      millis(faultBackoff),
		"RETRY_NORMAL_CAP":       millis(faultBackoff),
		"RETRY_MIN_DELAY":        millis(faultBackoff),
		"THROTTLE_DEFAULT_DELAY": millis(faultThrottleDefault),
		// The circuit that a provider-scoped answer opens (D26), in
		// milliseconds like every other duration.
		"BREAKER_OPEN_BASE":  "50ms",
		"BREAKER_OPEN_MAX":   "200ms",
		"BREAKER_FATAL_OPEN": millis(faultFatalOpen),
		"THROTTLE_MAX_DELAY": "1200ms",
	}
}

// mock is the real mockprovider handler behind an httptest server.
type mock struct {
	t      *testing.T
	srv    *httptest.Server
	client *http.Client
}

func newMock(t *testing.T) *mock {
	t.Helper()
	srv := httptest.NewServer(mockprovider.New(time.Now).Handler())
	tr := http.DefaultTransport.(*http.Transport).Clone() //nolint:forcetypeassert // the default transport is an *http.Transport
	m := &mock{t: t, srv: srv, client: &http.Client{Transport: tr, Timeout: waitTimeout}}
	t.Cleanup(func() {
		tr.CloseIdleConnections()
		srv.Close()
	})
	return m
}

// configure sends a configuration to PUT /admin/config.
func (m *mock) configure(cfg string) {
	m.t.Helper()
	code, body := m.do(http.MethodPut, "/admin/config", cfg)
	if code != http.StatusOK {
		m.t.Fatalf("configure mockprovider: status %d: %s", code, body)
	}
}

// report reads GET /admin/report.
func (m *mock) report() mockprovider.Report {
	m.t.Helper()
	code, body := m.do(http.MethodGet, "/admin/report", "")
	if code != http.StatusOK {
		m.t.Fatalf("mockprovider report: status %d: %s", code, body)
	}
	var rep mockprovider.Report
	if err := json.Unmarshal(body, &rep); err != nil {
		m.t.Fatalf("decode report: %v", err)
	}
	return rep
}

func (m *mock) do(method, path, body string) (int, []byte) {
	m.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
	defer cancel()
	var rd io.Reader
	if body != "" {
		rd = strings.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, m.srv.URL+path, rd)
	if err != nil {
		m.t.Fatal(err)
	}
	resp, err := m.client.Do(req)
	if err != nil {
		m.t.Fatalf("%s %s: %v", method, path, err)
	}
	defer func() { _ = resp.Body.Close() }()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		m.t.Fatal(err)
	}
	return resp.StatusCode, b
}

// counter is an httptest server that only counts the requests it receives.
type counter struct {
	srv *httptest.Server
	n   chan struct{}
}

func newCounter(t *testing.T) *counter {
	t.Helper()
	c := &counter{n: make(chan struct{}, 1000)}
	c.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		c.n <- struct{}{}
		w.WriteHeader(http.StatusAccepted)
	}))
	t.Cleanup(c.srv.Close)
	return c
}

func (c *counter) count() int { return len(c.n) }

// startFault starts the worker role and the relay on the fault durations
// with the provider at providerURL.
func (e *env) startFault(providerURL string) {
	e.t.Helper()
	cfg := e.config(faultVars(providerURL))
	e.startWorker(cfg, 0)
	e.startRelay(cfg)
}

// createOne inserts one due SMS notification of normal priority with the
// given max_failures and TTL.
func (e *env) createOne(maxFailures int, ttl time.Duration) uuid.UUID {
	e.t.Helper()
	ctx, cancel := e.ctx()
	defer cancel()
	row, err := e.store.CreateNotification(ctx, store.NewNotification{
		Channel:       domain.ChannelSMS,
		Priority:      domain.PriorityNormal,
		Recipient:     domain.Recipient(testRecipient),
		Content:       testContent,
		TTL:           ttl,
		MaxFailures:   maxFailures,
		CorrelationID: "corr-fault",
		TraceParent:   testTraceParent,
	})
	if err != nil {
		e.t.Fatalf("create: %v", err)
	}
	return row.ID
}

// waitFinal waits until the notification is sent or failed and returns it.
func (e *env) waitFinal(id uuid.UUID) store.Notification {
	e.t.Helper()
	var n store.Notification
	testutil.Eventually(e.t, func() bool {
		n = e.get(id)
		return n.Status == domain.StatusSent || n.Status == domain.StatusFailed
	}, waitTimeout)
	return n
}

func outcomes(a []store.Attempt) []domain.Outcome {
	out := make([]domain.Outcome, len(a))
	for i, x := range a {
		out[i] = x.Outcome
	}
	return out
}

func repeat(o domain.Outcome, n int) []domain.Outcome {
	out := make([]domain.Outcome, n)
	for i := range out {
		out[i] = o
	}
	return out
}

func reason(n store.Notification) string {
	if n.FailureReason == nil {
		return "none"
	}
	return string(*n.FailureReason)
}

// checkFinal asserts the final state of the row.
func checkFinal(t *testing.T, n store.Notification, status domain.Status, why string, failures int) {
	t.Helper()
	if n.Status != status || reason(n) != why || n.Failures != failures {
		t.Errorf("status %s, failure_reason %s, failures %d; want %s, %s, %d",
			n.Status, reason(n), n.Failures, status, why, failures)
	}
}

// checkAttempts asserts the outcomes of the attempt rows, in order, and that
// attempt_count matches them.
func checkAttempts(t *testing.T, n store.Notification, a []store.Attempt, want []domain.Outcome) {
	t.Helper()
	if got := outcomes(a); !slices.Equal(got, want) {
		t.Errorf("attempts %v, want %v", got, want)
	}
	if n.AttemptCount != len(want) {
		t.Errorf("attempt_count %d, want %d", n.AttemptCount, len(want))
	}
}

// checkAttemptsBesideAborted is checkAttempts for a notification that may
// also have aborted attempts (a circuit that opened between its claim and
// its slot): the other attempts must be want, in order, and attempt_count
// counts them all. An aborted attempt consumes no budget; the caller checks
// failures.
func checkAttemptsBesideAborted(t *testing.T, n store.Notification, a []store.Attempt, want []domain.Outcome) {
	t.Helper()
	var sent []domain.Outcome
	for _, x := range a {
		if x.Outcome != domain.OutcomeAborted {
			sent = append(sent, x.Outcome)
		}
	}
	if !slices.Equal(sent, want) {
		t.Errorf("attempts other than aborted %v, want %v (all: %v)", sent, want, outcomes(a))
	}
	if n.AttemptCount != len(a) {
		t.Errorf("attempt_count %d, want %d", n.AttemptCount, len(a))
	}
}

// arrivals returns the arrival times of the requests mockprovider received
// on a channel, in order.
func arrivals(t *testing.T, rep mockprovider.Report, channel string) []time.Time {
	t.Helper()
	out := make([]time.Time, 0, len(rep.Channels[channel].Arrivals))
	for _, s := range rep.Channels[channel].Arrivals {
		at, err := time.Parse(mockArrivalLayout, s)
		if err != nil {
			t.Fatalf("arrival %q: %v", s, err)
		}
		out = append(out, at)
	}
	return out
}

// mockArrivalLayout is the layout of mockprovider's arrival times.
const mockArrivalLayout = "2006-01-02T15:04:05.000000Z"

// gaps returns the time between consecutive arrivals.
func gaps(at []time.Time) []time.Duration {
	var out []time.Duration
	for i := 1; i < len(at); i++ {
		out = append(out, at[i].Sub(at[i-1]))
	}
	return out
}

// checkSeparated asserts that consecutive requests are at least d apart:
// each request after the first is the probe of a circuit that the previous
// one opened for d. The arrival times and the breaker's clock are the same
// clock (the mock runs in the test process), so no tolerance is needed
// below d.
func checkSeparated(t *testing.T, at []time.Time, d time.Duration) {
	t.Helper()
	for i, g := range gaps(at) {
		if g < d {
			t.Errorf("requests %d and %d arrived %s apart, want at least the open period %s (gaps %v)",
				i+1, i+2, g, d, gaps(at))
		}
	}
}

// checkKey asserts that the provider saw the notification's idempotency key
// n times and no other key.
func checkKey(t *testing.T, rep mockprovider.Report, id uuid.UUID, n int) {
	t.Helper()
	if len(rep.IdempotencyKeys) != 1 || rep.IdempotencyKeys[id.String()] != n {
		t.Errorf("idempotency keys %v, want {%s: %d}", rep.IdempotencyKeys, id, n)
	}
}

// 500 twice, then 202: two retryable attempts that consume budget, then the
// success, all with the same idempotency key.
func TestE2E_Fault5xxRetriedUntilSent(t *testing.T) {
	e := newEnv(t)
	m := newMock(t)
	m.configure(`{"channels": {"sms": {"fail_first": 2, "fail_status": 500}}}`)
	e.startFault(m.srv.URL)
	id := e.createOne(5, testTTL)

	n := e.waitFinal(id)
	checkFinal(t, n, domain.StatusSent, "none", 2)
	a := e.attempts(id)
	checkAttempts(t, n, a, []domain.Outcome{domain.OutcomeRetryable, domain.OutcomeRetryable, domain.OutcomeSuccess})
	for i, x := range a[:min(2, len(a))] {
		if x.HTTPStatus == nil || *x.HTTPStatus != 500 {
			t.Errorf("attempt %d: http_status %v, want 500", i+1, x.HTTPStatus)
		}
	}
	if n.ProviderMessageID == nil || *n.ProviderMessageID == "" {
		t.Error("provider_message_id is not set")
	}
	checkKey(t, m.report(), id, 3)
}

// 429 with Retry-After: 1, then 202: throttled does not consume budget, and
// the retry waits for the hint, which is larger than the back-off. The gap is
// measured on the database clock: the first attempt's finished_at and the
// retry's due_at come from the same transaction, and the second attempt's
// started_at is the claim's now().
func TestE2E_Fault429RetryAfterHonoured(t *testing.T) {
	e := newEnv(t)
	m := newMock(t)
	m.configure(`{"channels": {"sms": {"fail_first": 1, "fail_status": 429, "retry_after": "1"}}}`)
	e.startFault(m.srv.URL)
	id := e.createOne(5, testTTL)

	n := e.waitFinal(id)
	checkFinal(t, n, domain.StatusSent, "none", 0)
	a := e.attempts(id)
	checkAttempts(t, n, a, []domain.Outcome{domain.OutcomeThrottled, domain.OutcomeSuccess})
	if len(a) == 2 && a[0].FinishedAt != nil {
		if gap := a[1].StartedAt.Sub(*a[0].FinishedAt); gap < time.Second {
			t.Errorf("the retry started %s after the throttled attempt finished, want at least 1s", gap)
		}
	}
	checkKey(t, m.report(), id, 2)
}

// 429 without Retry-After, then 202: the retry waits for the throttle default
// (300 ms), not the back-off (50 ms).
func TestE2E_Fault429WithoutHintUsesThrottleDefault(t *testing.T) {
	e := newEnv(t)
	m := newMock(t)
	m.configure(`{"channels": {"sms": {"fail_first": 1, "fail_status": 429}}}`)
	e.startFault(m.srv.URL)
	id := e.createOne(5, testTTL)

	n := e.waitFinal(id)
	checkFinal(t, n, domain.StatusSent, "none", 0)
	a := e.attempts(id)
	checkAttempts(t, n, a, []domain.Outcome{domain.OutcomeThrottled, domain.OutcomeSuccess})
	if len(a) == 2 && a[0].FinishedAt != nil {
		if gap := a[1].StartedAt.Sub(*a[0].FinishedAt); gap < faultThrottleDefault {
			t.Errorf("the retry started %s after the throttled attempt finished, want at least %s", gap, faultThrottleDefault)
		}
	}
	checkKey(t, m.report(), id, 2)
}

// 404 more often than max_failures, then 202: provider_fatal never consumes
// budget, so the row is sent with failures 0. (The answer of a deleted
// webhook.site URL, assumption U2.) Every 404 opens the circuit for
// BREAKER_FATAL_OPEN, so the requests the provider received are separated by
// open periods; an aborted attempt, if one occurs, consumes no budget either.
func TestE2E_Fault404DoesNotConsumeBudget(t *testing.T) {
	const maxFailures = 2
	e := newEnv(t)
	m := newMock(t)
	m.configure(fmt.Sprintf(`{"channels": {"sms": {"fail_first": %d, "fail_status": 404}}}`, 2*maxFailures))
	e.startFault(m.srv.URL)
	id := e.createOne(maxFailures, testTTL)

	n := e.waitFinal(id)
	checkFinal(t, n, domain.StatusSent, "none", 0)
	checkAttemptsBesideAborted(t, n, e.attempts(id),
		append(repeat(domain.OutcomeProviderFatal, 2*maxFailures), domain.OutcomeSuccess))
	rep := m.report()
	checkKey(t, rep, id, 2*maxFailures+1)
	checkSeparated(t, arrivals(t, rep, "sms"), faultFatalOpen)
}

// 400: failed with provider_rejected after exactly one attempt, without
// consuming budget.
func TestE2E_Fault400Rejected(t *testing.T) {
	e := newEnv(t)
	m := newMock(t)
	m.configure(`{"channels": {"sms": {"status": 400}}}`)
	e.startFault(m.srv.URL)
	id := e.createOne(5, testTTL)

	n := e.waitFinal(id)
	checkFinal(t, n, domain.StatusFailed, string(domain.ReasonProviderRejected), 0)
	a := e.attempts(id)
	checkAttempts(t, n, a, []domain.Outcome{domain.OutcomePermanent})
	if len(a) == 1 && (a[0].HTTPStatus == nil || *a[0].HTTPStatus != 400) {
		t.Errorf("http_status %v, want 400", a[0].HTTPStatus)
	}
	checkKey(t, m.report(), id, 1)
}

// The connection is closed after the request was read: the delivery is
// ambiguous, so every attempt is unknown and consumes budget; with
// max_failures reached the row fails with max_failures_unknown_outcome.
func TestE2E_FaultConnectionClosedIsUnknown(t *testing.T) {
	const maxFailures = 3
	e := newEnv(t)
	m := newMock(t)
	m.configure(`{"channels": {"sms": {"close_connection": true}}}`)
	e.startFault(m.srv.URL)
	id := e.createOne(maxFailures, testTTL)

	n := e.waitFinal(id)
	checkFinal(t, n, domain.StatusFailed, string(domain.ReasonMaxFailuresUnknownOutcome), maxFailures)
	a := e.attempts(id)
	checkAttempts(t, n, a, repeat(domain.OutcomeUnknown, maxFailures))
	for _, x := range a {
		if x.HTTPStatus != nil || x.Error == nil || *x.Error == "" {
			t.Errorf("attempt %d: http_status %v, error set %v; want no status and the transport error",
				x.AttemptNo, x.HTTPStatus, x.Error != nil)
		}
	}
	rep := m.report()
	checkKey(t, rep, id, maxFailures)
	if got := rep.Channels["sms"].Statuses["closed"]; got != maxFailures {
		t.Errorf("mockprovider closed %d connections, want %d", got, maxFailures)
	}
}

// Nothing listens on the provider URL: every attempt is unreachable and none
// consumes budget, so with a short TTL the row ends failed with expired,
// after more attempts than max_failures, and never on budget.
func TestE2E_FaultUnreachableExpires(t *testing.T) {
	const (
		maxFailures = 2
		ttl         = 1_500 * time.Millisecond
	)
	e := newEnv(t)
	e.startFault(closedURL(t))
	id := e.createOne(maxFailures, ttl)

	n := e.waitFinal(id)
	checkFinal(t, n, domain.StatusFailed, string(domain.ReasonExpired), 0)
	a := e.attempts(id)
	if len(a) <= maxFailures {
		t.Errorf("%d attempts, want more than max_failures (%d)", len(a), maxFailures)
	}
	checkAttempts(t, n, a, repeat(domain.OutcomeUnreachable, len(a)))
	for _, x := range a {
		if x.HTTPStatus != nil || x.Error == nil || *x.Error == "" {
			t.Errorf("attempt %d: http_status %v, error set %v; want no status and the dial error",
				x.AttemptNo, x.HTTPStatus, x.Error != nil)
		}
	}
}

// closedURL returns an http URL on a local port on which nothing listens.
func closedURL(t *testing.T) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
	defer cancel()
	l, err := (&net.ListenConfig{}).Listen(ctx, "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	return "http://" + addr + "/"
}

// 302 with a Location to a second server: the redirect is not followed, so
// the second server receives nothing, and the attempt is provider_fatal. It
// opens the circuit, so the next attempt, answered 202, is its probe, one
// open period later; the row ends sent without budget used.
func TestE2E_FaultRedirectNotFollowed(t *testing.T) {
	e := newEnv(t)
	target := newCounter(t)
	m := newMock(t)
	cfg, err := json.Marshal(map[string]any{"channels": map[string]any{"sms": map[string]any{
		"fail_first": 1, "fail_status": 302, "location": target.srv.URL + "/elsewhere",
	}}})
	if err != nil {
		t.Fatal(err)
	}
	m.configure(string(cfg))
	e.startFault(m.srv.URL)
	id := e.createOne(5, testTTL)

	n := e.waitFinal(id)
	checkFinal(t, n, domain.StatusSent, "none", 0)
	a := e.attempts(id)
	checkAttemptsBesideAborted(t, n, a, []domain.Outcome{domain.OutcomeProviderFatal, domain.OutcomeSuccess})
	if len(a) > 0 && (a[0].HTTPStatus == nil || *a[0].HTTPStatus != 302) {
		t.Errorf("http_status %v, want 302", a[0].HTTPStatus)
	}
	if got := target.count(); got != 0 {
		t.Errorf("the redirect target received %d requests, want 0", got)
	}
	rep := m.report()
	checkKey(t, rep, id, 2)
	checkSeparated(t, arrivals(t, rep, "sms"), faultFatalOpen)
}

// A 2xx with a malformed body is a success: the provider accepted the
// message, and resending would create a duplicate.
func TestE2E_FaultMalformedSuccessBodyIsSent(t *testing.T) {
	for _, status := range []int{202, 200} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			e := newEnv(t)
			m := newMock(t)
			m.configure(fmt.Sprintf(`{"channels": {"sms": {"status": %d, "malformed_body": true}}}`, status))
			e.startFault(m.srv.URL)
			id := e.createOne(5, testTTL)

			n := e.waitFinal(id)
			checkFinal(t, n, domain.StatusSent, "none", 0)
			checkAttempts(t, n, e.attempts(id), []domain.Outcome{domain.OutcomeSuccess})
			if n.ProviderMessageID != nil {
				t.Errorf("provider_message_id %q from a malformed body", *n.ProviderMessageID)
			}
			checkKey(t, m.report(), id, 1)
		})
	}
}

//go:build integration

package integration

import (
	"encoding/json"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/store"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/testutil"
)

// The circuit breaker through the wired roles (ADR-0007 sections 2, 4, 7
// and 8; D23, D26, D30): the relay and the worker role against real
// Postgres, RabbitMQ and Redis, with the real mockprovider handler or the
// recording provider. Every duration is set in milliseconds through the
// configuration.

// circuitChange is one state change of a circuit, as the worker logs it.
type circuitChange struct {
	Time    time.Time `json:"time"`
	Level   string    `json:"level"`
	Msg     string    `json:"msg"`
	Channel string    `json:"channel"`
	From    string    `json:"from"`
	To      string    `json:"to"`
	Outcome string    `json:"outcome"`
}

// circuitChanges returns the logged state changes of the circuit of ch, in
// order.
func (e *env) circuitChanges(ch domain.Channel) []circuitChange {
	var out []circuitChange
	for line := range strings.SplitSeq(e.logs.String(), "\n") {
		var c circuitChange
		if json.Unmarshal([]byte(line), &c) != nil || !strings.HasPrefix(c.Msg, "worker: circuit ") {
			continue
		}
		if c.Channel == string(ch) {
			out = append(out, c)
		}
	}
	return out
}

// openings returns the changes of the circuit of ch that opened it.
func (e *env) openings(ch domain.Channel) []circuitChange {
	var out []circuitChange
	for _, c := range e.circuitChanges(ch) {
		if c.To == "open" {
			out = append(out, c)
		}
	}
	return out
}

// createOn inserts one due notification of normal priority on channel ch.
func (e *env) createOn(ch domain.Channel, maxFailures int) uuid.UUID {
	e.t.Helper()
	recipient := testRecipient
	if ch != domain.ChannelSMS {
		recipient = "circuit@example.com"
	}
	ctx, cancel := e.ctx()
	defer cancel()
	row, err := e.store.CreateNotification(ctx, store.NewNotification{
		Channel:       ch,
		Priority:      domain.PriorityNormal,
		Recipient:     domain.Recipient(recipient),
		Content:       testContent,
		TTL:           testTTL,
		MaxFailures:   maxFailures,
		CorrelationID: "corr-circuit",
		TraceParent:   testTraceParent,
	})
	if err != nil {
		e.t.Fatalf("create: %v", err)
	}
	return row.ID
}

// startCircuit starts the worker role and the relay on the fault durations,
// changed by vars, with the provider at providerURL.
func (e *env) startCircuit(providerURL string, vars map[string]string) {
	e.t.Helper()
	all := faultVars(providerURL)
	for k, v := range vars {
		all[k] = v
	}
	cfg := e.config(all)
	e.startWorker(cfg, 0)
	e.startRelay(cfg)
}

// checkUnfailed asserts that the notification has not failed and has not
// consumed budget.
func checkUnfailed(t *testing.T, n store.Notification) {
	t.Helper()
	if n.Status == domain.StatusFailed || n.Status == domain.StatusSent || n.Failures != 0 {
		t.Errorf("notification %s: status %s, failure_reason %s, failures %d; want unfinished with failures 0",
			n.ID, n.Status, reason(n), n.Failures)
	}
}

// checkOnly asserts that every attempt has one of the allowed outcomes.
func checkOnly(t *testing.T, id uuid.UUID, a []store.Attempt, allowed ...domain.Outcome) {
	t.Helper()
	for _, x := range a {
		if !slices.Contains(allowed, x.Outcome) {
			t.Errorf("notification %s: attempt %d is %s, want one of %v (all: %v)",
				id, x.AttemptNo, x.Outcome, allowed, outcomes(a))
		}
	}
}

// 401 is provider_fatal: the first answer opens the circuit at once for
// BREAKER_FATAL_OPEN, and from then on one probe per open period reaches the
// provider, while every waiting notification keeps its budget. Without the
// breaker each notification would be retried on its own after the back-off
// (50 ms), and the provider would receive about notifications × elapsed /
// back-off requests; the bound below allows the first requests of every
// notification plus one probe per elapsed open period. Every opening is
// logged once at error level. When the provider accepts again, the probe
// closes the circuit and every notification is sent.
func TestE2E_CircuitProviderFatalOpensAtOnce(t *testing.T) {
	const (
		notifications = 5
		fatalOpen     = 300 * time.Millisecond
		openings      = 4
	)
	e := newEnv(t)
	m := newMock(t)
	m.configure(`{"channels": {"sms": {"status": 401}}}`)
	e.startCircuit(m.srv.URL, map[string]string{"BREAKER_FATAL_OPEN": millis(fatalOpen)})
	ids := make([]uuid.UUID, notifications)
	for i := range ids {
		ids[i] = e.createOne(2, testTTL)
	}

	testutil.Eventually(t, func() bool { return len(e.openings(domain.ChannelSMS)) >= openings }, waitTimeout)
	rep := m.report()
	at := arrivals(t, rep, "sms")
	elapsed := at[len(at)-1].Sub(at[0])
	bound := notifications + int(elapsed/fatalOpen) + 1
	t.Logf("%d requests in %s with the circuit open for %s at a time (bound %d; without the breaker about %d)",
		len(at), elapsed.Round(time.Millisecond), fatalOpen, bound, notifications*int(elapsed/faultBackoff))
	if len(at) > bound {
		t.Errorf("the provider received %d requests in %s, want at most %d: the circuit did not hold them back (gaps %v)",
			len(at), elapsed, bound, gaps(at))
	}
	if st := rep.Channels["sms"].Statuses; len(st) != 1 || st["401"] != len(at) {
		t.Errorf("mockprovider answered %v, want only 401", st)
	}
	// Once the circuit is open, only probes go out: one per open period.
	if n := len(at); n > notifications {
		checkSeparated(t, at[notifications:], fatalOpen)
	}
	// An attempt can be open (in_flight): claimed and waiting for its slot,
	// where the open circuit will abort it.
	for _, id := range ids {
		checkUnfailed(t, e.get(id))
		checkOnly(t, id, e.attempts(id), domain.OutcomeProviderFatal, domain.OutcomeAborted, domain.OutcomeInFlight)
	}

	m.configure(`{}`)
	for _, id := range ids {
		n := e.waitFinal(id)
		checkFinal(t, n, domain.StatusSent, "none", 0)
		a := e.attempts(id)
		checkOnly(t, id, a, domain.OutcomeProviderFatal, domain.OutcomeAborted, domain.OutcomeSuccess)
		if n.AttemptCount != len(a) {
			t.Errorf("notification %s: attempt_count %d, want %d", id, n.AttemptCount, len(a))
		}
	}

	// The log: every opening at error level, once, by provider_fatal; every
	// open period ends half-open; the last probe closes the circuit.
	rep = m.report()
	changes := e.circuitChanges(domain.ChannelSMS)
	var opened, errorLines, halfOpen, closed int
	for _, c := range changes {
		switch c.To {
		case "open":
			opened++
			if c.Level != "ERROR" || c.Outcome != string(domain.OutcomeProviderFatal) || c.From == "open" {
				t.Errorf("opening logged as %+v, want level ERROR and outcome provider_fatal", c)
			}
		case "half_open":
			halfOpen++
		case "closed":
			closed++
		}
		if c.Level == "ERROR" {
			errorLines++
		}
	}
	if errorLines != opened {
		t.Errorf("%d error-level circuit lines for %d openings, want one per opening", errorLines, opened)
	}
	if halfOpen != opened || closed != 1 {
		t.Errorf("%d openings, %d half-open and %d closed changes; want as many half-open as openings and one close",
			opened, halfOpen, closed)
	}
	if last := changes[len(changes)-1]; last.From != "half_open" || last.To != "closed" || last.Outcome != string(domain.OutcomeSuccess) {
		t.Errorf("last change %+v, want half_open -> closed by success", last)
	}
	// Each opening after the first is one probe's 401; the first may have
	// come with up to one 401 per notification.
	if n401 := rep.Channels["sms"].Statuses["401"]; opened < n401-notifications+1 || opened > n401 {
		t.Errorf("%d openings for %d answers 401, want between %d and %d", opened, n401, n401-notifications+1, n401)
	}
}

// Rule 2 of the breaker (BREAKER_CONSECUTIVE_FAILURES, 5) is what protects
// low traffic: with the window rule out of reach (BREAKER_MIN_REQUESTS far
// above the traffic) and the provider answering 500, exactly five requests
// reach the provider, and then the circuit is open. One send at a time
// (WORKER_CONCURRENCY 1) makes "exactly five" exact: each outcome is
// recorded before the next send is admitted. The notifications created while
// the circuit is open are not sent and keep their budgets; when the provider
// accepts again, the probe closes the circuit and everything is sent.
//
// This is the target of the mutation check that removes streakExceeded()
// from recordClosed: then the circuit never opens and a sixth request
// follows the back-off.
func TestE2E_CircuitOpensAfterFiveConsecutiveFailures(t *testing.T) {
	const (
		first       = 2 // notifications that meet the failing provider
		later       = 3 // notifications created while the circuit is open
		streak      = 5
		maxFailures = 8
		openBase    = 2_000 * time.Millisecond
	)
	e := newEnv(t)
	m := newMock(t)
	m.configure(`{"channels": {"sms": {"status": 500}}}`)
	vars := map[string]string{
		"WORKER_CONCURRENCY":   "1",
		"BREAKER_MIN_REQUESTS": "1000",
		"BREAKER_OPEN_BASE":    millis(openBase),
		"BREAKER_OPEN_MAX":     millis(4 * openBase),
	}
	if cfg := e.config(faultVars(m.srv.URL)); cfg.Breaker.ConsecutiveFailures != streak {
		t.Fatalf("BREAKER_CONSECUTIVE_FAILURES defaults to %d, want %d", cfg.Breaker.ConsecutiveFailures, streak)
	}
	e.startCircuit(m.srv.URL, vars)
	var ids []uuid.UUID
	for range first {
		ids = append(ids, e.createOne(maxFailures, testTTL))
	}

	// The circuit opens, or a sixth request arrives because it did not.
	testutil.Eventually(t, func() bool {
		return len(e.openings(domain.ChannelSMS)) > 0 || m.report().Total > streak
	}, waitTimeout)
	if got := m.report().Total; got != streak {
		t.Fatalf("%d requests reached the provider before the circuit opened, want exactly %d (openings logged: %d)",
			got, streak, len(e.openings(domain.ChannelSMS)))
	}
	open := e.openings(domain.ChannelSMS)
	if len(open) != 1 || open[0].From != "closed" || open[0].Outcome != string(domain.OutcomeRetryable) || open[0].Level != "WARN" {
		t.Fatalf("openings %+v, want one from closed by retryable at level WARN", open)
	}
	openedAt := open[0].Time

	for range later {
		ids = append(ids, e.createOne(maxFailures, testTTL))
	}
	// Every notification is published and waits in the worker or the
	// queue: nothing is claimed while the circuit is open. The one whose
	// failure opened the circuit is due only when the open period ends.
	testutil.Eventually(t, func() bool {
		return e.countStatus(domain.StatusQueued) == first-1+later && e.countStatus(domain.StatusRetrying) == 1
	}, waitTimeout)
	if time.Since(openedAt) >= openBase {
		t.Fatalf("the check came %s after the opening, not within the open period %s", time.Since(openedAt), openBase)
	}
	if got := m.report().Total; got != streak {
		t.Errorf("%d requests while the circuit is open, want still %d", got, streak)
	}
	spent := 0
	for i, id := range ids {
		n := e.get(id)
		spent += n.Failures
		if i >= first && (n.Failures != 0 || n.AttemptCount != 0) {
			t.Errorf("notification created while open: failures %d, attempt_count %d; want 0 and 0", n.Failures, n.AttemptCount)
		}
	}
	if spent != streak {
		t.Errorf("%d failures spent, want %d, one per 500", spent, streak)
	}

	m.configure(`{}`)
	for i, id := range ids {
		n := e.waitFinal(id)
		a := e.attempts(id)
		if i < first {
			checkOnly(t, id, a, domain.OutcomeRetryable, domain.OutcomeAborted, domain.OutcomeSuccess)
			if n.Status != domain.StatusSent {
				t.Errorf("notification %s: status %s, want sent", id, n.Status)
			}
			continue
		}
		checkFinal(t, n, domain.StatusSent, "none", 0)
		checkAttemptsBesideAborted(t, n, a, []domain.Outcome{domain.OutcomeSuccess})
	}
	rep := m.report()
	if want := streak + first + later; rep.Total != want {
		t.Errorf("the provider received %d requests, want %d", rep.Total, want)
	}
	if at := arrivals(t, rep, "sms"); len(at) > streak {
		if g := at[streak].Sub(at[streak-1]); g < openBase {
			t.Errorf("the probe came %s after the fifth failure, want at least the open period %s", g, openBase)
		}
	}
}

// D23, the abort path: a notification claimed while the circuit was closed
// whose slot comes after the circuit opened is not sent. The order is
// forced: one send per second (RATE_LIMIT_PER_SECOND 1, with a horizon that
// reserves the next slot at once) gives the second notification a slot one
// second after the first; the provider holds the first request until the
// second notification is claimed, and then answers 401, which opens the
// circuit for BREAKER_FATAL_OPEN. At its slot the second notification is
// refused a permit: its attempt is aborted, the row is retrying with
// failures 0 and due at the end of the open period.
func TestE2E_CircuitOpenedAfterClaimAbortsTheAttempt(t *testing.T) {
	const fatalOpen = 2_000 * time.Millisecond
	e := newEnv(t)
	e.prov.answer(http.StatusUnauthorized)
	release := e.prov.block(t)
	e.startCircuit(e.prov.srv.URL, map[string]string{
		"RATE_LIMIT_PER_SECOND": "1",
		"RATE_LIMIT_HORIZON":    "1500ms",
		"BREAKER_FATAL_OPEN":    millis(fatalOpen),
	})

	opener := e.createOne(5, testTTL)
	testutil.Eventually(t, func() bool { return e.prov.count() == 1 }, waitTimeout)
	late := e.createOne(5, testTTL)
	// Claimed while the circuit is still closed: the first answer is held.
	e.waitStatus(late, domain.StatusProcessing)
	if c := e.circuitChanges(domain.ChannelSMS); len(c) != 0 {
		t.Fatalf("circuit changes before the answer: %+v", c)
	}
	release()
	e.prov.answer(http.StatusAccepted)

	testutil.Eventually(t, func() bool {
		a := e.attempts(late)
		return len(a) == 1 && a[0].FinishedAt != nil
	}, waitTimeout)
	n := e.get(late)
	a := e.attempts(late)
	if a[0].Outcome != domain.OutcomeAborted || a[0].HTTPStatus != nil {
		t.Errorf("attempt %s with http_status %v, want aborted without a status", a[0].Outcome, a[0].HTTPStatus)
	}
	if n.Status != domain.StatusRetrying || n.Failures != 0 || n.AttemptCount != 1 {
		t.Errorf("after the abort: status %s, failures %d, attempt_count %d; want retrying, 0, 1",
			n.Status, n.Failures, n.AttemptCount)
	}
	if got := e.prov.count(); got != 1 {
		t.Errorf("the provider received %d requests, want only the first", got)
	}
	// The circuit opened when the 401 was recorded, just before the first
	// notification's final write: its finished_at and the aborted row's
	// due_at are both on the database clock.
	first := e.attempts(opener)
	if len(first) != 1 || first[0].Outcome != domain.OutcomeProviderFatal || first[0].FinishedAt == nil {
		t.Fatalf("first notification's attempts %v, want one provider_fatal", outcomes(first))
	}
	if d := n.DueAt.Sub(*first[0].FinishedAt); d < fatalOpen-200*time.Millisecond || d > fatalOpen+100*time.Millisecond {
		t.Errorf("the aborted row is due %s after the circuit opened, want about the open period %s", d, fatalOpen)
	}

	n = e.waitFinal(late)
	checkFinal(t, n, domain.StatusSent, "none", 0)
	a = e.attempts(late)
	if len(a) < 2 || a[0].Outcome != domain.OutcomeAborted || a[len(a)-1].Outcome != domain.OutcomeSuccess {
		t.Errorf("attempts %v, want aborted first and success last", outcomes(a))
	}
	checkOnly(t, late, a, domain.OutcomeAborted, domain.OutcomeSuccess)
	if got := keys(e.prov.requests())[late.String()]; got != 1 {
		t.Errorf("the late notification reached the provider %d times, want once", got)
	}
	checkFinal(t, e.waitFinal(opener), domain.StatusSent, "none", 0)
}

// D30: a 429 without Retry-After on consecutive probes opens the circuit
// for THROTTLE_DEFAULT_DELAY, then twice and four times that, and then for
// THROTTLE_MAX_DELAY (eight times would be more). A 429 with Retry-After: 1
// opens it for one second. After a 202 the next 429 starts at the default
// again. One notification at a time, so every request after the first is
// the probe of the opening before it; the gaps between the requests the
// provider received measure the open periods. Throttling never consumes
// budget.
func TestE2E_CircuitThrottlingDoublesOnConsecutiveProbes(t *testing.T) {
	const (
		throttleDefault = 100 * time.Millisecond
		throttleMax     = 500 * time.Millisecond
		retryAfter      = time.Second
		// slack is how much later than its open period a probe may arrive:
		// the row becomes due, the relay publishes it, the worker claims it.
		slack = 250 * time.Millisecond
	)
	e := newEnv(t)
	m := newMock(t)
	m.configure(`{"channels": {"sms": {"status": 429}}}`)
	e.startCircuit(m.srv.URL, map[string]string{
		"THROTTLE_DEFAULT_DELAY": millis(throttleDefault),
		"THROTTLE_MAX_DELAY":     millis(throttleMax),
	})
	count := func() int { return m.report().Channels["sms"].Count }

	x := e.createOne(5, testTTL)
	// Requests 1 to 5 are answered 429 without a hint: open for 100, 200,
	// 400 and then the maximum, 500, ms; request 5 opens it for 500 ms too.
	testutil.Eventually(t, func() bool { return count() >= 5 }, waitTimeout)
	m.configure(`{"channels": {"sms": {"status": 429, "retry_after": "1"}}}`)
	// Request 6 is answered 429 with Retry-After: 1.
	testutil.Eventually(t, func() bool { return count() >= 6 }, waitTimeout)
	m.configure(`{}`)
	n := e.waitFinal(x)
	checkFinal(t, n, domain.StatusSent, "none", 0)
	checkAttemptsBesideAborted(t, n, e.attempts(x), append(repeat(domain.OutcomeThrottled, 6), domain.OutcomeSuccess))

	// A new throttling episode after the success starts at the default.
	m.configure(`{"channels": {"sms": {"status": 429}}}`)
	y := e.createOne(5, testTTL)
	testutil.Eventually(t, func() bool { return count() >= 8 }, waitTimeout)
	m.configure(`{}`)
	n = e.waitFinal(y)
	checkFinal(t, n, domain.StatusSent, "none", 0)

	at := arrivals(t, m.report(), "sms")
	if len(at) < 9 {
		t.Fatalf("%d requests, want at least 9", len(at))
	}
	want := []time.Duration{
		throttleDefault, 2 * throttleDefault, 4 * throttleDefault, throttleMax, throttleMax, retryAfter,
	}
	got := gaps(at[:7])
	got = append(got, at[8].Sub(at[7]))
	want = append(want, throttleDefault)
	t.Logf("open periods %v; gaps between the requests %v", want, got)
	for i := range want {
		if got[i] < want[i] || got[i] > want[i]+slack {
			t.Errorf("gap %d is %s, want the open period %s (up to %s later)", i+1, got[i], want[i], slack)
		}
	}
}

// Half-open lets one probe through: with many notifications waiting behind
// an open circuit, exactly one request reaches the provider when the open
// period ends. The provider holds that response; meanwhile nothing else is
// sent. Only after the probe succeeds do the others follow.
func TestE2E_CircuitHalfOpenSendsOneProbe(t *testing.T) {
	const (
		fatalOpen = 1_000 * time.Millisecond
		waiting   = 10
		// observe is how long the held probe is watched alone: at the
		// default rate about 30 slots pass.
		observe = 300 * time.Millisecond
	)
	e := newEnv(t)
	e.prov.answer(http.StatusUnauthorized)
	e.startCircuit(e.prov.srv.URL, map[string]string{"BREAKER_FATAL_OPEN": millis(fatalOpen)})

	ids := []uuid.UUID{e.createOne(5, testTTL)}
	testutil.Eventually(t, func() bool { return len(e.openings(domain.ChannelSMS)) == 1 }, waitTimeout)
	openedAt := e.openings(domain.ChannelSMS)[0].Time
	e.prov.answer(http.StatusAccepted)
	release := e.prov.block(t)
	for range waiting {
		ids = append(ids, e.createOne(5, testTTL))
	}
	testutil.Eventually(t, func() bool { return e.countStatus(domain.StatusQueued) >= waiting }, waitTimeout)
	if time.Since(openedAt) >= fatalOpen {
		t.Fatalf("the notifications were queued %s after the opening, not within the open period %s",
			time.Since(openedAt), fatalOpen)
	}

	testutil.Eventually(t, func() bool { return e.prov.count() == 2 }, waitTimeout)
	probeSeen := time.Now()
	testutil.Eventually(t, func() bool { return time.Since(probeSeen) >= observe }, waitTimeout)
	if got := e.prov.count(); got != 2 {
		t.Errorf("%d requests while the probe is held, want 2 (the 401 and the probe)", got)
	}
	if got := e.countStatus(domain.StatusSent); got != 0 {
		t.Errorf("%d notifications sent while the probe is held, want 0", got)
	}
	reqs := e.prov.requests()
	if g := reqs[1].at.Sub(reqs[0].at); g < fatalOpen {
		t.Errorf("the probe came %s after the 401, want at least the open period %s", g, fatalOpen)
	}
	released := time.Now()
	release()

	for _, id := range ids {
		n := e.waitFinal(id)
		checkFinal(t, n, domain.StatusSent, "none", 0)
		checkOnly(t, id, e.attempts(id), domain.OutcomeProviderFatal, domain.OutcomeAborted, domain.OutcomeSuccess)
	}
	reqs = e.prov.requests()
	if len(reqs) != waiting+2 {
		t.Errorf("the provider received %d requests, want %d", len(reqs), waiting+2)
	}
	for i, r := range reqs[min(2, len(reqs)):] {
		if r.at.Before(released) {
			t.Errorf("request %d arrived %s before the probe was answered", i+3, released.Sub(r.at))
		}
	}
	byKey := keys(reqs)
	for i, id := range ids {
		want := 1
		if i == 0 {
			want = 2
		}
		if byKey[id.String()] != want {
			t.Errorf("notification %s reached the provider %d times, want %d", id, byKey[id.String()], want)
		}
	}
	var halfOpen, closed int
	for _, c := range e.circuitChanges(domain.ChannelSMS) {
		switch c.To {
		case "half_open":
			halfOpen++
		case "closed":
			closed++
		}
	}
	if halfOpen != 1 || closed != 1 {
		t.Errorf("%d half-open and %d closed changes, want one each", halfOpen, closed)
	}
}

// One circuit per channel (D26): an open sms circuit does not stop email.
// The sms circuit is opened by a 401 for one minute, the default
// BREAKER_FATAL_OPEN; email notifications created meanwhile are sent at
// once, while the sms notification waits with its budget intact.
func TestE2E_CircuitChannelsAreIsolated(t *testing.T) {
	const emails = 3
	e := newEnv(t)
	m := newMock(t)
	m.configure(`{"channels": {"sms": {"status": 401}}}`)
	e.startCircuit(m.srv.URL, map[string]string{"BREAKER_FATAL_OPEN": "60000ms"})

	sms := e.createOn(domain.ChannelSMS, 5)
	testutil.Eventually(t, func() bool { return len(e.openings(domain.ChannelSMS)) == 1 }, waitTimeout)
	for range emails {
		id := e.createOn(domain.ChannelEmail, 5)
		n := e.waitFinal(id)
		checkFinal(t, n, domain.StatusSent, "none", 0)
		checkAttempts(t, n, e.attempts(id), []domain.Outcome{domain.OutcomeSuccess})
	}

	rep := m.report()
	if got := rep.Channels["sms"].Count; got != 1 {
		t.Errorf("mockprovider received %d sms requests, want 1", got)
	}
	if got := rep.Channels["email"].Count; got != emails {
		t.Errorf("mockprovider received %d email requests, want %d", got, emails)
	}
	n := e.get(sms)
	checkUnfailed(t, n)
	checkAttempts(t, n, e.attempts(sms), []domain.Outcome{domain.OutcomeProviderFatal})
	if c := e.circuitChanges(domain.ChannelEmail); len(c) != 0 {
		t.Errorf("the email circuit changed state: %+v", c)
	}
	if c := e.circuitChanges(domain.ChannelSMS); len(c) != 1 {
		t.Errorf("sms circuit changes %+v, want only the opening", c)
	}
}

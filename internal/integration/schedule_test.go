//go:build integration

package integration

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/api"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/platform/config"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/store"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/testutil"
)

// Durations of the scheduling scenarios, in milliseconds.
const (
	// scheduleLead is how far ahead of the database's clock a notification
	// is scheduled: long enough to watch it stay pending over many relay
	// ticks (10 ms here).
	scheduleLead = 1_500 * time.Millisecond
	// scheduleLateBound is how long after its scheduled_at a notification
	// may be sent at most: the relay's tick, the broker and the worker take
	// a few tens of milliseconds on an idle system.
	scheduleLateBound = 2_000 * time.Millisecond
	// scheduleCancelLead is how far ahead the cancelled notification and
	// its witness are scheduled. Nothing has to happen within it: the test
	// waits for it to pass before the relay starts.
	scheduleCancelLead = 100 * time.Millisecond
)

// apiServer serves the API role's handler on this test's database with the
// configuration's limits, as api.Service wires it, with one difference: its
// clock stands still, at the database's time when the server is made. The
// tests choose a scheduled_at ahead of the database's clock and the API
// judges it against its own. With a running clock there, a request that
// arrives after its scheduled_at (a stalled host, or a host whose clock is
// ahead of the database's container) is answered "in the past, so now" and
// stores no scheduled_at. With this one, a time chosen after the server was
// made is in the future whenever its request arrives; when a row is due is
// decided by the database's clock, as always.
func (e *env) apiServer(cfg config.Config) *httptest.Server {
	e.t.Helper()
	madeAt := e.dbNow()
	srv := httptest.NewServer(api.NewHandler(api.Options{
		Repository:           store.New(e.pool, store.ProfilesFrom(cfg.Retry)),
		DB:                   e.pool,
		Logger:               e.log,
		Retry:                cfg.Retry,
		MaxBodyBytes:         cfg.API.MaxBodyBytes,
		ExpiresAtMinLead:     cfg.API.ExpiresAtMinLead,
		ScheduledAtTolerance: cfg.API.SchedulePastTolerance,
		ScheduledAtMaxAhead:  cfg.API.ScheduleMaxHorizon,
		Now:                  func() time.Time { return madeAt },
	}))
	e.t.Cleanup(func() {
		srv.Client().CloseIdleConnections()
		srv.Close()
	})
	return srv
}

// call sends a request to the API and decodes the JSON answer.
func (e *env) call(srv *httptest.Server, method, path, body string) (int, map[string]json.RawMessage) {
	e.t.Helper()
	ctx, cancel := e.ctx()
	defer cancel()
	var rd io.Reader = http.NoBody
	if body != "" {
		rd = strings.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, srv.URL+path, rd)
	if err != nil {
		e.t.Fatal(err)
	}
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	res, err := srv.Client().Do(req)
	if err != nil {
		e.t.Fatalf("%s %s: %v", method, path, err)
	}
	defer func() { _ = res.Body.Close() }()
	b, err := io.ReadAll(res.Body)
	if err != nil {
		e.t.Fatal(err)
	}
	var m map[string]json.RawMessage
	if err := json.Unmarshal(b, &m); err != nil {
		e.t.Fatalf("%s %s: %s", method, path, b)
	}
	return res.StatusCode, m
}

// dbNow reads the database's clock, the one the relay decides "due" by.
func (e *env) dbNow() time.Time {
	e.t.Helper()
	ctx, cancel := e.ctx()
	defer cancel()
	var now time.Time
	if err := e.pool.QueryRow(ctx, `SELECT now()`).Scan(&now); err != nil {
		e.t.Fatalf("now: %v", err)
	}
	return now
}

// createScheduled creates an SMS notification through the API, scheduled at
// when, and returns its id. The answer must show the notification pending
// with its scheduled_at.
func (e *env) createScheduled(srv *httptest.Server, when time.Time) uuid.UUID {
	e.t.Helper()
	status, body := e.call(srv, http.MethodPost, "/v1/notifications",
		`{"channel":"sms","recipient":"`+testRecipient+`","content":"`+testContent+`","scheduled_at":"`+when.Format(time.RFC3339Nano)+`"}`)
	if status != http.StatusAccepted {
		e.t.Fatalf("create: status %d", status)
	}
	var (
		id    uuid.UUID
		st    string
		sched *time.Time
	)
	for name, dst := range map[string]any{"id": &id, "status": &st, "scheduled_at": &sched} {
		if err := json.Unmarshal(body[name], dst); err != nil {
			e.t.Fatalf("create: %s: %v", name, err)
		}
	}
	if st != string(domain.StatusPending) || sched == nil || !sched.Equal(when) {
		e.t.Fatalf("create: status %s, scheduled_at %v; want pending, %v", st, sched, when)
	}
	return id
}

// A notification scheduled a short time ahead stays pending, and is not sent,
// while the database's clock is before its scheduled_at; then it is sent once,
// not before its scheduled_at and not much later. Every time compared is the
// database's, so the test does not depend on the host's clock agreeing with
// it.
func TestE2E_ScheduledIsSentAtItsTimeNotBefore(t *testing.T) {
	e := newEnv(t)
	cfg := e.config(nil)
	_, w := e.startWorker(cfg, 0)
	e.startRelay(cfg)
	srv := e.apiServer(cfg)

	when := e.dbNow().Add(scheduleLead).Truncate(time.Microsecond).UTC()
	id := e.createScheduled(srv, when)

	// Before its time: each round reads the provider first, then the row's
	// status and whether the database's clock is still before due_at, in one
	// statement. When the clock is still before due_at, the status read with
	// it must be pending and the provider read before it must have received
	// nothing.
	roundsBefore := 0
	testutil.Eventually(t, func() bool {
		received := e.prov.count()
		ctx, cancel := e.ctx()
		defer cancel()
		var (
			status string
			before bool
		)
		if err := e.pool.QueryRow(ctx, `SELECT status, now() < due_at FROM notifications WHERE id = $1`, id).
			Scan(&status, &before); err != nil {
			t.Fatalf("read: %v", err)
		}
		if !before {
			return true
		}
		roundsBefore++
		if status != string(domain.StatusPending) || received != 0 {
			t.Fatalf("before its scheduled_at: status %s, provider requests %d; want pending, 0", status, received)
		}
		return false
	}, waitTimeout)
	if roundsBefore == 0 {
		t.Fatalf("the notification was never observed before its scheduled_at: the host took more than the lead of %s to create it and read it once", scheduleLead)
	}

	e.waitStatus(id, domain.StatusSent)
	n := e.get(id)
	if n.SentAt != nil {
		t.Logf("observed pending %d times before its scheduled_at; sent %s after it (bound %s)", roundsBefore, n.SentAt.Sub(when), scheduleLateBound)
	}
	if n.ScheduledAt == nil || !n.ScheduledAt.Equal(when) || !n.DueAt.Equal(when) {
		t.Fatalf("scheduled_at %v, due_at %v; want both %v", n.ScheduledAt, n.DueAt, when)
	}
	if n.QueuedAt == nil || n.QueuedAt.Before(when) {
		t.Errorf("queued_at %v is before scheduled_at %v", n.QueuedAt, when)
	}
	if n.SentAt == nil || n.SentAt.Before(when) || n.SentAt.Sub(when) > scheduleLateBound {
		t.Errorf("sent_at %v, want from scheduled_at %v to %s after it", n.SentAt, when, scheduleLateBound)
	}
	if a := e.attempts(id); len(a) != 1 || a[0].StartedAt.Before(when) || a[0].Outcome != domain.OutcomeSuccess {
		t.Errorf("attempts %+v, want one success started at or after %v", a, when)
	}
	if got := keys(e.prov.requests()); len(got) != 1 || got[id.String()] != 1 {
		t.Errorf("provider requests by key = %v, want one for %s", got, id)
	}
	if !e.queueEmpty(w) {
		t.Error("the dispatch queue is not empty")
	}
}

// A scheduled notification that was cancelled is not published when its
// time has come. A second notification, due just after the first, shows that
// the relay has passed the first one's time: the relay publishes in due
// order, so once the second is sent, the first would have been published
// too.
//
// Nothing here races the clock. The relay starts only after the database's
// clock has passed the time of both notifications, and the test waits for
// that. Until then nothing can publish the first one, so it is pending when
// it is cancelled however slow the host is, and it is due by the clock,
// cancelled, at the relay's first look. (With the relay running from the
// start, the cancel had to arrive within the lead, and a host that stalled
// for longer failed the test with a 409.) That a scheduled notification
// stays pending while the relay runs before its time is the test above.
func TestE2E_ScheduledCancelledIsNeverSent(t *testing.T) {
	e := newEnv(t)
	cfg := e.config(nil)
	_, w := e.startWorker(cfg, 0)
	srv := e.apiServer(cfg)

	when := e.dbNow().Add(scheduleCancelLead).Truncate(time.Microsecond).UTC()
	cancelled := e.createScheduled(srv, when)
	status, body := e.call(srv, http.MethodPost, "/v1/notifications/"+cancelled.String()+"/cancel", "")
	if status != http.StatusOK {
		t.Fatalf("cancel: status %d", status)
	}
	var mayHaveBeenSent bool
	if err := json.Unmarshal(body["may_have_been_sent"], &mayHaveBeenSent); err != nil || mayHaveBeenSent {
		t.Errorf("may_have_been_sent %s", body["may_have_been_sent"])
	}
	witnessAt := when.Add(time.Millisecond)
	witness := e.createScheduled(srv, witnessAt)

	// Both are due by the database's clock before the relay first looks.
	testutil.Eventually(t, func() bool { return e.dbNow().After(witnessAt) }, waitTimeout)
	e.startRelay(cfg)

	e.waitStatus(witness, domain.StatusSent)
	n := e.get(cancelled)
	if n.Status != domain.StatusCancelled || n.AttemptCount != 0 || n.QueuedAt != nil {
		t.Errorf("cancelled notification: status %s, attempt_count %d, queued_at %v", n.Status, n.AttemptCount, n.QueuedAt)
	}
	if a := e.attempts(cancelled); len(a) != 0 {
		t.Errorf("attempts of the cancelled notification: %+v", a)
	}
	if got := keys(e.prov.requests()); len(got) != 1 || got[witness.String()] != 1 {
		t.Errorf("provider requests by key = %v, want one for the witness %s", got, witness)
	}
	if !e.queueEmpty(w) {
		t.Error("the dispatch queue is not empty")
	}
}

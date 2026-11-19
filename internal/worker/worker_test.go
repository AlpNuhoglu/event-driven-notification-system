package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/ratewindow"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/store"
)

func delayFor(d time.Duration, ids ...uuid.UUID) func(uuid.UUID) time.Duration {
	return func(id uuid.UUID) time.Duration {
		if len(ids) == 0 || slices.Contains(ids, id) {
			return d
		}
		return 0
	}
}

func notApplied(ids ...uuid.UUID) func(uuid.UUID) error {
	return func(id uuid.UUID) error {
		if slices.Contains(ids, id) {
			return store.ErrNotApplied
		}
		return nil
	}
}

func assertTimes(t *testing.T, what string, got []time.Time, want ...time.Time) {
	t.Helper()
	if !slices.EqualFunc(got, want, time.Time.Equal) {
		t.Errorf("%s at %v, want %v", what, got, want)
	}
}

func sendTimes(h *harness) []time.Time {
	var ts []time.Time
	for _, s := range h.prov.sent() {
		ts = append(ts, s.at)
	}
	return ts
}

// Test 1: the missed-slot rule (D77).
func TestMissedSlot(t *testing.T) {
	t.Run("claim returns within the tolerance: send at once", func(t *testing.T) {
		h := newHarness(t)
		h.lim.setScript(reserveAnswer{wait: 20 * ms, ok: true})
		h.st.claimDelay = delayFor(23 * ms)
		h.push(prioNormal)
		h.start()

		h.waitBlocked(1) // the claim
		h.clk.Advance(23 * ms)
		h.eventually(func() bool { return len(h.prov.sent()) == 1 })

		assertTimes(t, "send", sendTimes(h), t0.Add(23*ms))
		assertTimes(t, "reserve", h.lim.callTimes(), t0)
		if n := h.w.MissedSlots(); n != 0 {
			t.Errorf("missed slots = %d, want 0", n)
		}
	})

	t.Run("claim returns after the tolerance: reserve again, send at the new slot", func(t *testing.T) {
		h := newHarness(t)
		h.lim.setScript(reserveAnswer{wait: 20 * ms, ok: true}, reserveAnswer{wait: 7 * ms, ok: true})
		h.st.claimDelay = delayFor(30 * ms)
		h.push(prioNormal)
		h.start()

		h.waitBlocked(1) // the claim
		h.clk.Advance(30 * ms)
		h.eventually(func() bool { return h.clk.Waiters() == 1 || len(h.prov.sent()) > 0 })
		if s := sendTimes(h); len(s) > 0 {
			t.Fatalf("sent at %v when the claim returned after slot + tolerance", s)
		}
		assertTimes(t, "reserve", h.lim.callTimes(), t0, t0.Add(30*ms))

		h.clk.Advance(7 * ms)
		h.eventually(func() bool { return len(h.prov.sent()) == 1 })
		assertTimes(t, "send", sendTimes(h), t0.Add(37*ms))
		if n := h.w.MissedSlots(); n != 1 {
			t.Errorf("missed slots = %d, want 1", n)
		}
	})
}

// Test 2: the send deadline (D77): lease 30 s - HTTP timeout 10 s - 5 s = 15 s.
func TestSendDeadline(t *testing.T) {
	tests := []struct {
		name        string
		script      []reserveAnswer
		claimDelay  time.Duration
		wantReserve int
	}{
		{
			name:        "claim returns after the deadline",
			script:      []reserveAnswer{{wait: 20 * ms, ok: true}},
			claimDelay:  16 * time.Second,
			wantReserve: 1, // no re-reservation once the deadline has passed
		},
		{
			name:        "re-reserved slot lands after the deadline",
			script:      []reserveAnswer{{wait: 20 * ms, ok: true}, {wait: 15 * time.Second, ok: true}},
			claimDelay:  30 * ms,
			wantReserve: 2,
		},
		{
			name:        "retry after the horizon would pass the deadline",
			script:      []reserveAnswer{{wait: 20 * ms, ok: true}, {ok: false}},
			claimDelay:  14960 * ms,
			wantReserve: 2,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newHarness(t)
			h.lim.setScript(tt.script...)
			h.st.claimDelay = delayFor(tt.claimDelay)
			h.push(prioNormal)
			h.start()

			h.waitBlocked(1) // the claim
			h.clk.Advance(tt.claimDelay)
			h.eventually(func() bool { return h.st.finals() == 1 || h.clk.Waiters() == 1 })
			if h.st.finals() == 0 {
				t.Fatal("waiting for a slot after the send deadline instead of aborting")
			}

			_, sent, failures, aborts := h.st.snapshot()
			if len(sent)+len(failures) != 0 || len(h.prov.sent()) != 0 {
				t.Fatalf("provider called %d times, %d sent, %d failures; want nothing sent",
					len(h.prov.sent()), len(sent), len(failures))
			}
			if len(aborts) != 1 || aborts[0].delay != 0 || aborts[0].attemptNo != 3 || !aborts[0].at.Equal(t0.Add(tt.claimDelay)) {
				t.Errorf("aborts = %+v, want one with delay 0, attempt 3, at claim return", aborts)
			}
			if n := len(h.lim.callTimes()); n != tt.wantReserve {
				t.Errorf("Reserve called %d times, want %d", n, tt.wantReserve)
			}
		})
	}
}

// Test 3: a slot is kept for the next message after a claim that leaves it
// unused, unless it is stale or the buffer is empty.
func TestKeptSlot(t *testing.T) {
	for _, tc := range []struct {
		name    string
		prepare func(h *harness, first uuid.UUID)
	}{
		{"zero-row claim", func(h *harness, first uuid.UUID) { h.st.claimErr = notApplied(first) }},
		{"expired at claim", func(h *harness, first uuid.UUID) {
			h.st.expired = func(id uuid.UUID) bool { return id == first }
		}},
	} {
		t.Run(tc.name+": next message sent at the same slot", func(t *testing.T) {
			h := newHarness(t)
			h.lim.setScript(reserveAnswer{wait: 20 * ms, ok: true})
			m1 := h.push(prioHigh)
			m2 := h.push(prioNormal)
			tc.prepare(h, m1.id)
			h.start()

			h.waitBlocked(1) // m2's dispatch waits for the kept slot
			h.clk.Advance(20 * ms)
			h.eventually(func() bool { return h.st.finals() == 1 })

			claims, _, _, _ := h.st.snapshot()
			if len(claims) != 2 || claims[0].id != m1.id || claims[1].id != m2.id {
				t.Fatalf("claims = %v, want m1 then m2", claims)
			}
			sends := h.prov.sent()
			if len(sends) != 1 || sends[0].req.NotificationID != m2.id || !sends[0].at.Equal(t0.Add(20*ms)) {
				t.Errorf("sends = %v, want m2 at the first slot", sends)
			}
			if n := len(h.lim.callTimes()); n != 1 {
				t.Errorf("Reserve called %d times, want 1", n)
			}
			if m1.acks.Load() != 1 || m2.acks.Load() != 1 || m1.nacks.Load()+m2.nacks.Load() != 0 {
				t.Errorf("acks %d, %d; nacks %d, %d; want one ack each", m1.acks.Load(), m2.acks.Load(), m1.nacks.Load(), m2.nacks.Load())
			}
		})
	}

	t.Run("expired at claim emits its event", func(t *testing.T) {
		h := newHarness(t)
		m1 := h.push(prioNormal)
		h.st.expired = func(uuid.UUID) bool { return true }
		h.start()
		h.eventually(func() bool { return m1.acks.Load() == 1 && len(h.pub.published()) == 1 })
		if ev := h.pub.published()[0]; ev.ID != m1.id || ev.Status != domain.StatusFailed {
			t.Errorf("event = %+v, want failed for m1", ev)
		}
	})

	t.Run("stale kept slot is dropped", func(t *testing.T) {
		h := newHarness(t)
		h.lim.setScript(reserveAnswer{wait: 20 * ms, ok: true}) // then: a slot at once
		m1 := h.push(prioHigh)
		m2 := h.push(prioNormal)
		h.st.claimDelay = delayFor(30*ms, m1.id)
		h.st.claimErr = notApplied(m1.id)
		h.start()

		h.waitBlocked(1) // m1's claim
		h.clk.Advance(30 * ms)
		h.eventually(func() bool { return len(h.prov.sent()) == 1 })

		claimM2 := "claim " + m2.id.String()
		want := []string{"allow", "reserve", "claim " + m1.id.String(), "allow", "reserve", claimM2}
		if got := h.tr.get(); !slices.Equal(got[:len(want)], want) {
			t.Errorf("calls = %v, want prefix %v", got, want)
		}
		assertTimes(t, "send", sendTimes(h), t0.Add(30*ms))
		if n := h.w.MissedSlots(); n != 1 {
			t.Errorf("missed slots = %d, want 1", n)
		}
	})

	t.Run("empty buffer: the dispatch ends without a send", func(t *testing.T) {
		h := newHarness(t)
		h.lim.setScript(reserveAnswer{wait: 20 * ms, ok: true})
		m1 := h.push(prioNormal)
		h.st.claimErr = notApplied(m1.id)
		h.start()
		h.eventually(func() bool { return m1.acks.Load() == 1 })
		h.stop() // returns once the dispatch has ended

		if n := len(h.prov.sent()); n != 0 {
			t.Errorf("provider called %d times, want 0", n)
		}
		if n := len(h.lim.callTimes()); n != 1 {
			t.Errorf("Reserve called %d times, want 1", n)
		}
	})
}

// Test 4: the highest priority is taken; arrival order within a priority.
func TestHighestPriorityTaken(t *testing.T) {
	h := newHarness(t)
	low1 := h.push(prioLow)
	high1 := h.push(prioHigh)
	normal1 := h.push(prioNormal)
	high2 := h.push(prioHigh)
	low2 := h.push(prioLow)
	h.start()
	h.eventually(func() bool { return len(h.prov.sent()) == 5 })

	want := []uuid.UUID{high1.id, high2.id, normal1.id, low1.id, low2.id}
	claims, _, _, _ := h.st.snapshot()
	var got []uuid.UUID
	for _, c := range claims {
		got = append(got, c.id)
	}
	if !slices.Equal(got, want) {
		t.Errorf("claim order = %v, want %v", got, want)
	}
}

// Test 5: a slot is reserved only when a message waits; the breaker is asked
// before the limiter; not allowed means no Reserve and no claim.
func TestBreakerBeforeLimiter(t *testing.T) {
	h := newHarness(t)
	h.br.answers = []time.Time{t0.Add(100 * ms)}
	h.start()
	m := h.push(prioNormal)

	h.waitBlocked(1) // the pacer waits for the breaker
	if n := len(h.lim.callTimes()); n != 0 {
		t.Fatalf("Reserve called %d times while the breaker was open", n)
	}
	if claims, _, _, _ := h.st.snapshot(); len(claims) != 0 {
		t.Fatalf("claimed %d while the breaker was open", len(claims))
	}

	h.clk.Advance(100 * ms)
	h.eventually(func() bool { return h.st.finals() == 1 })
	h.stop()

	want := []string{"allow", "allow", "reserve", "claim " + m.id.String(), "allow", "send " + m.id.String()}
	if got := h.tr.get(); !slices.Equal(got, want) {
		t.Errorf("calls = %v, want %v", got, want)
	}
	assertTimes(t, "reserve", h.lim.callTimes(), t0.Add(100*ms))
}

// Test 6: beyond the horizon (or on a limiter error) nothing is claimed and
// the pacer asks again after the retry interval.
func TestHorizon(t *testing.T) {
	for _, first := range []reserveAnswer{{ok: false}, {err: errors.New("redis unavailable")}} {
		t.Run(fmt.Sprintf("ok=%v err=%v", first.ok, first.err), func(t *testing.T) {
			h := newHarness(t)
			h.lim.setScript(first) // then: a slot at once
			h.push(prioNormal)
			h.start()

			h.waitBlocked(1) // the pacer waits for the retry interval
			if claims, _, _, _ := h.st.snapshot(); len(claims) != 0 {
				t.Fatalf("claimed %d without a slot", len(claims))
			}
			h.clk.Advance(50 * ms)
			h.eventually(func() bool { return len(h.prov.sent()) == 1 })

			assertTimes(t, "reserve", h.lim.callTimes(), t0, t0.Add(50*ms))
			claims, _, _, _ := h.st.snapshot()
			assertTimes(t, "claim", []time.Time{claims[0].at}, t0.Add(50*ms))
		})
	}
}

// Test 7: never more than Concurrency dispatches at once.
func TestConcurrencyBound(t *testing.T) {
	h := newHarness(t)
	h.cfg.Concurrency = 3
	h.build()
	h.lim.interval, h.lim.horizon = 10*ms, 250*ms
	h.prov.latency = 100 * ms
	for range 30 {
		h.push(prioNormal)
	}
	h.start()
	h.runUntil(t0.Add(500 * ms))

	if n := len(h.prov.sent()); n < 10 {
		t.Fatalf("only %d sends", n)
	}
	h.st.mu.Lock()
	claimed := h.st.maxActive
	h.st.mu.Unlock()
	h.prov.mu.Lock()
	sending := h.prov.maxActive
	h.prov.mu.Unlock()
	if claimed != 3 || sending != 3 {
		t.Errorf("at most %d claimed and %d sending at once, want 3 and 3", claimed, sending)
	}
}

// Test 8: pacing at full rate. Every send starts in [slot, slot + tolerance]
// of a distinct slot the limiter granted, and no one-second window holds
// more than 100 sends, also when a share of the claims miss their slots.
func TestPacingAtFullRate(t *testing.T) {
	for _, slowEvery := range []int{0, 5} {
		t.Run(fmt.Sprintf("every %dth claim slow", slowEvery), func(t *testing.T) {
			h := newHarness(t)
			h.cfg.Concurrency = 25
			h.build()
			h.lim.interval = (time.Second + 97) / 98 // 98 per second, rounded up
			h.lim.horizon = 250 * ms
			var slow []uuid.UUID
			for i := range 300 {
				m := h.push(prioNormal)
				if slowEvery > 0 && i%slowEvery == 0 {
					slow = append(slow, m.id)
				}
			}
			if slowEvery > 0 {
				// Slots lie at most 250 ms ahead, so a 300 ms claim misses its slot.
				h.st.claimDelay = delayFor(300*ms, slow...)
			}
			h.start()
			h.runUntil(t0.Add(1500 * ms))

			sends := sendTimes(h)
			unused := h.lim.granted()
			for _, at := range sends {
				i := slices.IndexFunc(unused, func(s time.Time) bool {
					return !at.Before(s) && !at.After(s.Add(h.cfg.SlotTolerance))
				})
				if i < 0 {
					t.Fatalf("send at +%v is not within the tolerance of an unused granted slot", at.Sub(t0))
				}
				unused = slices.Delete(unused, i, i+1)
			}
			busiest := ratewindow.MaxInWindow(sends, time.Second)
			if busiest > 100 {
				t.Errorf("%d sends in one second, want at most 100", busiest)
			}
			missed := h.w.MissedSlots()
			t.Logf("%d sends, busiest second %d, missed slots %d", len(sends), busiest, missed)
			if slowEvery == 0 && (busiest != 98 || missed != 0) {
				t.Errorf("busiest second %d, missed %d; want 98 and 0", busiest, missed)
			}
			if slowEvery > 0 && missed == 0 {
				t.Error("no slot was missed")
			}
		})
	}
}

// Test 9: breaker closed at claim time, open at the slot.
func TestBreakerOpenAtSlot(t *testing.T) {
	h := newHarness(t)
	until := t0.Add(20*ms + 3*time.Second)
	h.br.answers = []time.Time{{}, until}
	h.lim.setScript(reserveAnswer{wait: 20 * ms, ok: true})
	h.push(prioNormal)
	h.start()

	h.waitBlocked(1) // the dispatch waits for its slot
	h.clk.Advance(20 * ms)
	h.eventually(func() bool { return h.st.finals() == 1 })

	_, _, _, aborts := h.st.snapshot()
	if len(aborts) != 1 || aborts[0].delay != 3*time.Second {
		t.Errorf("aborts = %+v, want one with delay 3s", aborts)
	}
	if n := len(h.prov.sent()); n != 0 {
		t.Errorf("provider called %d times, want 0", n)
	}
	if n := len(h.pub.published()); n != 2 {
		t.Errorf("%d events, want 2 (processing, abort)", n)
	}
}

// Test 10: outcomes of the send and the final write.
func TestOutcomes(t *testing.T) {
	ptr := func(s string) *string { return &s }
	tests := []struct {
		name      string
		respond   func(Request) Response
		finalErr  error
		wantSent  bool
		outcome   domain.Outcome
		status    int // 0: no status recorded
		messageID *string
		hasError  bool
		events    int
	}{
		{
			"2xx", func(Request) Response { return Response{StatusCode: 202, MessageID: "pm-42"} },
			nil, true, domain.OutcomeSuccess, 202, ptr("pm-42"), false, 2,
		},
		{
			"other status", func(Request) Response { return Response{StatusCode: 500} },
			nil, false, domain.OutcomeRetryable, 500, nil, false, 2,
		},
		{
			"no response, request written", func(Request) Response {
				return Response{Err: context.DeadlineExceeded, WroteRequest: true}
			},
			nil, false, domain.OutcomeUnknown, 0, nil, true, 2,
		},
		{
			"no response, request not written", func(Request) Response { return Response{Err: context.DeadlineExceeded} },
			nil, false, domain.OutcomeUnreachable, 0, nil, true, 2,
		},
		{
			"404", func(Request) Response { return Response{StatusCode: 404, WroteRequest: true} },
			nil, false, domain.OutcomeProviderFatal, 404, nil, false, 2,
		},
		{
			"400", func(Request) Response { return Response{StatusCode: 400, WroteRequest: true} },
			nil, false, domain.OutcomePermanent, 400, nil, false, 2,
		},
		{
			"redirect", func(Request) Response { return Response{StatusCode: 302, WroteRequest: true} },
			nil, false, domain.OutcomeProviderFatal, 302, nil, false, 2,
		},
		{
			"429", func(Request) Response { return Response{StatusCode: 429, WroteRequest: true} },
			nil, false, domain.OutcomeThrottled, 429, nil, false, 2,
		},
		{
			"panic in Send", func(Request) Response { panic("provider client bug") },
			nil, false, domain.OutcomeInternalError, 0, nil, true, 2,
		},
		{
			"final write not applied", func(Request) Response { return Response{StatusCode: 202, MessageID: "pm-42"} },
			store.ErrNotApplied, true, domain.OutcomeSuccess, 202, ptr("pm-42"), false, 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newHarness(t)
			h.prov.respond = tt.respond
			h.st.finalErr = tt.finalErr
			h.push(prioNormal)
			h.start()
			h.eventually(func() bool { return h.st.finals() == 1 })
			h.stop()

			_, sent, failures, _ := h.st.snapshot()
			var rec finalRecord
			switch {
			case tt.wantSent && len(sent) == 1:
				rec = sent[0]
				if rec.provider != "fake" {
					t.Errorf("provider = %q", rec.provider)
				}
			case !tt.wantSent && len(failures) == 1:
				rec = failures[0]
				if rec.jitter != 0.25 {
					t.Errorf("jitter = %v, want the injected 0.25", rec.jitter)
				}
			default:
				t.Fatalf("sent %d, failures %d", len(sent), len(failures))
			}
			r := rec.result
			if r.Outcome != tt.outcome || rec.attemptNo != 3 {
				t.Errorf("outcome %q attempt %d, want %q attempt 3", r.Outcome, rec.attemptNo, tt.outcome)
			}
			if (r.HTTPStatus == nil) != (tt.status == 0) || (r.HTTPStatus != nil && *r.HTTPStatus != tt.status) {
				t.Errorf("HTTP status %v, want %d", r.HTTPStatus, tt.status)
			}
			if (r.ProviderMessageID == nil) != (tt.messageID == nil) ||
				(r.ProviderMessageID != nil && *r.ProviderMessageID != *tt.messageID) {
				t.Errorf("provider message id %v, want %v", r.ProviderMessageID, tt.messageID)
			}
			if (r.Error != nil) != tt.hasError {
				t.Errorf("error %v, want present: %v", r.Error, tt.hasError)
			}
			if n := len(h.pub.published()); n != tt.events {
				t.Errorf("%d events, want %d", n, tt.events)
			}
			if tt.finalErr != nil && strings.Contains(h.logs.String(), `"level":"ERROR"`) {
				t.Errorf("a final write that was not applied logged an error:\n%s", h.logs)
			}
		})
	}
}

// The Retry-After header becomes the retry hint only on 429 and 503, and
// only when it is usable; an HTTP-date is measured on the worker's clock.
func TestRetryAfterHint(t *testing.T) {
	tests := []struct {
		name   string
		status int
		header func(now time.Time) string
		want   time.Duration
	}{
		{"429 delta-seconds", 429, func(time.Time) string { return "7" }, 7 * time.Second},
		{"503 delta-seconds", 503, func(time.Time) string { return "120" }, 2 * time.Minute},
		{"429 HTTP-date", 429, func(now time.Time) string {
			return now.Add(90 * time.Second).UTC().Format("Mon, 02 Jan 2006 15:04:05 GMT")
		}, 90 * time.Second},
		{"429 without the header", 429, func(time.Time) string { return "" }, 0},
		{"429 zero", 429, func(time.Time) string { return "0" }, 0},
		{"429 date in the past", 429, func(now time.Time) string {
			return now.Add(-time.Minute).UTC().Format("Mon, 02 Jan 2006 15:04:05 GMT")
		}, 0},
		{"429 garbage", 429, func(time.Time) string { return "soon" }, 0},
		{"500 ignores the header", 500, func(time.Time) string { return "7" }, 0},
		{"502 ignores the header", 502, func(time.Time) string { return "7" }, 0},
		{"301 ignores the header", 301, func(time.Time) string { return "7" }, 0},
		{"404 ignores the header", 404, func(time.Time) string { return "7" }, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newHarness(t)
			// Whole seconds, so that an HTTP-date round-trips exactly.
			now := h.clk.Now().Truncate(time.Second)
			h.prov.respond = func(Request) Response {
				return Response{StatusCode: tt.status, WroteRequest: true, RetryAfter: tt.header(now)}
			}
			h.push(prioNormal)
			h.start()
			h.eventually(func() bool { return h.st.finals() == 1 })
			h.stop()

			_, _, failures, _ := h.st.snapshot()
			if len(failures) != 1 {
				t.Fatalf("%d failures recorded, want 1", len(failures))
			}
			got := failures[0].result.RetryAfter
			// The fake clock may have advanced past `now` by the slot wait,
			// which shortens a date-based hint by less than a second.
			if tt.want == 0 && got != 0 {
				t.Errorf("retry hint = %s, want none", got)
			}
			if tt.want != 0 && (got > tt.want || got <= tt.want-time.Second) {
				t.Errorf("retry hint = %s, want in (%s, %s]", got, tt.want-time.Second, tt.want)
			}
		})
	}
}

// The attempt result carries what the provider call produced: the HTTP
// status, the exact error text, the latency and the hint, and nothing it did
// not produce. TestOutcomes checks the outcomes; this checks the values.
func TestAttemptResultValues(t *testing.T) {
	ptr := func(s string) *string { return &s }
	tests := []struct {
		name    string
		res     Response
		panics  bool
		outcome domain.Outcome
		status  int // 0: no status recorded
		errText *string
		hint    time.Duration
	}{
		{
			name:    "no response, not written: unreachable with the transport error",
			res:     Response{Err: errors.New("dial tcp 127.0.0.1:1: connect: connection refused"), Latency: 3 * ms},
			outcome: domain.OutcomeUnreachable, errText: ptr("dial tcp 127.0.0.1:1: connect: connection refused"),
		},
		{
			name:    "no response, written: unknown with the transport error",
			res:     Response{Err: errors.New("EOF"), WroteRequest: true, Latency: 9 * ms},
			outcome: domain.OutcomeUnknown, errText: ptr("EOF"),
		},
		{
			name:    "a status wins over WroteRequest false",
			res:     Response{StatusCode: 500, Latency: 4 * ms},
			outcome: domain.OutcomeRetryable, status: 500,
		},
		{
			name:    "503 with an HTTP-date",
			res:     Response{StatusCode: 503, WroteRequest: true, RetryAfter: "Thu, 01 Jan 2026 12:00:30 GMT", Latency: 5 * ms},
			outcome: domain.OutcomeRetryable, status: 503, hint: 30 * time.Second,
		},
		{
			name:    "429 with a header padded by spaces",
			res:     Response{StatusCode: 429, WroteRequest: true, RetryAfter: " 12 ", Latency: 6 * ms},
			outcome: domain.OutcomeThrottled, status: 429, hint: 12 * time.Second,
		},
		{
			name:    "429 with a negative delta",
			res:     Response{StatusCode: 429, WroteRequest: true, RetryAfter: "-5", Latency: 6 * ms},
			outcome: domain.OutcomeThrottled, status: 429,
		},
		{
			name:    "301 carrying Retry-After: no hint",
			res:     Response{StatusCode: 301, WroteRequest: true, RetryAfter: "Thu, 01 Jan 2026 12:00:30 GMT", Latency: 2 * ms},
			outcome: domain.OutcomeProviderFatal, status: 301,
		},
		{
			name:   "panic: internal_error with the panic's type only",
			panics: true, outcome: domain.OutcomeInternalError, errText: ptr("panic: string"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newHarness(t)
			// The fake clock reads t0 = 12:00:00 UTC at the send.
			h.prov.respond = func(Request) Response {
				if tt.panics {
					panic("provider client bug")
				}
				return tt.res
			}
			h.push(prioNormal)
			h.start()
			h.eventually(func() bool { return h.st.finals() == 1 })
			h.stop()

			_, _, failures, _ := h.st.snapshot()
			if len(failures) != 1 {
				t.Fatalf("%d failures recorded, want 1", len(failures))
			}
			r := failures[0].result
			if r.Outcome != tt.outcome {
				t.Errorf("outcome %q, want %q", r.Outcome, tt.outcome)
			}
			if (r.HTTPStatus == nil) != (tt.status == 0) || (r.HTTPStatus != nil && *r.HTTPStatus != tt.status) {
				t.Errorf("HTTP status %v, want %d", r.HTTPStatus, tt.status)
			}
			if (r.Error == nil) != (tt.errText == nil) || (r.Error != nil && *r.Error != *tt.errText) {
				t.Errorf("error %v, want %v", r.Error, tt.errText)
			}
			if r.RetryAfter != tt.hint {
				t.Errorf("retry hint %s, want %s", r.RetryAfter, tt.hint)
			}
			if r.Latency != tt.res.Latency {
				t.Errorf("latency %s, want %s", r.Latency, tt.res.Latency)
			}
			if r.ProviderMessageID != nil {
				t.Errorf("provider message id %q on a failure", *r.ProviderMessageID)
			}
		})
	}
}

// Test 11: shutdown.
func TestShutdown(t *testing.T) {
	t.Run("buffered messages are nacked, a claimed dispatch sends", func(t *testing.T) {
		h := newHarness(t)
		h.lim.setScript(reserveAnswer{wait: 20 * ms, ok: true})
		m1 := h.push(prioHigh)
		m2 := h.push(prioNormal)
		m3 := h.push(prioNormal)
		h.start()
		h.waitBlocked(1) // m1's dispatch waits for its slot

		h.cancel()
		h.eventually(func() bool { return m2.nacks.Load() == 1 && m3.nacks.Load() == 1 })
		h.waitBlocked(2) // m1's dispatch and the drain timer
		if h.returned() {
			t.Fatal("Run returned before its dispatch ended")
		}
		late := h.push(prioHigh)
		if late.nacks.Load() != 1 {
			t.Error("a message pushed after shutdown was not nacked")
		}

		h.clk.Advance(20 * ms)
		h.eventually(h.returned)
		assertTimes(t, "send", sendTimes(h), t0.Add(20*ms))
		if m1.acks.Load() != 1 || m1.nacks.Load() != 0 || m2.acks.Load()+m3.acks.Load()+late.acks.Load() != 0 {
			t.Error("wrong acks: m1 must be acked only, the others never")
		}
	})

	t.Run("drain ends before the send: the attempt is aborted", func(t *testing.T) {
		h := newHarness(t)
		h.cfg.Drain = 100 * ms
		h.build()
		h.lim.setScript(reserveAnswer{wait: 200 * ms, ok: true})
		h.push(prioNormal)
		h.start()
		h.waitBlocked(1)

		h.cancel()
		h.waitBlocked(2)
		h.clk.Advance(100 * ms)
		h.eventually(h.returned)

		_, _, _, aborts := h.st.snapshot()
		if len(aborts) != 1 || aborts[0].delay != 0 || len(h.prov.sent()) != 0 {
			t.Errorf("aborts %+v, sends %d; want one abort with delay 0 and no send", aborts, len(h.prov.sent()))
		}
	})
}

// Test 12: the real recipient reaches the provider and nothing else.
func TestRecipientPrivacy(t *testing.T) {
	t.Run("the provider request carries the real recipient", func(t *testing.T) {
		h := newHarness(t)
		m := h.push(prioNormal)
		h.start()
		h.eventually(func() bool { return len(h.prov.sent()) == 1 })
		req := h.prov.sent()[0].req
		if req.To != testRecipient || req.Content != testContent || req.NotificationID != m.id ||
			req.Channel != domain.ChannelSMS || req.TraceParent == "" {
			t.Errorf("request = %#v (To %q)", req, req.To)
		}
	})

	t.Run("no log line or request formatting carries recipient or content", func(t *testing.T) {
		h := newHarness(t)
		h.lim.setScript(reserveAnswer{err: errors.New("redis unavailable")})
		m1 := h.push(prioHigh)
		m2 := h.push(prioNormal)
		h.push(prioLow)
		// m1's first claim fails; it is retried after the back-off (100 ms).
		h.st.claimErr = perCall(nil, map[uuid.UUID][]error{m1.id: {errDatabase}})
		h.st.finalErr = errDatabase
		h.prov.respond = func(req Request) Response {
			if req.NotificationID == m2.id {
				panic(fmt.Sprintf("cannot send %s to %s", req.Content, req.To))
			}
			return Response{StatusCode: 503, Err: errors.New("bad gateway")}
		}
		h.start()
		h.waitBlocked(1) // the pacer waits for the limiter's retry interval
		h.clk.Advance(50 * ms)
		h.waitBlocked(1) // m1's dispatch waits for its claim back-off
		h.clk.Advance(100 * ms)
		h.eventually(func() bool { return h.st.finals() == 3 })
		h.stop()

		req := h.prov.sent()[0].req
		var formatted strings.Builder
		fmt.Fprintf(&formatted, "%v %+v %#v %s\n", req, req, req, req)
		slog.New(slog.NewTextHandler(&formatted, nil)).Info("request", "req", req)

		logs := h.logs.String()
		if !strings.Contains(logs, "claim failed") || !strings.Contains(logs, "claim succeeded after retries") ||
			!strings.Contains(logs, "panic during send") ||
			!strings.Contains(logs, "final write failed") || !strings.Contains(logs, "reservation failed") {
			t.Fatalf("the scenario did not log what it should:\n%s", logs)
		}
		masked := domain.Recipient(testRecipient).Masked()
		for _, out := range []string{logs, formatted.String()} {
			for _, secret := range []string{testRecipient, masked, testContent} {
				if strings.Contains(out, secret) {
					t.Errorf("output contains %q:\n%s", secret, out)
				}
			}
		}
	})
}

func TestNewRejectsNonPositiveSendDeadline(t *testing.T) {
	h := newHarness(t)
	cfg := h.cfg
	cfg.Lease = 15 * time.Second // 15 s - 10 s - 5 s = 0
	if _, err := New(cfg, Deps{}); err == nil || !strings.Contains(err.Error(), "send deadline") {
		t.Errorf("New = %v, want a send deadline error", err)
	}
}

func TestBuffer(t *testing.T) {
	b := newBuffer()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := b.wait(ctx); err == nil {
		t.Error("wait on an empty buffer with a cancelled context returned nil")
	}
	if b.take() != nil {
		t.Error("take from an empty buffer returned a message")
	}
	low := &fakeMessage{id: uuid.New(), prio: prioLow}
	high1 := &fakeMessage{id: uuid.New(), prio: prioHigh}
	high2 := &fakeMessage{id: uuid.New(), prio: prioHigh}
	for _, m := range []*fakeMessage{low, high1, high2} {
		b.push(m)
	}
	if err := b.wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	if b.len() != 3 {
		t.Fatal("wait took a message")
	}
	if b.take() != high1 || b.take() != high2 {
		t.Error("take did not return the high messages in arrival order")
	}
	if left := b.close(); len(left) != 1 || left[0] != low {
		t.Errorf("close returned %v", left)
	}
	if b.push(high1) {
		t.Error("push after close succeeded")
	}
}

func TestSystemClock(t *testing.T) {
	c := SystemClock{}
	if err := c.WaitUntil(context.Background(), c.Now().Add(-time.Second)); err != nil {
		t.Errorf("WaitUntil in the past = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := c.WaitUntil(ctx, c.Now().Add(time.Hour)); !errors.Is(err, context.Canceled) {
		t.Errorf("WaitUntil with a cancelled context = %v", err)
	}
}

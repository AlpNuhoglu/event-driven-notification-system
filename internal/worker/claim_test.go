package worker

import (
	"encoding/json"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/store"
)

// The claim retried in place (open issue 7): a claim that fails with a
// database error keeps its delivery and its concurrency slot and is issued
// again after a back-off; it is nacked once, when shutdown begins.

const (
	msgClaimFailed    = "worker: claim failed; retrying with back-off"
	msgClaimRecovered = "worker: claim succeeded after retries"
	msgClaimAbandoned = "worker: claim still failing at shutdown; message requeued"
)

// logLine is one JSON log line of the harness.
type logLine struct {
	Level string `json:"level"`
	Msg   string `json:"msg"`
	Tries int    `json:"tries"`
}

func logLines(t *testing.T, h *harness) []logLine {
	t.Helper()
	var out []logLine
	for raw := range strings.SplitSeq(strings.TrimSpace(h.logs.String()), "\n") {
		if raw == "" {
			continue
		}
		var l logLine
		if err := json.Unmarshal([]byte(raw), &l); err != nil {
			t.Fatalf("log line %q: %v", raw, err)
		}
		out = append(out, l)
	}
	return out
}

func linesWith(lines []logLine, msg string) []logLine {
	var out []logLine
	for _, l := range lines {
		if l.Msg == msg {
			out = append(out, l)
		}
	}
	return out
}

func failing(n int) []error {
	errs := make([]error, n)
	for i := range errs {
		errs[i] = errDatabase
	}
	return errs
}

func claimTimes(h *harness) []time.Time {
	claims, _, _, _ := h.st.snapshot()
	var ts []time.Time
	for _, c := range claims {
		ts = append(ts, c.at)
	}
	return ts
}

// stepBackoff waits until the waiters goroutines are blocked on the clock,
// advances it to the earliest wait and returns how long that wait was.
func (h *harness) stepBackoff(waiters int) time.Duration {
	h.t.Helper()
	h.waitBlocked(waiters)
	next, ok := h.clk.Next()
	if !ok {
		h.t.Fatal("nothing waits on the clock")
	}
	d := next.Sub(h.clk.Now())
	h.clk.AdvanceTo(next)
	return d
}

// A claim that fails seven times and then succeeds: the back-off doubles from
// 100 ms and is capped at 5 s; the message is sent once; the limiter is asked
// for the first slot and once more after the late claim (the missed-slot
// rule), not once per try; the delivery is acked once and never nacked; one
// warn line and one info line are logged.
func TestClaimRetry_FailsThenSucceeds(t *testing.T) {
	const failures = 7
	h := newHarness(t)
	m := h.push(prioNormal)
	h.st.claimErr = perCall(nil, map[uuid.UUID][]error{m.id: failing(failures)})
	h.start()

	var waits []time.Duration
	for range failures {
		waits = append(waits, h.stepBackoff(1))
	}
	h.eventually(func() bool { return h.st.finals() == 1 })
	h.stop()

	wantWaits := []time.Duration{100 * ms, 200 * ms, 400 * ms, 800 * ms, 1600 * ms, 3200 * ms, 5000 * ms}
	if !slices.Equal(waits, wantWaits) {
		t.Errorf("back-off waits %v, want %v", waits, wantWaits)
	}
	late := t0.Add(11300 * ms) // the sum of the waits
	assertTimes(t, "claim", claimTimes(h),
		t0, t0.Add(100*ms), t0.Add(300*ms), t0.Add(700*ms), t0.Add(1500*ms), t0.Add(3100*ms), t0.Add(6300*ms), late)
	assertTimes(t, "reserve", h.lim.callTimes(), t0, late)
	assertTimes(t, "send", sendTimes(h), late)
	_, sent, _, _ := h.st.snapshot()
	if len(sent) != 1 || sent[0].id != m.id {
		t.Errorf("sent records %+v, want one for the message", sent)
	}
	if m.acks.Load() != 1 || m.nacks.Load() != 0 {
		t.Errorf("acks %d, nacks %d; want 1 and 0", m.acks.Load(), m.nacks.Load())
	}
	if n := h.w.MissedSlots(); n != 1 {
		t.Errorf("missed slots %d, want 1 (the first slot, given up after the late claim)", n)
	}

	lines := logLines(t, h)
	failed, recovered := linesWith(lines, msgClaimFailed), linesWith(lines, msgClaimRecovered)
	if len(failed) != 1 || failed[0].Level != "WARN" {
		t.Errorf("claim-failed lines %+v, want one at WARN", failed)
	}
	if len(recovered) != 1 || recovered[0].Level != "INFO" || recovered[0].Tries != failures+1 {
		t.Errorf("claim-recovered lines %+v, want one at INFO with tries %d", recovered, failures+1)
	}
	for _, l := range lines {
		if l.Level == "ERROR" {
			t.Errorf("error line logged: %+v", l)
		}
	}
}

// The send deadline (15 s with the harness's defaults) is measured from
// issuing the claim call that succeeded: here the second call, issued at
// +100 ms and returning at +130 ms, so the deadline is +15.1 s. A slot at
// +15.08 s is used (measured from the first call it would be past the
// deadline); a slot at +15.12 s is not (measured from the call's return it
// would be before it).
func TestClaimRetry_SendDeadlineFromSuccessfulCall(t *testing.T) {
	for _, tc := range []struct {
		name     string
		wait     time.Duration // of the slot reserved after the late claim
		wantSent bool
	}{
		{"slot before the deadline: sent", 14950 * ms, true},
		{"slot after the deadline: aborted", 14990 * ms, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t)
			m := h.push(prioNormal)
			h.st.claimErr = perCall(nil, map[uuid.UUID][]error{m.id: failing(1)})
			h.st.claimDelay = perCall(0, map[uuid.UUID][]time.Duration{m.id: {0, 30 * ms}})
			h.lim.setScript(reserveAnswer{ok: true}, reserveAnswer{wait: tc.wait, ok: true})
			h.start()

			h.stepBackoff(1) // the back-off, 100 ms
			h.stepBackoff(1) // the second call, 30 ms
			h.eventually(func() bool { return h.st.finals() == 1 || h.clk.Waiters() == 1 })
			if h.st.finals() == 0 {
				h.stepBackoff(1) // the wait for the slot
			}
			h.eventually(func() bool { return h.st.finals() == 1 })

			assertTimes(t, "claim", claimTimes(h), t0, t0.Add(100*ms))
			assertTimes(t, "reserve", h.lim.callTimes(), t0, t0.Add(130*ms))
			_, sent, _, aborts := h.st.snapshot()
			if tc.wantSent {
				assertTimes(t, "send", sendTimes(h), t0.Add(130*ms+tc.wait))
				if len(sent) != 1 || len(aborts) != 0 {
					t.Errorf("sent %d, aborted %d; want 1 and 0", len(sent), len(aborts))
				}
			} else {
				if len(h.prov.sent()) != 0 || len(aborts) != 1 || !aborts[0].at.Equal(t0.Add(130*ms)) {
					t.Errorf("sends %d, aborts %+v; want no send and one abort at the claim's return",
						len(h.prov.sent()), aborts)
				}
			}
			if m.acks.Load() != 1 || m.nacks.Load() != 0 {
				t.Errorf("acks %d, nacks %d; want 1 and 0", m.acks.Load(), m.nacks.Load())
			}
		})
	}
}

// With every concurrency slot held by a dispatch whose claim keeps failing,
// the pacer takes no further message and reserves no slot, however long the
// outage lasts. When the database is back, every message is sent once.
func TestClaimRetry_AllSlotsRetryingStopThePacer(t *testing.T) {
	h := newHarness(t)
	h.cfg.Concurrency = 3
	h.build()
	var down atomic.Bool
	down.Store(true)
	h.st.claimErr = func(uuid.UUID) error {
		if down.Load() {
			return errDatabase
		}
		return nil
	}
	var msgs []*fakeMessage
	for range 5 {
		msgs = append(msgs, h.push(prioNormal))
	}
	h.start()

	h.eventually(func() bool { return h.clk.Waiters() == 3 && len(claimTimes(h)) == 3 })
	// More than a minute of outage on the fake clock: the back-off reaches
	// its cap of 5 s after six waits (6.3 s), then twelve more.
	for range 18 {
		h.stepBackoff(3)
	}
	h.waitBlocked(3)
	elapsed := h.clk.Now().Sub(t0)
	claims, _, _, _ := h.st.snapshot()
	claimed := map[uuid.UUID]int{}
	for _, c := range claims {
		claimed[c.id]++
	}
	t.Logf("outage of %s: %d claim calls", elapsed, len(claims))
	if elapsed < time.Minute {
		t.Fatalf("the outage lasted %s on the fake clock, want at least a minute", elapsed)
	}
	if n := len(h.lim.callTimes()); n != 3 {
		t.Errorf("Reserve called %d times during the outage, want 3 (one per message taken)", n)
	}
	if len(claimed) != 3 || claimed[msgs[3].id]+claimed[msgs[4].id] != 0 {
		t.Errorf("claimed ids %v, want the first three only", claimed)
	}
	if n := h.w.buf.len(); n != 2 {
		t.Errorf("buffer holds %d, want 2", n)
	}
	for i, m := range msgs {
		if m.acks.Load()+m.nacks.Load() != 0 {
			t.Errorf("message %d answered during the outage: %d acks, %d nacks", i, m.acks.Load(), m.nacks.Load())
		}
	}

	down.Store(false)
	h.stepBackoff(3)
	h.eventually(func() bool { return len(h.prov.sent()) == 5 })
	h.stop()
	for i, m := range msgs {
		if m.acks.Load() != 1 || m.nacks.Load() != 0 {
			t.Errorf("message %d: %d acks, %d nacks; want 1 and 0", i, m.acks.Load(), m.nacks.Load())
		}
	}
	// 3 before the outage, 3 after the late claims (missed-slot rule), 2 for
	// the messages that waited in the buffer.
	if n := len(h.lim.callTimes()); n != 8 {
		t.Errorf("Reserve called %d times in all, want 8", n)
	}
	if n := h.w.MissedSlots(); n != 3 {
		t.Errorf("missed slots %d, want 3", n)
	}
}

// Shutdown while the claims wait for their next try: each such delivery is
// nacked once, no claim is issued again, and Run returns at once, without
// waiting for the drain timeout (the clock does not move).
func TestClaimRetry_ShutdownDuringBackoffNacksOnce(t *testing.T) {
	h := newHarness(t)
	h.cfg.Concurrency = 2
	h.build()
	h.st.claimErr = func(uuid.UUID) error { return errDatabase }
	m1, m2, m3 := h.push(prioNormal), h.push(prioNormal), h.push(prioNormal)
	h.start()
	h.stepBackoff(2) // one failed try each, then the first back-off
	h.waitBlocked(2) // the second back-off

	h.cancel()
	h.eventually(h.returned)

	if now := h.clk.Now(); !now.Equal(t0.Add(100 * ms)) {
		t.Errorf("the clock moved to +%s", now.Sub(t0))
	}
	for i, m := range []*fakeMessage{m1, m2, m3} {
		if m.acks.Load() != 0 || m.nacks.Load() != 1 {
			t.Errorf("message %d: %d acks, %d nacks; want 0 and 1", i+1, m.acks.Load(), m.nacks.Load())
		}
	}
	if n := len(claimTimes(h)); n != 4 {
		t.Errorf("%d claim calls, want 4 (two each)", n)
	}
	if n := h.st.finals(); n != 0 {
		t.Errorf("%d final writes, want 0", n)
	}
	abandoned := linesWith(logLines(t, h), msgClaimAbandoned)
	if len(abandoned) != 2 || abandoned[0].Tries != 2 {
		t.Errorf("abandon lines %+v, want two with tries 2", abandoned)
	}
}

// Shutdown while a claim call is in flight, after a failed try: the call is
// allowed to finish. If it fails, the delivery is nacked once. If it
// succeeds, the normal path continues: ack, then the send, or the abort when
// the drain timeout ends before the slot.
func TestClaimRetry_ShutdownDuringClaimCall(t *testing.T) {
	for _, tc := range []struct {
		name    string
		second  error
		drain   time.Duration
		slot    time.Duration // the wait of the slot reserved after the late claim
		outcome string
	}{
		{"call fails: nacked", errDatabase, 15 * time.Second, 0, "nack"},
		{"call succeeds: sent", nil, 15 * time.Second, 0, "send"},
		{"call succeeds, drain ends before the slot: aborted", nil, 100 * ms, time.Second, "abort"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t)
			h.cfg.Drain = tc.drain
			h.build()
			m := h.push(prioNormal)
			h.st.claimErr = perCall(nil, map[uuid.UUID][]error{m.id: {errDatabase, tc.second}})
			h.st.claimDelay = perCall(0, map[uuid.UUID][]time.Duration{m.id: {0, 50 * ms}})
			h.lim.setScript(reserveAnswer{ok: true}, reserveAnswer{wait: tc.slot, ok: true})
			h.start()

			h.stepBackoff(1) // the back-off, 100 ms
			h.waitBlocked(1) // the second call, in flight until +150 ms
			h.cancel()
			h.waitBlocked(2) // and the drain timer
			if m.nacks.Load() != 0 {
				t.Fatal("nacked while the claim call was in flight")
			}
			h.clk.Advance(50 * ms)
			if tc.outcome == "abort" {
				h.waitBlocked(2) // the wait for the slot and the drain timer
				h.clk.AdvanceTo(t0.Add(100*ms + tc.drain))
			}
			h.eventually(h.returned)

			acks, nacks := m.acks.Load(), m.nacks.Load()
			_, sent, _, aborts := h.st.snapshot()
			switch tc.outcome {
			case "nack":
				if acks != 0 || nacks != 1 || h.st.finals() != 0 {
					t.Errorf("acks %d, nacks %d, finals %d; want 0, 1, 0", acks, nacks, h.st.finals())
				}
				if n := len(claimTimes(h)); n != 2 {
					t.Errorf("%d claim calls, want 2", n)
				}
			case "send":
				if acks != 1 || nacks != 0 || len(sent) != 1 {
					t.Errorf("acks %d, nacks %d, sent %d; want 1, 0, 1", acks, nacks, len(sent))
				}
				assertTimes(t, "send", sendTimes(h), t0.Add(150*ms))
			case "abort":
				if acks != 1 || nacks != 0 || len(aborts) != 1 || len(h.prov.sent()) != 0 {
					t.Errorf("acks %d, nacks %d, aborts %d, sends %d; want 1, 0, 1, 0",
						acks, nacks, len(aborts), len(h.prov.sent()))
				}
			}
		})
	}
}

// A claim that matches zero rows after failed tries (the row was cancelled,
// expired or already sent meanwhile) is acked and dropped: no send, no
// final write, no nack, no further reservation.
func TestClaimRetry_NotAppliedAfterFailures(t *testing.T) {
	h := newHarness(t)
	m := h.push(prioNormal)
	h.st.claimErr = perCall(nil, map[uuid.UUID][]error{m.id: {errDatabase, errDatabase, store.ErrNotApplied}})
	h.start()
	h.stepBackoff(1)
	h.stepBackoff(1)
	h.eventually(func() bool { return m.acks.Load() == 1 })
	h.stop()

	assertTimes(t, "claim", claimTimes(h), t0, t0.Add(100*ms), t0.Add(300*ms))
	if m.nacks.Load() != 0 || len(h.prov.sent()) != 0 || h.st.finals() != 0 || len(h.pub.published()) != 0 {
		t.Errorf("nacks %d, sends %d, finals %d, events %d; want all 0",
			m.nacks.Load(), len(h.prov.sent()), h.st.finals(), len(h.pub.published()))
	}
	if n := len(h.lim.callTimes()); n != 1 {
		t.Errorf("Reserve called %d times, want 1", n)
	}
}

func TestNewRejectsClaimRetrySettings(t *testing.T) {
	h := newHarness(t)
	for _, tc := range []struct{ base, maxWait time.Duration }{{0, time.Second}, {2 * time.Second, time.Second}} {
		cfg := h.cfg
		cfg.ClaimRetryBase, cfg.ClaimRetryMax = tc.base, tc.maxWait
		if _, err := New(cfg, Deps{}); err == nil || !strings.Contains(err.Error(), "claim retry") {
			t.Errorf("base %s max %s: New = %v, want a claim retry error", tc.base, tc.maxWait, err)
		}
	}
}

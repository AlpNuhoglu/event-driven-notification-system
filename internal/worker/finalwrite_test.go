package worker

import (
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/store"
)

// The retry of a write after the claim (D18): the final write of an attempt,
// or an abort, is made again with back-off while the database cannot be
// reached, for up to FinalWriteRetry or until the drain timeout of a shutdown
// ends. Deterministic, on the fake clock.

const (
	msgWriteFailed    = "worker: final write failed; retrying with back-off"
	msgWriteRecovered = "worker: final write succeeded after retries"
	msgWriteLost      = "worker: final write lost; the lease sweeper recovers the notification"
	msgAbortRecovered = "worker: abort attempt succeeded after retries"
	msgLateSuccess    = "worker: late success applied: the lease had expired, no newer attempt had started"
)

func writeHarness(t *testing.T, window time.Duration) *harness {
	t.Helper()
	h := newHarness(t)
	h.cfg.FinalWriteRetry = window
	h.build()
	return h
}

func noErrorLines(t *testing.T, lines []logLine) {
	t.Helper()
	for _, l := range lines {
		if l.Level == "ERROR" {
			t.Errorf("error line logged: %+v", l)
		}
	}
}

// A final write that fails five times and then lands: the back-off doubles
// from the claim's base; the provider was called once; the result is
// recorded once and its event emitted once; one warn line and one info line.
func TestFinalWrite_RetriedUntilItLands(t *testing.T) {
	const failures = 5
	h := writeHarness(t, 60*time.Second)
	h.st.finalErrs = failing(failures)
	m := h.push(prioNormal)
	h.start()

	var waits []time.Duration
	for range failures {
		waits = append(waits, h.stepBackoff(1))
	}
	h.eventually(func() bool { return h.st.finals() == 1 })
	h.stop()

	if want := []time.Duration{100 * ms, 200 * ms, 400 * ms, 800 * ms, 1600 * ms}; !slices.Equal(waits, want) {
		t.Errorf("back-off waits %v, want %v", waits, want)
	}
	assertTimes(t, "write", h.st.writeTimes(),
		t0, t0.Add(100*ms), t0.Add(300*ms), t0.Add(700*ms), t0.Add(1500*ms), t0.Add(3100*ms))
	_, sent, failed, aborts := h.st.snapshot()
	if len(sent) != 1 || sent[0].id != m.id || len(failed) != 0 || len(aborts) != 0 {
		t.Errorf("sent %+v, failures %d, aborts %d; want one sent record of the message", sent, len(failed), len(aborts))
	}
	if n := len(h.prov.sent()); n != 1 {
		t.Errorf("provider called %d times, want 1: a retried write must not send again", n)
	}
	if n := len(h.pub.published()); n != 2 {
		t.Errorf("%d events, want 2 (processing, sent)", n)
	}
	lines := logLines(t, h)
	failedLines, recovered := linesWith(lines, msgWriteFailed), linesWith(lines, msgWriteRecovered)
	if len(failedLines) != 1 || failedLines[0].Level != "WARN" {
		t.Errorf("write-failed lines %+v, want one at WARN", failedLines)
	}
	if len(recovered) != 1 || recovered[0].Level != "INFO" || recovered[0].Tries != failures+1 {
		t.Errorf("write-recovered lines %+v, want one at INFO with tries %d", recovered, failures+1)
	}
	noErrorLines(t, lines)
}

// The window bounds the retries: with 2 s, tries are made at 0, 100, 300,
// 700 and 1500 ms; the next one would come at 3100 ms, after the window, so
// the write is given up, logged once at error level, and the dispatch ends.
func TestFinalWrite_GivesUpAfterTheWindow(t *testing.T) {
	h := writeHarness(t, 2*time.Second)
	h.st.finalErrs = failing(100)
	h.push(prioNormal)
	h.start()

	for range 4 {
		h.stepBackoff(1)
	}
	h.eventually(func() bool { return len(linesWith(logLines(t, h), msgWriteLost)) == 1 })
	if now := h.clk.Now(); !now.Equal(t0.Add(1500 * ms)) {
		t.Errorf("given up at +%s, want +1.5s: no wait is started that would end after the window", now.Sub(t0))
	}
	// The dispatch has ended and given its concurrency slot back: a second
	// message is taken and sent.
	h.st.finalErrs = nil
	h.push(prioNormal)
	h.eventually(func() bool { return h.st.finals() == 1 })
	h.stop()

	if writes := h.st.writeTimes(); len(writes) != 6 {
		t.Errorf("%d writes, want 6: five tries of the first message and one of the second", len(writes))
	} else {
		assertTimes(t, "write of the first message", writes[:5],
			t0, t0.Add(100*ms), t0.Add(300*ms), t0.Add(700*ms), t0.Add(1500*ms))
	}
	lost := linesWith(logLines(t, h), msgWriteLost)
	if len(lost) != 1 || lost[0].Level != "ERROR" || lost[0].Tries != 5 {
		t.Errorf("write-lost lines %+v, want one at ERROR with tries 5", lost)
	}
	if n := len(h.pub.published()); n != 3 {
		t.Errorf("%d events, want 3 (processing of both messages, sent of the second)", n)
	}
}

// A window of zero is one try, as before the retry existed.
func TestFinalWrite_ZeroWindowIsOneTry(t *testing.T) {
	h := writeHarness(t, 0)
	h.st.finalErrs = failing(100)
	h.push(prioNormal)
	h.start()
	h.eventually(func() bool { return len(linesWith(logLines(t, h), msgWriteLost)) == 1 })
	h.stop()
	if n := len(h.st.writeTimes()); n != 1 {
		t.Errorf("%d writes, want 1", n)
	}
}

// Shutdown: the retries go on during the drain and end with it. Run does not
// wait for the rest of the window.
func TestFinalWrite_DrainEndStopsTheRetry(t *testing.T) {
	h := newHarness(t)
	h.cfg.FinalWriteRetry = 60 * time.Second
	h.cfg.Drain = 300 * ms
	h.build()
	h.st.finalErrs = failing(100)
	m := h.push(prioNormal)
	h.start()
	h.stepBackoff(1) // the second try, at +100 ms
	h.waitBlocked(1) // waiting for the third, at +300 ms

	h.cancel()       // shutdown begins at +100 ms; the drain ends at +400 ms
	h.waitBlocked(2) // the retry's wait and the drain timer
	h.clk.AdvanceTo(t0.Add(300 * ms))
	h.eventually(func() bool { return len(h.st.writeTimes()) == 3 }) // retried during the drain
	h.waitBlocked(2)
	if h.returned() {
		t.Fatal("Run returned before the drain timeout ended")
	}
	h.clk.AdvanceTo(t0.Add(400 * ms))
	h.eventually(h.returned)

	assertTimes(t, "write", h.st.writeTimes(), t0, t0.Add(100*ms), t0.Add(300*ms))
	lost := linesWith(logLines(t, h), msgWriteLost)
	if len(lost) != 1 || lost[0].Tries != 3 {
		t.Errorf("write-lost lines %+v, want one with tries 3", lost)
	}
	if m.acks.Load() != 1 || m.nacks.Load() != 0 {
		t.Errorf("acks %d, nacks %d; want 1 and 0 (the claim had committed)", m.acks.Load(), m.nacks.Load())
	}
}

// A write that is not applied (a late worker, or the repeat of a write that
// had committed) is not an error and is not retried.
func TestFinalWrite_NotAppliedIsNotRetried(t *testing.T) {
	h := writeHarness(t, 60*time.Second)
	h.st.finalErr = store.ErrNotApplied
	h.push(prioNormal)
	h.start()
	h.eventually(func() bool { return h.st.finals() == 1 })
	h.stop()

	if n := len(h.st.writeTimes()); n != 1 {
		t.Errorf("%d writes, want 1", n)
	}
	lines := logLines(t, h)
	if n := len(linesWith(lines, msgWriteFailed)) + len(linesWith(lines, msgWriteLost)); n != 0 {
		t.Errorf("%d failure lines for a write that was not applied, want 0", n)
	}
	noErrorLines(t, lines)
	if n := len(h.pub.published()); n != 1 {
		t.Errorf("%d events, want 1 (processing only)", n)
	}
}

// An abort (the circuit refused at the slot time) is retried the same way.
func TestAbort_RetriedUntilItLands(t *testing.T) {
	h := writeHarness(t, 60*time.Second)
	h.br.answers = []time.Time{{}, t0.Add(time.Second)}
	h.st.finalErrs = failing(2)
	h.push(prioNormal)
	h.start()
	h.stepBackoff(1)
	h.stepBackoff(1)
	h.eventually(func() bool { return h.st.finals() == 1 })
	h.stop()

	_, sent, failed, aborts := h.st.snapshot()
	if len(aborts) != 1 || len(sent) != 0 || len(failed) != 0 {
		t.Fatalf("aborts %d, sent %d, failures %d; want one abort", len(aborts), len(sent), len(failed))
	}
	// The delay is the time until the circuit can be asked again, as it was
	// when the attempt was aborted; the row's due time is set on the
	// database clock when the write lands.
	if aborts[0].delay != time.Second {
		t.Errorf("abort delay %s, want 1s", aborts[0].delay)
	}
	if n := len(h.prov.sent()); n != 0 {
		t.Errorf("provider called %d times, want 0", n)
	}
	recovered := linesWith(logLines(t, h), msgAbortRecovered)
	if len(recovered) != 1 || recovered[0].Tries != 3 {
		t.Errorf("abort-recovered lines %+v, want one with tries 3", recovered)
	}
}

// A success that the store applied as transition 12 is logged with the
// status the row left, and its event is emitted like any other.
func TestFinalWrite_LateSuccessIsLogged(t *testing.T) {
	h := writeHarness(t, 60*time.Second)
	h.st.late = store.Sent{Late: true, From: domain.StatusRetrying}
	h.push(prioNormal)
	h.start()
	h.eventually(func() bool { return h.st.finals() == 1 })
	h.stop()

	late := linesWith(logLines(t, h), msgLateSuccess)
	if len(late) != 1 || late[0].Level != "INFO" {
		t.Fatalf("late-success lines %+v, want one at INFO", late)
	}
	if logs := h.logs.String(); !strings.Contains(logs, `"from":"retrying"`) || !strings.Contains(logs, `"attempt_no":3`) {
		t.Errorf("the late-success line lacks from or attempt_no:\n%s", logs)
	}
	if n := len(h.pub.published()); n != 2 {
		t.Errorf("%d events, want 2 (processing, sent)", n)
	}
}

func TestNewRejectsNegativeFinalWriteRetry(t *testing.T) {
	h := newHarness(t)
	h.cfg.FinalWriteRetry = -time.Second
	if _, err := New(h.cfg, Deps{}); err == nil {
		t.Error("a negative final-write retry window was accepted")
	}
}

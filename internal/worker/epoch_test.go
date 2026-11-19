package worker

import (
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

// The connection epoch in the pacer (D76). A fakeMessage marked stale models
// a delivery whose connection was reset after it arrived: its Ack and Nack
// send nothing (the check is part of the answer, as in internal/queue) and
// are counted as refused.

const (
	msgAckStale  = "worker: ack not sent: the delivery is from an older connection epoch"
	msgNackStale = "worker: nack not sent: the delivery is from an older connection epoch"
	msgDropped   = "worker: deliveries from an older connection epoch dropped from the buffer"
)

func claimed(h *harness) []uuid.UUID {
	claims, _, _, _ := h.st.snapshot()
	var ids []uuid.UUID
	for _, c := range claims {
		ids = append(ids, c.id)
	}
	return ids
}

// The connection is reset while the claim is in flight: the claim commits,
// the ack is not sent, and the send goes ahead (the claim stands).
func TestEpoch_StaleAtTheAck(t *testing.T) {
	h := newHarness(t)
	m := h.push(prioNormal)
	h.st.claimDelay = func(uuid.UUID) time.Duration { return time.Second }
	h.start()
	h.waitBlocked(1) // the claim waits on the clock
	m.stale.Store(true)
	h.clk.Advance(time.Second)
	h.eventually(func() bool { return h.st.finals() == 1 })
	h.stop()

	if m.acks.Load() != 0 || m.nacks.Load() != 0 || m.refused.Load() != 1 {
		t.Errorf("acks %d, nacks %d, refused %d; want 0, 0, 1", m.acks.Load(), m.nacks.Load(), m.refused.Load())
	}
	_, sent, _, _ := h.st.snapshot()
	if len(sent) != 1 || sent[0].id != m.id || len(h.prov.sent()) != 1 {
		t.Errorf("sent records %+v, provider sends %d; want the message sent once", sent, len(h.prov.sent()))
	}
	lines := logLines(t, h)
	if l := linesWith(lines, msgAckStale); len(l) != 1 || l[0].Level != "INFO" {
		t.Errorf("stale-ack lines %+v, want one at INFO", l)
	}
	for _, l := range lines {
		if l.Level == "ERROR" {
			t.Errorf("error line logged: %+v", l)
		}
	}
}

// A delivery held through a failing claim becomes stale, and shutdown ends
// the claim retry: the nack is not sent.
func TestEpoch_StaleAtTheNack(t *testing.T) {
	h := newHarness(t)
	m := h.push(prioNormal)
	h.st.claimErr = func(uuid.UUID) error { return errDatabase }
	h.start()
	h.stepBackoff(1)
	h.waitBlocked(1) // the second back-off
	m.stale.Store(true)
	h.stop()

	if m.acks.Load() != 0 || m.nacks.Load() != 0 || m.refused.Load() != 1 {
		t.Errorf("acks %d, nacks %d, refused %d; want 0, 0, 1", m.acks.Load(), m.nacks.Load(), m.refused.Load())
	}
	if n := len(h.prov.sent()); n != 0 {
		t.Errorf("%d sends, want none", n)
	}
	if l := linesWith(logLines(t, h), msgNackStale); len(l) != 1 {
		t.Errorf("stale-nack lines %+v, want one", l)
	}
}

// Stale deliveries in the buffer: DropStale removes them without an answer,
// the pacer never takes one that turned stale after the purge, a delivery
// that is stale on arrival is not buffered, and a stale delivery still
// buffered at shutdown is not nacked. Only the valid deliveries are claimed.
func TestEpoch_StaleInTheBuffer(t *testing.T) {
	h := newHarness(t) // concurrency 1
	h.prov.latency = time.Second
	first := h.push(prioNormal)
	h.start()
	h.waitBlocked(1) // the first send holds the only concurrency slot

	purged := h.push(prioHigh)
	skipped := h.push(prioHigh)
	valid := h.push(prioLow)
	purged.stale.Store(true)
	h.w.DropStale()
	if n := h.w.buf.len(); n != 2 {
		t.Fatalf("%d buffered after DropStale, want 2", n)
	}
	skipped.stale.Store(true) // after the purge: take must skip it
	arrivedStale := &fakeMessage{id: uuid.New(), prio: prioHigh}
	arrivedStale.stale.Store(true)
	h.w.Push(arrivedStale)
	if n := h.w.buf.len(); n != 2 {
		t.Fatalf("%d buffered after a stale push, want 2", n)
	}

	// The first send ends; the pacer takes the valid message, the only one
	// left that is not stale.
	h.clk.Advance(time.Second)
	h.waitBlocked(1) // the valid message's send holds the slot
	atShutdown := h.push(prioNormal)
	atShutdown.stale.Store(true)
	h.stop() // the running send finishes within the drain time
	if n := h.st.finals(); n != 2 {
		t.Errorf("%d final writes, want 2", n)
	}

	if got := claimed(h); len(got) != 2 || got[0] != first.id || got[1] != valid.id {
		t.Errorf("claimed %v, want the first and the valid message", got)
	}
	for name, m := range map[string]*fakeMessage{"purged": purged, "skipped": skipped, "arrived stale": arrivedStale} {
		if m.acks.Load()+m.nacks.Load()+m.refused.Load() != 0 {
			t.Errorf("%s: acks %d, nacks %d, refused %d; want no answer at all",
				name, m.acks.Load(), m.nacks.Load(), m.refused.Load())
		}
	}
	if atShutdown.nacks.Load() != 0 || atShutdown.refused.Load() != 1 {
		t.Errorf("stale at shutdown: nacks %d, refused %d; want 0 and 1", atShutdown.nacks.Load(), atShutdown.refused.Load())
	}
	if first.acks.Load() != 1 || valid.acks.Load() != 1 {
		t.Errorf("acks of the valid deliveries: %d and %d, want 1 each", first.acks.Load(), valid.acks.Load())
	}
	if !strings.Contains(h.logs.String(), `"msg":"`+msgDropped+`","channel":"sms","count":1`) {
		t.Errorf("no buffer-purge line with count 1:\n%s", h.logs.String())
	}
}

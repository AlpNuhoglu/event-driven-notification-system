package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/store"
)

// dispatch delivers m at slot. It owns one concurrency slot and releases it
// when it ends. shutdown ends when the worker's shutdown begins; ctx ends
// when the drain timeout does.
//
// Invariants enforced here (D23, D77, open issue 7):
//   - every send starts within [slot, slot + tolerance] of a slot reserved
//     for it (awaitSlot);
//   - a slot is never returned to the limiter and never used after
//     slot + tolerance (nextForSlot, awaitSlot);
//   - the ack comes after the claim commits and before the send (deliver);
//   - from the moment it takes a delivery from the buffer, this goroutine
//     alone owns it, and answers it exactly once: one ack, or one nack
//     (deliver), never both.
func (w *Worker) dispatch(shutdown, ctx context.Context, m Message, slot time.Time) {
	defer func() { <-w.sem }()
	defer func() {
		if p := recover(); p != nil {
			w.log.Error("worker: panic in dispatch recovered", slog.String("panic_type", fmt.Sprintf("%T", p)))
		}
	}()
	// A claim that leaves the slot unused hands it to the next message.
	for m != nil {
		m = w.deliver(shutdown, ctx, m, slot)
	}
}

// deliver claims m and, if the claim opened an attempt, sends it at slot. It
// returns the next message to try at the same slot when the claim left the
// slot unused, or nil. It answers m exactly once: the nack below or the ack
// below, never both.
func (w *Worker) deliver(shutdown, ctx context.Context, m Message, slot time.Time) Message {
	// a. claimStart is when the claim call that succeeded was issued; the
	// send deadline is measured from it.
	c, ev, claimStart, err := w.claim(shutdown, ctx, m.NotificationID())
	if err != nil && !errors.Is(err, store.ErrNotApplied) {
		// The claim never succeeded and shutdown has begun. The only nack
		// of a taken delivery.
		w.nack(m)
		return nil
	}
	// The claim has committed, or matched zero rows: the database owns the
	// work now. The only ack of a taken delivery: after the claim, before
	// the send. (P3 adds: not for a delivery from an older epoch, D76.)
	w.ack(m)
	if err != nil {
		// store.ErrNotApplied: a duplicate, cancelled, expired or not-due row.
		return w.nextForSlot(slot)
	}
	w.events.Publish(ev)
	if c.Expired {
		return w.nextForSlot(slot)
	}
	w.attempt(ctx, c.Notification, claimStart, slot)
	return nil
}

// claim makes the claim of id (transition 3). A call that fails with a
// database error, store.ErrNotApplied aside, is issued again after a back-off
// on the worker's clock, starting at ClaimRetryBase and doubling up to
// ClaimRetryMax. Meanwhile the calling dispatch keeps its delivery and its
// concurrency slot, and no rate-limit slot is reserved: the slot reserved for
// the first call is given up by the missed-slot rule if the claim succeeds
// after it (open issue 7, D77).
//
// It returns the result of the last call and the time that call was issued.
// The error is nil or store.ErrNotApplied, unless shutdown has begun: a call
// that fails once shutdown has begun (also one that was in flight when it
// began) is not retried, and neither is a back-off that shutdown interrupts.
// The caller then nacks.
func (w *Worker) claim(shutdown, ctx context.Context, id uuid.UUID) (store.Claim, domain.StatusChanged, time.Time, error) {
	backoff := w.cfg.ClaimRetryBase
	for tries := 1; ; tries++ {
		issued := w.clock.Now()
		c, ev, err := w.store.Claim(ctx, id, w.cfg.Lease, w.provider.Name())
		if err == nil || errors.Is(err, store.ErrNotApplied) {
			if tries > 1 {
				w.log.Info("worker: claim succeeded after retries",
					slog.String("notification_id", id.String()), slog.Int("tries", tries))
			}
			return c, ev, issued, err
		}
		if tries == 1 {
			w.log.Warn("worker: claim failed; retrying with back-off",
				slog.String("notification_id", id.String()), slog.Any("error", err))
		}
		// WaitUntil returns at once if shutdown has already begun.
		if w.clock.WaitUntil(shutdown, w.clock.Now().Add(backoff)) != nil {
			w.log.Warn("worker: claim still failing at shutdown; message requeued",
				slog.String("notification_id", id.String()), slog.Int("tries", tries), slog.Any("error", err))
			return c, ev, issued, err
		}
		backoff = min(2*backoff, w.cfg.ClaimRetryMax)
	}
}

// nextForSlot keeps a slot that a claim left unused for the next buffered
// message. A slot that is already stale is given up: it is never returned to
// the limiter and never used after slot + tolerance.
func (w *Worker) nextForSlot(slot time.Time) Message {
	if w.clock.Now().After(slot.Add(w.cfg.SlotTolerance)) {
		// Counted as missed only when a message is ready for it; with an
		// empty buffer the slot would have gone unused anyway.
		if w.buf.len() > 0 {
			w.missed.Add(1)
		}
		return nil
	}
	return w.buf.take()
}

// attempt waits for the slot, asks the breaker for a permit and sends:
// steps b to d.
func (w *Worker) attempt(ctx context.Context, n store.Notification, claimStart, slot time.Time) {
	// b.
	if !w.awaitSlot(ctx, claimStart.Add(w.sendDeadline), slot) {
		w.abort(ctx, n, 0)
		return
	}
	// c. At the slot time, also for a re-reserved slot. A circuit that
	// opened after the claim sends nothing: the attempt is aborted and the
	// row is due again when the circuit can be asked again (D23).
	permit, admitted, until := w.breaker.Admit()
	if !admitted {
		w.abort(ctx, n, max(until.Sub(w.clock.Now()), 0))
		return
	}
	// d.
	w.complete(ctx, n, w.send(ctx, n, permit))
}

// send makes the admitted send and records its outcome on the permit,
// exactly once on every path: in a half-open circuit this send is the probe,
// and a probe that is never recorded would keep the circuit half-open for
// good. call turns a panic of the provider client into internal_error; the
// deferred record covers a panic anywhere else in between.
//
// When the circuit is open after the record, the retry is not due before the
// circuit can be probed: the hint becomes max(Retry-After, time until the
// next half-open probe), and the retry decision takes the larger of that and
// the back-off (ADR-0007 section 4).
func (w *Worker) send(ctx context.Context, n store.Notification, permit Permit) store.AttemptResult {
	recorded := false
	defer func() {
		if !recorded {
			permit.Record(domain.OutcomeInternalError, 0)
		}
	}()
	r := w.call(ctx, n)
	openUntil := permit.Record(r.Outcome, r.RetryAfter)
	recorded = true
	if r.Outcome != domain.OutcomeSuccess && !openUntil.IsZero() {
		r.RetryAfter = max(r.RetryAfter, openUntil.Sub(w.clock.Now()))
	}
	return r
}

// awaitSlot applies the missed-slot rule (D77). It returns true at a time in
// [slot, slot + tolerance] of a slot reserved for this message, and false
// when nothing may be sent: the send deadline is reached, the slot lies after
// it, or the drain timeout ended (ctx).
func (w *Worker) awaitSlot(ctx context.Context, deadline, slot time.Time) bool {
	for {
		if ctx.Err() != nil {
			return false
		}
		now := w.clock.Now()
		switch {
		case !now.Before(deadline):
			return false
		case now.After(slot.Add(w.cfg.SlotTolerance)):
			// Missed: give the slot up (never returned, never used) and
			// reserve a new one, then re-evaluate.
			w.missed.Add(1)
			var ok bool
			if slot, ok = w.reserveBefore(ctx, deadline); !ok {
				return false
			}
		case slot.After(deadline):
			return false
		case now.Before(slot):
			if w.clock.WaitUntil(ctx, slot) != nil {
				return false
			}
		default: // slot <= now <= slot + tolerance
			return true
		}
	}
}

// reserveBefore reserves a slot through the same limiter call as the pacer
// loop, horizon and retry interval included, but never waits past deadline.
func (w *Worker) reserveBefore(ctx context.Context, deadline time.Time) (time.Time, bool) {
	for {
		if slot, ok := w.reserve(ctx); ok {
			return slot, true
		}
		retry := w.clock.Now().Add(w.cfg.RetryInterval)
		if !retry.Before(deadline) || w.clock.WaitUntil(ctx, retry) != nil {
			return time.Time{}, false
		}
	}
}

// call sends the notification and classifies the result. A panic in the
// provider client or the classification is recorded as internal_error.
func (w *Worker) call(ctx context.Context, n store.Notification) (r store.AttemptResult) {
	defer func() {
		if p := recover(); p != nil {
			msg := fmt.Sprintf("panic: %T", p)
			w.log.Error("worker: panic during send recovered",
				slog.String("notification_id", n.ID.String()), slog.String("panic_type", fmt.Sprintf("%T", p)))
			r = store.AttemptResult{Outcome: domain.OutcomeInternalError, Error: &msg}
		}
	}()
	sendCtx, cancel := context.WithTimeout(ctx, w.cfg.HTTPTimeout)
	defer cancel()
	res := w.provider.Send(sendCtx, Request{
		NotificationID: n.ID,
		Channel:        n.Channel,
		To:             n.Recipient.Reveal(), // the recipient leaves the process here, and only here
		Content:        n.Content,
		TraceParent:    n.TraceParent,
	})
	return w.attemptResult(res)
}

// attemptResult classifies a provider result (ADR-0007 section 2). Without a
// response, WroteRequest decides between unreachable (nothing was sent) and
// unknown. A Retry-After header becomes the retry hint only on the statuses
// that honour it (429 and 503) and only when it is usable.
func (w *Worker) attemptResult(res Response) store.AttemptResult {
	r := store.AttemptResult{
		Outcome: domain.Classify(domain.ProviderResult{StatusCode: res.StatusCode, WroteRequest: res.WroteRequest}),
		Latency: res.Latency,
	}
	if res.StatusCode != 0 {
		r.HTTPStatus = &res.StatusCode
	}
	if res.Err != nil {
		msg := res.Err.Error()
		r.Error = &msg
	}
	if res.MessageID != "" {
		r.ProviderMessageID = &res.MessageID
	}
	if domain.HonoursRetryAfter(res.StatusCode) {
		// An HTTP-date is compared with the worker's wall clock; the cap of
		// the retry decision bounds what a skewed clock can do.
		if d, ok := domain.ParseRetryAfter(res.RetryAfter, w.clock.Now()); ok {
			r.RetryAfter = d
		}
	}
	return r
}

// complete makes the fenced final write of the attempt and emits its event.
func (w *Worker) complete(ctx context.Context, n store.Notification, r store.AttemptResult) {
	wctx, cancel := w.writeContext(ctx)
	defer cancel()
	var (
		ev  domain.StatusChanged
		err error
	)
	if r.Outcome == domain.OutcomeSuccess {
		_, ev, err = w.store.CompleteSent(wctx, n.ID, n.AttemptCount, w.provider.Name(), r)
	} else {
		ev, err = w.store.CompleteFailure(wctx, n.ID, n.AttemptCount, r, w.jitter())
	}
	w.finished("final write", n.ID, ev, err)
}

// abort closes the attempt without a send: the row returns to retrying,
// due after delay, without consuming budget.
func (w *Worker) abort(ctx context.Context, n store.Notification, delay time.Duration) {
	wctx, cancel := w.writeContext(ctx)
	defer cancel()
	ev, err := w.store.AbortAttempt(wctx, n.ID, n.AttemptCount, delay)
	w.finished("abort attempt", n.ID, ev, err)
}

// writeContext returns a fresh context for a write after the claim: the
// drain timeout cancelling ctx must not lose the write.
func (w *Worker) writeContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), w.cfg.WriteMargin)
}

func (w *Worker) finished(op string, id uuid.UUID, ev domain.StatusChanged, err error) {
	switch {
	case err == nil:
		w.events.Publish(ev)
	case errors.Is(err, store.ErrNotApplied):
		// A late worker: a newer attempt started or the row left processing.
		// The attempt row holds the true outcome. Not an error.
		w.log.Debug("worker: "+op+" not applied", slog.String("notification_id", id.String()))
	default:
		// The lease sweeper recovers the row (the 60-second retry of the
		// final write is P3).
		w.log.Error("worker: "+op+" failed", slog.String("notification_id", id.String()), slog.Any("error", err))
	}
}

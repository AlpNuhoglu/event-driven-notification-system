package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/platform/logging"
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
//     alone owns it, and answers it at most once: one ack, or one nack
//     (deliver), never both; none if the delivery is from an older
//     connection epoch by then (D76).
func (w *Worker) dispatch(shutdown, ctx context.Context, m Message, slot time.Time) {
	defer func() { <-w.sem }()
	w.metrics.inflight.Inc()
	defer w.metrics.inflight.Dec()
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
	// The worker.process span covers this delivery from the claim to the
	// final write; ctx carries it to every call below (tracing.go).
	ctx, span := w.startProcess(ctx, m)
	defer span.End()
	// a. claimStart is when the claim call that succeeded was issued; the
	// send deadline is measured from it.
	c, ev, claimStart, err := w.claim(shutdown, ctx, m.NotificationID())
	if err != nil && !errors.Is(err, store.ErrNotApplied) {
		// The claim never succeeded and shutdown has begun. The only nack
		// of a taken delivery; none for a delivery that the connection's
		// reset made stale meanwhile, e.g. during a long claim retry (D76).
		processOutcome(span, processRequeued)
		w.nack(m)
		return nil
	}
	// The claim has committed, or matched zero rows: the database owns the
	// work now. The only ack of a taken delivery: after the claim, before
	// the send. A delivery from an older connection epoch is not acked (the
	// check is part of the ack); its claim stands and the send goes ahead
	// (D76).
	w.ack(m)
	if err != nil {
		// store.ErrNotApplied: a duplicate, cancelled, expired or not-due row.
		processOutcome(span, processNotApplied)
		return w.nextForSlot(slot)
	}
	processClaimed(span, c.Notification)
	// From here on every line of this notification carries its correlation
	// id, read from its row by the claim (ADR-0009 §5).
	ctx = logging.WithCorrelationID(ctx, c.Notification.CorrelationID)
	w.events.Publish(ev)
	logging.Transition(ctx, w.log, ev)
	if c.Expired {
		processOutcome(span, processExpired)
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
				w.log.InfoContext(ctx, "worker: claim succeeded after retries",
					slog.String("notification_id", id.String()), slog.Int("tries", tries))
			}
			return c, ev, issued, err
		}
		if tries == 1 {
			w.log.WarnContext(ctx, "worker: claim failed; retrying with back-off",
				slog.String("notification_id", id.String()), slog.Any("error", err))
		}
		// WaitUntil returns at once if shutdown has already begun.
		if w.clock.WaitUntil(shutdown, w.clock.Now().Add(backoff)) != nil {
			w.log.WarnContext(ctx, "worker: claim still failing at shutdown; message requeued",
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
			w.log.ErrorContext(ctx, "worker: panic during send recovered",
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
	})
	r = w.attemptResult(res)
	// What is stored in notification_attempts.error, and nowhere else: the
	// transport error, or the body of an answer that is not a 2xx. Both are
	// texts the application does not choose (a transport error quotes the
	// URL, a provider can quote the request), so the message's own recipient
	// and content are removed first and the result is cut to 1 KB (the
	// value-based redaction of ADR-0009 §4). It is never logged.
	text, cut := res.ErrorBody, res.ErrorBodyCut
	if res.Err != nil {
		text, cut = res.Err.Error(), false
	}
	if text != "" {
		msg := domain.RedactProviderError(text, cut, n.Recipient, n.Content)
		r.Error = &msg
	}
	return r
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
// A success is transition 4, or transition 12 when the row has left
// processing meanwhile and no newer attempt has started (D14).
func (w *Worker) complete(ctx context.Context, n store.Notification, r store.AttemptResult) {
	w.metrics.attempted(r)
	attemptOutcome(ctx, r.Outcome)
	// The line of the transition names the attempt's outcome and status
	// code; never the provider's error text (ADR-0009 §4).
	attrs := []slog.Attr{slog.String("outcome", string(r.Outcome))}
	if r.HTTPStatus != nil {
		attrs = append(attrs, slog.Int("http_status", *r.HTTPStatus))
	}
	w.write(ctx, "final write", n, attrs, func(wctx context.Context) (store.Sent, domain.StatusChanged, error) {
		if r.Outcome == domain.OutcomeSuccess {
			return w.store.CompleteSent(wctx, n.ID, n.AttemptCount, w.provider.Name(), r)
		}
		ev, err := w.store.CompleteFailure(wctx, n.ID, n.AttemptCount, r, w.jitter())
		return store.Sent{}, ev, err
	})
}

// abort closes the attempt without a send: the row returns to retrying,
// due after delay, without consuming budget.
func (w *Worker) abort(ctx context.Context, n store.Notification, delay time.Duration) {
	w.metrics.aborted()
	attemptOutcome(ctx, domain.OutcomeAborted)
	attrs := []slog.Attr{slog.String("outcome", string(domain.OutcomeAborted))}
	w.write(ctx, "abort attempt", n, attrs, func(wctx context.Context) (store.Sent, domain.StatusChanged, error) {
		ev, err := w.store.AbortAttempt(wctx, n.ID, n.AttemptCount, delay)
		return store.Sent{}, ev, err
	})
}

// write makes a write after the claim (the final write of an attempt, or an
// abort) and emits its event. While the write fails with a database error it
// is made again, with the back-off of the claim's retry, for up to
// FinalWriteRetry (D18, 60 s): the provider has answered, and the answer is
// lost to the row if it cannot be written.
//
// What makes repeating it safe: every such write is fenced by attempt_count.
//   - A write whose commit succeeded but whose answer was lost finds the row
//     changed and returns store.ErrNotApplied: nothing is written twice.
//   - The retries can outlast the lease (30 s). The lease sweeper then marks
//     the attempt unknown and moves the row on. A success that lands
//     afterwards still moves the row to sent (transition 12) unless a newer
//     attempt has started; a failure or an abort that lands afterwards only
//     corrects the attempt row.
//
// The retries end when the window is used up or when the drain timeout of a
// shutdown ends (ctx): the result is then lost to the row, the lease sweeper
// recovers the notification, and a send that had succeeded may be repeated
// (ADR-0004 section 4). Each try runs on a context of its own that shutdown
// does not cancel, bounded by the write margin, so a try that has started is
// finished. Meanwhile the dispatch keeps its concurrency slot, like a claim
// that is being retried.
func (w *Worker) write(ctx context.Context, op string, n store.Notification, attrs []slog.Attr,
	do func(context.Context) (store.Sent, domain.StatusChanged, error),
) {
	id := slog.String("notification_id", n.ID.String())
	deadline := w.clock.Now().Add(w.cfg.FinalWriteRetry)
	backoff := w.cfg.ClaimRetryBase
	for tries := 1; ; tries++ {
		wctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), w.cfg.WriteMargin)
		sent, ev, err := do(wctx)
		cancel()
		switch {
		case err == nil:
			w.events.Publish(ev)
			if sent.Late {
				attrs = append(attrs, slog.Bool("late", true), slog.String("from", string(sent.From)))
				w.log.InfoContext(ctx, "worker: late success applied: the lease had expired, no newer attempt had started",
					id, slog.Int("attempt_no", n.AttemptCount), slog.String("from", string(sent.From)))
			}
			logging.Transition(ctx, w.log, ev, attrs...)
			if tries > 1 {
				w.log.InfoContext(ctx, "worker: "+op+" succeeded after retries", id, slog.Int("tries", tries))
			}
			return
		case errors.Is(err, store.ErrNotApplied):
			// A late worker: a newer attempt started, or the row left
			// processing and the result was not a success; or this is the
			// repeat of a write that had committed. The attempt row holds
			// the true outcome. Not an error.
			w.log.DebugContext(ctx, "worker: "+op+" not applied", id, slog.Int("tries", tries))
			return
		}
		if tries == 1 {
			w.log.WarnContext(ctx, "worker: "+op+" failed; retrying with back-off", id, slog.Any("error", err))
		}
		next := w.clock.Now().Add(backoff)
		if !next.Before(deadline) || w.clock.WaitUntil(ctx, next) != nil {
			w.log.ErrorContext(ctx, "worker: "+op+" lost; the lease sweeper recovers the notification",
				id, slog.Int("tries", tries), slog.Any("error", err))
			return
		}
		backoff = min(2*backoff, w.cfg.ClaimRetryMax)
	}
}

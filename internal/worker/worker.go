// Package worker delivers the notifications of one channel. One pacer loop
// pairs buffered messages with rate-limit slots; each taken message gets its
// own goroutine (a dispatch) that claims it, waits for its slot and calls the
// provider (ADR-0005 "Worker internals", ADR-0007 sections 5 and 8; decisions
// D23, D24, D76 and D77).
//
// The pacer (worker.go, dispatch.go, buffer.go) imports no broker, Redis or
// HTTP client: it consumes the interfaces in deps.go. service.go wires the
// role on the real ones.
package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/events"
)

// DefaultWriteMargin is the part of the lease that the send deadline keeps
// for the final write: send deadline = lease - HTTP timeout - 5 s (D77). It
// is part of the derivation, not an environment variable; the wiring always
// passes this value, and tests shorten it like every other duration (D52).
const DefaultWriteMargin = 5 * time.Second

// Config configures the worker of one channel.
type Config struct {
	Channel domain.Channel
	// Concurrency is the number of dispatches that may run at once (25).
	Concurrency int
	// Lease is the lease a claim takes (LEASE_DURATION, 30 s).
	Lease time.Duration
	// HTTPTimeout bounds one provider request (PROVIDER_TIMEOUT, 10 s).
	HTTPTimeout time.Duration
	// SlotTolerance is how late a send may start after its slot
	// (PACER_SLOT_TOLERANCE, 5 ms).
	SlotTolerance time.Duration
	// RetryInterval is the wait before asking the limiter again after it
	// reserved nothing (RATE_LIMIT_RETRY_INTERVAL, 50 ms).
	RetryInterval time.Duration
	// Drain is how long running dispatches may go on after shutdown begins
	// (SHUTDOWN_DRAIN_TIMEOUT, 15 s).
	Drain time.Duration
	// WriteMargin is the part of the lease kept for the final write
	// (DefaultWriteMargin in production). The send deadline is
	// Lease - HTTPTimeout - WriteMargin, and each write made after the claim
	// (final write, abort) is bounded by it. Those writes run on a context
	// that shutdown does not cancel.
	WriteMargin time.Duration
}

// Deps are the worker's collaborators. All are required.
type Deps struct {
	Store    Store
	Limiter  Limiter
	Breaker  Breaker
	Provider Provider
	Events   events.Publisher
	Clock    Clock
	// Jitter returns a number in [0, 1) for the retry decision.
	Jitter func() float64
	Logger *slog.Logger
}

// Worker paces and delivers the notifications of one channel.
type Worker struct {
	cfg          Config
	sendDeadline time.Duration

	store    Store
	limiter  Limiter
	breaker  Breaker
	provider Provider
	events   events.Publisher
	clock    Clock
	jitter   func() float64
	log      *slog.Logger

	buf *buffer
	// sem holds one token per concurrency slot in use. The pacer loop
	// acquires a token; the dispatch it starts releases it.
	sem chan struct{}
	// missed counts slots given up because they were stale (D77); exported
	// as pacer_slots_missed_total{channel} when metrics land.
	missed atomic.Uint64
}

// New validates cfg and returns the worker of cfg.Channel.
func New(cfg Config, d Deps) (*Worker, error) {
	sendDeadline := cfg.Lease - cfg.HTTPTimeout - cfg.WriteMargin
	switch {
	case cfg.Channel == "":
		return nil, errors.New("worker: channel is empty")
	case cfg.Concurrency <= 0:
		return nil, fmt.Errorf("worker: concurrency must be positive, got %d", cfg.Concurrency)
	case cfg.HTTPTimeout <= 0, cfg.RetryInterval <= 0, cfg.WriteMargin <= 0:
		return nil, errors.New("worker: HTTP timeout, retry interval and write margin must be positive")
	case cfg.SlotTolerance < 0, cfg.Drain < 0:
		return nil, errors.New("worker: slot tolerance and drain timeout must not be negative")
	case sendDeadline <= 0:
		return nil, fmt.Errorf("worker: send deadline (lease %s - HTTP timeout %s - %s) must be positive",
			cfg.Lease, cfg.HTTPTimeout, cfg.WriteMargin)
	case d.Store == nil, d.Limiter == nil, d.Breaker == nil, d.Provider == nil,
		d.Events == nil, d.Clock == nil, d.Jitter == nil, d.Logger == nil:
		return nil, errors.New("worker: every dependency is required")
	}
	return &Worker{
		cfg:          cfg,
		sendDeadline: sendDeadline,
		store:        d.Store,
		limiter:      d.Limiter,
		breaker:      d.Breaker,
		provider:     d.Provider,
		events:       d.Events,
		clock:        d.Clock,
		jitter:       d.Jitter,
		log:          d.Logger.With(slog.String("channel", string(cfg.Channel))),
		buf:          newBuffer(),
		sem:          make(chan struct{}, cfg.Concurrency),
	}, nil
}

// Push hands a delivery from the consumer to the buffer. Once shutdown has
// begun, the delivery is requeued at once.
func (w *Worker) Push(m Message) {
	if !w.buf.push(m) {
		w.nack(m)
	}
}

// MissedSlots returns how many slots were given up because they were stale.
func (w *Worker) MissedSlots() uint64 { return w.missed.Load() }

// Run paces the channel until ctx is cancelled, then shuts down: it stops the
// pacer loop, requeues every buffered message and lets the running dispatches
// finish within the drain timeout. A dispatch whose drain time runs out
// before its send starts aborts its attempt. Run returns when every dispatch
// has ended.
func (w *Worker) Run(ctx context.Context) {
	// Dispatches outlive ctx by up to the drain timeout.
	dispatchCtx, stopDispatches := context.WithCancel(context.WithoutCancel(ctx))
	defer stopDispatches()

	var running sync.WaitGroup
	w.pace(ctx, dispatchCtx, &running)

	for _, m := range w.buf.close() {
		w.nack(m)
	}

	drainEnd := w.clock.Now().Add(w.cfg.Drain)
	timerCtx, stopTimer := context.WithCancel(dispatchCtx)
	var timer sync.WaitGroup
	timer.Go(func() {
		if w.clock.WaitUntil(timerCtx, drainEnd) == nil {
			stopDispatches()
		}
	})
	running.Wait()
	stopTimer()
	timer.Wait()
}

// pace is the pacer loop. It returns when ctx is cancelled.
func (w *Worker) pace(ctx, dispatchCtx context.Context, running *sync.WaitGroup) {
	for {
		// 1. Acquire a concurrency slot. It passes to the dispatch started
		// in step 6, which releases it when it ends.
		select {
		case w.sem <- struct{}{}:
		case <-ctx.Done():
			return
		}
		m, slot, ok := w.next(ctx)
		if !ok {
			<-w.sem
			return
		}
		// 6. Dispatch the message with its slot, and go back to 1.
		running.Go(func() { w.dispatch(dispatchCtx, m, slot) })
	}
}

// next runs steps 2 to 5 of the pacer loop: it returns a message and the
// slot reserved for it, or false when ctx ends. The concurrency slot stays
// held throughout.
func (w *Worker) next(ctx context.Context) (Message, time.Time, bool) {
	for {
		// 2. Wait until the buffer holds a message; nothing is taken yet.
		if w.buf.wait(ctx) != nil {
			return nil, time.Time{}, false
		}
		// 3. Breaker before limiter: while the circuit is open, no slot is
		// reserved and nothing is claimed.
		if allowed, until := w.breaker.Allow(); !allowed {
			if w.clock.WaitUntil(ctx, until) != nil {
				return nil, time.Time{}, false
			}
			continue
		}
		// 4. A slot is reserved only here, when a message is known to be
		// waiting.
		slot, ok := w.reserve(ctx)
		if !ok {
			if w.clock.WaitUntil(ctx, w.clock.Now().Add(w.cfg.RetryInterval)) != nil {
				return nil, time.Time{}, false
			}
			continue
		}
		// 5. The message is chosen after the slot is reserved: the highest
		// priority waiting now. If a dispatch with a kept slot took the last
		// message meanwhile, this reservation is dropped (never returned).
		if m := w.buf.take(); m != nil {
			return m, slot, true
		}
	}
}

// reserve asks the limiter for a slot and returns its time on the worker's
// clock, read after the call returned; false when nothing was reserved.
func (w *Worker) reserve(ctx context.Context) (time.Time, bool) {
	wait, ok, err := w.limiter.Reserve(ctx, w.cfg.Channel)
	if err != nil {
		if ctx.Err() == nil {
			w.log.Warn("worker: rate-limit reservation failed", slog.Any("error", err))
		}
		return time.Time{}, false
	}
	if !ok {
		return time.Time{}, false
	}
	return w.clock.Now().Add(wait), true
}

func (w *Worker) ack(m Message) {
	if err := m.Ack(); err != nil {
		w.log.Error("worker: ack failed",
			slog.String("notification_id", m.NotificationID().String()), slog.Any("error", err))
	}
}

func (w *Worker) nack(m Message) {
	if err := m.Nack(); err != nil {
		w.log.Error("worker: nack failed",
			slog.String("notification_id", m.NotificationID().String()), slog.Any("error", err))
	}
}

package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/store/db"
)

// Claim is the result of a successful claim statement.
type Claim struct {
	Notification Notification
	// Expired is true when the notification had expired at claim time: it is
	// now failed/expired, no attempt was opened and nothing is to be sent.
	Expired bool
}

// Claim performs transition 3 in one statement (ADR-0004 section 3): a due
// pending, retrying or queued row becomes processing with a lease and a new
// in_flight attempt, or failed/expired when expires_at has passed. If the
// relay's lane transaction holds the row lock, the statement waits for it and
// re-evaluates its guard after the commit or rollback.
//
// Zero rows (already claimed, cancelled, expired, sent or not yet due) returns
// ErrNotApplied; the caller acks and drops the message.
func (s *Store) Claim(ctx context.Context, id uuid.UUID, lease time.Duration, provider string) (Claim, domain.StatusChanged, error) {
	if lease <= 0 {
		return Claim{}, domain.StatusChanged{}, fmt.Errorf("claim: lease must be positive, got %s", lease)
	}
	row, err := s.q.ClaimNotification(ctx, db.ClaimNotificationParams{
		Lease:    interval(lease),
		ID:       id,
		Provider: provider,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return Claim{}, domain.StatusChanged{}, ErrNotApplied
	}
	if err != nil {
		return Claim{}, domain.StatusChanged{}, fmt.Errorf("claim: %w", err)
	}
	n := fromRow(db.Notification(row))
	return Claim{Notification: n, Expired: n.Status == domain.StatusFailed}, n.Event(), nil
}

// AttemptResult is the outcome of one delivery attempt as the worker saw it.
type AttemptResult struct {
	Outcome           domain.Outcome
	HTTPStatus        *int
	Latency           time.Duration
	Error             *string
	ProviderMessageID *string
	// RetryAfter is the provider's Retry-After hint; zero when absent.
	RetryAfter time.Duration
}

func (r AttemptResult) finishParams(id uuid.UUID, attemptNo int32) (db.FinishAttemptParams, error) {
	httpStatus, err := int32Ptr(r.HTTPStatus)
	if err != nil {
		return db.FinishAttemptParams{}, fmt.Errorf("http status: %w", err)
	}
	latency, err := toInt32(int(r.Latency.Milliseconds()))
	if err != nil {
		return db.FinishAttemptParams{}, fmt.Errorf("latency: %w", err)
	}
	return db.FinishAttemptParams{
		Outcome:           string(r.Outcome),
		HttpStatus:        httpStatus,
		LatencyMs:         &latency,
		Error:             r.Error,
		ProviderMessageID: r.ProviderMessageID,
		NotificationID:    id,
		AttemptNo:         attemptNo,
	}, nil
}

// CompleteSent performs transition 4 in one transaction: the fenced MarkSent
// (status = 'processing' AND attempt_count = attemptNo), then the attempt row
// gets the true outcome. r.Outcome must be domain.OutcomeSuccess.
//
// When the fenced statement matches zero rows (a newer attempt has started,
// or the row left processing), the attempt row is still updated with the true
// outcome and the transaction commits; ErrNotApplied is then returned.
func (s *Store) CompleteSent(ctx context.Context, id uuid.UUID, attemptNo int, provider string, r AttemptResult) (domain.StatusChanged, error) {
	if r.Outcome != domain.OutcomeSuccess {
		return domain.StatusChanged{}, fmt.Errorf("complete sent: outcome must be success, got %q", r.Outcome)
	}
	no, err := toInt32(attemptNo)
	if err != nil {
		return domain.StatusChanged{}, fmt.Errorf("complete sent: attempt number: %w", err)
	}
	finish, err := r.finishParams(id, no)
	if err != nil {
		return domain.StatusChanged{}, fmt.Errorf("complete sent: %w", err)
	}
	var (
		event   domain.StatusChanged
		applied bool
	)
	err = s.inTx(ctx, func(q *db.Queries) error {
		row, err := q.MarkSent(ctx, db.MarkSentParams{
			ProviderMessageID: r.ProviderMessageID,
			Provider:          &provider,
			ID:                id,
			AttemptNo:         no,
		})
		switch {
		case errors.Is(err, pgx.ErrNoRows):
			applied = false
		case err != nil:
			return fmt.Errorf("mark sent: %w", err)
		default:
			applied = true
			event = fromRow(row).Event()
		}
		return finishAttempt(ctx, q, finish, applied)
	})
	if err != nil {
		return domain.StatusChanged{}, fmt.Errorf("complete sent: %w", err)
	}
	if !applied {
		return domain.StatusChanged{}, ErrNotApplied
	}
	return event, nil
}

// CompleteFailure performs transition 5 or 6 in one transaction: it locks the
// row with the fencing guard, asks domain.DecideRetry with the database now()
// of this transaction, applies MarkRetrying or MarkFailed (failures is
// incremented only when the decision consumes budget), then writes the true
// outcome to the attempt row. jitter is in [0, 1).
//
// When the fencing guard matches zero rows, the attempt row is still updated
// with the true outcome and the transaction commits; ErrNotApplied is then
// returned.
func (s *Store) CompleteFailure(ctx context.Context, id uuid.UUID, attemptNo int, r AttemptResult, jitter float64) (domain.StatusChanged, error) {
	switch r.Outcome {
	case "", domain.OutcomeSuccess, domain.OutcomeInFlight:
		return domain.StatusChanged{}, fmt.Errorf("complete failure: invalid outcome %q", r.Outcome)
	}
	no, err := toInt32(attemptNo)
	if err != nil {
		return domain.StatusChanged{}, fmt.Errorf("complete failure: attempt number: %w", err)
	}
	finish, err := r.finishParams(id, no)
	if err != nil {
		return domain.StatusChanged{}, fmt.Errorf("complete failure: %w", err)
	}
	var (
		event   domain.StatusChanged
		applied bool
	)
	err = s.inTx(ctx, func(q *db.Queries) error {
		locked, err := q.LockProcessing(ctx, db.LockProcessingParams{ID: id, AttemptNo: no})
		switch {
		case errors.Is(err, pgx.ErrNoRows):
			return finishAttempt(ctx, q, finish, false)
		case err != nil:
			return fmt.Errorf("lock processing: %w", err)
		}
		prof, err := s.profile(domain.Priority(locked.Priority))
		if err != nil {
			return err
		}
		decision := s.decide(r.Outcome, int(locked.Failures), int(locked.MaxFailures), locked.ExpiresAt,
			r.RetryAfter, prof, locked.DbNow, jitter)
		row, err := applyDecision(ctx, q, id, no, decision)
		if err != nil {
			return err
		}
		applied = true
		event = fromRow(row).Event()
		return finishAttempt(ctx, q, finish, true)
	})
	if err != nil {
		return domain.StatusChanged{}, fmt.Errorf("complete failure: %w", err)
	}
	if !applied {
		return domain.StatusChanged{}, ErrNotApplied
	}
	return event, nil
}

// AbortAttempt closes an attempt that sent nothing: transition 5 without
// consuming budget. In one transaction, the fenced MarkRetrying leaves
// failures unchanged and sets due_at = now() + delay, then the attempt row
// gets the outcome aborted. It is used when the send deadline passes before
// the send can start (D77) and, from P3, when the circuit opens between the
// claim and the send (D23).
//
// When the fenced statement matches zero rows, the attempt row is still
// closed as aborted (nothing was sent, whatever happened to the row since)
// and the transaction commits; ErrNotApplied is then returned.
func (s *Store) AbortAttempt(ctx context.Context, id uuid.UUID, attemptNo int, delay time.Duration) (domain.StatusChanged, error) {
	if delay < 0 {
		return domain.StatusChanged{}, fmt.Errorf("abort attempt: negative delay %s", delay)
	}
	no, err := toInt32(attemptNo)
	if err != nil {
		return domain.StatusChanged{}, fmt.Errorf("abort attempt: attempt number: %w", err)
	}
	finish := db.FinishAttemptParams{
		Outcome:        string(domain.OutcomeAborted),
		NotificationID: id,
		AttemptNo:      no,
	}
	var (
		event   domain.StatusChanged
		applied bool
	)
	err = s.inTx(ctx, func(q *db.Queries) error {
		row, err := q.MarkRetrying(ctx, db.MarkRetryingParams{
			FailuresDelta: 0,
			Delay:         interval(delay),
			ID:            id,
			AttemptNo:     no,
		})
		switch {
		case errors.Is(err, pgx.ErrNoRows):
			applied = false
		case err != nil:
			return fmt.Errorf("mark retrying: %w", err)
		default:
			applied = true
			event = fromRow(row).Event()
		}
		return finishAttempt(ctx, q, finish, applied)
	})
	if err != nil {
		return domain.StatusChanged{}, fmt.Errorf("abort attempt: %w", err)
	}
	if !applied {
		return domain.StatusChanged{}, ErrNotApplied
	}
	return event, nil
}

// finishAttempt writes the true outcome of an attempt. When the fenced
// transition was applied, the attempt was in_flight under the row lock this
// transaction holds, so exactly one row must change.
func finishAttempt(ctx context.Context, q *db.Queries, p db.FinishAttemptParams, applied bool) error {
	n, err := q.FinishAttempt(ctx, p)
	if err != nil {
		return fmt.Errorf("finish attempt: %w", err)
	}
	if applied && n != 1 {
		return fmt.Errorf("finish attempt: %d rows updated for an applied transition, want 1", n)
	}
	return nil
}

// applyDecision runs the fenced MarkRetrying or MarkFailed for a decision
// (transitions 5, 6 and 7). The caller holds the row lock, so zero rows is an
// error here, not a lost race.
func applyDecision(ctx context.Context, q *db.Queries, id uuid.UUID, attemptNo int32, d domain.Decision) (db.Notification, error) {
	var delta int32
	if d.ConsumesBudget {
		delta = 1
	}
	switch d.Action {
	case domain.ActionRetry:
		row, err := q.MarkRetrying(ctx, db.MarkRetryingParams{
			FailuresDelta: delta,
			Delay:         interval(d.Delay),
			ID:            id,
			AttemptNo:     attemptNo,
		})
		if err != nil {
			return db.Notification{}, fmt.Errorf("mark retrying: %w", err)
		}
		return row, nil
	case domain.ActionFail:
		reason := string(d.Reason)
		row, err := q.MarkFailed(ctx, db.MarkFailedParams{
			FailureReason: &reason,
			FailuresDelta: delta,
			ID:            id,
			AttemptNo:     attemptNo,
		})
		if err != nil {
			return db.Notification{}, fmt.Errorf("mark failed: %w", err)
		}
		return row, nil
	}
	return db.Notification{}, fmt.Errorf("unknown retry action %v", d.Action)
}

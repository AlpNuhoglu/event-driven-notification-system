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

// Refusal names why the state of a notification does not allow a cancel or
// a redrive.
type Refusal string

const (
	// RefusalNotCancellable means that the notification is processing,
	// sent or failed.
	RefusalNotCancellable Refusal = "not_cancellable"
	// RefusalNotFailed refuses a redrive of a notification that is not
	// failed.
	RefusalNotFailed Refusal = "not_failed"
	// RefusalExpiresAtRequired means that the stored expires_at has passed
	// and the redrive gave no new one.
	RefusalExpiresAtRequired Refusal = "expires_at_required"
	// RefusalExpiresAtNotAllowed means that the redrive gave a new
	// expires_at while the stored one is still in the future; the stored
	// one is kept.
	RefusalExpiresAtNotAllowed Refusal = "expires_at_not_allowed"
	// RefusalTooSoon means that the new expires_at is closer than the
	// minimum lead.
	RefusalTooSoon Refusal = "too_soon"
	// RefusalStateChanged means that the row changed between the
	// transition and the read of its state in every round; the last state
	// read would have allowed the transition.
	RefusalStateChanged Refusal = "state_changed"
)

// RefusedError is returned when a cancel or a redrive matched no row and the
// notification's state explains why. Current is the state read on the
// database after the transition's statement.
type RefusedError struct {
	Refusal Refusal
	Current domain.Status
	// StoredExpiresAt is the notification's expires_at as read with Current.
	// It is set only with RefusalExpiresAtNotAllowed (the stored value that
	// is still in the future and is kept, D16), zero otherwise.
	StoredExpiresAt time.Time
}

func (e *RefusedError) Error() string {
	return fmt.Sprintf("store: transition refused: %s (status %s)", e.Refusal, e.Current)
}

// maxTransitionRounds bounds how often a cancel or a redrive runs its
// statement again when the state read after a miss says that it should have
// applied (the row moved between the two statements).
const maxTransitionRounds = 3

// Cancelled is the result of CancelNotification.
type Cancelled struct {
	// Notification is the row after the call.
	Notification Notification
	// MayHaveBeenSent is true when the notification has an attempt whose
	// outcome is unknown (D15).
	MayHaveBeenSent bool
	// Applied is true when this call cancelled the notification, false when
	// it was cancelled before (then no event is returned).
	Applied bool
}

// CancelNotification performs transition 9 (ADR-0004 section 3, D15). The
// decision is the guard of the UPDATE (status IN pending, retrying, queued),
// never a status read before it. In one transaction:
//
//   - the UPDATE matched: the row and the may-have-been-sent flag are
//     returned with the transition's event;
//   - it matched zero rows: the row is read. No row: ErrNotFound. Already
//     cancelled: the row and the flag, Applied false and a zero event (the
//     call is idempotent). Processing, sent or failed: a *RefusedError with
//     RefusalNotCancellable. Cancellable after all (the row moved between
//     the two statements): the UPDATE runs again, at most
//     maxTransitionRounds times in all, then a *RefusedError with
//     RefusalStateChanged and the last state read.
//
// The caller publishes the event after this function has returned (the
// transaction has committed).
func (s *Store) CancelNotification(ctx context.Context, id uuid.UUID) (Cancelled, domain.StatusChanged, error) {
	var out Cancelled
	err := s.inTx(ctx, func(q *db.Queries) error {
		var last domain.Status
		for range maxTransitionRounds {
			row, err := q.CancelNotification(ctx, id)
			switch {
			case err == nil:
				out = Cancelled{Notification: fromRow(row), Applied: true}
				return mayHaveBeenSent(ctx, q, &out)
			case !errors.Is(err, pgx.ErrNoRows):
				return fmt.Errorf("cancel: %w", err)
			}
			cur, err := q.GetNotification(ctx, id)
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrNotFound
			}
			if err != nil {
				return fmt.Errorf("read state: %w", err)
			}
			last = domain.Status(cur.Status)
			switch last {
			case domain.StatusCancelled:
				out = Cancelled{Notification: fromRow(cur)}
				return mayHaveBeenSent(ctx, q, &out)
			case domain.StatusPending, domain.StatusRetrying, domain.StatusQueued:
				continue
			}
			return &RefusedError{Refusal: RefusalNotCancellable, Current: last}
		}
		return &RefusedError{Refusal: RefusalStateChanged, Current: last}
	})
	if err != nil {
		return Cancelled{}, domain.StatusChanged{}, fmt.Errorf("cancel notification: %w", err)
	}
	if !out.Applied {
		return out, domain.StatusChanged{}, nil
	}
	return out, out.Notification.Event(), nil
}

func mayHaveBeenSent(ctx context.Context, q *db.Queries, c *Cancelled) error {
	flag, err := q.HasUnknownAttempt(ctx, c.Notification.ID)
	if err != nil {
		return fmt.Errorf("has unknown attempt: %w", err)
	}
	c.MayHaveBeenSent = flag
	return nil
}

// CancelledBatch is the result of CancelBatch: the counts of the batch after
// the call (D69).
type CancelledBatch struct {
	// Cancelled is the number of notifications of the batch that are
	// cancelled, by this call or an earlier one.
	Cancelled int
	// NotCancellable is the number of the others (total - Cancelled).
	NotCancellable int
	// MayHaveBeenSent is the number of cancelled notifications that have an
	// attempt whose outcome is unknown.
	MayHaveBeenSent int
}

// CancelBatch performs transition 9 for every notification of a batch that
// no worker holds, in one statement (ADR-0006 section 7), then reads the
// counts of the batch in the same transaction. It returns one event per row
// this call cancelled; a repeated call returns no event and the same counts,
// unless a notification that a worker held has become cancellable since.
// An unknown batch returns ErrBatchNotFound.
func (s *Store) CancelBatch(ctx context.Context, batchID uuid.UUID) (CancelledBatch, []domain.StatusChanged, error) {
	var (
		out    CancelledBatch
		events []domain.StatusChanged
	)
	err := s.inTx(ctx, func(q *db.Queries) error {
		rows, err := q.CancelBatchNotifications(ctx, &batchID)
		if err != nil {
			return fmt.Errorf("cancel: %w", err)
		}
		counts, err := q.BatchCancelCounts(ctx, &batchID)
		if err != nil {
			return fmt.Errorf("counts: %w", err)
		}
		if counts.Total == 0 {
			// A batch row is written only with at least one accepted item,
			// so no notification almost always means no batch; the batches
			// table decides.
			if _, err := q.GetBatch(ctx, batchID); errors.Is(err, pgx.ErrNoRows) {
				return ErrBatchNotFound
			} else if err != nil {
				return fmt.Errorf("get batch: %w", err)
			}
		}
		events = make([]domain.StatusChanged, len(rows))
		for i, r := range rows {
			events[i] = fromRow(r).Event()
		}
		out = CancelledBatch{
			Cancelled:       int(counts.Cancelled),
			NotCancellable:  int(counts.Total - counts.Cancelled),
			MayHaveBeenSent: int(counts.MayHaveBeenSent),
		}
		return nil
	})
	if err != nil {
		return CancelledBatch{}, nil, fmt.Errorf("cancel batch: %w", err)
	}
	return out, events, nil
}

// RedriveNotification performs transition 11 (ADR-0004 section 3, D16):
// failed -> pending with failures = 0 and due_at = now(); attempt_count,
// max_failures, the dedupe_key and the attempts are untouched. newExpiresAt
// is nil when the request gave none; it is truncated to the database's
// microsecond precision.
//
// The decision is the guard of the UPDATE, on the database clock. When it
// matches zero rows, the state is read in the same transaction: no row is
// ErrNotFound; otherwise a *RefusedError says why (RefusalNotFailed,
// RefusalExpiresAtRequired, RefusalExpiresAtNotAllowed, RefusalTooSoon).
// When the state read says that the UPDATE should have applied (the row
// moved between the two statements), the UPDATE runs again, at most
// maxTransitionRounds times in all, then RefusalStateChanged is returned
// with the last state read.
func (s *Store) RedriveNotification(ctx context.Context, id uuid.UUID, newExpiresAt *time.Time, minLead time.Duration) (Notification, domain.StatusChanged, error) {
	if minLead < 0 {
		return Notification{}, domain.StatusChanged{}, fmt.Errorf("redrive: negative min lead %s", minLead)
	}
	if newExpiresAt != nil {
		t := newExpiresAt.Truncate(time.Microsecond)
		newExpiresAt = &t
	}
	var out Notification
	err := s.inTx(ctx, func(q *db.Queries) error {
		var last domain.Status
		for range maxTransitionRounds {
			row, err := q.RedriveNotification(ctx, db.RedriveNotificationParams{
				NewExpiresAt: newExpiresAt,
				ID:           id,
				MinLead:      interval(minLead),
			})
			switch {
			case err == nil:
				out = fromRow(row)
				return nil
			case !errors.Is(err, pgx.ErrNoRows):
				return fmt.Errorf("redrive: %w", err)
			}
			st, err := q.TransitionState(ctx, id)
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrNotFound
			}
			if err != nil {
				return fmt.Errorf("read state: %w", err)
			}
			last = domain.Status(st.Status)
			if r, refused := redriveRefusal(st, newExpiresAt, minLead); refused {
				e := &RefusedError{Refusal: r, Current: last}
				if r == RefusalExpiresAtNotAllowed {
					e.StoredExpiresAt = st.ExpiresAt
				}
				return e
			}
		}
		return &RefusedError{Refusal: RefusalStateChanged, Current: last}
	})
	if err != nil {
		return Notification{}, domain.StatusChanged{}, fmt.Errorf("redrive notification: %w", err)
	}
	return out, out.Event(), nil
}

// redriveRefusal explains a redrive that matched no row from the state read
// after it, with the same rules as the guard of RedriveNotification. refused
// is false when the state allows the redrive.
func redriveRefusal(st db.TransitionStateRow, newExpiresAt *time.Time, minLead time.Duration) (r Refusal, refused bool) {
	storedPassed := !st.ExpiresAt.After(st.DbNow)
	switch {
	case domain.Status(st.Status) != domain.StatusFailed:
		return RefusalNotFailed, true
	case newExpiresAt == nil && storedPassed:
		return RefusalExpiresAtRequired, true
	case newExpiresAt == nil:
		return "", false
	case !storedPassed:
		return RefusalExpiresAtNotAllowed, true
	case newExpiresAt.Before(st.DbNow.Add(minLead)):
		return RefusalTooSoon, true
	}
	return "", false
}

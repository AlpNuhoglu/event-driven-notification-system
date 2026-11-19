package store

import (
	"context"
	"fmt"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
)

// IdempotencyCleanupBatch is the number of expired idempotency keys one
// cleanup statement deletes at most. D78 fixes it; it is not configuration.
const IdempotencyCleanupBatch = 1000

// ExpireDue performs transition 10 (ADR-0002 "Expiry", ADR-0004 section 3)
// in one statement: at most limit pending, retrying or queued rows with
// expires_at <= now() become failed with failure_reason 'expired', oldest
// expires_at first. The rows are selected with FOR UPDATE SKIP LOCKED, so the
// statement never waits for a row another transaction holds; such a row is
// left to a later pass or to the claim's own expiry branch.
//
// Like the claim's expiry branch, the transition changes only status,
// failure_reason, version and updated_at: attempt_count, failures and the
// attempt rows are untouched, and lease_expires_at is already NULL in the
// three source states.
//
// The statement is planned with its values on every call (customPlanDB): a
// cached generic plan, built while nothing was expired, joins each batch
// against the whole expired backlog.
//
// One event per failed row is returned after the statement has committed.
// Fewer than limit events means that no further row was due and unlocked when
// the statement ran.
func (s *Store) ExpireDue(ctx context.Context, limit int) ([]domain.StatusChanged, error) {
	batchLimit, err := toInt32(limit)
	if err != nil || batchLimit <= 0 {
		return nil, fmt.Errorf("expire due: invalid limit %d", limit)
	}
	rows, err := s.custom.ExpireDue(ctx, batchLimit)
	if err != nil {
		return nil, fmt.Errorf("expire due: %w", err)
	}
	events := make([]domain.StatusChanged, len(rows))
	for i, r := range rows {
		events[i] = fromRow(r).Event()
	}
	return events, nil
}

// DeleteExpiredIdempotencyKeys deletes at most IdempotencyCleanupBatch
// expired idempotency keys in one statement, oldest expires_at first, and
// returns the number deleted (D78, ADR-0006 section 11). The keys are
// selected with FOR UPDATE SKIP LOCKED: a key that a request holds is
// skipped, and a key taken over in the meantime is live again and kept.
// Correctness never depends on this cleanup (an expired key is taken over on
// write); it makes the stated retention real. Like ExpireDue it is planned
// with its values on every call.
func (s *Store) DeleteExpiredIdempotencyKeys(ctx context.Context) (int, error) {
	n, err := s.custom.DeleteExpiredIdempotencyKeys(ctx, IdempotencyCleanupBatch)
	if err != nil {
		return 0, fmt.Errorf("delete expired idempotency keys: %w", err)
	}
	return int(n), nil
}

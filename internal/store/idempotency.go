package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/store/db"
)

// ErrIdempotencyKeyReused is returned when a live idempotency key was stored
// for a request with a different hash (ADR-0006 section 11: answered 422).
var ErrIdempotencyKeyReused = errors.New("store: idempotency key reused with a different request")

// IdempotencyKey is the request layer of duplicate prevention: the client's
// Idempotency-Key with the canonical hash of its request (ADR-0006 section 11).
type IdempotencyKey struct {
	Key string
	// RequestHash covers the method, the path and the re-serialised body.
	RequestHash string
	// Retention is how long the key stays live (IDEMPOTENCY_KEY_TTL).
	Retention time.Duration
}

// StoredResponse is the response stored on an idempotency key and replayed
// to a request with the same key and hash.
//
// Body must be a JSON document. It is stored as jsonb, so a replayed body is
// the same JSON value but not necessarily the same bytes: Postgres normalises
// whitespace and the order of object keys.
type StoredResponse struct {
	Status int
	Body   []byte
}

// CreateOutcome says what CreateSingle or CreateBatch did.
type CreateOutcome int

const (
	// OutcomeCreated means that at least one notification was inserted and,
	// under an idempotency key, the response was stored on the key; all in
	// one transaction.
	OutcomeCreated CreateOutcome = iota + 1
	// OutcomeDuplicate means that nothing was inserted because every item
	// conflicted on its dedupe_key. Nothing was stored: no key, no batch, no
	// notification.
	OutcomeDuplicate
	// OutcomeReplayed means that the idempotency key is live and was stored
	// for the same request hash. Nothing was done; the stored response is
	// returned.
	OutcomeReplayed
)

// errNothingCreated rolls back a create transaction that inserted nothing,
// so that no idempotency key and no batch row is stored without a
// notification.
var errNothingCreated = errors.New("store: nothing created")

// createFunc inserts the notifications inside the transaction and returns
// the response to store on the idempotency key. It returns errNothingCreated
// when nothing was inserted.
type createFunc func(q *db.Queries) (StoredResponse, error)

// createTx runs create in one transaction, under key when key is not nil.
//
// The key is claimed first, before any other work (ADR-0006 section 11):
//
//   - The claim returns a row (the key is new, or had expired and was taken
//     over): create runs, its response is stored on the key, and everything
//     commits together.
//   - No row (the key is live): create does not run. The same hash returns
//     the stored response to replay; a different hash returns
//     ErrIdempotencyKeyReused.
//
// A concurrent request with the same key waits inside the claim statement
// until this transaction ends: after a commit it sees a live key and replays,
// after a rollback its insert proceeds as new. No other lock is taken.
//
// create returning errNothingCreated rolls the transaction back, and the
// error is returned to the caller unchanged (wrapped); so does every other
// error.
func (s *Store) createTx(ctx context.Context, key *IdempotencyKey, create createFunc) (replay *StoredResponse, err error) {
	if key != nil {
		if key.Key == "" || key.RequestHash == "" || key.Retention <= 0 {
			return nil, errors.New("store: idempotency key needs a key, a request hash and a positive retention")
		}
	}
	err = s.inTx(ctx, func(q *db.Queries) error {
		if key != nil {
			_, err := q.ClaimIdempotencyKey(ctx, db.ClaimIdempotencyKeyParams{
				Key:         key.Key,
				RequestHash: key.RequestHash,
				Retention:   interval(key.Retention),
			})
			if errors.Is(err, pgx.ErrNoRows) {
				replay, err = liveKey(ctx, q, key)
				return err
			}
			if err != nil {
				return fmt.Errorf("claim idempotency key: %w", err)
			}
		}
		resp, err := create(q)
		if err != nil {
			return err
		}
		if key == nil {
			return nil
		}
		status, err := toInt32(resp.Status)
		if err != nil {
			return fmt.Errorf("response status: %w", err)
		}
		n, err := q.StoreIdempotencyResponse(ctx, db.StoreIdempotencyResponseParams{
			Key:            key.Key,
			ResponseStatus: &status,
			ResponseBody:   resp.Body,
		})
		if err != nil {
			return fmt.Errorf("store idempotency response: %w", err)
		}
		if n != 1 {
			return fmt.Errorf("store idempotency response: updated %d rows, want 1", n)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return replay, nil
}

// LookupIdempotencyKey reads key outside any create transaction, read-only
// and without a lock, with the liveness rule of the claim (a key is live
// while its expires_at is in the future). A live key with the same request
// hash returns its stored response to replay; with a different hash,
// ErrIdempotencyKeyReused. No live key returns (nil, nil): the create
// transaction then claims the key itself and remains the only place that
// decides between concurrent requests (createTx). A claim that has not
// committed yet is not seen.
//
// The API calls it before the work that must not run for a replay: a
// request that renders a template could render differently on its retry
// (ADR-0008 §5).
func (s *Store) LookupIdempotencyKey(ctx context.Context, key *IdempotencyKey) (*StoredResponse, error) {
	if key == nil || key.Key == "" || key.RequestHash == "" {
		return nil, errors.New("store: idempotency key look-up needs a key and a request hash")
	}
	row, err := s.q.GetLiveIdempotencyKey(ctx, key.Key)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("look up idempotency key: %w", err)
	}
	return storedFor(db.GetIdempotencyKeyRow(row), key)
}

// liveKey reads a live key after a claim that returned no row. The claim
// locked the row (ON CONFLICT DO UPDATE locks the conflicting row even when
// its WHERE is false), so it still exists and is unchanged; it was committed
// by the transaction that claimed it, together with its response.
func liveKey(ctx context.Context, q *db.Queries, key *IdempotencyKey) (*StoredResponse, error) {
	row, err := q.GetIdempotencyKey(ctx, key.Key)
	if err != nil {
		return nil, fmt.Errorf("read idempotency key: %w", err)
	}
	return storedFor(row, key)
}

// storedFor compares a live key's row with the request: the stored response
// for the same hash, ErrIdempotencyKeyReused for another. A live key always
// has a response: the claim and the response commit together.
func storedFor(row db.GetIdempotencyKeyRow, key *IdempotencyKey) (*StoredResponse, error) {
	if row.RequestHash != key.RequestHash {
		return nil, ErrIdempotencyKeyReused
	}
	if row.ResponseStatus == nil || row.ResponseBody == nil {
		return nil, errors.New("store: live idempotency key has no stored response")
	}
	return &StoredResponse{Status: int(*row.ResponseStatus), Body: row.ResponseBody}, nil
}

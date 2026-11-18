package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/store/db"
)

// SingleResult is the result of CreateSingle.
type SingleResult struct {
	Outcome CreateOutcome
	// Notification is the inserted row (OutcomeCreated).
	Notification Notification
	// ExistingID is the notification that holds the dedupe_key
	// (OutcomeDuplicate). It is nil when that row left the dedupe index
	// between the insert and the look-up (it was cancelled before any
	// attempt); the race is harmless (ADR-0006 section 6).
	ExistingID *uuid.UUID
	// Response is the response built by respond (OutcomeCreated, when respond
	// is not nil) or the stored response to replay (OutcomeReplayed).
	Response *StoredResponse
}

// CreateSingle inserts one pending notification with the record layer and,
// when key is not nil, the request layer of duplicate prevention (ADR-0006
// section 11).
//
//   - With key, the key is claimed first (see IdempotencyKey). A live key
//     with the same request hash returns OutcomeReplayed; with a different
//     hash, ErrIdempotencyKeyReused. Nothing else happens in either case.
//   - The insert skips a row whose dedupe_key is held by another notification
//     (any status other than cancelled before an attempt). Then
//     OutcomeDuplicate is returned with the holder's id, and the transaction
//     rolls back: the key is not stored.
//   - Otherwise respond, when not nil, is called inside the transaction with
//     the inserted row; its response is stored on the key, and the key and
//     the notification commit together. respond is required with a key. An
//     error from respond rolls everything back and is returned.
//
// A concurrent create with the same dedupe_key waits for this transaction to
// end (the unique index), and then is created or reports this notification.
func (s *Store) CreateSingle(ctx context.Context, key *IdempotencyKey, n NewNotification,
	respond func(Notification) (StoredResponse, error),
) (SingleResult, error) {
	if key != nil && respond == nil {
		return SingleResult{}, errors.New("store: create with an idempotency key needs respond")
	}
	params, err := createParams(n)
	if err != nil {
		return SingleResult{}, err
	}
	var res SingleResult
	replay, err := s.createTx(ctx, key, func(q *db.Queries) (StoredResponse, error) {
		row, err := q.CreateNotification(ctx, params)
		if errors.Is(err, pgx.ErrNoRows) {
			if n.DedupeKey == nil {
				return StoredResponse{}, errors.New("create notification: no row inserted without a dedupe key")
			}
			existing, err := findConflicts(ctx, q, []string{*n.DedupeKey})
			if err != nil {
				return StoredResponse{}, err
			}
			res = SingleResult{Outcome: OutcomeDuplicate, ExistingID: existing[*n.DedupeKey]}
			return StoredResponse{}, errNothingCreated
		}
		if err != nil {
			return StoredResponse{}, fmt.Errorf("create notification: %w", err)
		}
		res = SingleResult{Outcome: OutcomeCreated, Notification: fromRow(row)}
		if respond == nil {
			return StoredResponse{}, nil
		}
		resp, err := respond(res.Notification)
		if err != nil {
			return StoredResponse{}, fmt.Errorf("respond: %w", err)
		}
		res.Response = &resp
		return resp, nil
	})
	switch {
	case errors.Is(err, errNothingCreated):
		return res, nil
	case err != nil:
		return SingleResult{}, err
	case replay != nil:
		return SingleResult{Outcome: OutcomeReplayed, Response: replay}, nil
	}
	return res, nil
}

// findConflicts returns the id of the notification that holds each key in
// the dedupe index. A key whose holder left the index has no entry.
func findConflicts(ctx context.Context, q *db.Queries, keys []string) (map[string]*uuid.UUID, error) {
	rows, err := q.FindDedupeConflicts(ctx, keys)
	if err != nil {
		return nil, fmt.Errorf("find dedupe conflicts: %w", err)
	}
	out := make(map[string]*uuid.UUID, len(rows))
	for _, r := range rows {
		if r.DedupeKey != nil {
			id := r.ID
			out[*r.DedupeKey] = &id
		}
	}
	return out, nil
}

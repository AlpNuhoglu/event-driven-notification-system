package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/store/db"
)

// ErrBatchNotFound is returned when no batch has the given id.
var ErrBatchNotFound = errors.New("store: batch not found")

// NewBatch is the input of CreateBatch.
type NewBatch struct {
	// Total is the number of items in the request, including the items the
	// API rejected in validation.
	Total int
	// Rejected is the number of items the API rejected in validation; they
	// are not in Items.
	Rejected int
	// CorrelationID is the batch row's correlation id.
	CorrelationID string
	// Items are the validated items, in request order. Each carries its own
	// priority, hence its own TTL and MaxFailures. Their BatchID is ignored:
	// every inserted row gets the new batch's id. Their dedupe keys must be
	// distinct (the API rejects duplicates inside a batch before the call).
	Items []NewNotification
}

// BatchItem is the result of one item of CreateBatch.
type BatchItem struct {
	// Inserted reports whether the item was inserted; ID is then its id.
	Inserted bool
	ID       uuid.UUID
	// ExistingID is the notification that holds the item's dedupe_key, when
	// the item was a duplicate. It is nil when that row left the dedupe index
	// between the insert and the look-up; the race is harmless (ADR-0006
	// section 6).
	ExistingID *uuid.UUID
}

// BatchResult is the result of CreateBatch.
type BatchResult struct {
	Outcome CreateOutcome
	// BatchID is the new batch (OutcomeCreated).
	BatchID uuid.UUID
	// Total, Accepted and Rejected are the counts of the batches row:
	// Accepted is the number of inserted items, Rejected the items rejected in
	// validation plus the duplicates. They are also set for OutcomeDuplicate,
	// although no row was stored.
	Total, Accepted, Rejected int
	// Items holds one result per NewBatch item, in the same order
	// (OutcomeCreated and OutcomeDuplicate).
	Items []BatchItem
	// Response is the response built by respond (OutcomeCreated) or the
	// stored response to replay (OutcomeReplayed).
	Response *StoredResponse
}

// CreateBatch creates a batch of 1 to 1,000 validated items with the record
// layer and, when key is not nil, the request layer of duplicate prevention
// (ADR-0006 sections 6 and 11).
//
//   - With key, the key is claimed first, as in CreateSingle: a live key
//     returns OutcomeReplayed or ErrIdempotencyKeyReused and nothing else
//     happens.
//   - The batches row and every item are inserted in one transaction, the
//     items with one INSERT … ON CONFLICT DO NOTHING; the dedupe keys that
//     were not inserted are looked up with one more query.
//   - No item inserted: OutcomeDuplicate with the per-item results, and the
//     transaction rolls back (no key, no batch, no notification).
//   - Otherwise respond is called inside the transaction with the result;
//     its response is stored on the key, and everything commits together.
//     An error from respond rolls everything back and is returned.
//
// Items are inserted in dedupe_key order, so concurrent batches whose keys
// overlap do not deadlock (see the InsertBatchNotifications query).
//
// Contract: Total = Rejected + len(Items), and the non-nil dedupe keys of
// Items are distinct; a violation is an error and nothing is done.
func (s *Store) CreateBatch(ctx context.Context, key *IdempotencyKey, b NewBatch,
	respond func(BatchResult) (StoredResponse, error),
) (BatchResult, error) {
	if respond == nil {
		return BatchResult{}, errors.New("store: create batch needs respond")
	}
	if len(b.Items) == 0 || b.Rejected < 0 || b.Total != b.Rejected+len(b.Items) {
		return BatchResult{}, fmt.Errorf("store: invalid batch counts: total %d, rejected %d, items %d",
			b.Total, b.Rejected, len(b.Items))
	}
	batchID, err := uuid.NewV7()
	if err != nil {
		return BatchResult{}, fmt.Errorf("new id: %w", err)
	}
	params, err := batchParams(batchID, b.Items)
	if err != nil {
		return BatchResult{}, err
	}
	total, err := toInt32(b.Total)
	if err != nil {
		return BatchResult{}, fmt.Errorf("total: %w", err)
	}
	// Total fits in int32, and every other count is at most Total.
	count := func(v int) int32 { return int32(v) } //nolint:gosec // bounded by Total, checked above

	var res BatchResult
	replay, err := s.createTx(ctx, key, func(q *db.Queries) (StoredResponse, error) {
		// The counts assume every item is inserted; they are corrected
		// below when there are duplicates.
		if err := q.InsertBatch(ctx, db.InsertBatchParams{
			ID:            batchID,
			Total:         total,
			Accepted:      count(len(b.Items)),
			Rejected:      count(b.Rejected),
			CorrelationID: b.CorrelationID,
		}); err != nil {
			return StoredResponse{}, fmt.Errorf("insert batch: %w", err)
		}
		rows, err := q.InsertBatchNotifications(ctx, params)
		if err != nil {
			return StoredResponse{}, fmt.Errorf("insert batch notifications: %w", err)
		}
		res, err = batchItems(ctx, q, b, params.Ids, rows)
		if err != nil {
			return StoredResponse{}, err
		}
		if res.Accepted == 0 {
			res.Outcome = OutcomeDuplicate
			return StoredResponse{}, errNothingCreated
		}
		if res.Accepted != len(b.Items) {
			n, err := q.SetBatchCounts(ctx, db.SetBatchCountsParams{
				ID:       batchID,
				Accepted: count(res.Accepted),
				Rejected: count(res.Rejected),
			})
			if err != nil {
				return StoredResponse{}, fmt.Errorf("set batch counts: %w", err)
			}
			if n != 1 {
				return StoredResponse{}, fmt.Errorf("set batch counts: updated %d rows, want 1", n)
			}
		}
		res.Outcome = OutcomeCreated
		res.BatchID = batchID
		resp, err := respond(res)
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
		return BatchResult{}, err
	case replay != nil:
		return BatchResult{Outcome: OutcomeReplayed, Response: replay}, nil
	}
	return res, nil
}

// batchItems maps the inserted rows back to the items and looks up the
// holders of the dedupe keys that were not inserted.
func batchItems(ctx context.Context, q *db.Queries, b NewBatch, ids []uuid.UUID,
	inserted []db.InsertBatchNotificationsRow,
) (BatchResult, error) {
	isInserted := make(map[uuid.UUID]bool, len(inserted))
	for _, r := range inserted {
		isInserted[r.ID] = true
	}
	var missing []string
	for i, n := range b.Items {
		if isInserted[ids[i]] {
			continue
		}
		if n.DedupeKey == nil {
			return BatchResult{}, fmt.Errorf("insert batch notifications: item %d without a dedupe key was not inserted", i)
		}
		missing = append(missing, *n.DedupeKey)
	}
	existing := map[string]*uuid.UUID{}
	if len(missing) > 0 {
		var err error
		if existing, err = findConflicts(ctx, q, missing); err != nil {
			return BatchResult{}, err
		}
	}
	res := BatchResult{Total: b.Total, Items: make([]BatchItem, len(b.Items))}
	for i, n := range b.Items {
		if isInserted[ids[i]] {
			res.Items[i] = BatchItem{Inserted: true, ID: ids[i]}
			res.Accepted++
			continue
		}
		res.Items[i] = BatchItem{ExistingID: existing[*n.DedupeKey]}
	}
	res.Rejected = b.Total - res.Accepted
	return res, nil
}

// batchParams builds the column arrays of InsertBatchNotifications, one
// element per item and a new UUIDv7 id for each. The nullable columns are
// pointer slices, so that a nil element is a NULL.
func batchParams(batchID uuid.UUID, items []NewNotification) (db.InsertBatchNotificationsParams, error) {
	n := len(items)
	p := db.InsertBatchNotificationsParams{
		BatchID:        batchID,
		Ids:            make([]uuid.UUID, n),
		Channels:       make([]string, n),
		Priorities:     make([]int16, n),
		Recipients:     make([]string, n),
		Contents:       make([]string, n),
		TemplateVars:   make([][]byte, n),
		Ttls:           make([]pgtype.Interval, n),
		MaxFailures:    make([]int32, n),
		CorrelationIds: make([]string, n),
		TraceParents:   make([]string, n),
	}
	var (
		dedupeKeys       = make([]*string, n)
		smsEncodings     = make([]*string, n)
		smsSegments      = make([]*int16, n)
		templateNames    = make([]*string, n)
		templateVersions = make([]*int32, n)
		scheduledAts     = make([]*time.Time, n)
		expiresAts       = make([]*time.Time, n)
	)
	seen := make(map[string]bool, n)
	for i, it := range items {
		if it.DedupeKey != nil {
			if seen[*it.DedupeKey] {
				return db.InsertBatchNotificationsParams{}, fmt.Errorf("store: item %d repeats a dedupe key of the batch", i)
			}
			seen[*it.DedupeKey] = true
		}
		id, err := uuid.NewV7()
		if err != nil {
			return db.InsertBatchNotificationsParams{}, fmt.Errorf("new id: %w", err)
		}
		maxFailures, err := toInt32(it.MaxFailures)
		if err != nil {
			return db.InsertBatchNotificationsParams{}, fmt.Errorf("item %d max failures: %w", i, err)
		}
		p.Ids[i] = id
		dedupeKeys[i] = it.DedupeKey
		p.Channels[i] = string(it.Channel)
		p.Priorities[i] = int16(it.Priority)
		p.Recipients[i] = it.Recipient.Reveal() // the database stores the real value
		p.Contents[i] = it.Content
		smsEncodings[i] = it.SMSEncoding
		smsSegments[i] = it.SMSSegments
		templateNames[i] = it.TemplateName
		templateVersions[i] = it.TemplateVersion
		p.TemplateVars[i] = it.TemplateVars // nil is NULL
		scheduledAts[i] = it.ScheduledAt
		expiresAts[i] = it.ExpiresAt
		p.Ttls[i] = interval(it.TTL)
		p.MaxFailures[i] = maxFailures
		p.CorrelationIds[i] = it.CorrelationID
		p.TraceParents[i] = it.TraceParent
	}
	p.DedupeKeys = dedupeKeys
	p.SmsEncodings = smsEncodings
	p.SmsSegments = smsSegments
	p.TemplateNames = templateNames
	p.TemplateVersions = templateVersions
	p.ScheduledAts = scheduledAts
	p.ExpiresAts = expiresAts
	return p, nil
}

// Batch is a batches row with the per-status counts of its notifications.
type Batch struct {
	ID            uuid.UUID
	Total         int
	Accepted      int
	Rejected      int
	CorrelationID string
	CreatedAt     time.Time
	// StatusCounts holds the number of the batch's notifications in each
	// status; statuses without a notification are absent.
	StatusCounts map[domain.Status]int
}

// GetBatch returns the batch with the given id and the per-status counts of
// its notifications (GROUP BY status), or ErrBatchNotFound. The two are read
// with two statements; the counts move on as the notifications progress.
func (s *Store) GetBatch(ctx context.Context, id uuid.UUID) (Batch, error) {
	row, err := s.q.GetBatch(ctx, id)
	if errors.Is(err, pgx.ErrNoRows) {
		return Batch{}, ErrBatchNotFound
	}
	if err != nil {
		return Batch{}, fmt.Errorf("get batch: %w", err)
	}
	counts, err := s.q.CountBatchByStatus(ctx, &id)
	if err != nil {
		return Batch{}, fmt.Errorf("count batch by status: %w", err)
	}
	out := Batch{
		ID:            row.ID,
		Total:         int(row.Total),
		Accepted:      int(row.Accepted),
		Rejected:      int(row.Rejected),
		CorrelationID: row.CorrelationID,
		CreatedAt:     row.CreatedAt,
		StatusCounts:  make(map[domain.Status]int, len(counts)),
	}
	for _, c := range counts {
		out.StatusCounts[domain.Status(c.Status)] = int(c.Count)
	}
	return out, nil
}

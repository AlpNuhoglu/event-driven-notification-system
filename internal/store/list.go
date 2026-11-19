package store

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/store/db"
)

// Bounds of ListFilter.Limit.
const (
	MinListLimit = 1
	MaxListLimit = 200
)

// ListFilter selects one page of the listing. Every filter that is nil is
// not applied. Limit is required.
type ListFilter struct {
	Status   *domain.Status
	Channel  *domain.Channel
	Priority *domain.Priority
	BatchID  *uuid.UUID
	// CreatedFrom is inclusive, CreatedTo exclusive. A range with
	// CreatedFrom at or after CreatedTo selects nothing.
	CreatedFrom, CreatedTo *time.Time
	// After continues after a cursor returned by an earlier call.
	After *domain.Cursor
	// Limit is the page size, MinListLimit to MaxListLimit.
	Limit int
}

// ListNotifications returns one page of notifications, newest first
// (created_at descending, then id descending), and the cursor of the next
// page, or nil when this is the last one. It is a keyset pagination, so a
// row inserted while a client pages does not shift the pages: a row newer
// than the cursor is never returned by a later page, and no row older than
// it is skipped or repeated.
func (s *Store) ListNotifications(ctx context.Context, f ListFilter) ([]Notification, *domain.Cursor, error) {
	if f.Limit < MinListLimit || f.Limit > MaxListLimit {
		return nil, nil, fmt.Errorf("store: list limit %d outside %d to %d", f.Limit, MinListLimit, MaxListLimit)
	}
	var channel *string
	if f.Channel != nil {
		c := string(*f.Channel)
		channel = &c
	}
	var priority *int16
	if f.Priority != nil {
		p := int16(*f.Priority)
		priority = &p
	}
	var cursorAt *time.Time
	var cursorID *uuid.UUID
	if f.After != nil {
		at, id := f.After.CreatedAt, f.After.ID
		cursorAt, cursorID = &at, &id
	}
	// One more row than the page tells whether a next page exists.
	// Limit is at most MaxListLimit, so Limit + 1 fits in int32.
	pageLimit := int32(f.Limit + 1) //nolint:gosec // bounded above

	var rows []db.Notification
	var err error
	if f.Status != nil {
		rows, err = s.custom.ListNotificationsByStatus(ctx, db.ListNotificationsByStatusParams{
			Status:          string(*f.Status),
			CursorCreatedAt: cursorAt,
			CursorID:        cursorID,
			CreatedFrom:     f.CreatedFrom,
			CreatedTo:       f.CreatedTo,
			Channel:         channel,
			Priority:        priority,
			BatchID:         f.BatchID,
			PageLimit:       pageLimit,
		})
	} else {
		rows, err = s.custom.ListNotifications(ctx, db.ListNotificationsParams{
			CursorCreatedAt: cursorAt,
			CursorID:        cursorID,
			CreatedFrom:     f.CreatedFrom,
			CreatedTo:       f.CreatedTo,
			Channel:         channel,
			Priority:        priority,
			BatchID:         f.BatchID,
			PageLimit:       pageLimit,
		})
	}
	if err != nil {
		return nil, nil, fmt.Errorf("list notifications: %w", err)
	}
	var next *domain.Cursor
	if len(rows) > f.Limit {
		rows = rows[:f.Limit]
		last := rows[len(rows)-1]
		next = &domain.Cursor{CreatedAt: last.CreatedAt, ID: last.ID}
	}
	page := make([]Notification, len(rows))
	for i, r := range rows {
		page[i] = fromRow(r)
	}
	return page, next, nil
}

// customPlanDB runs a statement with pgx.QueryExecModeDescribeExec: the
// unnamed statement, which Postgres plans with the bound values every time
// and which never enters pgx's statement cache. Two kinds of statement need
// it:
//
//   - The listing: its optional filters ("$n IS NULL OR col = $n") make the
//     row estimate of a generic plan meaningless, and the EXPLAIN check showed
//     such a plan sorting the whole status.
//   - The expiry pass and the idempotency-key cleanup (expiry.go): they run
//     every second or minute and usually find nothing, so Postgres caches a
//     generic plan built for one or two rows. When a backlog expires at once
//     (an outage), that plan joins the batch against every expired row. With
//     200,000 expired keys one delete of 1,000 took 12 s while holding its
//     row locks; planned with the values it takes 4 ms.
//
// QueryRow passes through unchanged.
type customPlanDB struct{ db.DBTX }

func (c customPlanDB) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	return c.DBTX.Query(ctx, sql, append([]any{pgx.QueryExecModeDescribeExec}, args...)...)
}

func (c customPlanDB) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	return c.DBTX.Exec(ctx, sql, append([]any{pgx.QueryExecModeDescribeExec}, args...)...)
}

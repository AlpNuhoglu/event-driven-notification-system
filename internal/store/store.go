// Package store is the repository layer: every state transition of a
// notification is one conditional UPDATE in Postgres, executed by a function
// of this package that returns the transition's domain.StatusChanged event
// (ADR-0004). No other package updates notifications.status.
package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/store/db"
)

var (
	// ErrNotFound is returned when no notification has the given id.
	ErrNotFound = errors.New("store: notification not found")
	// ErrDuplicate is returned by CreateNotification when another
	// notification holds the dedupe_key.
	ErrDuplicate = errors.New("store: dedupe_key is held by another notification")
	// ErrNotApplied is returned when a transition's guard matched zero rows:
	// another actor changed the row first.
	ErrNotApplied = errors.New("store: transition not applied")
)

// Profiles maps each priority to its retry back-off profile.
type Profiles map[domain.Priority]domain.Profile

// decideFunc has the signature of domain.DecideRetry.
type decideFunc func(outcome domain.Outcome, failures, maxFailures int, expiresAt time.Time,
	retryAfter time.Duration, profile domain.Profile, now time.Time, jitter float64) domain.Decision

// Store executes the notification queries and transitions.
type Store struct {
	pool     *pgxpool.Pool
	q        *db.Queries
	profiles Profiles
	decide   decideFunc
}

// New returns a Store on pool. profiles supplies the back-off profile of each
// priority for the retry decision of transitions 5, 6 and 7.
func New(pool *pgxpool.Pool, profiles Profiles) *Store {
	return &Store{pool: pool, q: db.New(pool), profiles: profiles, decide: domain.DecideRetry}
}

// Notification is one row of the notifications table.
type Notification struct {
	ID                uuid.UUID
	BatchID           *uuid.UUID
	DedupeKey         *string
	Channel           domain.Channel
	Priority          domain.Priority
	Recipient         domain.Recipient
	Content           string
	SMSEncoding       *string
	SMSSegments       *int16
	TemplateName      *string
	TemplateVersion   *int32
	TemplateVars      []byte
	Status            domain.Status
	FailureReason     *domain.FailureReason
	DueAt             time.Time
	ScheduledAt       *time.Time
	ExpiresAt         time.Time
	AttemptCount      int
	Failures          int
	MaxFailures       int
	QueuedAt          *time.Time
	LeaseExpiresAt    *time.Time
	Provider          *string
	ProviderMessageID *string
	CorrelationID     string
	TraceParent       string
	Version           int64
	CreatedAt         time.Time
	UpdatedAt         time.Time
	SentAt            *time.Time
}

// Event returns the StatusChanged event that describes the row's current state.
func (n Notification) Event() domain.StatusChanged {
	return domain.StatusChanged{
		ID:            n.ID,
		BatchID:       n.BatchID,
		Channel:       n.Channel,
		Priority:      n.Priority,
		Status:        n.Status,
		Version:       n.Version,
		FailureReason: n.FailureReason,
		AttemptCount:  n.AttemptCount,
		UpdatedAt:     n.UpdatedAt,
	}
}

// Attempt is one row of the notification_attempts table.
type Attempt struct {
	NotificationID    uuid.UUID
	AttemptNo         int
	Channel           domain.Channel
	Provider          string
	Outcome           domain.Outcome
	HTTPStatus        *int
	LatencyMS         *int
	Error             *string
	ProviderMessageID *string
	StartedAt         time.Time
	FinishedAt        *time.Time
}

// NewNotification is the input of CreateNotification. DueAt and ExpiresAt are
// computed from the database clock: due_at = COALESCE(ScheduledAt, now()) and,
// unless ExpiresAt is given, expires_at = due_at + TTL.
type NewNotification struct {
	BatchID         *uuid.UUID
	DedupeKey       *string
	Channel         domain.Channel
	Priority        domain.Priority
	Recipient       domain.Recipient
	Content         string
	SMSEncoding     *string
	SMSSegments     *int16
	TemplateName    *string
	TemplateVersion *int32
	TemplateVars    []byte
	ScheduledAt     *time.Time
	ExpiresAt       *time.Time
	TTL             time.Duration
	MaxFailures     int
	CorrelationID   string
	TraceParent     string
}

// CreateNotification inserts a pending notification with a new UUIDv7 id,
// without an idempotency key. It is CreateSingle for callers that need no
// more; a dedupe_key conflict returns ErrDuplicate.
func (s *Store) CreateNotification(ctx context.Context, n NewNotification) (Notification, error) {
	res, err := s.CreateSingle(ctx, nil, n, nil)
	if err != nil {
		return Notification{}, err
	}
	if res.Outcome != OutcomeCreated {
		return Notification{}, ErrDuplicate
	}
	return res.Notification, nil
}

// createParams builds the parameters of the single-row insert with a new
// UUIDv7 id.
func createParams(n NewNotification) (db.CreateNotificationParams, error) {
	id, err := uuid.NewV7()
	if err != nil {
		return db.CreateNotificationParams{}, fmt.Errorf("new id: %w", err)
	}
	maxFailures, err := toInt32(n.MaxFailures)
	if err != nil {
		return db.CreateNotificationParams{}, fmt.Errorf("max failures: %w", err)
	}
	return db.CreateNotificationParams{
		ID:              id,
		BatchID:         n.BatchID,
		DedupeKey:       n.DedupeKey,
		Channel:         string(n.Channel),
		Priority:        int16(n.Priority),
		Recipient:       n.Recipient.Reveal(), // the database stores the real value
		Content:         n.Content,
		SmsEncoding:     n.SMSEncoding,
		SmsSegments:     n.SMSSegments,
		TemplateName:    n.TemplateName,
		TemplateVersion: n.TemplateVersion,
		TemplateVars:    n.TemplateVars,
		ScheduledAt:     n.ScheduledAt,
		ExpiresAt:       n.ExpiresAt,
		Ttl:             interval(n.TTL),
		MaxFailures:     maxFailures,
		CorrelationID:   n.CorrelationID,
		TraceParent:     n.TraceParent,
	}, nil
}

// GetNotification returns the notification with the given id, or ErrNotFound.
func (s *Store) GetNotification(ctx context.Context, id uuid.UUID) (Notification, error) {
	row, err := s.q.GetNotification(ctx, id)
	if errors.Is(err, pgx.ErrNoRows) {
		return Notification{}, ErrNotFound
	}
	if err != nil {
		return Notification{}, fmt.Errorf("get notification: %w", err)
	}
	return fromRow(row), nil
}

// ListAttempts returns the attempts of a notification ordered by attempt number.
func (s *Store) ListAttempts(ctx context.Context, id uuid.UUID) ([]Attempt, error) {
	rows, err := s.q.ListAttempts(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("list attempts: %w", err)
	}
	out := make([]Attempt, 0, len(rows))
	for _, r := range rows {
		out = append(out, Attempt{
			NotificationID:    r.NotificationID,
			AttemptNo:         int(r.AttemptNo),
			Channel:           domain.Channel(r.Channel),
			Provider:          r.Provider,
			Outcome:           domain.Outcome(r.Outcome),
			HTTPStatus:        intPtr(r.HttpStatus),
			LatencyMS:         intPtr(r.LatencyMs),
			Error:             r.Error,
			ProviderMessageID: r.ProviderMessageID,
			StartedAt:         r.StartedAt,
			FinishedAt:        r.FinishedAt,
		})
	}
	return out, nil
}

// inTx runs fn in a transaction and commits when fn returns nil. Otherwise,
// and also when fn panics or exits its goroutine, the transaction is rolled
// back with a context that is not cancelled, so the row locks are released
// promptly even when ctx is done.
func (s *Store) inTx(ctx context.Context, fn func(q *db.Queries) error) (err error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			err = errors.Join(err, rollback(ctx, tx))
		}
	}()
	if err := fn(s.q.WithTx(tx)); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	committed = true
	return nil
}

func rollback(ctx context.Context, tx pgx.Tx) error {
	err := tx.Rollback(context.WithoutCancel(ctx))
	if err == nil || errors.Is(err, pgx.ErrTxClosed) {
		return nil
	}
	return fmt.Errorf("rollback: %w", err)
}

func (s *Store) profile(p domain.Priority) (domain.Profile, error) {
	prof, ok := s.profiles[p]
	if !ok {
		return domain.Profile{}, fmt.Errorf("no retry profile for priority %s", p)
	}
	return prof, nil
}

func fromRow(r db.Notification) Notification {
	var reason *domain.FailureReason
	if r.FailureReason != nil {
		fr := domain.FailureReason(*r.FailureReason)
		reason = &fr
	}
	return Notification{
		ID:                r.ID,
		BatchID:           r.BatchID,
		DedupeKey:         r.DedupeKey,
		Channel:           domain.Channel(r.Channel),
		Priority:          domain.Priority(r.Priority),
		Recipient:         domain.Recipient(r.Recipient),
		Content:           r.Content,
		SMSEncoding:       r.SmsEncoding,
		SMSSegments:       r.SmsSegments,
		TemplateName:      r.TemplateName,
		TemplateVersion:   r.TemplateVersion,
		TemplateVars:      r.TemplateVars,
		Status:            domain.Status(r.Status),
		FailureReason:     reason,
		DueAt:             r.DueAt,
		ScheduledAt:       r.ScheduledAt,
		ExpiresAt:         r.ExpiresAt,
		AttemptCount:      int(r.AttemptCount),
		Failures:          int(r.Failures),
		MaxFailures:       int(r.MaxFailures),
		QueuedAt:          r.QueuedAt,
		LeaseExpiresAt:    r.LeaseExpiresAt,
		Provider:          r.Provider,
		ProviderMessageID: r.ProviderMessageID,
		CorrelationID:     r.CorrelationID,
		TraceParent:       r.TraceParent,
		Version:           r.Version,
		CreatedAt:         r.CreatedAt,
		UpdatedAt:         r.UpdatedAt,
		SentAt:            r.SentAt,
	}
}

// interval converts a duration to a Postgres interval with microsecond precision.
func interval(d time.Duration) pgtype.Interval {
	return pgtype.Interval{Microseconds: d.Microseconds(), Valid: true}
}

func toInt32(v int) (int32, error) {
	if v < -1<<31 || v > 1<<31-1 {
		return 0, fmt.Errorf("value %d out of int32 range", v)
	}
	return int32(v), nil
}

func intPtr(v *int32) *int {
	if v == nil {
		return nil
	}
	i := int(*v)
	return &i
}

func int32Ptr(v *int) (*int32, error) {
	if v == nil {
		return nil, nil
	}
	i, err := toInt32(*v)
	if err != nil {
		return nil, err
	}
	return &i, nil
}

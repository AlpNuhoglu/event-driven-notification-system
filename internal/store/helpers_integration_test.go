//go:build integration

package store

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/testutil"
)

func TestMain(m *testing.M) { testutil.Main(m) }

// Durations are parameters, in milliseconds.
const (
	testLease    = 60_000 * time.Millisecond // long: lease expiry is produced by back-dating
	testTTL      = 3_600_000 * time.Millisecond
	waitTimeout  = 10_000 * time.Millisecond
	testDelay    = 60_000 * time.Millisecond
	testProvider = "mock"
)

// profilesWith returns profiles whose retry delay is exactly base for every
// jitter value: the floor equals the cap.
func profilesWith(base time.Duration) Profiles {
	p := domain.Profile{Base: base, Cap: base, MinDelay: base}
	return Profiles{domain.PriorityHigh: p, domain.PriorityNormal: p, domain.PriorityLow: p}
}

// zeroDelay makes retried rows due immediately; delayed keeps them in the future.
var (
	zeroDelay = profilesWith(0)
	delayed   = profilesWith(testDelay)
)

func newTestStore(t *testing.T, profiles Profiles) (*Store, *pgxpool.Pool) {
	t.Helper()
	pool := testutil.NewPool(t, OpenPool)
	return New(pool, profiles), pool
}

func ctxT(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
	t.Cleanup(cancel)
	return ctx
}

func baseNew() NewNotification {
	return NewNotification{
		Channel:       domain.ChannelSMS,
		Priority:      domain.PriorityNormal,
		Recipient:     domain.Recipient("+905551234567"),
		Content:       "Your code is 1234",
		TTL:           testTTL,
		MaxFailures:   5,
		CorrelationID: "corr-1",
		TraceParent:   "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
	}
}

func create(t *testing.T, s *Store, mods ...func(*NewNotification)) Notification {
	t.Helper()
	n := baseNew()
	for _, m := range mods {
		m(&n)
	}
	row, err := s.CreateNotification(ctxT(t), n)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	return row
}

func withMaxFailures(m int) func(*NewNotification) {
	return func(n *NewNotification) { n.MaxFailures = m }
}

func withLane(c domain.Channel, p domain.Priority) func(*NewNotification) {
	return func(n *NewNotification) { n.Channel, n.Priority = c, p }
}

func get(t *testing.T, s *Store, id uuid.UUID) Notification {
	t.Helper()
	n, err := s.GetNotification(ctxT(t), id)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	return n
}

func attempts(t *testing.T, s *Store, id uuid.UUID) []Attempt {
	t.Helper()
	a, err := s.ListAttempts(ctxT(t), id)
	if err != nil {
		t.Fatalf("list attempts: %v", err)
	}
	return a
}

func exec(t *testing.T, pool *pgxpool.Pool, sql string, args ...any) {
	t.Helper()
	if _, err := pool.Exec(ctxT(t), sql, args...); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
}

// expireLease back-dates the lease of a processing row (a lease-expiry case).
func expireLease(t *testing.T, pool *pgxpool.Pool, id uuid.UUID) {
	t.Helper()
	exec(t, pool, `UPDATE notifications SET lease_expires_at = now() - interval '1 millisecond' WHERE id = $1`, id)
}

// expire back-dates expires_at (an expiry case).
func expire(t *testing.T, pool *pgxpool.Pool, id uuid.UUID) {
	t.Helper()
	exec(t, pool, `UPDATE notifications SET expires_at = now() - interval '1 millisecond' WHERE id = $1`, id)
}

func claim(t *testing.T, s *Store, id uuid.UUID) Claim {
	t.Helper()
	c, _, err := s.Claim(ctxT(t), id, testLease, testProvider)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	return c
}

func publishAll(_ context.Context, rows []DueRow) ([]uuid.UUID, error) {
	ids := make([]uuid.UUID, len(rows))
	for i, r := range rows {
		ids[i] = r.ID
	}
	return ids, nil
}

func ptr[T any](v T) *T { return &v }

// immutableCols are the columns ADR-0004 section 6 declares immutable after insert.
type immutableCols struct {
	ID              uuid.UUID
	BatchID         *uuid.UUID
	DedupeKey       *string
	Channel         domain.Channel
	Priority        domain.Priority
	Recipient       string
	Content         string
	TemplateName    *string
	TemplateVersion *int32
	TemplateVars    string
	ScheduledAt     *time.Time
	CreatedAt       time.Time
	CorrelationID   string
	TraceParent     string
}

func immutableOf(n Notification) immutableCols {
	return immutableCols{
		ID: n.ID, BatchID: n.BatchID, DedupeKey: n.DedupeKey, Channel: n.Channel, Priority: n.Priority,
		Recipient: n.Recipient.Reveal(), Content: n.Content, TemplateName: n.TemplateName,
		TemplateVersion: n.TemplateVersion, TemplateVars: string(n.TemplateVars), ScheduledAt: n.ScheduledAt,
		CreatedAt: n.CreatedAt, CorrelationID: n.CorrelationID, TraceParent: n.TraceParent,
	}
}

// checkTransition asserts the rules every transition obeys: version + 1,
// updated_at set by the transition, the event matches the stored row, and no
// immutable column changed.
func checkTransition(t *testing.T, before, after Notification, ev domain.StatusChanged, want domain.Status) {
	t.Helper()
	if after.Status != want {
		t.Errorf("status = %s, want %s", after.Status, want)
	}
	if after.Version != before.Version+1 {
		t.Errorf("version = %d, want %d", after.Version, before.Version+1)
	}
	if !after.UpdatedAt.After(before.UpdatedAt) {
		t.Errorf("updated_at %v not after %v", after.UpdatedAt, before.UpdatedAt)
	}
	// The published members match the row. The correlation id is not one of
	// them: transition 8 returns only the event's columns and leaves it
	// empty; every other transition carries the row's.
	published := func(e domain.StatusChanged) domain.StatusChanged { e.CorrelationID = ""; return e }
	if !reflect.DeepEqual(published(ev), published(after.Event())) {
		t.Errorf("event %+v does not match stored row %+v", ev, after.Event())
	}
	if ev.CorrelationID != "" && ev.CorrelationID != after.CorrelationID {
		t.Errorf("event correlation id %q, row %q", ev.CorrelationID, after.CorrelationID)
	}
	if ev.CorrelationID == "" && after.CorrelationID != "" && want != domain.StatusPending {
		t.Errorf("a transition to %s returned an event without the row's correlation id", want)
	}
	if !reflect.DeepEqual(immutableOf(before), immutableOf(after)) {
		t.Errorf("immutable columns changed:\nbefore %+v\nafter  %+v", immutableOf(before), immutableOf(after))
	}
}

// transitionOf returns which transition a Sent describes (Late and From),
// without its end-to-end latency, which depends on the clock.
func transitionOf(s Sent) Sent { return Sent{Late: s.Late, From: s.From} }

func sqlState(err error) string {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code
	}
	return ""
}

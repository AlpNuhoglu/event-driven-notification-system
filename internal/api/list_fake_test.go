package api

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/store"
)

// fakeRepo answers the listing and the attempts read with an empty result
// (or its err); a test that needs more uses listRepo.
func (f *fakeRepo) ListNotifications(context.Context, store.ListFilter) ([]store.Notification, *domain.Cursor, error) {
	return nil, nil, f.err
}

func (f *fakeRepo) ListAttempts(context.Context, uuid.UUID) ([]store.Attempt, error) {
	if f.err != nil {
		return nil, f.err
	}
	return nil, store.ErrNotFound
}

// listRepo is a fakeRepo whose listing and attempts read return what the
// test sets, and which records the filter it was called with.
type listRepo struct {
	fakeRepo
	rows     []store.Notification
	next     *domain.Cursor
	attempts []store.Attempt
	filter   store.ListFilter
	calls    int
}

func (l *listRepo) ListNotifications(_ context.Context, f store.ListFilter) ([]store.Notification, *domain.Cursor, error) {
	l.calls++
	l.filter = f
	if l.err != nil {
		return nil, nil, l.err
	}
	return l.rows, l.next, nil
}

func (l *listRepo) ListAttempts(context.Context, uuid.UUID) ([]store.Attempt, error) {
	if l.err != nil {
		return nil, l.err
	}
	return l.attempts, nil
}

func newListAPI(t *testing.T, repo *listRepo) (*apiClient, *syncBuffer) {
	t.Helper()
	logs := &syncBuffer{}
	h := NewHandler(Options{
		Repository:   repo,
		DB:           repo,
		Logger:       testLogger(logs),
		Retry:        testRetry(),
		MaxBodyBytes: 1024,
		DBTimeout:    1000 * time.Millisecond,
	})
	return newAPIClient(t, h), logs
}

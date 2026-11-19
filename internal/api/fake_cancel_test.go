package api

import (
	"context"
	"time"

	"github.com/google/uuid"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/store"
)

// The canceller methods of fakeRepo: cancel and redrive are tested against
// Postgres; without a configured error every id is unknown.

func (f *fakeRepo) CancelNotification(context.Context, uuid.UUID) (store.Cancelled, domain.StatusChanged, error) {
	if f.err != nil {
		return store.Cancelled{}, domain.StatusChanged{}, f.err
	}
	return store.Cancelled{}, domain.StatusChanged{}, store.ErrNotFound
}

func (f *fakeRepo) CancelBatch(context.Context, uuid.UUID) (store.CancelledBatch, []domain.StatusChanged, error) {
	if f.err != nil {
		return store.CancelledBatch{}, nil, f.err
	}
	return store.CancelledBatch{}, nil, store.ErrBatchNotFound
}

func (f *fakeRepo) RedriveNotification(context.Context, uuid.UUID, *time.Time, time.Duration) (store.Notification, domain.StatusChanged, error) {
	if f.err != nil {
		return store.Notification{}, domain.StatusChanged{}, f.err
	}
	return store.Notification{}, domain.StatusChanged{}, store.ErrNotFound
}

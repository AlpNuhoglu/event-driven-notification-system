package api

import (
	"context"
	"time"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/store"
)

// The summarizer methods of fakeRepo: the figures are tested against
// Postgres and a counting wrapper; here a lane has nothing and an attempt
// never finished, unless an error is configured.

func (f *fakeRepo) SummaryQueued(context.Context) ([]store.LaneCount, error) { return nil, f.err }

func (f *fakeRepo) SummaryBacklogDue(context.Context, time.Time) ([]store.LaneCount, error) {
	return nil, f.err
}

func (f *fakeRepo) SummaryWindow(context.Context, time.Time) (map[domain.Channel]store.ChannelWindow, error) {
	return nil, f.err
}

package api

import (
	"context"
	"errors"

	"github.com/google/uuid"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/api/apigen"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/store"
)

// lister is the part of the store that the listing and the attempts read use.
type lister interface {
	ListNotifications(ctx context.Context, f store.ListFilter) ([]store.Notification, *domain.Cursor, error)
	ListAttempts(ctx context.Context, id uuid.UUID) ([]store.Attempt, error)
}

// ListNotifications returns one page, newest first. createGuard has
// validated the query string and hands the filter over in the context.
func (h *handlers) ListNotifications(ctx context.Context, _ apigen.ListNotificationsRequestObject) (apigen.ListNotificationsResponseObject, error) {
	f, ok := listFilterFrom(ctx)
	if !ok {
		return nil, &problem{kind: kindInternal, detail: kindInternal.detail, cause: errors.New("list request without a validated filter")}
	}
	dbCtx, cancel := context.WithTimeout(ctx, h.dbTimeout)
	defer cancel()
	rows, next, err := h.repo.ListNotifications(dbCtx, f)
	if err != nil {
		return nil, storeProblem(err)
	}
	body := apigen.NotificationList{Data: make([]apigen.Notification, len(rows))}
	for i, n := range rows {
		body.Data[i] = toNotification(n)
	}
	if next != nil {
		c := next.Encode()
		body.NextCursor = &c
	}
	return apigen.ListNotifications200JSONResponse{
		Body:    body,
		Headers: apigen.ListNotifications200ResponseHeaders{XRequestID: stateFrom(ctx).correlationID},
	}, nil
}

// ListNotificationAttempts returns the delivery attempts of a notification in
// the order they were made, or 404 when no notification has the id.
func (h *handlers) ListNotificationAttempts(ctx context.Context, req apigen.ListNotificationAttemptsRequestObject) (apigen.ListNotificationAttemptsResponseObject, error) {
	id, p := pathID(req.Id)
	if p != nil {
		return nil, p
	}
	dbCtx, cancel := context.WithTimeout(ctx, h.dbTimeout)
	defer cancel()
	rows, err := h.repo.ListAttempts(dbCtx, id)
	if errors.Is(err, store.ErrNotFound) {
		return nil, newProblem(kindNotFound, "No notification has this id.")
	}
	if err != nil {
		return nil, storeProblem(err)
	}
	body := apigen.AttemptList{Data: make([]apigen.Attempt, len(rows))}
	for i, a := range rows {
		body.Data[i] = toAttempt(a)
	}
	return apigen.ListNotificationAttempts200JSONResponse{
		Body:    body,
		Headers: apigen.ListNotificationAttempts200ResponseHeaders{XRequestID: stateFrom(ctx).correlationID},
	}, nil
}

// toAttempt builds the response representation of an attempt.
func toAttempt(a store.Attempt) apigen.Attempt {
	i32 := func(v *int) *int32 {
		if v == nil {
			return nil
		}
		n := int32Of(*v)
		return &n
	}
	return apigen.Attempt{
		AttemptNo:         int32Of(a.AttemptNo),
		Provider:          a.Provider,
		Outcome:           apigen.Outcome(a.Outcome),
		HttpStatus:        i32(a.HTTPStatus),
		LatencyMs:         i32(a.LatencyMS),
		Error:             a.Error,
		ProviderMessageId: a.ProviderMessageID,
		StartedAt:         a.StartedAt.UTC(),
		FinishedAt:        utcPtr(a.FinishedAt),
	}
}

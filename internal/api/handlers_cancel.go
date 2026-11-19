package api

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/api/apigen"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/store"
)

// canceller is the part of the store that cancel and redrive use. Each
// method returns the events of the transitions it made; the handler
// publishes them after the method has returned, so after the commit.
type canceller interface {
	CancelNotification(ctx context.Context, id uuid.UUID) (store.Cancelled, domain.StatusChanged, error)
	CancelBatch(ctx context.Context, batchID uuid.UUID) (store.CancelledBatch, []domain.StatusChanged, error)
	RedriveNotification(ctx context.Context, id uuid.UUID, newExpiresAt *time.Time, minLead time.Duration) (store.Notification, domain.StatusChanged, error)
}

// CancelNotification performs transition 9 (ADR-0006 section 7). createGuard
// has checked that the request has no body. A repeated cancel answers 200
// again with the current row and publishes nothing.
func (h *handlers) CancelNotification(ctx context.Context, req apigen.CancelNotificationRequestObject) (apigen.CancelNotificationResponseObject, error) {
	id, p := pathID(req.Id)
	if p != nil {
		return nil, p
	}
	dbCtx, cancel := context.WithTimeout(ctx, h.dbTimeout)
	defer cancel()
	res, ev, err := h.repo.CancelNotification(dbCtx, id)
	if err != nil {
		return nil, h.transitionProblem(err)
	}
	if res.Applied {
		h.events.Publish(ev)
	}
	return apigen.CancelNotification200JSONResponse{
		Body: apigen.CancelResult{
			Notification:    toNotification(res.Notification),
			MayHaveBeenSent: res.MayHaveBeenSent,
		},
		Headers: apigen.CancelNotification200ResponseHeaders{XRequestID: stateFrom(ctx).correlationID},
	}, nil
}

// CancelBatch cancels every notification of a batch that no worker holds
// and answers the counts of the batch after the call (ADR-0006 section 7,
// D69). createGuard has checked that the request has no body. One event is
// published per notification this call cancelled.
func (h *handlers) CancelBatch(ctx context.Context, req apigen.CancelBatchRequestObject) (apigen.CancelBatchResponseObject, error) {
	id, p := pathID(req.Id)
	if p != nil {
		return nil, p
	}
	dbCtx, cancel := context.WithTimeout(ctx, h.dbTimeout)
	defer cancel()
	res, evs, err := h.repo.CancelBatch(dbCtx, id)
	if errors.Is(err, store.ErrBatchNotFound) {
		return nil, newProblem(kindNotFound, "No batch has this id.")
	}
	if err != nil {
		return nil, storeProblem(err)
	}
	for _, ev := range evs {
		h.events.Publish(ev)
	}
	return apigen.CancelBatch200JSONResponse{
		Body: apigen.BatchCancelResult{
			Cancelled:       int32Of(res.Cancelled),
			NotCancellable:  int32Of(res.NotCancellable),
			MayHaveBeenSent: int32Of(res.MayHaveBeenSent),
		},
		Headers: apigen.CancelBatch200ResponseHeaders{XRequestID: stateFrom(ctx).correlationID},
	}, nil
}

// RedriveNotification performs transition 11 (ADR-0006 section 7, D16).
// createGuard has validated the optional body and passes it as req.Body
// (nil or without expires_at when the request gave none).
func (h *handlers) RedriveNotification(ctx context.Context, req apigen.RedriveNotificationRequestObject) (apigen.RedriveNotificationResponseObject, error) {
	id, p := pathID(req.Id)
	if p != nil {
		return nil, p
	}
	var newExpiresAt *time.Time
	if req.Body != nil {
		newExpiresAt = req.Body.ExpiresAt
	}
	dbCtx, cancel := context.WithTimeout(ctx, h.dbTimeout)
	defer cancel()
	n, ev, err := h.repo.RedriveNotification(dbCtx, id, newExpiresAt, h.minLead)
	if err != nil {
		return nil, h.transitionProblem(err)
	}
	h.events.Publish(ev)
	return apigen.RedriveNotification200JSONResponse{
		Body:    toNotification(n),
		Headers: apigen.RedriveNotification200ResponseHeaders{XRequestID: stateFrom(ctx).correlationID},
	}, nil
}

// transitionProblem maps an error of a cancel or a redrive: 404 for an
// unknown notification; for a refusal, 409 with current_status (422 for a
// new expires_at that is too soon); otherwise 500 or 503.
func (h *handlers) transitionProblem(err error) *problem {
	if errors.Is(err, store.ErrNotFound) {
		return newProblem(kindNotFound, "No notification has this id.")
	}
	var refused *store.RefusedError
	if !errors.As(err, &refused) {
		return storeProblem(err)
	}
	if refused.Refusal == store.RefusalTooSoon {
		return newProblem(kindValidation, "",
			fieldError(fieldExpiresAt, codeTooSoon,
				fmt.Sprintf("expires_at must be at least %s in the future", h.minLead)))
	}
	var p *problem
	switch refused.Refusal {
	case store.RefusalNotCancellable:
		p = newProblem(kindConflict, "",
			fieldError(fieldStatus, codeNotCancellable,
				"a notification can be cancelled only while it is pending, queued or retrying"))
	case store.RefusalNotFailed:
		p = newProblem(kindConflict, "",
			fieldError(fieldStatus, codeNotFailed, "only a failed notification can be redriven"))
	case store.RefusalExpiresAtRequired:
		p = newProblem(kindConflict, "",
			fieldError(fieldExpiresAt, codeExpiresAtRequired,
				"the stored expires_at has passed; the request must give a new expires_at"))
	case store.RefusalExpiresAtNotAllowed:
		p = newProblem(kindConflict, "",
			fieldError(fieldExpiresAt, codeExpiresAtNotAllowed,
				"the stored expires_at is still in the future and is kept; the request must not give one"))
		stored := refused.StoredExpiresAt
		p.expiresAt = &stored
	default: // store.RefusalStateChanged
		p = newProblem(kindConflict, "The notification changed while the request ran; the request can be repeated.")
	}
	status := apigen.Status(refused.Current)
	p.currentStatus = &status
	return p
}

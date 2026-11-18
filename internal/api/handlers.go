package api

import (
	"bytes"
	"context"
	"errors"
	"time"

	"github.com/google/uuid"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/api/apigen"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/platform/config"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/store"
)

// limit is what a priority's profile fixes at creation: the default TTL and
// the failure budget.
type limit struct {
	ttl         time.Duration
	maxFailures int
}

func limitsFrom(c config.RetryConfig) map[domain.Priority]limit {
	l := func(r config.Retry) limit { return limit{ttl: r.TTL, maxFailures: r.MaxFailures} }
	return map[domain.Priority]limit{
		domain.PriorityHigh:   l(c.High),
		domain.PriorityNormal: l(c.Normal),
		domain.PriorityLow:    l(c.Low),
	}
}

// handlers implements the generated strict server interface.
type handlers struct {
	repo      Repository
	db        Pinger
	limits    map[domain.Priority]limit
	dbTimeout time.Duration
	spec      []byte
}

var _ apigen.StrictServerInterface = (*handlers)(nil)

// CreateNotification stores a pending notification. The body has already
// passed validateCreate. Creation is not a state transition and emits no
// event.
func (h *handlers) CreateNotification(ctx context.Context, req apigen.CreateNotificationRequestObject) (apigen.CreateNotificationResponseObject, error) {
	if req.Body == nil {
		return nil, newProblem(kindMalformed, "")
	}
	channel, err := domain.ParseChannel(string(req.Body.Channel))
	if err != nil {
		return nil, newProblem(kindValidation, "",
			fieldError(fieldChannel, codeInvalidValue, "channel must be one of: sms, email, push"))
	}
	priority := domain.PriorityNormal
	if req.Body.Priority != nil {
		if priority, err = domain.ParsePriority(string(*req.Body.Priority)); err != nil {
			return nil, newProblem(kindValidation, "",
				fieldError(fieldPriority, codeInvalidValue, "priority must be one of: high, normal, low"))
		}
	}
	lim, ok := h.limits[priority]
	if !ok {
		return nil, &problem{kind: kindInternal, detail: kindInternal.detail, cause: errors.New("no limits for priority")}
	}
	st := stateFrom(ctx)

	dbCtx, cancel := context.WithTimeout(ctx, h.dbTimeout)
	defer cancel()
	n, err := h.repo.CreateNotification(dbCtx, store.NewNotification{
		Channel:       channel,
		Priority:      priority,
		Recipient:     domain.Recipient(req.Body.Recipient),
		Content:       req.Body.Content,
		TTL:           lim.ttl,
		MaxFailures:   lim.maxFailures,
		CorrelationID: st.correlationID,
		TraceParent:   st.traceParent,
	})
	if err != nil {
		return nil, storeProblem(err)
	}
	return apigen.CreateNotification202JSONResponse{
		Body: toNotification(n),
		Headers: apigen.CreateNotification202ResponseHeaders{
			Location:   "/v1/notifications/" + n.ID.String(),
			XRequestID: st.correlationID,
		},
	}, nil
}

// GetNotification returns the notification or 404.
func (h *handlers) GetNotification(ctx context.Context, req apigen.GetNotificationRequestObject) (apigen.GetNotificationResponseObject, error) {
	dbCtx, cancel := context.WithTimeout(ctx, h.dbTimeout)
	defer cancel()
	n, err := h.repo.GetNotification(dbCtx, uuid.UUID(req.Id))
	if errors.Is(err, store.ErrNotFound) {
		return nil, newProblem(kindNotFound, "No notification has this id.")
	}
	if err != nil {
		return nil, storeProblem(err)
	}
	return apigen.GetNotification200JSONResponse{
		Body:    toNotification(n),
		Headers: apigen.GetNotification200ResponseHeaders{XRequestID: stateFrom(ctx).correlationID},
	}, nil
}

// GetHealthz answers while the process serves; it checks no dependency.
func (h *handlers) GetHealthz(ctx context.Context, _ apigen.GetHealthzRequestObject) (apigen.GetHealthzResponseObject, error) {
	return apigen.GetHealthz200JSONResponse{
		Body:    apigen.Health{Status: apigen.HealthStatusOk},
		Headers: apigen.GetHealthz200ResponseHeaders{XRequestID: stateFrom(ctx).correlationID},
	}, nil
}

// GetReadyz pings Postgres (ADR-0009 §7: the api's readiness is Postgres
// only). Both answers carry the check's status and duration: the 200 in its
// body, the 503 as the checks member of the problem document.
func (h *handlers) GetReadyz(ctx context.Context, _ apigen.GetReadyzRequestObject) (apigen.GetReadyzResponseObject, error) {
	dbCtx, cancel := context.WithTimeout(ctx, h.dbTimeout)
	defer cancel()
	start := time.Now()
	err := h.db.Ping(dbCtx)
	check := apigen.Check{
		Name:       "postgres",
		Status:     apigen.CheckStatusOk,
		DurationMs: float64(time.Since(start).Microseconds()) / 1000,
	}
	if err != nil {
		check.Status = apigen.CheckStatusFail
		p := newProblem(kindUnavailable, "Postgres does not answer.")
		p.checks = []apigen.Check{check}
		p.cause = err
		return nil, p
	}
	return apigen.GetReadyz200JSONResponse{
		Body: apigen.Readiness{
			Status: apigen.ReadinessStatusOk,
			Checks: []apigen.Check{check},
		},
		Headers: apigen.GetReadyz200ResponseHeaders{XRequestID: stateFrom(ctx).correlationID},
	}, nil
}

// GetOpenAPISpec serves the embedded specification the server was generated from.
func (h *handlers) GetOpenAPISpec(ctx context.Context, _ apigen.GetOpenAPISpecRequestObject) (apigen.GetOpenAPISpecResponseObject, error) {
	return apigen.GetOpenAPISpec200ApplicationyamlResponse{
		Body:          bytes.NewReader(h.spec),
		ContentLength: int64(len(h.spec)),
		Headers:       apigen.GetOpenAPISpec200ResponseHeaders{XRequestID: stateFrom(ctx).correlationID},
	}, nil
}

// toNotification builds the response representation. The recipient is the
// real value: domain.Recipient masks itself in encoding/json, so it is
// revealed explicitly here, the one place where the API hands it back.
func toNotification(n store.Notification) apigen.Notification {
	out := apigen.Notification{
		Id:                n.ID,
		BatchId:           n.BatchID,
		Channel:           apigen.Channel(n.Channel),
		Recipient:         n.Recipient.Reveal(),
		Priority:          apigen.Priority(n.Priority.String()),
		Status:            apigen.Status(n.Status),
		Version:           n.Version,
		ScheduledAt:       utcPtr(n.ScheduledAt),
		ExpiresAt:         n.ExpiresAt.UTC(),
		AttemptCount:      int32(n.AttemptCount), //nolint:gosec // the column is an int4
		ProviderMessageId: n.ProviderMessageID,
		CreatedAt:         n.CreatedAt.UTC(),
		UpdatedAt:         n.UpdatedAt.UTC(),
		SentAt:            utcPtr(n.SentAt),
	}
	if n.FailureReason != nil {
		r := apigen.FailureReason(*n.FailureReason)
		out.FailureReason = &r
	}
	return out
}

func utcPtr(t *time.Time) *time.Time {
	if t == nil {
		return nil
	}
	u := t.UTC()
	return &u
}

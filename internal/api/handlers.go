package api

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"slices"
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

// CreateNotification stores a pending notification through the record and
// request layers of duplicate prevention (ADR-0006 section 11). The body has
// already passed validateCreate, and createGuard has read the
// Idempotency-Key. Creation is not a state transition and emits no event.
func (h *handlers) CreateNotification(ctx context.Context, req apigen.CreateNotificationRequestObject) (apigen.CreateNotificationResponseObject, error) {
	if req.Body == nil {
		return nil, newProblem(kindMalformed, "")
	}
	in, _ := createInputFrom(ctx) // without it: no key
	st := stateFrom(ctx)
	n, p := h.newNotification(st, *req.Body)
	if p != nil {
		return nil, p
	}

	dbCtx, cancel := context.WithTimeout(ctx, h.dbTimeout)
	defer cancel()
	var created apigen.Notification
	res, err := h.repo.CreateSingle(dbCtx, in.key, n, func(row store.Notification) (store.StoredResponse, error) {
		created = toNotification(row)
		return jsonResponse(http.StatusAccepted, created)
	})
	if errors.Is(err, store.ErrIdempotencyKeyReused) {
		return nil, idempotencyKeyReusedProblem()
	}
	if err != nil {
		return nil, storeProblem(err)
	}
	switch res.Outcome {
	case store.OutcomeCreated:
		return apigen.CreateNotification202JSONResponse{
			Body: created,
			Headers: apigen.CreateNotification202ResponseHeaders{
				Location:   "/v1/notifications/" + created.Id.String(),
				XRequestID: st.correlationID,
			},
		}, nil
	case store.OutcomeDuplicate:
		return nil, newProblem(kindDuplicate, "", duplicateFieldError(res.ExistingID))
	case store.OutcomeReplayed:
		return replay(res.Response, "id", "/v1/notifications/", st.correlationID)
	}
	return nil, unexpectedOutcome(res.Outcome)
}

// CreateNotificationBatch stores the valid items of a batch (ADR-0006
// section 6). createGuard has validated the body and calls this method with
// the result in the context; the request object carries no body.
func (h *handlers) CreateNotificationBatch(ctx context.Context, _ apigen.CreateNotificationBatchRequestObject) (apigen.CreateNotificationBatchResponseObject, error) {
	in, ok := createInputFrom(ctx)
	if !ok || in.batch.total == 0 {
		return nil, &problem{kind: kindInternal, detail: kindInternal.detail, cause: errors.New("batch request without validated input")}
	}
	if len(in.batch.valid) == 0 {
		return nil, allRejected(in.batch.total, in.batch.rejected)
	}
	st := stateFrom(ctx)
	items := make([]store.NewNotification, len(in.batch.valid))
	for i, v := range in.batch.valid {
		n, p := h.newNotification(st, v.request)
		if p != nil {
			return nil, p
		}
		items[i] = n
	}

	dbCtx, cancel := context.WithTimeout(ctx, h.dbTimeout)
	defer cancel()
	var accepted apigen.BatchAccepted
	res, err := h.repo.CreateBatch(dbCtx, in.key, store.NewBatch{
		Total:         in.batch.total,
		Rejected:      len(in.batch.rejected),
		CorrelationID: st.correlationID,
		Items:         items,
	}, func(r store.BatchResult) (store.StoredResponse, error) {
		accepted = batchAccepted(r, in.batch)
		return jsonResponse(http.StatusAccepted, accepted)
	})
	if errors.Is(err, store.ErrIdempotencyKeyReused) {
		return nil, idempotencyKeyReusedProblem()
	}
	if err != nil {
		return nil, storeProblem(err)
	}
	switch res.Outcome {
	case store.OutcomeCreated:
		return apigen.CreateNotificationBatch202JSONResponse{
			Body: accepted,
			Headers: apigen.CreateNotificationBatch202ResponseHeaders{
				Location:   "/v1/batches/" + accepted.BatchId.String(),
				XRequestID: st.correlationID,
			},
		}, nil
	case store.OutcomeDuplicate:
		return nil, allRejected(in.batch.total, mergeRejected(in.batch, res.Items))
	case store.OutcomeReplayed:
		return replay(res.Response, "batch_id", "/v1/batches/", st.correlationID)
	}
	return nil, unexpectedOutcome(res.Outcome)
}

// GetBatch returns the batch with the count of its notifications per status,
// or 404.
func (h *handlers) GetBatch(ctx context.Context, req apigen.GetBatchRequestObject) (apigen.GetBatchResponseObject, error) {
	dbCtx, cancel := context.WithTimeout(ctx, h.dbTimeout)
	defer cancel()
	b, err := h.repo.GetBatch(dbCtx, uuid.UUID(req.Id))
	if errors.Is(err, store.ErrBatchNotFound) {
		return nil, newProblem(kindNotFound, "No batch has this id.")
	}
	if err != nil {
		return nil, storeProblem(err)
	}
	return apigen.GetBatch200JSONResponse{
		Body:    toBatch(b),
		Headers: apigen.GetBatch200ResponseHeaders{XRequestID: stateFrom(ctx).correlationID},
	}, nil
}

// newNotification builds the store input of one validated create request:
// the TTL and failure budget of its priority, the request's correlation id
// and trace parent.
func (h *handlers) newNotification(st *requestState, req apigen.CreateNotificationRequest) (store.NewNotification, *problem) {
	channel, err := domain.ParseChannel(string(req.Channel))
	if err != nil {
		return store.NewNotification{}, newProblem(kindValidation, "",
			fieldError(fieldChannel, codeInvalidValue, "channel must be one of: sms, email, push"))
	}
	priority := domain.PriorityNormal
	if req.Priority != nil {
		if priority, err = domain.ParsePriority(string(*req.Priority)); err != nil {
			return store.NewNotification{}, newProblem(kindValidation, "",
				fieldError(fieldPriority, codeInvalidValue, "priority must be one of: high, normal, low"))
		}
	}
	lim, ok := h.limits[priority]
	if !ok {
		return store.NewNotification{}, &problem{kind: kindInternal, detail: kindInternal.detail, cause: errors.New("no limits for priority")}
	}
	return store.NewNotification{
		DedupeKey:     req.DedupeKey,
		Channel:       channel,
		Priority:      priority,
		Recipient:     domain.Recipient(req.Recipient),
		Content:       req.Content,
		TTL:           lim.ttl,
		MaxFailures:   lim.maxFailures,
		CorrelationID: st.correlationID,
		TraceParent:   st.traceParent,
	}, nil
}

// acceptedItem is the element type of BatchAccepted.Accepted, which the
// specification declares inline.
type acceptedItem = struct {
	Id uuid.UUID `json:"id"` //nolint:revive // the generated field name

	// Index The item's position in the request's array.
	Index int `json:"index"`
}

// batchAccepted builds the 202 body: the inserted items and every rejected
// one (in validation or as a duplicate), each list in ascending order of
// index. Both lists are arrays, never null.
func batchAccepted(r store.BatchResult, in batchInput) apigen.BatchAccepted {
	out := apigen.BatchAccepted{
		BatchId:  r.BatchID,
		Accepted: make([]acceptedItem, 0, r.Accepted),
		Rejected: mergeRejected(in, r.Items),
	}
	for i, it := range r.Items {
		if it.Inserted {
			out.Accepted = append(out.Accepted, acceptedItem{Id: it.ID, Index: in.valid[i].index})
		}
	}
	return out
}

// mergeRejected adds the items the store reported as duplicates (items is
// parallel to in.valid) to the items rejected in validation, in ascending
// order of index.
func mergeRejected(in batchInput, items []store.BatchItem) []apigen.RejectedItem {
	out := make([]apigen.RejectedItem, 0, len(in.rejected)+len(items))
	out = append(out, in.rejected...)
	for i, it := range items {
		if !it.Inserted {
			out = append(out, apigen.RejectedItem{
				Index:  in.valid[i].index,
				Errors: []apigen.FieldError{duplicateFieldError(it.ExistingID)},
			})
		}
	}
	slices.SortFunc(out, func(a, b apigen.RejectedItem) int { return cmp.Compare(a.Index, b.Index) })
	return out
}

// allRejected is the 422 of a batch in which no item was stored: rejected
// lists every item, and nothing was stored.
func allRejected(total int, rejected []apigen.RejectedItem) *problem {
	p := newProblem(kindValidation, fmt.Sprintf("All %d item(s) were rejected; no batch was created.", total))
	p.rejected = rejected
	return p
}

// jsonResponse is the response stored on an idempotency key.
func jsonResponse(status int, body any) (store.StoredResponse, error) {
	b, err := json.Marshal(body)
	if err != nil {
		return store.StoredResponse{}, fmt.Errorf("encode response: %w", err)
	}
	return store.StoredResponse{Status: status, Body: b}, nil
}

// replay returns the stored response of an earlier request with the same
// Idempotency-Key. Location is rebuilt from the id member idField of the
// stored body; X-Request-ID is the current request's.
func replay(stored *store.StoredResponse, idField, locationPrefix, requestID string) (replayedResponse, error) {
	if stored == nil {
		return replayedResponse{}, &problem{kind: kindInternal, detail: kindInternal.detail, cause: errors.New("replay without a stored response")}
	}
	var ids map[string]json.RawMessage
	var id uuid.UUID
	err := json.Unmarshal(stored.Body, &ids)
	if err == nil {
		err = json.Unmarshal(ids[idField], &id)
	}
	if err != nil {
		return replayedResponse{}, &problem{kind: kindInternal, detail: kindInternal.detail, cause: fmt.Errorf("stored response has no %s: %w", idField, err)}
	}
	return replayedResponse{
		status:    stored.Status,
		body:      stored.Body,
		location:  locationPrefix + id.String(),
		requestID: requestID,
	}, nil
}

// replayedResponse writes a stored response with Idempotent-Replayed: true.
// The body is the stored JSON value (jsonb keeps the value, not the bytes).
type replayedResponse struct {
	status    int
	body      []byte
	location  string
	requestID string
}

func (r replayedResponse) VisitCreateNotificationResponse(w http.ResponseWriter) error {
	return r.write(w)
}

func (r replayedResponse) VisitCreateNotificationBatchResponse(w http.ResponseWriter) error {
	return r.write(w)
}

func (r replayedResponse) write(w http.ResponseWriter) error {
	h := w.Header()
	h.Set("Content-Type", "application/json")
	h.Set("Idempotent-Replayed", "true")
	h.Set("Location", r.location)
	h.Set(headerRequestID, r.requestID)
	w.WriteHeader(r.status)
	_, err := w.Write(r.body)
	return err
}

func unexpectedOutcome(o store.CreateOutcome) *problem {
	return &problem{kind: kindInternal, detail: kindInternal.detail, cause: fmt.Errorf("unexpected create outcome %d", o)}
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

// toBatch builds the batch representation; every status is present in
// status_counts, 0 when no notification has it.
func toBatch(b store.Batch) apigen.Batch {
	c := func(s domain.Status) int32 { return int32Of(b.StatusCounts[s]) }
	return apigen.Batch{
		Id:        b.ID,
		Total:     int32Of(b.Total),
		Accepted:  int32Of(b.Accepted),
		Rejected:  int32Of(b.Rejected),
		CreatedAt: b.CreatedAt.UTC(),
		StatusCounts: apigen.BatchStatusCounts{
			Pending:    c(domain.StatusPending),
			Queued:     c(domain.StatusQueued),
			Processing: c(domain.StatusProcessing),
			Retrying:   c(domain.StatusRetrying),
			Sent:       c(domain.StatusSent),
			Failed:     c(domain.StatusFailed),
			Cancelled:  c(domain.StatusCancelled),
		},
	}
}

// int32Of converts a count read from an int4 column or bounded by one.
func int32Of(v int) int32 {
	return int32(v) //nolint:gosec // the counts come from int4 columns or are bounded by them
}

func utcPtr(t *time.Time) *time.Time {
	if t == nil {
		return nil
	}
	u := t.UTC()
	return &u
}

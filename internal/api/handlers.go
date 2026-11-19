package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/google/uuid"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/api/apigen"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/events"
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
	// events receives the event of every transition the API makes, after
	// its transaction has committed.
	events events.Publisher
	// minLead is how far ahead a new expires_at must be.
	minLead time.Duration
	// phone is the level of the check of an SMS recipient.
	phone domain.PhoneValidation
	// schedule holds the limits of scheduled_at and expires_at of a create,
	// and now the clock that judges them.
	schedule scheduleRules
	now      func() time.Time
}

// createRules returns the rules a create request is validated by, with the
// clock read once: every item of a batch is judged at the same instant. The
// instant is cut to the microsecond, like the times it is compared with.
func (h *handlers) createRules() createRules {
	s := h.schedule
	s.now = asStored(h.now())
	return createRules{phone: h.phone, schedule: s}
}

var _ apigen.StrictServerInterface = (*handlers)(nil)

// CreateNotification stores a pending notification through the record and
// request layers of duplicate prevention (ADR-0006 section 11). createGuard
// has validated and parsed the body (validateCreate) and read the
// Idempotency-Key, and calls this method with both in the context; the
// request object's body is not read. A notification from a template is
// rendered first (renderForCreate), after an early look-up of its key
// (earlyReplay). Creation is not a state transition and emits no event.
func (h *handlers) CreateNotification(ctx context.Context, _ apigen.CreateNotificationRequestObject) (apigen.CreateNotificationResponseObject, error) {
	in, ok := createInputFrom(ctx)
	if !ok || in.single == nil {
		return nil, internalProblem(errors.New("create request without validated input"))
	}
	st := stateFrom(ctx)
	cr := *in.single
	if cr.template != nil {
		if r, p := h.earlyReplay(ctx, in.key, "id", "/v1/notifications/", st.correlationID); r != nil || p != nil {
			return replayOrProblem(r, p)
		}
		rendered, errs, p := h.newTemplateCache().renderForCreate(ctx, domain.Channel(cr.request.Channel), cr.template)
		if p != nil {
			return nil, p
		}
		if len(errs) > 0 {
			return nil, newProblem(kindValidation, fmt.Sprintf("%d field(s) are invalid.", len(errs)), errs...)
		}
		cr.rendered = rendered
	}
	n, p := h.newNotification(st, cr)
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
// the result in the context; the request object carries no body. When an
// item uses a template, the key is looked up early (earlyReplay) and the
// template stage runs before the dedupe keys are claimed (prepareBatch).
func (h *handlers) CreateNotificationBatch(ctx context.Context, _ apigen.CreateNotificationBatchRequestObject) (apigen.CreateNotificationBatchResponseObject, error) {
	in, ok := createInputFrom(ctx)
	if !ok || in.batch.total == 0 {
		return nil, internalProblem(errors.New("batch request without validated input"))
	}
	st := stateFrom(ctx)
	if len(in.batch.valid) == 0 {
		// Every item was rejected. If one of them was rejected only by the
		// rules judged against the clock, this can be the retry of a batch
		// that was accepted when it was first sent: a live key for the same
		// request replays its stored answer. (A retry with at least one
		// item still valid reaches the claim, which replays.)
		if in.key != nil && in.batch.rejectedByClockOnly() {
			r, p := h.earlyReplay(ctx, in.key, "batch_id", "/v1/batches/", st.correlationID)
			switch {
			case r != nil:
				return *r, nil
			case p != nil && p.kind != kindIdempotencyKeyReused:
				return nil, p
			}
		}
		return nil, allRejected(in.batch.total, in.batch.rejected)
	}
	if in.batch.usesTemplate() {
		if r, p := h.earlyReplay(ctx, in.key, "batch_id", "/v1/batches/", st.correlationID); r != nil || p != nil {
			if p != nil {
				return nil, p
			}
			return *r, nil
		}
	}
	batch, p := prepareBatch(ctx, h.newTemplateCache(), in.batch)
	if p != nil {
		return nil, p
	}
	in.batch = batch
	if len(in.batch.valid) == 0 {
		return nil, allRejected(in.batch.total, in.batch.rejected)
	}
	items := make([]store.NewNotification, len(in.batch.valid))
	for i, v := range in.batch.valid {
		n, p := h.newNotification(st, v.createRequest)
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

// newTemplateCache returns the template cache of one request.
func (h *handlers) newTemplateCache() *templateCache {
	return newTemplateCache(h.repo, h.dbTimeout)
}

// earlyReplay looks up the Idempotency-Key of a request that renders a
// template before the template stage: what the request renders can change
// between it and its retry (a new latest version, a deactivation), and a
// retry must get the stored answer back, not a 422. A live key with the
// same request hash gives the stored response to replay, with another hash
// the 422 of a reused key; otherwise (no key, or none live) both results are
// nil and the request goes on: the create transaction still claims the key
// and decides between concurrent requests.
func (h *handlers) earlyReplay(ctx context.Context, key *store.IdempotencyKey, idField, locationPrefix, requestID string) (*replayedResponse, *problem) {
	if key == nil {
		return nil, nil
	}
	dbCtx, cancel := context.WithTimeout(ctx, h.dbTimeout)
	defer cancel()
	stored, err := h.repo.LookupIdempotencyKey(dbCtx, key)
	switch {
	case errors.Is(err, store.ErrIdempotencyKeyReused):
		return nil, idempotencyKeyReusedProblem()
	case err != nil:
		return nil, storeProblem(err)
	case stored == nil:
		return nil, nil
	}
	r, err := replay(stored, idField, locationPrefix, requestID)
	if err != nil {
		var p *problem
		if errors.As(err, &p) {
			return nil, p
		}
		return nil, internalProblem(err)
	}
	return &r, nil
}

// replayOrProblem turns the result of earlyReplay into the single create's
// answer.
func replayOrProblem(r *replayedResponse, p *problem) (apigen.CreateNotificationResponseObject, error) {
	if p != nil {
		return nil, p
	}
	return *r, nil
}

// newNotification builds the store input of one validated create request:
// the TTL and failure budget of its priority, the request's correlation id
// and trace parent, the content (the request's, or the rendered text with
// its template provenance), and the schedule: scheduled_at (nil when absent
// or within the tolerance, so the notification is due at once) and an
// explicit expires_at (nil: the insert computes due time + TTL).
func (h *handlers) newNotification(st *requestState, cr createRequest) (store.NewNotification, *problem) {
	req := cr.request
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
	var content string
	switch {
	case cr.rendered != nil:
		content = cr.rendered.content
	case req.Content != nil:
		content = *req.Content
	default:
		return store.NewNotification{}, internalProblem(errors.New("notification without content"))
	}
	n := store.NewNotification{
		DedupeKey:     req.DedupeKey,
		Channel:       channel,
		Priority:      priority,
		Recipient:     domain.Recipient(req.Recipient),
		Content:       content,
		TTL:           lim.ttl,
		MaxFailures:   lim.maxFailures,
		CorrelationID: st.correlationID,
		TraceParent:   st.traceParent,
		ScheduledAt:   req.ScheduledAt,
		ExpiresAt:     req.ExpiresAt,
	}
	if r := cr.rendered; r != nil {
		name, version := r.name, r.version
		n.TemplateName, n.TemplateVersion, n.TemplateVars = &name, &version, r.vars
	}
	if channel == domain.ChannelSMS {
		// The content has passed validation: at most MaxSMSSegments segments.
		info := domain.CountSMS(content)
		enc := string(info.Encoding)
		segments := int16(info.Segments) //nolint:gosec // at most domain.MaxSMSSegments
		n.SMSEncoding, n.SMSSegments = &enc, &segments
	}
	return n, nil
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
	sortRejected(out)
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
		Sms:               smsInfo(n),
		Template:          templateProvenance(n),
	}
	if n.FailureReason != nil {
		r := apigen.FailureReason(*n.FailureReason)
		out.FailureReason = &r
	}
	return out
}

// smsInfo is the sms member of a notification: the stored encoding and
// segment count for the channel sms, nil (null) for every other channel.
func smsInfo(n store.Notification) *apigen.SMSInfo {
	if n.Channel != domain.ChannelSMS || n.SMSEncoding == nil || n.SMSSegments == nil {
		return nil
	}
	return &apigen.SMSInfo{
		Encoding: apigen.SMSInfoEncoding(*n.SMSEncoding),
		Segments: int32(*n.SMSSegments),
	}
}

// templateProvenance is the template member of a notification: the template
// version that produced the content, nil (null) for a notification created
// with content. The variables are not returned.
func templateProvenance(n store.Notification) *apigen.TemplateProvenance {
	if n.TemplateName == nil || n.TemplateVersion == nil {
		return nil
	}
	return &apigen.TemplateProvenance{Name: *n.TemplateName, Version: *n.TemplateVersion}
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

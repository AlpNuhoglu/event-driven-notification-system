//go:build integration

package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/store"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/testutil"
)

// Durations are parameters, in milliseconds.
const (
	itMinLead    = 30_000 * time.Millisecond
	itShortLease = 1 * time.Millisecond // the lease expires at once
	itProvider   = "mock"
)

// eventLog is an events.Publisher that records every event.
type eventLog struct {
	mu  sync.Mutex
	evs []domain.StatusChanged
}

func (l *eventLog) Publish(e domain.StatusChanged) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.evs = append(l.evs, e)
}

func (l *eventLog) all() []domain.StatusChanged {
	l.mu.Lock()
	defer l.mu.Unlock()
	return slices.Clone(l.evs)
}

// cancelAPI is the real handler on a real database, with the events it
// publishes recorded.
type cancelAPI struct {
	*testAPI
	events *eventLog
}

func newCancelAPI(t *testing.T) *cancelAPI {
	t.Helper()
	evs := &eventLog{}
	a := newTestAPIOn(t, testutil.NewPool(t), &syncBuffer{}, func(o *Options) {
		o.Events = evs
		o.ExpiresAtMinLead = itMinLead
	})
	return &cancelAPI{testAPI: a, events: evs}
}

// create creates a notification through the API and returns its id.
func (a *cancelAPI) create(t *testing.T, body string) uuid.UUID {
	t.Helper()
	res := a.post(body)
	if res.status != http.StatusAccepted {
		t.Fatalf("create: %d %s", res.status, res.body)
	}
	return uuid.MustParse(res.object(t)["id"].(string))
}

func (a *cancelAPI) get(t *testing.T, id uuid.UUID) store.Notification {
	t.Helper()
	n, err := a.store.GetNotification(ctxT(t), id)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func (a *cancelAPI) attempts(t *testing.T, id uuid.UUID) []store.Attempt {
	t.Helper()
	at, err := a.store.ListAttempts(ctxT(t), id)
	if err != nil {
		t.Fatal(err)
	}
	return at
}

func (a *cancelAPI) claim(t *testing.T, id uuid.UUID, lease time.Duration) store.Claim {
	t.Helper()
	c, _, err := a.store.Claim(ctxT(t), id, lease, itProvider)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	return c
}

// toSent: claim and a successful attempt.
func (a *cancelAPI) toSent(t *testing.T, id uuid.UUID) {
	t.Helper()
	a.claim(t, id, itLease)
	if _, _, err := a.store.CompleteSent(ctxT(t), id, 1, itProvider, store.AttemptResult{Outcome: domain.OutcomeSuccess}); err != nil {
		t.Fatal(err)
	}
}

// failAttempt ends the open attempt n with a failure; the row becomes
// retrying (due at once) or failed when the budget ends.
func (a *cancelAPI) failAttempt(t *testing.T, id uuid.UUID, n int) domain.StatusChanged {
	t.Helper()
	ev, err := a.store.CompleteFailure(ctxT(t), id, n, store.AttemptResult{Outcome: domain.OutcomeRetryable}, 0)
	if err != nil {
		t.Fatal(err)
	}
	return ev
}

// toFailed fails a notification by exhausting its failure budget; its
// expires_at stays in the future.
func (a *cancelAPI) toFailed(t *testing.T, id uuid.UUID) {
	t.Helper()
	for n := 1; ; n++ {
		a.claim(t, id, itLease)
		if ev := a.failAttempt(t, id, n); ev.Status == domain.StatusFailed {
			return
		}
	}
}

// toExpired fails a notification through the claim's expiry branch after
// one real attempt, so its attempt_count is 1.
func (a *cancelAPI) toExpired(t *testing.T, id uuid.UUID) {
	t.Helper()
	a.claim(t, id, itLease)
	a.failAttempt(t, id, 1)
	a.backdateExpiry(t, id)
	if c := a.claim(t, id, itLease); !c.Expired {
		t.Fatalf("the claim did not expire the row")
	}
}

func (a *cancelAPI) backdateExpiry(t *testing.T, id uuid.UUID) {
	t.Helper()
	if _, err := a.pool.Exec(ctxT(t), `UPDATE notifications SET expires_at = now() - interval '1 millisecond' WHERE id = $1`, id); err != nil {
		t.Fatal(err)
	}
}

// toUnknown gives a notification an attempt with outcome unknown through the
// real lease sweeper: a claim whose lease expires at once, then a sweep.
func (a *cancelAPI) toUnknown(t *testing.T, id uuid.UUID) {
	t.Helper()
	a.claim(t, id, itShortLease)
	testutil.Eventually(t, func() bool {
		evs, err := a.store.SweepExpiredLeases(ctxT(t), 10, func() float64 { return 0 })
		if err != nil {
			t.Fatalf("sweep: %v", err)
		}
		return len(evs) == 1
	}, waitT)
	at := a.attempts(t, id)
	if len(at) != 1 || at[0].Outcome != domain.OutcomeUnknown {
		t.Fatalf("attempts %+v, want one unknown", at)
	}
}

func (a *cancelAPI) cancel(id uuid.UUID) response {
	a.t.Helper()
	return a.do(http.MethodPost, "/v1/notifications/"+id.String()+"/cancel", "", nil)
}

func (a *cancelAPI) cancelBatch(id uuid.UUID) response {
	a.t.Helper()
	return a.do(http.MethodPost, "/v1/batches/"+id.String()+"/cancel", "", nil)
}

func (a *cancelAPI) redrive(id uuid.UUID, body string) response {
	a.t.Helper()
	return a.do(http.MethodPost, "/v1/notifications/"+id.String()+"/redrive", body, nil)
}

// cancelDoc is a CancelResult.
type cancelDoc struct {
	Notification    json.RawMessage `json:"notification"`
	MayHaveBeenSent *bool           `json:"may_have_been_sent"`
}

func decodeCancel(t *testing.T, r response) (cancelDoc, map[string]any) {
	t.Helper()
	if r.status != http.StatusOK {
		t.Fatalf("cancel: %d %s", r.status, r.body)
	}
	var d cancelDoc
	if err := json.Unmarshal(r.body, &d); err != nil {
		t.Fatal(err)
	}
	if d.MayHaveBeenSent == nil {
		t.Fatalf("may_have_been_sent absent: %s", r.body)
	}
	var n map[string]any
	if err := json.Unmarshal(d.Notification, &n); err != nil {
		t.Fatal(err)
	}
	return d, n
}

// expectConflict checks a 409 with current_status and the one error.
func expectConflict(t *testing.T, r response, current domain.Status, fieldCode string) {
	t.Helper()
	if r.status != http.StatusConflict {
		t.Fatalf("status %d, want 409: %s", r.status, r.body)
	}
	if got := r.object(t)["current_status"]; got != string(current) {
		t.Errorf("current_status %v, want %s", got, current)
	}
	if got := r.problemCodes(t); !slices.Equal(got, []string{fieldCode}) {
		t.Errorf("errors %v, want [%s]", got, fieldCode)
	}
	if !strings.HasSuffix(r.object(t)["type"].(string), "#conflict") {
		t.Errorf("type %v", r.object(t)["type"])
	}
	// Only the expires_at_not_allowed 409 carries the stored expires_at.
	if _, ok := r.object(t)["expires_at"]; ok != (fieldCode == "expires_at:expires_at_not_allowed") {
		t.Errorf("expires_at member present %v: %s", ok, r.body)
	}
}

func expectCodes(t *testing.T, r response, status int, codes ...string) {
	t.Helper()
	if r.status != status {
		t.Fatalf("status %d, want %d: %s", r.status, status, r.body)
	}
	if got := r.problemCodes(t); !slices.Equal(got, codes) {
		t.Errorf("errors %v, want %v", got, codes)
	}
	if _, ok := r.object(t)["current_status"]; ok {
		t.Errorf("current_status on a %d: %s", status, r.body)
	}
}

const smsBody = `{"channel":"sms","recipient":"+905551234567","content":"hello"}`

func smsWithKey(key string) string {
	return `{"channel":"sms","recipient":"+905551234567","content":"hello","dedupe_key":"` + key + `"}`
}

func TestCancel_Idempotent(t *testing.T) {
	a := newCancelAPI(t)
	id := a.create(t, smsBody)

	first, n1 := decodeCancel(t, a.cancel(id))
	if n1["status"] != "cancelled" || n1["version"] != float64(2) || *first.MayHaveBeenSent {
		t.Fatalf("first cancel: %v, may_have_been_sent %v", n1, *first.MayHaveBeenSent)
	}
	evs := a.events.all()
	if len(evs) != 1 || evs[0].ID != id || evs[0].Status != domain.StatusCancelled || evs[0].Version != 2 {
		t.Fatalf("events %+v", evs)
	}

	second, n2 := decodeCancel(t, a.cancel(id))
	if !reflect.DeepEqual(n1, n2) || *second.MayHaveBeenSent {
		t.Errorf("second cancel: %v, want %v", n2, n1)
	}
	if got := a.get(t, id); got.Version != 2 || got.Status != domain.StatusCancelled {
		t.Errorf("row version %d status %s", got.Version, got.Status)
	}
	if got := a.events.all(); len(got) != 1 {
		t.Errorf("%d events after the repeat, want 1", len(got))
	}
}

func TestCancel_WhileProcessingReturns409(t *testing.T) {
	cases := []struct {
		name  string
		setup func(t *testing.T, a *cancelAPI, id uuid.UUID)
		want  domain.Status
	}{
		{"processing", func(t *testing.T, a *cancelAPI, id uuid.UUID) { a.claim(t, id, itLease) }, domain.StatusProcessing},
		{"sent", func(t *testing.T, a *cancelAPI, id uuid.UUID) { a.toSent(t, id) }, domain.StatusSent},
		{"failed", func(t *testing.T, a *cancelAPI, id uuid.UUID) { a.toFailed(t, id) }, domain.StatusFailed},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			a := newCancelAPI(t)
			id := a.create(t, smsBody)
			tc.setup(t, a, id)
			before := a.get(t, id)
			expectConflict(t, a.cancel(id), tc.want, "status:not_cancellable")
			if after := a.get(t, id); !reflect.DeepEqual(before, after) {
				t.Errorf("row changed:\n%+v\n%+v", before, after)
			}
			if got := a.events.all(); len(got) != 0 {
				t.Errorf("events %+v", got)
			}
		})
	}
}

// may_have_been_sent after an unknown attempt produced by the real lease
// sweeper; the attempt and the dedupe_key are kept. A cancel before any
// attempt releases the dedupe_key.
func TestCancel_MayHaveBeenSentAndDedupeKey(t *testing.T) {
	a := newCancelAPI(t)

	t.Run("unknown attempt", func(t *testing.T) {
		id := a.create(t, smsWithKey("dk-unknown"))
		a.toUnknown(t, id)
		if got := a.get(t, id).Status; got != domain.StatusRetrying {
			t.Fatalf("status %s after the sweep, want retrying", got)
		}
		before := a.attempts(t, id)
		d, n := decodeCancel(t, a.cancel(id))
		if !*d.MayHaveBeenSent || n["status"] != "cancelled" || n["attempt_count"] != float64(1) {
			t.Errorf("cancel: may_have_been_sent %v, notification %v", *d.MayHaveBeenSent, n)
		}
		if got := a.attempts(t, id); !reflect.DeepEqual(got, before) {
			t.Errorf("attempts changed: %+v -> %+v", before, got)
		}
		expectCodes(t, a.post(smsWithKey("dk-unknown")), http.StatusConflict, "dedupe_key:duplicate")
	})

	t.Run("no attempt", func(t *testing.T) {
		id := a.create(t, smsWithKey("dk-fresh"))
		d, _ := decodeCancel(t, a.cancel(id))
		if *d.MayHaveBeenSent {
			t.Error("may_have_been_sent without an attempt")
		}
		if again := a.create(t, smsWithKey("dk-fresh")); again == id {
			t.Error("the create returned the cancelled notification")
		}
	})
}

func TestCancelBatch_Mixed(t *testing.T) {
	a := newCancelAPI(t)
	res := a.do(http.MethodPost, pathBatch, batchBody(
		item(""), // 0 pending
		`{"channel":"email","recipient":"user@example.com","content":"hello"}`, // 1 queued
		item(""), // 2 processing
		item(""), // 3 sent
		item(""), // 4 unknown attempt, then retrying
	), nil)
	if res.status != http.StatusAccepted {
		t.Fatalf("batch: %d %s", res.status, res.body)
	}
	doc := decodeBatch(t, res)
	ids := make([]uuid.UUID, len(doc.Accepted))
	for _, it := range doc.Accepted {
		ids[it.Index] = uuid.MustParse(it.ID)
	}
	batchID := uuid.MustParse(doc.BatchID)

	if _, err := a.store.PublishLane(ctxT(t), store.Lane{Channel: domain.ChannelEmail, Priority: domain.PriorityNormal}, 10,
		func(_ context.Context, _ []store.DueRow) ([]uuid.UUID, error) { return []uuid.UUID{ids[1]}, nil }); err != nil {
		t.Fatal(err)
	}
	a.claim(t, ids[2], itLease)
	a.toSent(t, ids[3])
	a.toUnknown(t, ids[4])
	want := map[uuid.UUID]domain.Status{
		ids[0]: domain.StatusPending, ids[1]: domain.StatusQueued, ids[2]: domain.StatusProcessing,
		ids[3]: domain.StatusSent, ids[4]: domain.StatusRetrying,
	}
	for id, s := range want {
		if got := a.get(t, id).Status; got != s {
			t.Fatalf("setup: %s is %s, want %s", id, got, s)
		}
	}

	wantCounts := map[string]any{"cancelled": float64(3), "not_cancellable": float64(2), "may_have_been_sent": float64(1)}
	for round := range 2 {
		r := a.cancelBatch(batchID)
		if r.status != http.StatusOK {
			t.Fatalf("round %d: %d %s", round, r.status, r.body)
		}
		if got := r.object(t); !reflect.DeepEqual(got, wantCounts) {
			t.Errorf("round %d: counts %v, want %v", round, got, wantCounts)
		}
	}
	evs := a.events.all()
	got := map[uuid.UUID]int{}
	for _, ev := range evs {
		got[ev.ID]++
		if ev.Status != domain.StatusCancelled || !reflect.DeepEqual(ev, a.get(t, ev.ID).Event()) {
			t.Errorf("event %+v", ev)
		}
	}
	if len(evs) != 3 || got[ids[0]] != 1 || got[ids[1]] != 1 || got[ids[4]] != 1 {
		t.Errorf("events per id %v, want one each for items 0, 1 and 4", got)
	}
	if s := a.get(t, ids[2]).Status; s != domain.StatusProcessing {
		t.Errorf("the processing item is %s", s)
	}

	r := a.cancelBatch(uuid.New())
	if r.status != http.StatusNotFound {
		t.Errorf("unknown batch: %d %s", r.status, r.body)
	}
}

// TestRedrive_VersusExpiry: the stored expires_at decides whether the
// request must, or must not, give a new one (D16).
func TestRedrive_VersusExpiry(t *testing.T) {
	a := newCancelAPI(t)
	at := func(d time.Duration) string { return time.Now().Add(d).UTC().Format(time.RFC3339Nano) }
	body := func(ts string) string { return `{"expires_at":"` + ts + `"}` }

	t.Run("expired", func(t *testing.T) {
		id := a.create(t, smsBody)
		a.toExpired(t, id)
		before := a.get(t, id)
		if before.FailureReason == nil || *before.FailureReason != domain.ReasonExpired || before.AttemptCount != 1 {
			t.Fatalf("setup: %+v", before)
		}

		expectConflict(t, a.redrive(id, ""), domain.StatusFailed, "expires_at:expires_at_required")
		expectConflict(t, a.redrive(id, "{}"), domain.StatusFailed, "expires_at:expires_at_required")
		expectCodes(t, a.redrive(id, body(at(1000*time.Millisecond))), http.StatusUnprocessableEntity, "expires_at:too_soon")
		if got := a.get(t, id); !reflect.DeepEqual(got, before) {
			t.Fatalf("a refused redrive changed the row")
		}

		newExp := time.Now().Add(3_600_000 * time.Millisecond).UTC().Truncate(time.Microsecond)
		r := a.redrive(id, body(newExp.Format(time.RFC3339Nano)))
		if r.status != http.StatusOK {
			t.Fatalf("redrive: %d %s", r.status, r.body)
		}
		n := r.object(t)
		if n["status"] != "pending" || n["expires_at"] != newExp.Format(time.RFC3339Nano) ||
			n["attempt_count"] != float64(1) || n["failure_reason"] != nil {
			t.Errorf("redriven: %v", n)
		}
		row := a.get(t, id)
		if row.Failures != 0 || row.AttemptCount != 1 || row.MaxFailures != before.MaxFailures || !row.DueAt.Equal(row.UpdatedAt) {
			t.Errorf("row %+v", row)
		}
		evs := a.events.all()
		if len(evs) != 1 || !reflect.DeepEqual(evs[0], row.Event()) {
			t.Errorf("events %+v", evs)
		}

		// The next claim opens attempt attempt_count + 1.
		if c := a.claim(t, id, itLease); c.Expired || c.Notification.AttemptCount != 2 {
			t.Errorf("next claim: expired %v attempt_count %d", c.Expired, c.Notification.AttemptCount)
		}
		if at := a.attempts(t, id); len(at) != 2 || at[1].AttemptNo != 2 {
			t.Errorf("attempts %+v", at)
		}
	})

	t.Run("expired again after the redrive", func(t *testing.T) {
		id := a.create(t, smsBody)
		a.toExpired(t, id)
		if r := a.redrive(id, body(at(3_600_000*time.Millisecond))); r.status != http.StatusOK {
			t.Fatalf("redrive: %d %s", r.status, r.body)
		}
		a.backdateExpiry(t, id)
		c := a.claim(t, id, itLease)
		row := a.get(t, id)
		if !c.Expired || row.Status != domain.StatusFailed || *row.FailureReason != domain.ReasonExpired || row.AttemptCount != 1 {
			t.Errorf("claim after expiry: expired %v row %+v", c.Expired, row)
		}
		if at := a.attempts(t, id); len(at) != 1 {
			t.Errorf("attempts %+v, want the first only", at)
		}
	})

	t.Run("failed for another reason", func(t *testing.T) {
		id := a.create(t, smsBody)
		a.toFailed(t, id)
		before := a.get(t, id)
		r := a.redrive(id, body(at(7_200_000*time.Millisecond)))
		expectConflict(t, r, domain.StatusFailed, "expires_at:expires_at_not_allowed")
		// D16: the problem carries the stored expires_at, in UTC with
		// microsecond precision like every timestamp of the API.
		if got, want := r.object(t)["expires_at"], before.ExpiresAt.UTC().Format(time.RFC3339Nano); got != want {
			t.Errorf("expires_at %v, want the stored %s", got, want)
		}
		if got := a.get(t, id); !reflect.DeepEqual(got, before) {
			t.Fatalf("a refused redrive changed the row")
		}
		r = a.redrive(id, "")
		if r.status != http.StatusOK {
			t.Fatalf("redrive: %d %s", r.status, r.body)
		}
		row := a.get(t, id)
		if row.Status != domain.StatusPending || !row.ExpiresAt.Equal(before.ExpiresAt) ||
			row.Failures != 0 || row.AttemptCount != before.AttemptCount {
			t.Errorf("row %+v, before %+v", row, before)
		}
	})

	t.Run("not failed", func(t *testing.T) {
		pending := a.create(t, smsBody)
		expectConflict(t, a.redrive(pending, ""), domain.StatusPending, "status:not_failed")
		expectConflict(t, a.redrive(pending, body(at(3_600_000*time.Millisecond))), domain.StatusPending, "status:not_failed")
		sent := a.create(t, smsBody)
		a.toSent(t, sent)
		expectConflict(t, a.redrive(sent, ""), domain.StatusSent, "status:not_failed")
		cancelled := a.create(t, smsBody)
		decodeCancel(t, a.cancel(cancelled))
		expectConflict(t, a.redrive(cancelled, ""), domain.StatusCancelled, "status:not_failed")
	})

	// The same rules against the relay's expiry pass (transition 10) instead
	// of the claim's expiry branch.
	t.Run("relay expiry pass", func(t *testing.T) {
		pass := func(t *testing.T, id uuid.UUID) []domain.StatusChanged {
			t.Helper()
			evs, err := a.store.ExpireDue(ctxT(t), 1000)
			if err != nil {
				t.Fatalf("expiry pass: %v", err)
			}
			var mine []domain.StatusChanged
			for _, e := range evs {
				if e.ID == id {
					mine = append(mine, e)
				}
			}
			return mine
		}

		id := a.create(t, smsBody)
		a.backdateExpiry(t, id)
		if evs := pass(t, id); len(evs) != 1 || evs[0].Status != domain.StatusFailed {
			t.Fatalf("pass events %+v", evs)
		}
		before := a.get(t, id)
		if before.FailureReason == nil || *before.FailureReason != domain.ReasonExpired || before.AttemptCount != 0 {
			t.Fatalf("expired by the pass: %+v", before)
		}
		expectConflict(t, a.redrive(id, ""), domain.StatusFailed, "expires_at:expires_at_required")

		// A redriven row with a new future expires_at is not expired by a
		// pass that runs after it.
		if r := a.redrive(id, body(at(3_600_000*time.Millisecond))); r.status != http.StatusOK {
			t.Fatalf("redrive: %d %s", r.status, r.body)
		}
		redriven := a.get(t, id)
		if evs := pass(t, id); len(evs) != 0 {
			t.Errorf("pass after the redrive: %+v", evs)
		}
		if got := a.get(t, id); got.Status != domain.StatusPending || got.Version != redriven.Version {
			t.Errorf("redriven row changed by the pass: %+v", got)
		}
	})

	// A pass and a redrive on the same row, concurrently, from an expired
	// pending row and from an expired failed row: whatever the order, the row
	// is never pending with an expires_at that has passed, and a later pass
	// leaves a pending row alone.
	t.Run("relay expiry pass and redrive concurrently", func(t *testing.T) {
		const rounds = 20
		for round := range rounds {
			id := a.create(t, smsBody)
			a.backdateExpiry(t, id)
			if round%2 == 1 {
				if _, err := a.store.ExpireDue(ctxT(t), 1000); err != nil {
					t.Fatal(err)
				}
			}
			newExp := time.Now().Add(3_600_000 * time.Millisecond)
			start := make(chan struct{})
			var passErr, redriveErr error
			var wg sync.WaitGroup
			wg.Go(func() {
				<-start
				_, passErr = a.store.ExpireDue(context.Background(), 1000)
			})
			wg.Go(func() {
				<-start
				_, _, redriveErr = a.store.RedriveNotification(context.Background(), id, &newExp, itMinLead)
			})
			close(start)
			wg.Wait()
			if passErr != nil {
				t.Fatalf("round %d: pass: %v", round, passErr)
			}
			var refused *store.RefusedError
			if redriveErr != nil && !errors.As(redriveErr, &refused) {
				t.Fatalf("round %d: redrive: %v", round, redriveErr)
			}

			var status string
			var expired bool
			if err := a.pool.QueryRow(ctxT(t), `SELECT status, expires_at <= now() FROM notifications WHERE id = $1`, id).
				Scan(&status, &expired); err != nil {
				t.Fatal(err)
			}
			switch {
			case status == string(domain.StatusPending) && expired:
				t.Fatalf("round %d: a pending row that has already expired", round)
			case status == string(domain.StatusPending):
				if redriveErr != nil {
					t.Fatalf("round %d: pending although the redrive was refused: %v", round, redriveErr)
				}
				v := a.get(t, id).Version
				if _, err := a.store.ExpireDue(ctxT(t), 1000); err != nil {
					t.Fatal(err)
				}
				if got := a.get(t, id); got.Status != domain.StatusPending || got.Version != v {
					t.Fatalf("round %d: a later pass changed the redriven row: %+v", round, got)
				}
			case status == string(domain.StatusFailed):
				if redriveErr == nil {
					t.Fatalf("round %d: failed although the redrive applied", round)
				}
				// The redrive came first, on the pending row, and was refused.
				// Once failed, it applies.
				if _, _, err := a.store.RedriveNotification(ctxT(t), id, &newExp, itMinLead); err != nil {
					t.Fatalf("round %d: redrive of the expired row: %v", round, err)
				}
			default:
				t.Fatalf("round %d: status %s", round, status)
			}
		}
	})
}

// The request rules of the three operations; every answer is checked
// against the contract by the client.
func TestCancelRedrive_Requests(t *testing.T) {
	a := newCancelAPI(t)
	unknown := uuid.New()

	t.Run("unknown ids", func(t *testing.T) {
		for _, r := range []response{a.cancel(unknown), a.cancelBatch(unknown), a.redrive(unknown, "")} {
			if r.status != http.StatusNotFound {
				t.Errorf("status %d: %s", r.status, r.body)
			}
		}
		r := a.do(http.MethodPost, "/v1/notifications/not-a-uuid/cancel", "", nil)
		expectCodes(t, r, http.StatusBadRequest, "id:invalid_uuid")
	})

	t.Run("cancel takes no body", func(t *testing.T) {
		id := a.create(t, smsBody)
		for _, path := range []string{"/v1/notifications/" + id.String() + "/cancel", "/v1/batches/" + unknown.String() + "/cancel"} {
			expectCodes(t, a.do(http.MethodPost, path, "{}", nil), http.StatusBadRequest, ":unexpected_body")
			expectCodes(t, a.do(http.MethodPost, path, "x", map[string][]string{"Content-Type": {"text/plain"}}),
				http.StatusBadRequest, ":unexpected_body")
			if r := a.do(http.MethodPost, path, strings.Repeat("x", itMaxBody+1), nil); r.status != http.StatusRequestEntityTooLarge {
				t.Errorf("large body: %d", r.status)
			}
		}
		if got := a.get(t, id).Status; got != domain.StatusPending {
			t.Fatalf("a refused cancel changed the row: %s", got)
		}
		// White space is no body.
		decodeCancel(t, a.do(http.MethodPost, "/v1/notifications/"+id.String()+"/cancel", " \n\t ", nil))
	})

	t.Run("redrive body", func(t *testing.T) {
		path := "/v1/notifications/" + unknown.String() + "/redrive"
		ok := `{"expires_at":"2099-01-01T00:00:00Z"}`
		r := a.do(http.MethodPost, path, ok, map[string][]string{"Content-Type": {"text/plain"}})
		expectCodes(t, r, http.StatusUnsupportedMediaType)
		if r := a.do(http.MethodPost, path, strings.Repeat(" ", itMaxBody+1), nil); r.status != http.StatusRequestEntityTooLarge {
			t.Errorf("large body: %d", r.status)
		}
		for _, tc := range []struct {
			body   string
			status int
			codes  []string
		}{
			{`{`, http.StatusBadRequest, []string{":malformed_json"}},
			{`[]`, http.StatusBadRequest, []string{":not_an_object"}},
			{`"x"`, http.StatusBadRequest, []string{":not_an_object"}},
			{`{"expires_at":1}`, http.StatusUnprocessableEntity, []string{"expires_at:invalid_type"}},
			{`{"expires_at":null}`, http.StatusUnprocessableEntity, []string{"expires_at:invalid_type"}},
			{`{"expires_at":"2099-01-01T00:00:00"}`, http.StatusUnprocessableEntity, []string{"expires_at:invalid_value"}},
			{`{"expires_at":"2099-01-01"}`, http.StatusUnprocessableEntity, []string{"expires_at:invalid_value"}},
			{`{"expires_at":"tomorrow"}`, http.StatusUnprocessableEntity, []string{"expires_at:invalid_value"}},
			{
				`{"expires_at":"2099-01-01T00:00:00Z","expires_at":"2099-01-02T00:00:00Z"}`, http.StatusUnprocessableEntity,
				[]string{"expires_at:duplicate_field"},
			},
			{`{"scheduled_at":"2099-01-01T00:00:00Z"}`, http.StatusUnprocessableEntity, []string{"scheduled_at:unknown_field"}},
			{
				`{"expires_at":"x","a":1,"a":2,"b":true}`, http.StatusUnprocessableEntity,
				[]string{"expires_at:invalid_value", "a:unknown_field", "b:unknown_field"},
			},
		} {
			expectCodes(t, a.do(http.MethodPost, path, tc.body, nil), tc.status, tc.codes...)
		}
		// Valid bodies reach the store: 404 for an unknown id.
		for _, body := range []string{"", "  ", "{}", ok, `{"expires_at":"2099-01-01T03:00:00.123456+03:00"}`} {
			if r := a.do(http.MethodPost, path, body, nil); r.status != http.StatusNotFound {
				t.Errorf("%q: %d %s", body, r.status, r.body)
			}
		}
		// An empty body needs no Content-Type.
		if r := a.do(http.MethodPost, path, "", map[string][]string{"Content-Type": {"text/plain"}}); r.status != http.StatusNotFound {
			t.Errorf("empty body with text/plain: %d", r.status)
		}
	})

	t.Run("X-Request-ID echoed", func(t *testing.T) {
		id := a.create(t, smsBody)
		h := map[string][]string{"X-Request-ID": {"req-cancel-redrive"}}
		for _, r := range []response{
			a.do(http.MethodPost, "/v1/notifications/"+id.String()+"/cancel", "", h),
			a.do(http.MethodPost, "/v1/notifications/"+id.String()+"/redrive", "", h),
			a.do(http.MethodPost, "/v1/batches/"+unknown.String()+"/cancel", "", h),
			a.do(http.MethodPost, "/v1/notifications/"+id.String()+"/redrive", "{", h),
		} {
			if got := r.header.Get("X-Request-ID"); got != "req-cancel-redrive" {
				t.Errorf("status %d: X-Request-ID %q", r.status, got)
			}
		}
	})

	t.Run("no value in logs or problems", func(t *testing.T) {
		if strings.Contains(a.logs.String(), "+905551234567") || strings.Contains(a.logs.String(), "hello") {
			t.Error("a log line contains a recipient or a content")
		}
	})
}

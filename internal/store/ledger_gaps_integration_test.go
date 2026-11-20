//go:build integration

package store

import (
	"fmt"
	"testing"

	"github.com/google/uuid"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
)

// The expiry pass covers pending, retrying and queued (D12): one expired row
// in each of the three statuses, and one pass fails all three with one
// event each, without touching attempt_count, failures or the attempt rows.
func TestExpireDue_FailsPendingRetryingAndQueued(t *testing.T) {
	s, pool := newTestStore(t, zeroDelay)

	// Queued first, alone in its lane, so that the lane transaction takes
	// no other row.
	queued := create(t, s)
	if evs, err := s.PublishLane(ctxT(t), smsNormal, 10, publishAll); err != nil || len(evs) != 1 {
		t.Fatalf("publish: %+v %v", evs, err)
	}
	pending := create(t, s)
	retrying := create(t, s)
	claim(t, s, retrying.ID)
	failAttempt(t, s, retrying.ID, 1, domain.OutcomeRetryable)

	rows := map[domain.Status]uuid.UUID{
		domain.StatusQueued:   queued.ID,
		domain.StatusPending:  pending.ID,
		domain.StatusRetrying: retrying.ID,
	}
	before := map[uuid.UUID]Notification{}
	beforeAttempts := map[uuid.UUID][]Attempt{}
	for st, id := range rows {
		expire(t, pool, id)
		n := get(t, s, id)
		if n.Status != st {
			t.Fatalf("setup: row is %s, want %s", n.Status, st)
		}
		before[id] = n
		beforeAttempts[id] = attempts(t, s, id)
	}

	evs, err := s.ExpireDue(ctxT(t), 10)
	if err != nil || len(evs) != len(rows) {
		t.Fatalf("pass: %d events %v, want %d", len(evs), err, len(rows))
	}
	for _, ev := range evs {
		if _, ok := before[ev.ID]; !ok || ev.Status != domain.StatusFailed {
			t.Errorf("event %+v, want failed for one of the three rows", ev)
		}
	}
	for st, id := range rows {
		t.Run(string(st), func(t *testing.T) {
			requireExpiredOnce(t, s, before[id], beforeAttempts[id])
		})
	}
}

// The provider's messageId is not assumed unique (D28): two notifications
// whose sends the provider answered with the same messageId are both sent
// and both keep it.
func TestCompleteSent_ProviderMessageIDIsNotUnique(t *testing.T) {
	s, _ := newTestStore(t, zeroDelay)
	for _, n := range []Notification{create(t, s), create(t, s)} {
		claim(t, s, n.ID)
		if _, _, err := s.CompleteSent(ctxT(t), n.ID, 1, testProvider,
			AttemptResult{Outcome: domain.OutcomeSuccess, ProviderMessageID: ptr("same-message-id")}); err != nil {
			t.Fatalf("complete sent: %v", err)
		}
		got := get(t, s, n.ID)
		if got.Status != domain.StatusSent || got.ProviderMessageID == nil || *got.ProviderMessageID != "same-message-id" {
			t.Errorf("row %s provider_message_id %v, want sent with the shared id", got.Status, got.ProviderMessageID)
		}
	}
}

// Ids are UUIDv7 (D19): the id of a notification created alone, through
// the single create, and the batch id and every item id of a batch.
func TestCreate_IDsAreUUIDv7(t *testing.T) {
	s, _ := newTestStore(t, zeroDelay)
	check := func(what string, id uuid.UUID) {
		t.Helper()
		if v := id.Version(); v != 7 || id.Variant() != uuid.RFC4122 {
			t.Errorf("%s %s: version %d variant %v, want a UUIDv7", what, id, v, id.Variant())
		}
	}

	check("notification", create(t, s).ID)

	single, err := s.CreateSingle(ctxT(t), nil, baseNew(), nil)
	if err != nil || single.Outcome != OutcomeCreated {
		t.Fatalf("create single: %+v %v", single.Outcome, err)
	}
	check("single create", single.Notification.ID)

	batchID, ids := createBatchOf(t, s, 3)
	check("batch", batchID)
	for i, id := range ids {
		check(fmt.Sprintf("batch item %d", i), id)
	}
}

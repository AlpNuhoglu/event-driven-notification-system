//go:build e2e

package e2e

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"
)

const (
	// scheduleAhead is how far ahead the scheduled notification is due.
	scheduleAhead = 4 * time.Second
	// schedulePast is how far in the past the "now" notification's
	// scheduled_at is: inside the 30-second tolerance.
	schedulePast = 5 * time.Second
	// scheduleLateBound bounds how long after its due time a notification
	// is sent on the idle stack (the relay ticks every 100 ms).
	scheduleLateBound = 5 * time.Second
)

// scheduled holds the fields of the Notification schema that the scheduling
// scenario reads.
type scheduled struct {
	ID           string     `json:"id"`
	Status       string     `json:"status"`
	AttemptCount int        `json:"attempt_count"`
	ScheduledAt  *time.Time `json:"scheduled_at"`
	CreatedAt    time.Time  `json:"created_at"`
	SentAt       *time.Time `json:"sent_at"`
}

// Scheduled delivery on the stack (ADR-0008 "Scheduling"):
//
//   - a notification scheduled a few seconds ahead is pending with its
//     scheduled_at right after creation, reaches sent, reaches the provider
//     once, and its sent_at is not earlier than its scheduled_at and not much
//     later;
//   - a scheduled_at a few seconds in the past means "now": it is answered as
//     null and the notification is sent at once;
//   - a notification scheduled an hour ahead and then cancelled is never
//     sent.
//
// sent_at, created_at and the relay's decision that a row is due all come
// from the database's clock, and scheduled_at is the stored value: the
// comparisons do not depend on the host's clock agreeing with the
// containers'. The host's clock only chooses the values sent.
func TestComposeE2E_Scheduling(t *testing.T) {
	s := newStack(t)
	s.resetMock(t)
	create := func(body map[string]any) scheduled {
		t.Helper()
		var n scheduled
		s.postJSON(t, "/v1/notifications", body, http.StatusAccepted, &n)
		return n
	}
	read := func(id string) scheduled {
		t.Helper()
		code, _, b, err := s.do(http.MethodGet, s.apiURL+"/v1/notifications/"+id, nil)
		if err != nil || code != http.StatusOK {
			t.Fatalf("get %s: %d %v", id, code, err)
		}
		var n scheduled
		if err := json.Unmarshal(b, &n); err != nil {
			t.Fatal(err)
		}
		return n
	}

	// Scheduled an hour ahead, then cancelled while pending.
	later := create(map[string]any{
		"channel": "email", "recipient": fakeEmail, "content": "see you in an hour",
		"scheduled_at": time.Now().Add(time.Hour).Format(time.RFC3339),
	})
	if later.Status != "pending" || later.ScheduledAt == nil {
		t.Fatalf("scheduled an hour ahead: %+v", later)
	}
	var cancelled struct {
		Notification    scheduled `json:"notification"`
		MayHaveBeenSent bool      `json:"may_have_been_sent"`
	}
	s.postJSON(t, "/v1/notifications/"+later.ID+"/cancel", nil, http.StatusOK, &cancelled)
	if cancelled.Notification.Status != "cancelled" || cancelled.MayHaveBeenSent {
		t.Fatalf("cancel: %+v", cancelled)
	}

	// Scheduled a few seconds ahead, in another zone's offset.
	when := time.Now().Add(scheduleAhead).Truncate(time.Microsecond).In(time.FixedZone("+03:00", 3*3600))
	ahead := create(map[string]any{
		"channel": "sms", "recipient": fakePhone, "content": "scheduled hello",
		"scheduled_at": when.Format(time.RFC3339Nano),
	})
	if ahead.Status != "pending" || ahead.ScheduledAt == nil || !ahead.ScheduledAt.Equal(when) || ahead.ScheduledAt.Location() != time.UTC {
		t.Fatalf("scheduled a few seconds ahead: %+v, want pending with scheduled_at %v in UTC", ahead, when.UTC())
	}

	// A few seconds in the past: "now", answered as null.
	now := create(map[string]any{
		"channel": "push", "recipient": fakeDevice, "content": "now",
		"scheduled_at": time.Now().Add(-schedulePast).Format(time.RFC3339Nano),
	})
	if now.Status != "pending" || now.ScheduledAt != nil {
		t.Fatalf("scheduled in the tolerance: %+v, want pending with scheduled_at null", now)
	}

	// The "now" notification is sent at once, the scheduled one at its time.
	s.waitSent(t, now.ID, sendTimeout)
	if n := read(now.ID); n.SentAt == nil || n.SentAt.Sub(n.CreatedAt) > scheduleLateBound {
		t.Errorf("now: sent_at %v, created_at %v; want sent within %s", n.SentAt, n.CreatedAt, scheduleLateBound)
	}
	s.waitSent(t, ahead.ID, sendTimeout)
	got := read(ahead.ID)
	switch {
	case got.ScheduledAt == nil || !got.ScheduledAt.Equal(when):
		t.Errorf("scheduled: scheduled_at %v, want %v", got.ScheduledAt, when)
	case got.SentAt == nil || got.SentAt.Before(*got.ScheduledAt):
		t.Errorf("scheduled: sent_at %v is earlier than scheduled_at %v", got.SentAt, *got.ScheduledAt)
	case got.SentAt.Sub(*got.ScheduledAt) > scheduleLateBound:
		t.Errorf("scheduled: sent_at %v is more than %s after scheduled_at %v", *got.SentAt, scheduleLateBound, *got.ScheduledAt)
	}
	if got.AttemptCount != 1 {
		t.Errorf("scheduled: attempt_count %d, want 1", got.AttemptCount)
	}
	if got.ScheduledAt != nil && got.SentAt != nil {
		t.Logf("scheduled_at %s, sent_at %s (%s after)", got.ScheduledAt.Format(time.RFC3339Nano),
			got.SentAt.Format(time.RFC3339Nano), got.SentAt.Sub(*got.ScheduledAt))
	}

	// The cancelled one stayed cancelled and never reached the provider.
	if n := read(later.ID); n.Status != "cancelled" || n.AttemptCount != 0 || n.SentAt != nil {
		t.Errorf("cancelled: %+v", n)
	}
	rep, err := s.report()
	if err != nil {
		t.Fatal(err)
	}
	if rep.Total != 2 || rep.IdempotencyKeys[ahead.ID] != 1 || rep.IdempotencyKeys[now.ID] != 1 || rep.IdempotencyKeys[later.ID] != 0 {
		t.Errorf("mockprovider received %d requests by key %v; want one each for %s and %s, none for %s",
			rep.Total, rep.IdempotencyKeys, ahead.ID, now.ID, later.ID)
	}
}

package queue

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
)

func testEvent() domain.StatusChanged {
	batch := uuid.MustParse("01a1f55e-0000-7000-8000-000000000002")
	reason := domain.ReasonExpired
	return domain.StatusChanged{
		ID:            uuid.MustParse("01a1f55e-0000-7000-8000-000000000001"),
		BatchID:       &batch,
		Channel:       domain.ChannelEmail,
		Priority:      domain.PriorityLow,
		Status:        domain.StatusFailed,
		Version:       7,
		FailureReason: &reason,
		AttemptCount:  3,
		UpdatedAt:     time.Date(2026, 11, 19, 20, 0, 0, 123456000, time.UTC),
	}
}

func TestEvent_RoundTrip(t *testing.T) {
	full := testEvent()
	plain := full
	plain.BatchID, plain.FailureReason, plain.Status = nil, nil, domain.StatusQueued
	for name, e := range map[string]domain.StatusChanged{"with batch and reason": full, "without": plain} {
		t.Run(name, func(t *testing.T) {
			body, err := EncodeEvent(e)
			if err != nil {
				t.Fatal(err)
			}
			got, err := DecodeEvent(body)
			if err != nil {
				t.Fatal(err)
			}
			if got.ID != e.ID || got.Channel != e.Channel || got.Priority != e.Priority || got.Status != e.Status ||
				got.Version != e.Version || got.AttemptCount != e.AttemptCount || !got.UpdatedAt.Equal(e.UpdatedAt) {
				t.Errorf("decoded %+v, want %+v", got, e)
			}
			if (got.BatchID == nil) != (e.BatchID == nil) || (e.BatchID != nil && *got.BatchID != *e.BatchID) {
				t.Errorf("batch_id %v, want %v", got.BatchID, e.BatchID)
			}
			if (got.FailureReason == nil) != (e.FailureReason == nil) || (e.FailureReason != nil && *got.FailureReason != *e.FailureReason) {
				t.Errorf("failure_reason %v, want %v", got.FailureReason, e.FailureReason)
			}
		})
	}
}

// The body has exactly these members, all of them always present, and the
// time is in UTC whatever the zone of the value.
func TestEvent_Body(t *testing.T) {
	e := testEvent()
	e.BatchID, e.FailureReason, e.Status = nil, nil, domain.StatusSent
	e.UpdatedAt = e.UpdatedAt.In(time.FixedZone("+03:00", 3*3600))
	body, err := EncodeEvent(e)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"id":"01a1f55e-0000-7000-8000-000000000001","batch_id":null,"channel":"email","priority":"low",` +
		`"status":"sent","version":7,"failure_reason":null,"attempt_count":3,"updated_at":"2026-11-19T20:00:00.123456Z"}`
	if string(body) != want {
		t.Errorf("body\n got %s\nwant %s", body, want)
	}
}

func TestEvent_Publishing(t *testing.T) {
	p, err := eventPublishing(testEvent())
	if err != nil {
		t.Fatal(err)
	}
	if p.Type != EventType || p.ContentType != "application/json" || p.DeliveryMode != amqp.Transient || !json.Valid(p.Body) {
		t.Errorf("publishing %+v", p)
	}
}

func TestDecodeEvent_Refused(t *testing.T) {
	valid, err := EncodeEvent(testEvent())
	if err != nil {
		t.Fatal(err)
	}
	replace := func(old, repl string) string {
		s := strings.Replace(string(valid), old, repl, 1)
		if s == string(valid) {
			t.Fatalf("%q is not in the body", old)
		}
		return s
	}
	for name, body := range map[string]string{
		"not JSON":         `{"id":`,
		"not an object":    `[]`,
		"no id":            replace(`"id":"01a1f55e-0000-7000-8000-000000000001"`, `"id":"00000000-0000-0000-0000-000000000000"`),
		"version 0":        replace(`"version":7`, `"version":0`),
		"unknown channel":  replace(`"channel":"email"`, `"channel":"fax"`),
		"unknown priority": replace(`"priority":"low"`, `"priority":"urgent"`),
		"unknown status":   replace(`"status":"failed"`, `"status":"lost"`),
	} {
		t.Run(name, func(t *testing.T) {
			if e, err := DecodeEvent([]byte(body)); err == nil {
				t.Errorf("decoded %+v, want an error", e)
			}
		})
	}
	// An id in a spelling the UUID library accepts but the canonical form
	// does not.
	const id, batch = "01a1f55e-0000-7000-8000-000000000001", "01a1f55e-0000-7000-8000-000000000002"
	for name, spell := range map[string]func(string) string{
		"urn prefix": func(s string) string { return "urn:uuid:" + s },
		"braces":     func(s string) string { return "{" + s + "}" },
		"32 digits":  func(s string) string { return strings.ReplaceAll(s, "-", "") },
		"wrapped id": func(s string) string { return "x" + s + "y" },
	} {
		for member, body := range map[string]string{
			"id":       replace(`"id":"`+id+`"`, `"id":"`+spell(id)+`"`),
			"batch_id": replace(`"batch_id":"`+batch+`"`, `"batch_id":"`+spell(batch)+`"`),
		} {
			if e, err := DecodeEvent([]byte(body)); err == nil {
				t.Errorf("%s in %s: decoded %+v, want an error", name, member, e)
			}
		}
	}
	// The canonical form in upper case is the same id.
	upper := replace(`"id":"`+id+`"`, `"id":"`+strings.ToUpper(id)+`"`)
	upper = strings.Replace(upper, `"batch_id":"`+batch+`"`, `"batch_id":"`+strings.ToUpper(batch)+`"`, 1)
	if e, err := DecodeEvent([]byte(upper)); err != nil || e.ID.String() != id || e.BatchID == nil || e.BatchID.String() != batch {
		t.Errorf("upper-case ids: %+v, %v", e, err)
	}
	// A member of a later version is ignored.
	if _, err := DecodeEvent([]byte(replace(`{"id"`, `{"later":1,"id"`))); err != nil {
		t.Errorf("an unknown member: %v", err)
	}
}

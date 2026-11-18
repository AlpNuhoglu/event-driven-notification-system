package queue

import (
	"testing"
	"time"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
)

func TestPublishing_Properties(t *testing.T) {
	want := map[domain.Priority]uint8{domain.PriorityHigh: 6, domain.PriorityNormal: 5, domain.PriorityLow: 4}
	for _, p := range domain.Priorities() {
		m := Message{ID: uuid.New(), Channel: domain.ChannelSMS, Priority: p, TraceParent: "tp"}
		got := publishing(m)
		if got.Priority != want[p] {
			t.Errorf("%s: priority %d, want %d", p, got.Priority, want[p])
		}
		if got.DeliveryMode != amqp.Persistent || got.MessageId != m.ID.String() || string(got.Body) != m.ID.String() {
			t.Errorf("%s: delivery mode %d, message id %q, body %q", p, got.DeliveryMode, got.MessageId, got.Body)
		}
		if got.Headers["traceparent"] != "tp" {
			t.Errorf("%s: headers %v", p, got.Headers)
		}
	}
	if h := publishing(Message{ID: uuid.New(), Priority: domain.PriorityLow}).Headers; h != nil {
		t.Errorf("headers without a trace parent: %v, want none", h)
	}
}

func TestDispatchQueueArgs(t *testing.T) {
	args := dispatchQueueArgs(24 * time.Hour)
	if v, ok := args[amqp.ConsumerTimeoutArg].(int64); !ok || v != 86_400_000 {
		t.Errorf("x-consumer-timeout = %#v, want int64(86400000)", args[amqp.ConsumerTimeoutArg])
	}
	if args[amqp.QueueTypeArg] != amqp.QueueTypeQuorum || args["x-dead-letter-exchange"] != DeadLetterExchange {
		t.Errorf("args %v", args)
	}
	if _, ok := args["x-max-priority"]; ok {
		t.Error("x-max-priority set on a quorum queue")
	}
	if DispatchQueue(domain.ChannelEmail) != "dispatch.email" || PoisonQueue(domain.ChannelEmail) != "dispatch.email.poison" {
		t.Error("queue names")
	}
}

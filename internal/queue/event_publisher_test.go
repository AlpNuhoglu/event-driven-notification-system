package queue

import (
	"log/slog"
	"testing"
	"time"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/platform/config"
)

// The constructor rejects settings that would make the publisher unbounded
// or busy; the configuration rejects them too, so they are programming
// errors.
func TestNewEventPublisher_RejectsInvalidSettings(t *testing.T) {
	log := slog.New(slog.DiscardHandler)
	ok := EventPublisherConfig{URL: "amqp://localhost/", BufferSize: 1, RetryInterval: time.Millisecond, FlushTimeout: time.Millisecond}
	for name, mod := range map[string]func(*EventPublisherConfig){
		"buffer size":    func(c *EventPublisherConfig) { c.BufferSize = 0 },
		"retry interval": func(c *EventPublisherConfig) { c.RetryInterval = 0 },
		"flush timeout":  func(c *EventPublisherConfig) { c.FlushTimeout = -time.Millisecond },
	} {
		cfg := ok
		mod(&cfg)
		if _, err := NewEventPublisher(cfg, log); err == nil {
			t.Errorf("%s: no error", name)
		}
	}
	if _, err := NewEventPublisher(ok, nil); err == nil {
		t.Error("nil logger: no error")
	}
	if _, err := NewEventPublisher(ok, log); err != nil {
		t.Errorf("valid settings: %v", err)
	}
}

// The roles' publishers take the broker, the buffer size, the retry interval
// and the flush time from the configuration.
func TestEventPublisherConfigFrom(t *testing.T) {
	vars := map[string]string{
		"RABBITMQ_URL":                 "amqp://broker:5672/",
		"EVENT_BUFFER_SIZE":            "7",
		"AMQP_RECOVERY_RETRY_INTERVAL": "300ms",
		"EVENT_FLUSH_TIMEOUT":          "40ms",
	}
	cfg, err := config.LoadFrom(func(k string) (string, bool) { v, ok := vars[k]; return v, ok })
	if err != nil {
		t.Fatal(err)
	}
	want := EventPublisherConfig{
		URL: "amqp://broker:5672/", BufferSize: 7,
		RetryInterval: 300 * time.Millisecond, FlushTimeout: 40 * time.Millisecond,
	}
	if got := EventPublisherConfigFrom(cfg); got != want {
		t.Errorf("got %+v, want %+v", got, want)
	}
}

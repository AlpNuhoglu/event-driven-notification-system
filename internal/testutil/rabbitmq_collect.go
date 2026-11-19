package testutil

import (
	"context"
	"net"
	"net/http"
	"net/url"
	"slices"
	"sync"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Collector receives the messages a topic exchange routes with one binding
// key, through an exclusive queue of its own, and keeps them in arrival
// order. The status-event tests bind one to the events exchange.
type Collector struct {
	mu   sync.Mutex
	msgs []amqp.Delivery
}

// NewCollector connects to amqpURL, declares the durable topic exchange (as
// its publishers declare it), binds a server-named exclusive queue to it
// with bindingKey and collects every message from then on. The connection
// is closed when the test ends. It does not reconnect: a test that wipes
// the vhost makes a new collector afterwards.
func NewCollector(t testing.TB, amqpURL, exchange, bindingKey string) *Collector {
	t.Helper()
	conn, err := amqp.Dial(amqpURL)
	if err != nil {
		t.Fatalf("collector: dial: %v", err)
	}
	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		t.Fatalf("collector: channel: %v", err)
	}
	fail := func(what string, err error) {
		_ = conn.Close()
		t.Fatalf("collector: %s: %v", what, err)
	}
	if err := ch.ExchangeDeclare(exchange, amqp.ExchangeTopic, true, false, false, false, nil); err != nil {
		fail("declare exchange", err)
	}
	q, err := ch.QueueDeclare("", false, true, true, false, nil)
	if err != nil {
		fail("declare queue", err)
	}
	if err := ch.QueueBind(q.Name, bindingKey, exchange, false, nil); err != nil {
		fail("bind", err)
	}
	deliveries, err := ch.Consume(q.Name, "", true, true, false, false, nil)
	if err != nil {
		fail("consume", err)
	}
	c := &Collector{}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for d := range deliveries {
			c.mu.Lock()
			c.msgs = append(c.msgs, d)
			c.mu.Unlock()
		}
	}()
	t.Cleanup(func() {
		_ = conn.Close()
		<-done
	})
	return c
}

// Messages returns the messages received so far, in arrival order.
func (c *Collector) Messages() []amqp.Delivery {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Clone(c.msgs)
}

// RabbitMQExchangeExists reports whether the vhost of amqpURL (a URL from
// NewRabbitMQVhost) has the exchange. It asks the management API, so it
// declares nothing and closes no channel.
func RabbitMQExchangeExists(t testing.TB, amqpURL, exchange string) bool {
	t.Helper()
	s := sharedRabbit(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	_, err := s.call(ctx, http.MethodGet, "/api/exchanges/"+url.PathEscape(vhostOf(t, amqpURL))+"/"+url.PathEscape(exchange), "")
	return err == nil
}

// UnusedAddr returns a local TCP address that nothing listens on: it was
// free a moment ago and is closed again. A dial to it is refused at once,
// as a dial to a stopped broker.
func UnusedAddr(t testing.TB) string {
	t.Helper()
	ln, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	if err := ln.Close(); err != nil {
		t.Fatalf("close listener: %v", err)
	}
	return addr
}

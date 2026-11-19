//go:build integration

package queue

import (
	"testing"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/testutil"
)

// Active is false before Run, true once the loop consumes, false from the
// loss of the connection until the consumer is subscribed again, true after
// that, and false once Run has returned.
func TestConsumer_ActiveFollowsTheSubscription(t *testing.T) {
	url := testutil.NewRabbitMQVhost(t)
	proxy := testutil.NewProxy(t, url)
	c := dial(t, proxy.URL(), 1000)
	if err := c.DeclareTopology(consumerTimeout); err != nil {
		t.Fatal(err)
	}
	cons := newConsumer(t, c)
	if cons.Active() {
		t.Fatal("Active before Run")
	}
	stop := runConsumer(t, cons, func(d Delivery) { _ = d.Ack() })
	testutil.Eventually(t, cons.Active, waitTimeout)

	proxy.Break()
	testutil.Eventually(t, func() bool { return !cons.Active() }, waitTimeout)
	if c.IsOpen() {
		t.Error("the connection is open while the proxy is broken")
	}

	proxy.Restore(t)
	testutil.Eventually(t, cons.Active, waitTimeout)
	if !c.IsOpen() {
		t.Error("the consumer is active but the connection is not open")
	}

	if err := stop(); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if cons.Active() {
		t.Error("Active after Run returned")
	}
}

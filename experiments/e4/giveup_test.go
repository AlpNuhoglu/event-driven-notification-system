//go:build integration

// Scenario 5: recovery gives up. The broker is kept down past the retry
// budget and the test records exactly what the application sees: state
// changes and their Err, NotifyClose, NotifyPublish/NotifyReturn/consumer
// channels, and what Publish/Channel() return afterwards. Two budgets:
//   - MaxRetryCount=1, RetryInterval=1s (smallest practical limit);
//   - the library defaults (Recovery{} with no ReconnectionConfig) with the
//     broker down for E4_OUTAGE (default 60s), the plan's outage.
//
// A connection WITHOUT recovery (Config.Recovery == nil) is observed alongside
// as the baseline.
//
// Run: go test -tags=integration -count=1 -v -timeout 20m -run TestRecoveryGivesUp ./...
package e4

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/testcontainers/testcontainers-go"
)

func TestRecoveryGivesUp(t *testing.T) {
	ctx := context.Background()
	outage := envDuration(t, "E4_OUTAGE", 60*time.Second)
	port := fixedHostPort(t)
	broker := startBroker(ctx, t, port)

	t.Run("MaxRetryCount=1", func(t *testing.T) {
		giveUp(ctx, t, broker, port, recoveryConfig(1, time.Second), outage, true)
	})
	t.Run("defaults", func(t *testing.T) {
		giveUp(ctx, t, broker, port, recoveryConfig(0, 0), outage, false)
	})
}

func giveUp(ctx context.Context, t *testing.T, broker *testcontainers.DockerContainer, port int, cfg amqp.Config, outage time.Duration, withBaseline bool) {
	rec := newRecorder()
	amqp.SetLogger(libLogger{rec})
	defer amqp.SetLogger(amqp.NullLogger{})

	conn, err := amqp.DialConfig(amqpURL(port), cfg)
	if err != nil {
		t.Fatal(err)
	}
	rc := conn.Config.Recovery.ReconnectionConfig
	rec.add("test", "recovery config in effect: MaxRetryCount=%d RetryInterval=%s", rc.MaxRetryCount, rc.RetryInterval)
	watchConn(rec, "rec.conn", conn)
	ch, err := conn.Channel()
	if err != nil {
		t.Fatal(err)
	}
	watchChan(rec, "rec.ch", ch)
	declareTopology(t, ch, "e4.x", map[string]string{"e4.work": "work"})
	if err := ch.Confirm(false); err != nil {
		t.Fatal(err)
	}
	pubs := ch.NotifyPublish(make(chan amqp.Confirmation, 16))
	rets := ch.NotifyReturn(make(chan amqp.Return, 16))
	dels, err := ch.Consume("e4.work", "", false, false, false, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		for range pubs {
		}
		rec.add("rec.notifyPublish", "listener chan closed")
	}()
	go func() {
		for range rets {
		}
		rec.add("rec.notifyReturn", "listener chan closed")
	}()
	go func() {
		for range dels {
		}
		rec.add("rec.deliveries", "consumer delivery chan closed")
	}()
	var closedErr error
	sc := make(chan *amqp.StateChanged, 16)
	conn.NotifyStateChange(sc)
	closedCh := make(chan struct{})
	go func() {
		for s := range sc {
			if s.To == amqp.StateClosed {
				closedErr = s.Err
				close(closedCh)
			}
		}
	}()

	var plain *amqp.Connection
	if withBaseline {
		plain, err = amqp.Dial(amqpURL(port))
		if err != nil {
			t.Fatal(err)
		}
		watchConn(rec, "plain.conn", plain)
		pch, err := plain.Channel()
		if err != nil {
			t.Fatal(err)
		}
		watchChan(rec, "plain.ch", pch)
	}

	rec.add("test", "docker stop broker (kept down up to %s)", outage)
	if err := broker.Stop(ctx, nil); err != nil {
		t.Fatal(err)
	}
	stoppedAt := time.Now()
	var gaveUp bool
	select {
	case <-closedCh:
		gaveUp = true
		rec.add("test", "recovering connection reached StateClosed %s after stop; StateChanged.Err=%v",
			time.Since(stoppedAt).Round(time.Millisecond), closedErr)
	case <-time.After(outage): // deliberate outage length
		rec.add("test", "recovering connection still not closed after %s", outage)
	}

	// What the application can still call.
	_, pubErr := ch.PublishWithDeferredConfirm("e4.x", "work", false, false, amqp.Publishing{Body: []byte("x")})
	_, chErr := conn.Channel()
	rec.add("rec", "after give-up: conn.IsClosed=%v ch.IsClosed=%v Publish err=%v Channel() err=%v",
		conn.IsClosed(), ch.IsClosed(), pubErr, chErr)
	var amqpErr *amqp.Error
	rec.add("rec", "StateChanged.Err: type=%T errors.As(*amqp.Error)=%v", closedErr, errors.As(closedErr, &amqpErr))

	rec.add("test", "docker start broker")
	if err := broker.Start(ctx); err != nil {
		t.Fatal(err)
	}
	// The broker is back; a given-up connection must not come back by itself.
	probe, err := amqp.Dial(amqpURL(port))
	for i := 0; err != nil && i < 100; i++ { // poll until the broker accepts connections
		time.Sleep(200 * time.Millisecond)
		probe, err = amqp.Dial(amqpURL(port))
	}
	if err != nil {
		t.Fatalf("broker not reachable after restart: %v", err)
	}
	_ = probe.Close()
	rec.add("rec", "broker reachable again; recovering connection IsClosed=%v", conn.IsClosed())

	// ---- assertions of what was observed ----
	if !gaveUp {
		t.Fatalf("recovery did not give up within %s", outage)
	}
	if es := rec.all("rec.conn.state", "reconnecting -> closed"); len(es) != 1 || !strings.Contains(es[0].what, "connection cleanup error") {
		t.Errorf("want reconnecting -> closed with a 'connection cleanup error' Err")
	}
	if closedErr == nil || !strings.Contains(closedErr.Error(), "connect: connection refused") {
		t.Errorf("StateChanged.Err: want the last dial error (connection refused), got %v", closedErr)
	}
	for _, src := range []string{"rec.conn.close", "rec.ch.close", "rec.notifyPublish", "rec.notifyReturn", "rec.deliveries"} {
		rec.waitFor(t, src, "chan closed", 5*time.Second)
	}
	if !errors.Is(pubErr, amqp.ErrClosed) || !errors.Is(chErr, amqp.ErrClosed) {
		t.Errorf("after give-up: want ErrClosed from Publish and Channel(), got %v / %v", pubErr, chErr)
	}
	if !conn.IsClosed() {
		t.Errorf("given-up connection reopened by itself")
	}
	if withBaseline {
		// Baseline: no recovery -> NotifyClose gets the 320 and is then closed;
		// the state goes straight open -> closed.
		rec.waitFor(t, "plain.conn.close", "(320)", 5*time.Second)
		rec.waitFor(t, "plain.conn.close", "chan closed", 5*time.Second)
		rec.waitFor(t, "plain.conn.state", "open -> closed", 5*time.Second)
		if len(rec.all("plain.conn.state", "reconnecting")) != 0 {
			t.Errorf("connection without Recovery entered reconnecting")
		}
	}
}

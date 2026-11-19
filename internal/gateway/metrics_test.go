package gateway

import (
	"slices"
	"testing"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/testutil"
)

// Before any connection, the gateway serves its three metrics at zero, with
// both reasons of a disconnection.
func TestMetrics_GatewayServesEveryMetricBeforeTraffic(t *testing.T) {
	reg := prometheus.NewRegistry()
	if err := RegisterMetrics(reg, New(testOptions(newFakeSnapshots()))); err != nil {
		t.Fatal(err)
	}
	want := []string{DisconnectedMetric, ConnectionsMetric, SubscriptionsMetric}
	if got := testutil.MetricNames(t, reg); !slices.Equal(got, want) {
		t.Errorf("metric names %v, want %v", got, want)
	}
	for _, s := range []testutil.Series{
		{Name: ConnectionsMetric},
		{Name: SubscriptionsMetric},
		{Name: DisconnectedMetric, Labels: testutil.Labels("reason", ReasonSlowClient)},
		{Name: DisconnectedMetric, Labels: testutil.Labels("reason", ReasonSnapshotTimeout)},
	} {
		if v := testutil.MetricValue(t, reg, s.Name, s.Labels); v != 0 {
			t.Errorf("%s%v = %v, want 0", s.Name, s.Labels, v)
		}
	}
}

// ws_connections counts the open connections and ws_subscriptions one per
// id and one per batch of each connection; both go down when a client
// unsubscribes or goes away.
func TestMetrics_ConnectionsAndSubscriptionsFollowTheClients(t *testing.T) {
	s := startServer(t, testOptions(newFakeSnapshots()))
	reg := prometheus.NewRegistry()
	if err := RegisterMetrics(reg, s.g); err != nil {
		t.Fatal(err)
	}
	value := func(name string) float64 { return testutil.MetricValue(t, reg, name, nil) }

	a, b := uuid.New(), uuid.New()
	batch := uuid.New()
	first, second := dial(t, s.url, nil), dial(t, s.url, nil)
	first.subscribe([]uuid.UUID{a, b, a}, &batch) // a twice: one subscription
	second.subscribe([]uuid.UUID{a}, nil)
	testutil.Eventually(t, func() bool { return value(ConnectionsMetric) == 2 && value(SubscriptionsMetric) == 4 }, waitTimeout)

	first.unsubscribe([]uuid.UUID{b}, nil)
	testutil.Eventually(t, func() bool { return value(SubscriptionsMetric) == 3 }, waitTimeout)

	_ = first.ws.CloseNow()
	testutil.Eventually(t, func() bool { return value(ConnectionsMetric) == 1 && value(SubscriptionsMetric) == 1 }, waitTimeout)
}

// ws_clients_disconnected_total reads the gateway's own counters of the
// connections it closed, by reason (the tests of the gateway prove when
// each counter moves).
func TestMetrics_DisconnectionsByReason(t *testing.T) {
	g := New(testOptions(newFakeSnapshots()))
	reg := prometheus.NewRegistry()
	if err := RegisterMetrics(reg, g); err != nil {
		t.Fatal(err)
	}
	g.slowClosed.Add(2)
	g.snapshotTimeouts.Add(1)
	if v := testutil.MetricValue(t, reg, DisconnectedMetric, testutil.Labels("reason", ReasonSlowClient)); v != 2 {
		t.Errorf("slow_client = %v, want 2", v)
	}
	if v := testutil.MetricValue(t, reg, DisconnectedMetric, testutil.Labels("reason", ReasonSnapshotTimeout)); v != 1 {
		t.Errorf("snapshot_timeout = %v, want 1", v)
	}
}

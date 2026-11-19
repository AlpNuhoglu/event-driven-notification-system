package gateway

import (
	"github.com/prometheus/client_golang/prometheus"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/platform/metrics"
)

// The names of the gateway's metrics (ADR-0009 §1).
const (
	ConnectionsMetric   = "ws_connections"
	SubscriptionsMetric = "ws_subscriptions"
	DisconnectedMetric  = "ws_clients_disconnected_total"
)

// The reasons of ws_clients_disconnected_total.
const (
	ReasonSlowClient      = "slow_client"
	ReasonSnapshotTimeout = "snapshot_timeout"
)

var (
	connectionsDesc = prometheus.NewDesc(ConnectionsMetric,
		"Open WebSocket connections.", nil, nil)
	subscriptionsDesc = prometheus.NewDesc(SubscriptionsMetric,
		"Subscriptions of the open connections: one per notification id and one per batch id of each connection.", nil, nil)
	disconnectedDesc = prometheus.NewDesc(DisconnectedMetric,
		"Connections the gateway closed, by reason: slow_client (the send queue was full), snapshot_timeout (the snapshot was not taken in time).",
		[]string{"reason"}, nil)
)

// collector reads the gateway's state when it is scraped: the connection
// registry and the subscription index under the gateway's read lock, and
// the counters it keeps.
type collector struct{ g *Gateway }

func (c collector) Describe(ch chan<- *prometheus.Desc) {
	ch <- connectionsDesc
	ch <- subscriptionsDesc
	ch <- disconnectedDesc
}

func (c collector) Collect(ch chan<- prometheus.Metric) {
	conns, subs := c.g.counts()
	ch <- prometheus.MustNewConstMetric(connectionsDesc, prometheus.GaugeValue, float64(conns))
	ch <- prometheus.MustNewConstMetric(subscriptionsDesc, prometheus.GaugeValue, float64(subs))
	ch <- prometheus.MustNewConstMetric(disconnectedDesc, prometheus.CounterValue, float64(c.g.SlowClientsClosed()), ReasonSlowClient)
	ch <- prometheus.MustNewConstMetric(disconnectedDesc, prometheus.CounterValue, float64(c.g.SnapshotTimeouts()), ReasonSnapshotTimeout)
}

// RegisterMetrics registers ws_connections, ws_subscriptions and
// ws_clients_disconnected_total{reason} of g on reg; a collector of an
// earlier gateway of the process is replaced (metrics.Replace).
func RegisterMetrics(reg prometheus.Registerer, g *Gateway) error {
	return metrics.Replace(reg, collector{g: g})
}

// counts returns the number of open connections and of their
// subscriptions: every (connection, id) pair of the subscription index.
func (g *Gateway) counts() (connections, subscriptions int) {
	g.mu.RLock()
	defer g.mu.RUnlock()
	for _, set := range g.byID {
		subscriptions += len(set)
	}
	for _, set := range g.byBatch {
		subscriptions += len(set)
	}
	return len(g.clients), subscriptions
}

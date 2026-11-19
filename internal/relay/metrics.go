package relay

import (
	"errors"
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
)

// The names of the relay's reconciliation counters (ADR-0009 §1).
const (
	ReconciliationRunsMetric = "reconciliation_runs_total"
	ReconciledRowsMetric     = "reconciled_rows_total"
)

// Metrics are the relay's counters.
type Metrics struct {
	reconciliationRuns prometheus.Counter
	reconciledRows     prometheus.Counter
}

// NewMetrics returns the relay's counters registered on reg (the role
// registers them on the default registry, which its admin port serves). A
// counter of the same name that is already registered there is reused, so a
// process that builds the relay twice keeps one series. A nil reg leaves the
// counters unregistered.
func NewMetrics(reg prometheus.Registerer) (*Metrics, error) {
	m := &Metrics{
		reconciliationRuns: prometheus.NewCounter(prometheus.CounterOpts{
			Name: ReconciliationRunsMetric,
			Help: "Reconciliation runs (transition 8 on a (re)connection to RabbitMQ) that completed.",
		}),
		reconciledRows: prometheus.NewCounter(prometheus.CounterOpts{
			Name: ReconciledRowsMetric,
			Help: "Queued notifications that reconciliation returned to pending.",
		}),
	}
	if reg == nil {
		return m, nil
	}
	var err error
	if m.reconciliationRuns, err = registerCounter(reg, m.reconciliationRuns); err != nil {
		return nil, fmt.Errorf("relay: register %s: %w", ReconciliationRunsMetric, err)
	}
	if m.reconciledRows, err = registerCounter(reg, m.reconciledRows); err != nil {
		return nil, fmt.Errorf("relay: register %s: %w", ReconciledRowsMetric, err)
	}
	return m, nil
}

func registerCounter(reg prometheus.Registerer, c prometheus.Counter) (prometheus.Counter, error) {
	err := reg.Register(c)
	var already prometheus.AlreadyRegisteredError
	if errors.As(err, &already) {
		if existing, ok := already.ExistingCollector.(prometheus.Counter); ok {
			return existing, nil
		}
	}
	return c, err
}

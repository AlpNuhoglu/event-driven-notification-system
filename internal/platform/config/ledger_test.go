package config

import (
	"fmt"
	"testing"
	"time"
)

// The defaults that the decision ledger fixes, each with the decision that
// fixes it (docs/ai/decision-ledger.md, docs/decision-tests.md).
// TestDefaultsMatchEnvExample keeps the code and .env.example together; this
// test keeps both at the decided value, so that a change of one of them is a
// change of a decision and fails here with its id. Values the ledger leaves
// open (buffers, queue limits, the alert thresholds of D74) are not listed:
// they are tuned in P6.
func TestDefaults_AreTheLedgerValues(t *testing.T) {
	c, err := LoadFrom(lookup(nil))
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		decision, name string
		got, want      any
	}{
		{"D3", "RELAY_TICK_INTERVAL", c.Relay.TickInterval, 100 * time.Millisecond},
		{"D5", "AMQP_RECOVERY_MAX_RETRIES", c.Connections.AMQPRecoveryMaxRetries, 24},
		{"D5", "AMQP_RECOVERY_RETRY_INTERVAL", c.Connections.AMQPRecoveryRetryInterval, 5 * time.Second},
		{"D5", "RECONCILE_MIN_INTERVAL", c.Relay.ReconcileMinEvery, 60 * time.Second},
		{"D8", "CONSUMER_TIMEOUT", c.Worker.ConsumerTimeout, 24 * time.Hour},
		{"D10", "RELAY_BATCH_SIZE", c.Relay.BatchSize, 200},
		{"D10", "RELAY_CONFIRM_TIMEOUT", c.Relay.ConfirmTimeout, 5 * time.Second},
		{"D12", "EXPIRY_INTERVAL", c.Relay.ExpiryInterval, time.Second},
		{"D12", "EXPIRY_BATCH_SIZE", c.Relay.ExpiryBatchSize, 1000},
		{"D16", "EXPIRES_AT_MIN_LEAD", c.API.ExpiresAtMinLead, 30 * time.Second},
		{"D18", "FINAL_WRITE_RETRY_WINDOW", c.Worker.FinalWriteRetryTime, 60 * time.Second},
		{"D20", "LEASE_DURATION", c.Worker.LeaseDuration, 30 * time.Second},
		{"D23", "WORKER_CONCURRENCY", c.Worker.Concurrency, 25},
		// D24 decided 98 "with the margin derived from load-smoke"; the author
		// lowered it to 97 in P6 after a run with a window of 100, and to 90 in
		// P7 after a run with a window of 102 (a margin of ten).
		{"D24", "RATE_LIMIT_PER_SECOND", c.RateLimit.PerSecond, 90},
		{"D24", "RATE_LIMIT_HORIZON", c.RateLimit.Horizon, 250 * time.Millisecond},
		{"D24", "RATE_LIMIT_RETRY_INTERVAL", c.RateLimit.RetryInterval, 50 * time.Millisecond},
		{"D26", "BREAKER_WINDOW", c.Breaker.Window, 30 * time.Second},
		{"D26", "BREAKER_MIN_REQUESTS", c.Breaker.MinRequests, 20},
		{"D26", "BREAKER_FAILURE_RATIO", c.Breaker.FailureRatio, 0.5},
		{"D26", "BREAKER_CONSECUTIVE_FAILURES", c.Breaker.ConsecutiveFailures, 5},
		{"D26", "BREAKER_OPEN_BASE", c.Breaker.OpenBase, 5 * time.Second},
		{"D26", "BREAKER_OPEN_MAX", c.Breaker.OpenMax, 2 * time.Minute},
		{"D26", "BREAKER_FATAL_OPEN", c.Breaker.FatalOpen, time.Minute},
		{"D28", "PROVIDER_IDEMPOTENCY_HEADER", c.Provider.IdempotencyHeader, "Idempotency-Key"},
		{"D29", "PROVIDER_CONNECT_TIMEOUT", c.Provider.ConnectTimeout, 2 * time.Second},
		{"D29", "PROVIDER_TIMEOUT", c.Provider.Timeout, 10 * time.Second},
		{"D30", "THROTTLE_DEFAULT_DELAY", c.Retry.ThrottleDefaultDelay, 5 * time.Second},
		{"D30", "THROTTLE_MAX_DELAY", c.Retry.ThrottleMaxDelay, 2 * time.Minute},
		{"D31", "TTL_HIGH", c.Retry.High.TTL, 10 * time.Minute},
		{"D31", "TTL_NORMAL", c.Retry.Normal.TTL, 6 * time.Hour},
		{"D31", "TTL_LOW", c.Retry.Low.TTL, 24 * time.Hour},
		{"D35", "MAX_BODY_BYTES", c.API.MaxBodyBytes, int64(5 << 20)},
		{"D36", "PHONE_VALIDATION", c.API.PhoneValidation, "possible"},
		{"D39", "SUMMARY_BACKLOG_CACHE_TTL", c.API.SummaryBacklogTTL, 10 * time.Second},
		{"D40", "TEMPLATE_MAX_BODY_BYTES", c.Templates.MaxBodyBytes, int64(64 << 10)},
		{"D40", "SCHEDULE_PAST_TOLERANCE", c.API.SchedulePastTolerance, 30 * time.Second},
		{"D46", "WORKER_CHANNELS", fmt.Sprint(c.Worker.Channels), fmt.Sprint([]string{"sms", "email", "push"})},
		{"D66", "API_ADDR", c.Ports.APIAddr, ":8080"},
		{"D66", "ADMIN_ADDR", c.Ports.AdminAddr, ":8081"},
		{"D66", "GATEWAY_ADDR", c.Ports.GatewayAddr, ":8082"},
		{"D68", "RETRY_MIN_DELAY", c.Retry.MinDelay, time.Second},
		{"D77", "PACER_SLOT_TOLERANCE", c.RateLimit.SlotTolerance, 5 * time.Millisecond},
		{"D78", "IDEMPOTENCY_CLEANUP_INTERVAL", c.Relay.IdempotencyCleanupEvery, time.Minute},
	} {
		if tc.got != tc.want {
			t.Errorf("%s: %s defaults to %v, the ledger decided %v", tc.decision, tc.name, tc.got, tc.want)
		}
	}
	// D20: the send deadline LEASE - HTTP timeout - 5 s is 15 s with the
	// defaults (D77), and the lease covers the HTTP timeout.
	if d := c.Worker.LeaseDuration - c.Provider.Timeout - 5*time.Second; d != 15*time.Second {
		t.Errorf("D20/D77: send deadline %v with the defaults, the ledger derived 15s", d)
	}
}

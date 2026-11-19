package config

import (
	"bufio"
	"os"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"
)

func envFile(t *testing.T) map[string]string {
	t.Helper()
	f, err := os.Open("../../../.env.example")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	m := map[string]string{}
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		name, value, ok := strings.Cut(line, "=")
		if !ok {
			t.Fatalf("unparsable line in .env.example: %q", name)
		}
		m[name] = value
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
	return m
}

func lookup(m map[string]string) func(string) (string, bool) {
	return func(k string) (string, bool) { v, ok := m[k]; return v, ok }
}

// composeOnly lists the variables of .env.example that docker-compose.yml
// interpolates itself (published host ports) and that no service reads.
var composeOnly = []string{"API_PORT", "MOCKPROVIDER_PORT", "GRAFANA_PORT", "PROMETHEUS_PORT", "JAEGER_UI_PORT"}

func TestKeysMatchEnvExample(t *testing.T) {
	file := envFile(t)
	keys := Keys()
	for _, name := range composeOnly {
		if slices.Contains(keys, name) {
			t.Errorf("%s is listed as Compose-only but is read by the loader", name)
		}
		if _, ok := file[name]; !ok {
			t.Errorf("%s is listed as Compose-only but missing from .env.example", name)
		}
	}
	for name := range file {
		if slices.Contains(composeOnly, name) {
			continue
		}
		if !slices.Contains(keys, name) {
			t.Errorf("%s is in .env.example but not read by the loader", name)
		}
	}
	for _, name := range keys {
		if _, ok := file[name]; !ok {
			t.Errorf("%s is read by the loader but missing from .env.example", name)
		}
	}
}

func TestDefaultsMatchEnvExample(t *testing.T) {
	want, err := LoadFrom(lookup(nil))
	if err != nil {
		t.Fatal(err)
	}
	file := envFile(t)
	file["PROVIDER_URL"] = ""
	got, err := LoadFrom(lookup(file))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(want, got) {
		t.Errorf("defaults differ from .env.example\ndefaults: %+v\nfile:     %+v", want, got)
	}
}

func TestOverrides(t *testing.T) {
	cfg, err := LoadFrom(lookup(map[string]string{
		"PROVIDER_NAME":           "secondary",
		"RELAY_TICK_INTERVAL":     "250ms",
		"MAX_BODY_BYTES":          "1024",
		"RELAY_BATCH_SIZE":        "7",
		"BREAKER_FAILURE_RATIO":   "0.25",
		"OTEL_TRACES_SAMPLER_ARG": "0",
		"WORKER_CHANNELS":         " email , push ",
		"WS_ORIGIN_PATTERNS":      "a.example.com, *.example.org",
		"RETRY_LOW_BASE":          "2s",
		"PHONE_VALIDATION":        "strict",
		"LOG_LEVEL":               "debug",
		"RATE_LIMIT_PER_SECOND":   "10",

		"RATE_LIMIT_REDIS_TIMEOUT":           "30ms",
		"RATE_LIMIT_FALLBACK_PROBE_INTERVAL": "40ms",

		"AMQP_RECOVERY_RETRY_INTERVAL": "20ms",
		"AMQP_RECOVERY_MAX_RETRIES":    "3",
	}))
	if err != nil {
		t.Fatal(err)
	}
	switch {
	case cfg.Provider.Name != "secondary",
		cfg.Relay.TickInterval != 250*time.Millisecond,
		cfg.API.MaxBodyBytes != 1024,
		cfg.Relay.BatchSize != 7,
		cfg.Breaker.FailureRatio != 0.25,
		cfg.Observability.TracesSamplerArg != 0,
		!reflect.DeepEqual(cfg.Worker.Channels, []string{"email", "push"}),
		!reflect.DeepEqual(cfg.Events.WSOriginPatterns, []string{"a.example.com", "*.example.org"}),
		cfg.Retry.Low.Base != 2*time.Second,
		cfg.Retry.Low.Cap != time.Hour,
		cfg.API.PhoneValidation != "strict",
		cfg.Observability.LogLevel != "debug",
		cfg.Connections.AMQPRecoveryRetryInterval != 20*time.Millisecond,
		cfg.Connections.AMQPRecoveryMaxRetries != 3,
		cfg.RateLimit.PerSecond != 10,
		cfg.RateLimit.RedisTimeout != 30*time.Millisecond,
		cfg.RateLimit.FallbackProbeInterval != 40*time.Millisecond:
		t.Errorf("overrides not applied: %+v", cfg)
	}
}

func TestInvalid(t *testing.T) {
	const secret = "s3cr3t-value"
	tests := []struct {
		name, value string
	}{
		{"RELAY_TICK_INTERVAL", secret},
		{"RELAY_TICK_INTERVAL", "0s"},
		{"PROVIDER_TIMEOUT", "-5s"},
		{"RELAY_BATCH_SIZE", secret},
		{"RELAY_BATCH_SIZE", "0"},
		{"AMQP_RECOVERY_RETRY_INTERVAL", "0s"},
		{"AMQP_RECOVERY_MAX_RETRIES", "0"},
		{"MAX_BODY_BYTES", "-1"},
		{"TEMPLATE_MAX_BODY_BYTES", secret},
		{"BREAKER_FAILURE_RATIO", "0"},
		{"BREAKER_FAILURE_RATIO", "1.5"},
		{"BREAKER_FAILURE_RATIO", secret},
		{"OTEL_TRACES_SAMPLER_ARG", "1.1"},
		{"OTEL_TRACES_SAMPLER_ARG", "-0.1"},
		{"RATE_LIMIT_PER_SECOND", "9.5"},
		{"RATE_LIMIT_PER_SECOND", "0"},
		{"WORKER_CHANNELS", "sms,fax"},
		{"WORKER_CHANNELS", "sms,sms"},
		{"WORKER_CHANNELS", " ; "},
		{"PHONE_VALIDATION", secret},
		{"LOG_LEVEL", secret},
	}
	for _, tt := range tests {
		t.Run(tt.name+"="+tt.value, func(t *testing.T) {
			_, err := LoadFrom(lookup(map[string]string{tt.name: tt.value}))
			if err == nil {
				t.Fatal("expected error")
			}
			if !strings.Contains(err.Error(), tt.name) {
				t.Errorf("error does not name the variable: %v", err)
			}
			if strings.Contains(err.Error(), tt.value) {
				t.Errorf("error contains the value: %v", err)
			}
		})
	}
}

func TestMultipleErrorsReported(t *testing.T) {
	_, err := LoadFrom(lookup(map[string]string{
		"RELAY_TICK_INTERVAL": "bad-one",
		"LOG_LEVEL":           "bad-two",
	}))
	if err == nil {
		t.Fatal("expected error")
	}
	msg := err.Error()
	for _, want := range []string{"RELAY_TICK_INTERVAL", "LOG_LEVEL"} {
		if !strings.Contains(msg, want) {
			t.Errorf("error missing %s: %v", want, err)
		}
	}
	if strings.Contains(msg, "bad-one") || strings.Contains(msg, "bad-two") {
		t.Errorf("error contains a value: %v", err)
	}
}

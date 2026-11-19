package mockprovider

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"
)

const (
	defaultStatus     = http.StatusAccepted
	defaultFailStatus = http.StatusInternalServerError
	// maxLatencyMS bounds latency_ms and latency_jitter_ms (one hour), so the
	// conversion to a time.Duration can never overflow.
	maxLatencyMS = 3_600_000
)

// Rule describes how the mock answers the requests it applies to. Build one
// with defaultRule: the zero value is not the default rule.
type Rule struct {
	Status          int     `json:"status"`
	FailStatus      int     `json:"fail_status"`
	FailRate        float64 `json:"fail_rate"`
	FailFirst       int     `json:"fail_first"`
	RetryAfter      string  `json:"retry_after"`
	Location        string  `json:"location"`
	LatencyMS       int     `json:"latency_ms"`
	LatencyJitterMS int     `json:"latency_jitter_ms"`
	CloseConnection bool    `json:"close_connection"`
	Hang            bool    `json:"hang"`
	MalformedBody   bool    `json:"malformed_body"`
}

func defaultRule() Rule {
	return Rule{Status: defaultStatus, FailStatus: defaultFailStatus}
}

// Config is the whole configuration: a default rule and per-channel rules.
// It is also the body of the answer to PUT /admin/config; Status repeats
// Default.Status, so the shorthand case keeps its status member.
type Config struct {
	Status   int             `json:"status"`
	Default  Rule            `json:"default"`
	Channels map[string]Rule `json:"channels"`
}

func defaultConfig() Config {
	return Config{Status: defaultStatus, Default: defaultRule(), Channels: map[string]Rule{}}
}

// rule returns the rule for a channel, falling back to the default rule.
func (c Config) rule(channel string) Rule {
	if r, ok := c.Channels[channel]; ok {
		return r
	}
	return c.Default
}

// ruleIn is a rule as received: a nil member is absent.
type ruleIn struct {
	Status          *int     `json:"status"`
	FailStatus      *int     `json:"fail_status"`
	FailRate        *float64 `json:"fail_rate"`
	FailFirst       *int     `json:"fail_first"`
	RetryAfter      *string  `json:"retry_after"`
	Location        *string  `json:"location"`
	LatencyMS       *int     `json:"latency_ms"`
	LatencyJitterMS *int     `json:"latency_jitter_ms"`
	CloseConnection *bool    `json:"close_connection"`
	Hang            *bool    `json:"hang"`
	MalformedBody   *bool    `json:"malformed_body"`
}

type configIn struct {
	Status   *int               `json:"status"`
	Default  *ruleIn            `json:"default"`
	Channels map[string]*ruleIn `json:"channels"`
}

func checkStatus(name string, p *int) error {
	if p != nil && (*p < 100 || *p > 599) {
		return fmt.Errorf("%s must be 100-599", name)
	}
	return nil
}

func checkLatency(name string, p *int) error {
	if p != nil && (*p < 0 || *p > maxLatencyMS) {
		return fmt.Errorf("%s must be 0-%d", name, maxLatencyMS)
	}
	return nil
}

func setInt(dst, src *int) {
	if src != nil {
		*dst = *src
	}
}

// build validates the rule and applies the defaults to the absent members.
func (in *ruleIn) build(where string) (Rule, error) {
	r := defaultRule()
	if in == nil {
		return r, nil
	}
	for _, err := range []error{
		checkStatus("status", in.Status),
		checkStatus("fail_status", in.FailStatus),
		checkLatency("latency_ms", in.LatencyMS),
		checkLatency("latency_jitter_ms", in.LatencyJitterMS),
	} {
		if err != nil {
			return Rule{}, fmt.Errorf("%s: %w", where, err)
		}
	}
	if in.FailRate != nil && (*in.FailRate < 0 || *in.FailRate > 1) {
		return Rule{}, fmt.Errorf("%s: fail_rate must be 0-1", where)
	}
	if in.FailFirst != nil && *in.FailFirst < 0 {
		return Rule{}, fmt.Errorf("%s: fail_first must be 0 or more", where)
	}
	setInt(&r.Status, in.Status)
	setInt(&r.FailStatus, in.FailStatus)
	setInt(&r.FailFirst, in.FailFirst)
	setInt(&r.LatencyMS, in.LatencyMS)
	setInt(&r.LatencyJitterMS, in.LatencyJitterMS)
	if in.FailRate != nil {
		r.FailRate = *in.FailRate
	}
	if in.RetryAfter != nil {
		r.RetryAfter = *in.RetryAfter
	}
	if in.Location != nil {
		r.Location = *in.Location
	}
	if in.CloseConnection != nil {
		r.CloseConnection = *in.CloseConnection
	}
	if in.Hang != nil {
		r.Hang = *in.Hang
	}
	if in.MalformedBody != nil {
		r.MalformedBody = *in.MalformedBody
	}
	return r, nil
}

// parseConfig reads a strict configuration: unknown fields, wrong types,
// out-of-range values and trailing data are errors.
func parseConfig(body io.Reader) (Config, error) {
	dec := json.NewDecoder(body)
	dec.DisallowUnknownFields()
	var in configIn
	if err := dec.Decode(&in); err != nil {
		return Config{}, fmt.Errorf("invalid configuration: %w", err)
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return Config{}, errors.New("invalid configuration: unexpected data after the JSON object")
	}
	if in.Status != nil && in.Default != nil {
		return Config{}, errors.New("send either status or default, not both")
	}
	if err := checkStatus("status", in.Status); err != nil {
		return Config{}, err
	}
	def := in.Default
	if in.Status != nil {
		def = &ruleIn{Status: in.Status}
	}
	cfg := defaultConfig()
	var err error
	if cfg.Default, err = def.build("default"); err != nil {
		return Config{}, err
	}
	cfg.Status = cfg.Default.Status
	for ch, rin := range in.Channels {
		switch ch {
		case "sms", "email", "push":
		default:
			return Config{}, fmt.Errorf("unknown channel %q", ch)
		}
		if cfg.Channels[ch], err = rin.build("channels." + ch); err != nil {
			return Config{}, err
		}
	}
	return cfg, nil
}

// outcome is what the mock decided for one request.
type outcome struct {
	rule    Rule
	status  int
	latency time.Duration
}

// decideLocked applies a rule to a request. The caller holds s.mu; seen is
// the number of earlier requests with the same Idempotency-Key.
func (s *Server) decideLocked(rule Rule, seen int) outcome {
	o := outcome{rule: rule, status: rule.Status}
	failed := seen < rule.FailFirst
	if !failed && rule.FailRate > 0 {
		failed = s.rand() < rule.FailRate
	}
	if failed {
		o.status = rule.FailStatus
	}
	if rule.LatencyMS > 0 || rule.LatencyJitterMS > 0 {
		ms := float64(rule.LatencyMS)
		if rule.LatencyJitterMS > 0 {
			ms += s.rand() * float64(rule.LatencyJitterMS)
		}
		o.latency = time.Duration(ms * float64(time.Millisecond))
	}
	return o
}

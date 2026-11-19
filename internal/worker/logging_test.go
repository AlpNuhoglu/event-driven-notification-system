package worker

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
)

// workerLine is one line of the central handler, with the fields these
// tests read.
type workerLine struct {
	Level          string `json:"level"`
	Msg            string `json:"msg"`
	NotificationID string `json:"notification_id"`
	CorrelationID  string `json:"correlation_id"`
	Outcome        string `json:"outcome"`
	HTTPStatus     int    `json:"http_status"`
}

func workerLines(t *testing.T, h *harness) []workerLine {
	t.Helper()
	var out []workerLine
	for raw := range strings.Lines(h.logs.String()) {
		var l workerLine
		if err := json.Unmarshal([]byte(raw), &l); err != nil {
			t.Fatalf("log line %q: %v", raw, err)
		}
		out = append(out, l)
	}
	return out
}

// checkNotificationLines checks every line of the notification id: it
// carries the notification's correlation id, and the lines of the
// transitions are want (message to level), each exactly once.
func checkNotificationLines(t *testing.T, h *harness, id string, want map[string]string) {
	t.Helper()
	seen := map[string]int{}
	for _, l := range workerLines(t, h) {
		if l.NotificationID != id {
			continue
		}
		if l.CorrelationID != "corr-"+id {
			t.Errorf("line %q has correlation_id %q, want corr-%s", l.Msg, l.CorrelationID, id)
		}
		if lvl, ok := want[l.Msg]; ok {
			seen[l.Msg]++
			if l.Level != lvl {
				t.Errorf("%q at %s, want %s", l.Msg, l.Level, lvl)
			}
		}
	}
	for msg := range want {
		if seen[msg] != 1 {
			t.Errorf("%d lines %q, want 1:\n%s", seen[msg], msg, h.logs)
		}
	}
}

// A provider error that echoes the message's recipient and content is
// stored without them and never logged; every line of the notification
// carries its correlation id, the intermediate transition at debug and the
// final outcome at info (ADR-0009 §4, §5).
func TestLogs_FailureWithAnEchoingProviderError(t *testing.T) {
	h := newHarness(t)
	h.central = true
	h.build()
	h.st.failStatus = domain.StatusFailed
	echo := fmt.Sprintf(`{"error":"invalid recipient %s","message":%q,"pad":"%s"}`,
		testRecipient, testContent, strings.Repeat("z", 2*domain.MaxProviderErrorBytes))
	// The echo is the body of the provider's 400, as the client hands it on.
	h.prov.respond = func(Request) Response {
		return Response{StatusCode: 400, ErrorBody: echo, WroteRequest: true}
	}
	m := h.push(prioNormal)
	h.start()
	h.eventually(func() bool { return h.st.finals() == 1 })
	h.stop()

	_, _, failures, _ := h.st.snapshot()
	if len(failures) != 1 || failures[0].result.Error == nil {
		t.Fatalf("failures %+v, want one with an error", failures)
	}
	stored := *failures[0].result.Error
	masked := domain.Recipient(testRecipient).Masked()
	if strings.Contains(stored, testRecipient) || strings.Contains(stored, testContent) ||
		!strings.Contains(stored, masked) || !strings.Contains(stored, domain.ContentRedacted) {
		t.Errorf("stored error not redacted: %q", stored)
	}
	if len(stored) > domain.MaxProviderErrorBytes {
		t.Errorf("stored error is %d bytes, limit %d", len(stored), domain.MaxProviderErrorBytes)
	}

	logs := h.logs.String()
	for _, secret := range []string{testRecipient, masked, testContent, "invalid recipient", "zzzz"} {
		if strings.Contains(logs, secret) {
			t.Errorf("the log contains %q:\n%s", secret, logs)
		}
	}
	checkNotificationLines(t, h, m.id.String(), map[string]string{
		"notification processing": "DEBUG",
		"notification failed":     "INFO",
	})
	for _, l := range workerLines(t, h) {
		if l.Msg == "notification failed" && (l.Outcome != string(domain.OutcomePermanent) || l.HTTPStatus != 400) {
			t.Errorf("the final line names outcome %q and status %d, want permanent and 400", l.Outcome, l.HTTPStatus)
		}
	}
}

// A sent notification: one debug line for the claim, one info line for
// the outcome, both with the correlation id of its row.
func TestLogs_SentNotification(t *testing.T) {
	h := newHarness(t)
	h.central = true
	h.build()
	m := h.push(prioNormal)
	h.start()
	h.eventually(func() bool { return h.st.finals() == 1 })
	h.stop()

	checkNotificationLines(t, h, m.id.String(), map[string]string{
		"notification processing": "DEBUG",
		"notification sent":       "INFO",
	})
	logs := h.logs.String()
	for _, secret := range []string{testRecipient, domain.Recipient(testRecipient).Masked(), testContent} {
		if strings.Contains(logs, secret) {
			t.Errorf("the log contains %q:\n%s", secret, logs)
		}
	}
}

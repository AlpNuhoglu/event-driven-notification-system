//go:build e2e

package e2e

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
)

// templated holds the fields of the Notification schema that the template
// scenario reads.
type templated struct {
	ID       string `json:"id"`
	Channel  string `json:"channel"`
	Status   string `json:"status"`
	Template *struct {
		Name    string `json:"name"`
		Version int    `json:"version"`
	} `json:"template"`
	SMS *struct {
		Encoding string `json:"encoding"`
		Segments int    `json:"segments"`
	} `json:"sms"`
}

// postJSON posts body to the API and decodes the answer into out when the
// status is want.
func (s *stack) postJSON(t *testing.T, path string, body any, want int, out any) {
	t.Helper()
	code, _, b, err := s.do(http.MethodPost, s.apiURL+path, body)
	if err != nil {
		t.Fatalf("POST %s: %v", path, err)
	}
	if code != want {
		t.Fatalf("POST %s: status %d, want %d: %s", path, code, want, b)
	}
	if out != nil {
		if err := json.Unmarshal(b, out); err != nil {
			t.Fatalf("POST %s: decode %s: %v", path, b, err)
		}
	}
}

// Templates on the stack (ADR-0008): an SMS and an e-mail template are
// created and previewed, notifications are created from them (the e-mail in
// a batch), every one reaches sent, and the API shows the provenance and the
// segment count of the rendered text. mockprovider stores no content, so the
// rendered text is checked through the preview and the counts.
func TestComposeE2E_Templates(t *testing.T) {
	s := newStack(t)
	s.resetMock(t)
	// Unique names, so the scenario also runs against a stack it ran on.
	suffix := fmt.Sprint(time.Now().UnixNano())
	smsName, mailName := "e2e-otp-"+suffix, "e2e-welcome-"+suffix

	s.postJSON(t, "/v1/templates", map[string]any{
		"name": smsName, "channel": "sms",
		"body":      "Doğrulama kodunuz {{ .code }}. Bu kodu kimseyle paylaşmayın; {{ .minutes }} dakika geçerlidir.",
		"variables": []string{"code", "minutes"},
	}, http.StatusCreated, nil)
	s.postJSON(t, "/v1/templates", map[string]any{
		"name": mailName, "channel": "email",
		"body":               `<p>Hello {{ .name | default "Customer" }}</p>`,
		"optional_variables": []string{"name"},
	}, http.StatusCreated, nil)

	// The preview renders what a create will store, with its SMS cost.
	smsVars := map[string]any{"code": json.Number("482913"), "minutes": 5}
	want := "Doğrulama kodunuz 482913. Bu kodu kimseyle paylaşmayın; 5 dakika geçerlidir."
	info := domain.CountSMS(want)
	var preview struct {
		Content  string  `json:"content"`
		Encoding *string `json:"encoding"`
		Segments *int    `json:"segments"`
	}
	s.postJSON(t, "/v1/templates/"+smsName+"/preview", map[string]any{"variables": smsVars}, http.StatusOK, &preview)
	if preview.Content != want || preview.Encoding == nil || *preview.Encoding != "UCS-2" ||
		preview.Segments == nil || *preview.Segments != info.Segments || info.Segments < 2 {
		t.Fatalf("sms preview: %+v (want %d UCS-2 segments)", preview, info.Segments)
	}
	s.postJSON(t, "/v1/templates/"+mailName+"/preview", map[string]any{"variables": map[string]any{"name": "<b>Ada</b>"}}, http.StatusOK, &preview)
	if preview.Content != "<p>Hello &lt;b&gt;Ada&lt;/b&gt;</p>" || preview.Encoding != nil || preview.Segments != nil {
		t.Fatalf("e-mail preview: %+v", preview)
	}

	// A single create from the SMS template.
	var single templated
	s.postJSON(t, "/v1/notifications", map[string]any{
		"channel": "sms", "recipient": fakePhone, "priority": "high",
		"template": map[string]any{"name": smsName, "variables": smsVars},
	}, http.StatusAccepted, &single)
	// A batch with the e-mail template and a content item.
	var batch struct {
		Accepted []struct {
			Index int    `json:"index"`
			ID    string `json:"id"`
		} `json:"accepted"`
		Rejected []json.RawMessage `json:"rejected"`
	}
	s.postJSON(t, "/v1/notifications/batch", map[string]any{"notifications": []any{
		map[string]any{
			"channel": "email", "recipient": fakeEmail, "priority": "high",
			"template": map[string]any{"name": mailName, "version": 1, "variables": map[string]any{"name": "Ada"}},
		},
		map[string]any{"channel": "push", "recipient": fakeDevice, "priority": "high", "content": "hello"},
	}}, http.StatusAccepted, &batch)
	if len(batch.Accepted) != 2 || len(batch.Rejected) != 0 {
		t.Fatalf("batch: accepted %v, rejected %d", batch.Accepted, len(batch.Rejected))
	}

	ids := []string{single.ID, batch.Accepted[0].ID, batch.Accepted[1].ID}
	for _, id := range ids {
		s.waitSent(t, id, sendTimeout)
	}
	read := func(id string) templated {
		t.Helper()
		code, _, b, err := s.do(http.MethodGet, s.apiURL+"/v1/notifications/"+id, nil)
		if err != nil || code != http.StatusOK {
			t.Fatalf("get %s: %d %v", id, code, err)
		}
		var n templated
		if err := json.Unmarshal(b, &n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	sms := read(single.ID)
	if sms.Status != "sent" || sms.Template == nil || sms.Template.Name != smsName || sms.Template.Version != 1 {
		t.Errorf("sms: %+v", sms)
	}
	if sms.SMS == nil || sms.SMS.Encoding != "UCS-2" || sms.SMS.Segments != info.Segments {
		t.Errorf("sms cost %+v, want UCS-2 with %d segments", sms.SMS, info.Segments)
	}
	mail := read(batch.Accepted[0].ID)
	if mail.Status != "sent" || mail.Template == nil || mail.Template.Name != mailName || mail.Template.Version != 1 || mail.SMS != nil {
		t.Errorf("e-mail: %+v", mail)
	}
	if push := read(batch.Accepted[1].ID); push.Template != nil {
		t.Errorf("push from content has a template: %+v", push.Template)
	}

	rep, err := s.report()
	if err != nil {
		t.Fatal(err)
	}
	if rep.Total != 3 || rep.Channels["sms"].Count != 1 || rep.Channels["email"].Count != 1 || rep.Channels["push"].Count != 1 {
		t.Errorf("mockprovider received %d requests (%v), want one per channel", rep.Total, rep.Channels)
	}
	for _, id := range ids {
		if rep.IdempotencyKeys[id] != 1 {
			t.Errorf("notification %s reached the provider %d times, want once", id, rep.IdempotencyKeys[id])
		}
	}
}

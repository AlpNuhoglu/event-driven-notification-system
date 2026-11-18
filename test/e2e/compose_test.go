//go:build e2e

package e2e

import (
	"net/http"
	"slices"
	"strings"
	"testing"
)

func TestComposeE2E_Health(t *testing.T) {
	s := newStack(t)
	s.resetMock(t)
	for _, path := range []string{"/healthz", "/readyz"} {
		code, _, body, err := s.do(http.MethodGet, s.apiURL+path, nil)
		if err != nil {
			t.Fatalf("GET %s: %v", path, err)
		}
		if code != http.StatusOK {
			t.Errorf("GET %s: status %d, want 200: %s", path, code, body)
		}
	}
}

// A notification created through the API reaches sent at the provider, and
// the provider receives exactly the three contract fields (R23). One
// notification per channel, so that every channel's queue and pacer is used.
func TestComposeE2E_CreatedNotificationIsSent(t *testing.T) {
	s := newStack(t)
	cases := []createRequest{
		{Channel: "sms", Recipient: fakePhone, Content: "hello", Priority: "high"},
		{Channel: "email", Recipient: fakeEmail, Content: "hello", Priority: "high"},
		{Channel: "push", Recipient: fakeDevice, Content: "hello", Priority: "high"},
	}
	for _, req := range cases {
		t.Run(req.Channel, func(t *testing.T) {
			s.resetMock(t)

			created, location, err := s.create(req)
			if err != nil {
				t.Fatal(err)
			}
			if created.ID == "" {
				t.Fatal("create response has no id")
			}
			if want := "/v1/notifications/" + created.ID; location != want {
				t.Errorf("Location %q, want %q", location, want)
			}
			if created.Recipient != req.Recipient {
				t.Errorf("create response recipient is not the submitted recipient (got %d characters)", len(created.Recipient))
			}
			if created.Channel != req.Channel || created.Priority != req.Priority {
				t.Errorf("create response channel/priority %s/%s, want %s/%s",
					created.Channel, created.Priority, req.Channel, req.Priority)
			}

			got := s.waitSent(t, created.ID, sendTimeout)
			if got.AttemptCount != 1 {
				t.Errorf("attempt_count %d, want 1", got.AttemptCount)
			}
			if got.ProviderMessageID == nil || *got.ProviderMessageID == "" {
				t.Error("provider_message_id is not set")
			}
			if got.Recipient != req.Recipient {
				t.Errorf("get response recipient is not the submitted recipient (got %d characters)", len(got.Recipient))
			}

			rep, err := s.report()
			if err != nil {
				t.Fatal(err)
			}
			if rep.Total != 1 {
				t.Errorf("mockprovider received %d requests, want 1", rep.Total)
			}
			if n := rep.Channels[req.Channel].Count; n != 1 {
				t.Errorf("mockprovider received %d %s requests, want 1", n, req.Channel)
			}
			if len(rep.IdempotencyKeys) != 1 || rep.IdempotencyKeys[created.ID] != 1 {
				t.Errorf("idempotency keys %v, want exactly {%s: 1}", rep.IdempotencyKeys, created.ID)
			}
			if rep.DuplicateKeys != 0 {
				t.Errorf("duplicate_keys %d, want 0", rep.DuplicateKeys)
			}
			want := [][]string{{"channel", "content", "to"}}
			if !slices.EqualFunc(rep.FieldNameSets, want, slices.Equal[[]string]) {
				t.Errorf("field_name_sets %s, want %s", fieldSets(rep.FieldNameSets), fieldSets(want))
			}
		})
	}
}

func fieldSets(sets [][]string) string {
	parts := make([]string, len(sets))
	for i, s := range sets {
		parts[i] = "[" + strings.Join(s, ",") + "]"
	}
	return "[" + strings.Join(parts, " ") + "]"
}

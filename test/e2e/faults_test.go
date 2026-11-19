//go:build e2e

package e2e

import (
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
	"testing"
)

// failFirst5xx is the number of 500 answers before the 202.
const failFirst5xx = 2

// attempt holds the fields of the Attempt schema the tests read.
type attempt struct {
	AttemptNo  int    `json:"attempt_no"`
	Outcome    string `json:"outcome"`
	HTTPStatus *int   `json:"http_status"`
}

// attempts reads GET /v1/notifications/{id}/attempts.
func (s *stack) attempts(id string) ([]attempt, error) {
	code, _, body, err := s.do(http.MethodGet, s.apiURL+"/v1/notifications/"+id+"/attempts", nil)
	if err != nil {
		return nil, err
	}
	if code != http.StatusOK {
		return nil, fmt.Errorf("attempts %s: status %d, want 200: %s", id, code, body)
	}
	var list struct {
		Data []attempt `json:"data"`
	}
	if err := json.Unmarshal(body, &list); err != nil {
		return nil, fmt.Errorf("decode attempts: %w", err)
	}
	return list.Data, nil
}

// configureMock sends a configuration to mockprovider's PUT /admin/config.
func (s *stack) configureMock(t *testing.T, cfg any) {
	t.Helper()
	code, _, body, err := s.do(http.MethodPut, s.mockURL+"/admin/config", cfg)
	if err != nil || code != http.StatusOK {
		t.Fatalf("configure mockprovider: status %d, error %v: %s", code, err, body)
	}
}

// The exit criterion of the retry pull request: a provider that answers 5xx
// is retried until it accepts, and the notification ends sent. The mock
// answers 500 to the first two requests of every idempotency key on the sms
// channel and 202 after that; the stack runs with its default durations (the
// high profile retries after about one second).
func TestComposeE2E_5xxRetriedUntilSent(t *testing.T) {
	s := newStack(t)
	s.resetMock(t)
	// The mock is shared with the other tests of the package: leave it clean.
	t.Cleanup(func() { s.resetMock(t) })
	s.configureMock(t, map[string]any{"channels": map[string]any{
		"sms": map[string]any{"fail_first": failFirst5xx, "fail_status": 500},
	}})

	created, _, err := s.create(createRequest{Channel: "sms", Recipient: fakePhone, Content: "retry me", Priority: "high"})
	if err != nil {
		t.Fatal(err)
	}
	got := s.waitSent(t, created.ID, sendTimeout)
	if got.AttemptCount != failFirst5xx+1 {
		t.Errorf("attempt_count %d, want %d", got.AttemptCount, failFirst5xx+1)
	}
	if got.ProviderMessageID == nil || *got.ProviderMessageID == "" {
		t.Error("provider_message_id is not set")
	}

	list, err := s.attempts(created.ID)
	if err != nil {
		t.Fatal(err)
	}
	var outcomes []string
	var statuses []int
	for i, a := range list {
		if a.AttemptNo != i+1 {
			t.Errorf("attempt %d has attempt_no %d", i+1, a.AttemptNo)
		}
		outcomes = append(outcomes, a.Outcome)
		if a.HTTPStatus == nil {
			statuses = append(statuses, 0)
		} else {
			statuses = append(statuses, *a.HTTPStatus)
		}
	}
	wantOutcomes := []string{"retryable", "retryable", "success"}
	wantStatuses := []int{500, 500, 202}
	if !slices.Equal(outcomes, wantOutcomes) || !slices.Equal(statuses, wantStatuses) {
		t.Errorf("attempts: outcomes %v, statuses %v; want %v and %v", outcomes, statuses, wantOutcomes, wantStatuses)
	}

	rep, err := s.report()
	if err != nil {
		t.Fatal(err)
	}
	if len(rep.IdempotencyKeys) != 1 || rep.IdempotencyKeys[created.ID] != failFirst5xx+1 {
		t.Errorf("idempotency keys %v, want exactly {%s: %d}", rep.IdempotencyKeys, created.ID, failFirst5xx+1)
	}
	if st := rep.Channels["sms"].Statuses; st["500"] != failFirst5xx || st["202"] != 1 {
		t.Errorf("mockprovider answered %v, want 500 x %d and 202 x 1", st, failFirst5xx)
	}
}

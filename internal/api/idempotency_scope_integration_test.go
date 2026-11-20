//go:build integration

package api

import (
	"net/http"
	"testing"

	"github.com/google/uuid"
)

// The Idempotency-Key applies to the two create endpoints only (D35): on a
// cancel and on a redrive the header is ignored. The same key sent for two
// different notifications is answered for each one, nothing is replayed and
// no key is stored. If the key were honoured there, the second call would
// replay the first notification's answer.
func TestIdempotencyKey_IgnoredOutsideTheCreates(t *testing.T) {
	a := newCancelAPI(t)
	storedKeys := func() int { return a.rows(t, "idempotency_keys") }

	call := func(path, body, key string) response {
		t.Helper()
		return a.do(http.MethodPost, path, body, map[string][]string{"Idempotency-Key": {key}})
	}
	check := func(what string, r response, id uuid.UUID, wantStatus string) {
		t.Helper()
		if r.status != http.StatusOK {
			t.Fatalf("%s: status %d: %s", what, r.status, r.body)
		}
		if v := r.header.Values("Idempotent-Replayed"); len(v) != 0 {
			t.Errorf("%s: Idempotent-Replayed %q, want none", what, v)
		}
		n := r.object(t)
		if doc, ok := n["notification"].(map[string]any); ok { // a cancel's answer
			n = doc
		}
		if n["id"] != id.String() || n["status"] != wantStatus {
			t.Errorf("%s: answered id %v status %v, want %s %s", what, n["id"], n["status"], id, wantStatus)
		}
	}

	first, second := a.create(t, smsBody), a.create(t, smsBody)
	third, fourth := a.create(t, smsBody), a.create(t, smsBody)
	a.toFailed(t, third)
	a.toFailed(t, fourth)
	keys := storedKeys()

	check("first cancel", call("/v1/notifications/"+first.String()+"/cancel", "", "key-outside"), first, "cancelled")
	check("second cancel", call("/v1/notifications/"+second.String()+"/cancel", "", "key-outside"), second, "cancelled")
	check("first redrive", call("/v1/notifications/"+third.String()+"/redrive", "", "key-redrive"), third, "pending")
	check("second redrive", call("/v1/notifications/"+fourth.String()+"/redrive", "", "key-redrive"), fourth, "pending")

	if got := storedKeys(); got != keys {
		t.Errorf("%d idempotency keys after the cancels and redrives, want %d: a key was stored", got, keys)
	}
}

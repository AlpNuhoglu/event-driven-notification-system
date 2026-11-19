package store

import (
	"testing"
	"time"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/store/db"
)

// redriveRefusal follows the guard of RedriveNotification: "" with refused
// false means that the state allows the redrive (the statement runs again).
func TestRedriveRefusal(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	const lead = 30_000 * time.Millisecond
	past, future := now.Add(-time.Millisecond), now.Add(time.Hour)
	at := func(d time.Duration) *time.Time { v := now.Add(d); return &v }
	cases := []struct {
		name    string
		status  string
		stored  time.Time
		newExp  *time.Time
		want    Refusal
		refused bool
	}{
		{"not failed", "pending", future, nil, RefusalNotFailed, true},
		{"not failed, new one", "sent", past, at(time.Hour), RefusalNotFailed, true},
		{"stored passed, none given", "failed", past, nil, RefusalExpiresAtRequired, true},
		{"stored equals now, none given", "failed", now, nil, RefusalExpiresAtRequired, true},
		{"stored future, none given", "failed", future, nil, "", false},
		{"stored future, one given", "failed", future, at(time.Hour), RefusalExpiresAtNotAllowed, true},
		{"stored passed, too soon", "failed", past, at(lead - time.Microsecond), RefusalTooSoon, true},
		{"stored passed, exactly the lead", "failed", past, at(lead), "", false},
		{"stored passed, later", "failed", past, at(time.Hour), "", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			st := db.TransitionStateRow{Status: tc.status, ExpiresAt: tc.stored, DbNow: now}
			got, refused := redriveRefusal(st, tc.newExp, lead)
			if got != tc.want || refused != tc.refused {
				t.Errorf("got %q %v, want %q %v", got, refused, tc.want, tc.refused)
			}
		})
	}
}

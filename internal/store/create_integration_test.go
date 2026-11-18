//go:build integration

package store

import (
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
)

// due_at and expires_at come from the database clock: without scheduled_at
// both are derived from the insert's now(), which is also created_at.
func TestCreateNotification_Defaults(t *testing.T) {
	s, _ := newTestStore(t, zeroDelay)
	scheduled := time.Now().Add(90_000 * time.Millisecond).Truncate(time.Microsecond)
	explicit := time.Now().Add(7_200_000 * time.Millisecond).Truncate(time.Microsecond)

	t.Run("no scheduled_at", func(t *testing.T) {
		n := create(t, s)
		if !n.DueAt.Equal(n.CreatedAt) {
			t.Errorf("due_at %v, created_at (database now()) %v", n.DueAt, n.CreatedAt)
		}
		if !n.ExpiresAt.Equal(n.CreatedAt.Add(testTTL)) {
			t.Errorf("expires_at %v, want created_at + %s", n.ExpiresAt, testTTL)
		}
		if n.ScheduledAt != nil {
			t.Errorf("scheduled_at = %v", n.ScheduledAt)
		}
	})
	t.Run("scheduled_at", func(t *testing.T) {
		n := create(t, s, func(n *NewNotification) { n.ScheduledAt = &scheduled })
		if !n.DueAt.Equal(scheduled) || n.ScheduledAt == nil || !n.ScheduledAt.Equal(scheduled) {
			t.Errorf("due_at %v scheduled_at %v, want %v", n.DueAt, n.ScheduledAt, scheduled)
		}
		if !n.ExpiresAt.Equal(scheduled.Add(testTTL)) {
			t.Errorf("expires_at %v, want scheduled_at + %s", n.ExpiresAt, testTTL)
		}
	})
	t.Run("explicit expires_at", func(t *testing.T) {
		n := create(t, s, func(n *NewNotification) { n.ExpiresAt = &explicit })
		if !n.ExpiresAt.Equal(explicit) || !n.DueAt.Equal(n.CreatedAt) {
			t.Errorf("expires_at %v due_at %v", n.ExpiresAt, n.DueAt)
		}
	})
	t.Run("scheduled_at and explicit expires_at", func(t *testing.T) {
		n := create(t, s, func(n *NewNotification) { n.ScheduledAt, n.ExpiresAt = &scheduled, &explicit })
		if !n.ExpiresAt.Equal(explicit) || !n.DueAt.Equal(scheduled) {
			t.Errorf("expires_at %v due_at %v", n.ExpiresAt, n.DueAt)
		}
	})
}

func TestCreateNotification_Row(t *testing.T) {
	s, pool := newTestStore(t, zeroDelay)
	batchID := uuid.Must(uuid.NewV7())
	exec(t, pool, `INSERT INTO batches (id, total, accepted, rejected, correlation_id) VALUES ($1, 1, 1, 0, 'c')`, batchID)
	n := create(t, s, func(n *NewNotification) {
		n.BatchID = &batchID
		n.DedupeKey = ptr("order-42")
		n.SMSEncoding = ptr("GSM-7")
		n.SMSSegments = ptr(int16(1))
		n.TemplateName = ptr("otp")
		n.TemplateVersion = ptr(int32(3))
		n.TemplateVars = []byte(`{"code": "1234"}`)
	})
	if n.ID.Version() != 7 {
		t.Errorf("id version = %d, want 7", n.ID.Version())
	}
	if n.Status != domain.StatusPending || n.Version != 1 || n.AttemptCount != 0 || n.Failures != 0 || n.MaxFailures != 5 {
		t.Errorf("row = %+v", n)
	}
	got := get(t, s, n.ID)
	if *got.BatchID != batchID || *got.DedupeKey != "order-42" || *got.SMSEncoding != "GSM-7" ||
		*got.SMSSegments != 1 || *got.TemplateName != "otp" || *got.TemplateVersion != 3 ||
		string(got.TemplateVars) != `{"code": "1234"}` || got.CorrelationID != "corr-1" ||
		got.TraceParent != baseNew().TraceParent || got.Content != baseNew().Content {
		t.Errorf("row = %+v", got)
	}
	if _, err := s.GetNotification(ctxT(t), uuid.New()); !errors.Is(err, ErrNotFound) {
		t.Errorf("unknown id: err = %v, want ErrNotFound", err)
	}
}

// domain.Recipient masks itself in every formatting and encoding path, so the
// store must hand the real value to the database explicitly.
func TestCreateNotification_RecipientStoredUnmasked(t *testing.T) {
	s, pool := newTestStore(t, zeroDelay)
	for _, raw := range []string{"+905551234567", "alice@example.com", "device-token-0123456789"} {
		n := create(t, s, func(n *NewNotification) { n.Recipient = domain.Recipient(raw) })
		var stored string
		if err := pool.QueryRow(ctxT(t), `SELECT recipient FROM notifications WHERE id = $1`, n.ID).Scan(&stored); err != nil {
			t.Fatal(err)
		}
		if stored != raw {
			t.Errorf("stored recipient %q, want the real value %q", stored, raw)
		}
		if got := get(t, s, n.ID).Recipient.Reveal(); got != raw {
			t.Errorf("read back %q, want %q", got, raw)
		}
	}
}

//go:build integration

package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
)

// respondBatch builds the response the API would store for a batch.
func respondBatch(r BatchResult) (StoredResponse, error) {
	body, err := json.Marshal(map[string]any{"batch_id": r.BatchID, "accepted": r.Accepted, "rejected": r.Rejected})
	if err != nil {
		return StoredResponse{}, err
	}
	return StoredResponse{Status: 202, Body: body}, nil
}

func batchOf(items ...NewNotification) NewBatch {
	return NewBatch{Total: len(items), CorrelationID: "corr-batch", Items: items}
}

func item(mods ...func(*NewNotification)) NewNotification {
	n := baseNew()
	for _, m := range mods {
		m(&n)
	}
	return n
}

func createBatch(t *testing.T, s *Store, key *IdempotencyKey, b NewBatch) BatchResult {
	t.Helper()
	r, err := s.CreateBatch(ctxT(t), key, b, respondBatch)
	if err != nil {
		t.Fatalf("create batch: %v", err)
	}
	return r
}

// Conflicts are resolved through the partial index: a mix of inserted and
// duplicate items, each duplicate with the id of the row that holds its key.
func TestCreateBatch_Conflicts(t *testing.T) {
	s, pool := newTestStore(t, zeroDelay)
	holders := map[string]uuid.UUID{}
	for _, st := range holderStates {
		key := "k-" + st.name
		n := create(t, s, withDedupe(key))
		setStatus(t, pool, n.ID, st.set)
		holders[key] = n.ID
	}

	var items []NewNotification
	var wantDup []string // per item: the holder's key, or "" when inserted
	items = append(items, item(withDedupe("k-new-1")))
	wantDup = append(wantDup, "")
	items = append(items, item()) // no dedupe key
	wantDup = append(wantDup, "")
	for _, st := range holderStates {
		items = append(items, item(withDedupe("k-"+st.name)))
		if st.released {
			wantDup = append(wantDup, "")
		} else {
			wantDup = append(wantDup, "k-"+st.name)
		}
	}
	items = append(items, item(withDedupe("k-new-2")))
	wantDup = append(wantDup, "")

	b := batchOf(items...)
	b.Total += 3
	b.Rejected = 3 // rejected by the API in validation
	r := createBatch(t, s, nil, b)

	if r.Outcome != OutcomeCreated || r.BatchID == uuid.Nil || r.Response == nil || len(r.Items) != len(items) {
		t.Fatalf("result = %+v", r)
	}
	accepted := 0
	for i, it := range r.Items {
		if wantDup[i] == "" {
			accepted++
			if !it.Inserted || it.ID == uuid.Nil || it.ExistingID != nil {
				t.Errorf("item %d = %+v, want inserted", i, it)
				continue
			}
			got := get(t, s, it.ID)
			if got.BatchID == nil || *got.BatchID != r.BatchID {
				t.Errorf("item %d batch_id = %v, want %s", i, got.BatchID, r.BatchID)
			}
			continue
		}
		if it.Inserted || it.ExistingID == nil || *it.ExistingID != holders[wantDup[i]] {
			t.Errorf("item %d = %+v, want a duplicate of %s", i, it, holders[wantDup[i]])
		}
	}
	rejected := b.Total - accepted
	if r.Total != b.Total || r.Accepted != accepted || r.Rejected != rejected {
		t.Errorf("counts %d/%d/%d, want %d/%d/%d", r.Total, r.Accepted, r.Rejected, b.Total, accepted, rejected)
	}
	got, err := s.GetBatch(ctxT(t), r.BatchID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Total != b.Total || got.Accepted != accepted || got.Rejected != rejected || got.CorrelationID != "corr-batch" {
		t.Errorf("batch row = %+v, want total %d accepted %d rejected %d", got, b.Total, accepted, rejected)
	}
	if c := count(t, pool, `SELECT count(*) FROM notifications WHERE batch_id = $1`, r.BatchID); c != accepted {
		t.Errorf("%d rows in the batch, want %d", c, accepted)
	}
}

// Each item is stored with its own columns: priority, TTL and max_failures
// per item, due_at and expires_at from the database clock, NULLs kept.
func TestCreateBatch_ItemColumns(t *testing.T) {
	s, _ := newTestStore(t, zeroDelay)
	scheduled := time.Now().Add(90_000 * time.Millisecond).Truncate(time.Microsecond)
	explicit := time.Now().Add(7_200_000 * time.Millisecond).Truncate(time.Microsecond)
	const highTTL, lowTTL = 600_000 * time.Millisecond, 7_200_000 * time.Millisecond

	plain := item(func(n *NewNotification) {
		n.Priority, n.TTL, n.MaxFailures = domain.PriorityHigh, highTTL, 3
	})
	full := item(func(n *NewNotification) {
		n.Channel, n.Priority, n.TTL, n.MaxFailures = domain.ChannelEmail, domain.PriorityLow, lowTTL, 9
		n.Recipient = domain.Recipient("alice@example.com")
		n.DedupeKey = ptr("order-full")
		n.TemplateName, n.TemplateVersion = ptr("otp"), ptr(int32(2))
		n.TemplateVars = []byte(`{"code": "1234"}`)
		n.ScheduledAt = &scheduled
		n.CorrelationID, n.TraceParent = "corr-item", ""
	})
	sms := item(func(n *NewNotification) {
		n.SMSEncoding, n.SMSSegments = ptr("UCS-2"), ptr(int16(2))
		n.ExpiresAt = &explicit
	})
	r := createBatch(t, s, nil, batchOf(plain, full, sms))
	if r.Outcome != OutcomeCreated || r.Accepted != 3 {
		t.Fatalf("result = %+v", r)
	}

	p := get(t, s, r.Items[0].ID)
	if p.Priority != domain.PriorityHigh || p.MaxFailures != 3 || !p.DueAt.Equal(p.CreatedAt) ||
		!p.ExpiresAt.Equal(p.CreatedAt.Add(highTTL)) || p.ScheduledAt != nil || p.DedupeKey != nil ||
		p.SMSEncoding != nil || p.SMSSegments != nil || p.TemplateName != nil || p.TemplateVersion != nil ||
		p.TemplateVars != nil || p.Status != domain.StatusPending || p.Version != 1 {
		t.Errorf("plain item = %+v", p)
	}
	f := get(t, s, r.Items[1].ID)
	if f.Channel != domain.ChannelEmail || f.Priority != domain.PriorityLow || f.MaxFailures != 9 ||
		f.Recipient.Reveal() != "alice@example.com" || *f.DedupeKey != "order-full" ||
		*f.TemplateName != "otp" || *f.TemplateVersion != 2 || string(f.TemplateVars) != `{"code": "1234"}` ||
		!f.DueAt.Equal(scheduled) || !f.ScheduledAt.Equal(scheduled) || !f.ExpiresAt.Equal(scheduled.Add(lowTTL)) ||
		f.CorrelationID != "corr-item" || f.TraceParent != "" {
		t.Errorf("full item = %+v", f)
	}
	m := get(t, s, r.Items[2].ID)
	if *m.SMSEncoding != "UCS-2" || *m.SMSSegments != 2 || !m.ExpiresAt.Equal(explicit) ||
		!m.DueAt.Equal(m.CreatedAt) || m.Content != baseNew().Content || m.TraceParent != baseNew().TraceParent {
		t.Errorf("sms item = %+v", m)
	}
	if r.Items[0].ID.Version() != 7 {
		t.Errorf("id version = %d, want 7", r.Items[0].ID.Version())
	}
}

func TestCreateBatch_Thousand(t *testing.T) {
	s, pool := newTestStore(t, zeroDelay)
	items := make([]NewNotification, 1000)
	for i := range items {
		items[i] = baseNew()
		if i%2 == 0 {
			items[i].DedupeKey = ptr(fmt.Sprintf("bulk-%04d", i))
		}
	}
	// One existing holder, so the look-up runs too.
	held := create(t, s, withDedupe("bulk-0500"))
	r := createBatch(t, s, nil, batchOf(items...))
	if r.Outcome != OutcomeCreated || r.Accepted != 999 || r.Rejected != 1 || r.Total != 1000 {
		t.Fatalf("result: outcome %d accepted %d rejected %d total %d", r.Outcome, r.Accepted, r.Rejected, r.Total)
	}
	if it := r.Items[500]; it.Inserted || it.ExistingID == nil || *it.ExistingID != held.ID {
		t.Errorf("item 500 = %+v, want a duplicate of %s", it, held.ID)
	}
	if c := count(t, pool, `SELECT count(*) FROM notifications WHERE batch_id = $1`, r.BatchID); c != 999 {
		t.Errorf("%d rows in the batch, want 999", c)
	}
	seen := map[uuid.UUID]bool{}
	for i, it := range r.Items {
		if i != 500 && (!it.Inserted || seen[it.ID]) {
			t.Fatalf("item %d = %+v", i, it)
		}
		seen[it.ID] = true
	}
}

// A batch in which no item is inserted stores nothing: no key, no batch row,
// no notification.
func TestCreateBatch_NothingInserted(t *testing.T) {
	s, pool := newTestStore(t, zeroDelay)
	a := create(t, s, withDedupe("dup-a"))
	b := create(t, s, withDedupe("dup-b"))
	key := idemKey("key-batch-dup", "hash-a")

	called := false
	r, err := s.CreateBatch(ctxT(t), key, batchOf(item(withDedupe("dup-b")), item(withDedupe("dup-a"))),
		func(r BatchResult) (StoredResponse, error) { called = true; return respondBatch(r) })
	if err != nil {
		t.Fatal(err)
	}
	if r.Outcome != OutcomeDuplicate || called || r.Response != nil || r.BatchID != uuid.Nil || len(r.Items) != 2 ||
		r.Accepted != 0 || r.Rejected != 2 {
		t.Fatalf("result = %+v (respond called: %v)", r, called)
	}
	if *r.Items[0].ExistingID != b.ID || *r.Items[1].ExistingID != a.ID {
		t.Errorf("items = %+v, want duplicates of %s and %s", r.Items, b.ID, a.ID)
	}
	for table, want := range map[string]int{"notifications": 2, "batches": 0, "idempotency_keys": 0} {
		if c := count(t, pool, `SELECT count(*) FROM `+table); c != want {
			t.Errorf("%d rows in %s, want %d", c, table, want)
		}
	}

	// The key was not stored: once a key is released the same request is new.
	setStatus(t, pool, a.ID, `status = 'cancelled'`) // cancel before any attempt (no transition 9 yet)
	r = createBatch(t, s, key, batchOf(item(withDedupe("dup-b")), item(withDedupe("dup-a"))))
	if r.Outcome != OutcomeCreated || r.Accepted != 1 || !r.Items[1].Inserted || r.Items[0].Inserted {
		t.Fatalf("retry = %+v", r)
	}
	if c := count(t, pool, `SELECT count(*) FROM idempotency_keys`); c != 1 {
		t.Errorf("%d keys, want 1", c)
	}
}

func TestCreateBatch_Idempotency(t *testing.T) {
	s, pool := newTestStore(t, zeroDelay)
	key := idemKey("key-batch", "hash-a")
	first := createBatch(t, s, key, batchOf(item(), item()))
	if first.Outcome != OutcomeCreated {
		t.Fatalf("first = %+v", first)
	}
	again := createBatch(t, s, key, batchOf(item(), item()))
	if again.Outcome != OutcomeReplayed || again.Response.Status != 202 ||
		!jsonEqual(t, again.Response.Body, first.Response.Body) {
		t.Errorf("again = %+v, want the first response", again)
	}
	if _, err := s.CreateBatch(ctxT(t), idemKey("key-batch", "hash-b"), batchOf(item()), respondBatch); !errors.Is(err, ErrIdempotencyKeyReused) {
		t.Errorf("different hash: err = %v", err)
	}
	if c := count(t, pool, `SELECT count(*) FROM notifications`); c != 2 {
		t.Errorf("%d notifications, want 2", c)
	}
	if c := count(t, pool, `SELECT count(*) FROM batches`); c != 1 {
		t.Errorf("%d batches, want 1", c)
	}

	errRender := errors.New("render failed")
	_, err := s.CreateBatch(ctxT(t), idemKey("key-batch-failed", "hash-a"), batchOf(item()),
		func(BatchResult) (StoredResponse, error) { return StoredResponse{}, errRender })
	if !errors.Is(err, errRender) {
		t.Fatalf("err = %v", err)
	}
	if c := count(t, pool, `SELECT count(*) FROM batches`); c != 1 {
		t.Errorf("failed create stored a batch: %d batches", c)
	}
	if c := count(t, pool, `SELECT count(*) FROM idempotency_keys`); c != 1 {
		t.Errorf("failed create stored a key: %d keys", c)
	}
}

func TestCreateBatch_Contract(t *testing.T) {
	s, pool := newTestStore(t, zeroDelay)
	cases := map[string]NewBatch{
		"no items":            batchOf(),
		"total mismatch":      {Total: 5, Rejected: 1, Items: []NewNotification{item()}},
		"repeated dedupe key": batchOf(item(withDedupe("same")), item(withDedupe("same"))),
	}
	for name, b := range cases {
		if _, err := s.CreateBatch(ctxT(t), nil, b, respondBatch); err == nil {
			t.Errorf("%s: no error", name)
		}
	}
	if _, err := s.CreateBatch(ctxT(t), nil, batchOf(item()), nil); err == nil {
		t.Error("nil respond: no error")
	}
	if c := count(t, pool, `SELECT count(*) FROM notifications`); c != 0 {
		t.Errorf("%d notifications, want 0", c)
	}
}

func TestGetBatch_StatusCounts(t *testing.T) {
	s, _ := newTestStore(t, zeroDelay)
	sms := withLane(domain.ChannelSMS, domain.PriorityHigh)
	email := withLane(domain.ChannelEmail, domain.PriorityNormal)
	b := batchOf(item(sms), item(sms), item(email), item(email), item(email))
	b.Total, b.Rejected = 7, 2
	r := createBatch(t, s, nil, b)

	// Real transitions: the relay queues the sms lane, a worker claims one row.
	if _, err := s.PublishLane(ctxT(t), Lane{domain.ChannelSMS, domain.PriorityHigh}, 10, publishAll); err != nil {
		t.Fatal(err)
	}
	claim(t, s, r.Items[0].ID)

	got, err := s.GetBatch(ctxT(t), r.BatchID)
	if err != nil {
		t.Fatal(err)
	}
	want := map[domain.Status]int{domain.StatusPending: 3, domain.StatusQueued: 1, domain.StatusProcessing: 1}
	if fmt.Sprint(got.StatusCounts) != fmt.Sprint(want) {
		t.Errorf("status counts = %v, want %v", got.StatusCounts, want)
	}
	if got.ID != r.BatchID || got.Total != 7 || got.Accepted != 5 || got.Rejected != 2 || got.CreatedAt.IsZero() {
		t.Errorf("batch = %+v", got)
	}
	if _, err := s.GetBatch(ctxT(t), uuid.New()); !errors.Is(err, ErrBatchNotFound) {
		t.Errorf("unknown id: err = %v, want ErrBatchNotFound", err)
	}
}

// Concurrent batches whose dedupe keys overlap in opposite orders finish
// without a deadlock and insert each key exactly once. Without the
// deterministic insert order of InsertBatchNotifications, the first batch
// waits for a key the second inserted while the second waits for one the
// first inserted, and Postgres aborts one of them (SQLSTATE 40P01).
func TestCreateBatch_ConcurrentOverlappingKeysNoDeadlock(t *testing.T) {
	s, pool := newTestStore(t, zeroDelay)
	const (
		rounds = 10
		keys   = 300
	)
	for round := range rounds {
		forward := make([]NewNotification, keys)
		backward := make([]NewNotification, keys)
		for i := range keys {
			forward[i] = item(withDedupe(fmt.Sprintf("r%02d-k%03d", round, i)))
			backward[keys-1-i] = forward[i]
		}
		var (
			results [2]BatchResult
			errs    [2]error
			wg      sync.WaitGroup
		)
		start := make(chan struct{})
		for i, items := range [2][]NewNotification{forward, backward} {
			wg.Go(func() {
				<-start
				results[i], errs[i] = s.CreateBatch(context.Background(), nil, batchOf(items...), respondBatch)
			})
		}
		close(start)
		wg.Wait()
		for i, err := range errs {
			if err != nil {
				t.Fatalf("round %d batch %d: %v", round, i, err)
			}
		}

		// Per key: the id that inserted it in one batch, the duplicate in the other.
		inserted := map[string]uuid.UUID{}
		for bi, items := range [2][]NewNotification{forward, backward} {
			for i, it := range results[bi].Items {
				if it.Inserted {
					k := *items[i].DedupeKey
					if _, dup := inserted[k]; dup {
						t.Fatalf("round %d: key %s inserted twice", round, k)
					}
					inserted[k] = it.ID
				}
			}
		}
		if len(inserted) != keys {
			t.Fatalf("round %d: %d keys inserted, want %d", round, len(inserted), keys)
		}
		for bi, items := range [2][]NewNotification{forward, backward} {
			for i, it := range results[bi].Items {
				k := *items[i].DedupeKey
				if !it.Inserted && (it.ExistingID == nil || *it.ExistingID != inserted[k]) {
					t.Errorf("round %d batch %d key %s = %+v, want a duplicate of %s", round, bi, k, it, inserted[k])
				}
			}
		}
		if c := count(t, pool, `SELECT count(*) FROM notifications WHERE dedupe_key LIKE $1`,
			fmt.Sprintf("r%02d-%%", round)); c != keys {
			t.Errorf("round %d: %d rows, want %d", round, c, keys)
		}
	}
}

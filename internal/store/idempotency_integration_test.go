//go:build integration

package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/testutil"
)

// testKeyRetention stands in for IDEMPOTENCY_KEY_TTL; expiry is produced by
// back-dating.
const testKeyRetention = 86_400_000 * time.Millisecond

// Markers of the generated statements in pg_stat_activity.query.
const (
	claimKeyStmt     = "%name: ClaimIdempotencyKey %"
	dedupeInsertStmt = "%name: CreateNotification :one%"
)

func idemKey(key, hash string) *IdempotencyKey {
	return &IdempotencyKey{Key: key, RequestHash: hash, Retention: testKeyRetention}
}

// respond202 builds the response the API would store for a single create.
func respond202(n Notification) (StoredResponse, error) {
	body, err := json.Marshal(map[string]any{"id": n.ID, "status": n.Status})
	if err != nil {
		return StoredResponse{}, err
	}
	return StoredResponse{Status: 202, Body: body}, nil
}

// jsonEqual compares two JSON documents as values: jsonb does not keep the
// bytes.
func jsonEqual(t *testing.T, a, b []byte) bool {
	t.Helper()
	var va, vb any
	if err := json.Unmarshal(a, &va); err != nil {
		t.Fatalf("unmarshal %s: %v", a, err)
	}
	if err := json.Unmarshal(b, &vb); err != nil {
		t.Fatalf("unmarshal %s: %v", b, err)
	}
	return reflect.DeepEqual(va, vb)
}

// stmtLockWaiters counts backends of this test's database that are blocked
// on a lock while running a statement whose text matches pattern (LIKE).
func stmtLockWaiters(ctx context.Context, t *testing.T, pool *pgxpool.Pool, pattern string) int {
	t.Helper()
	var n int
	err := pool.QueryRow(ctx, `
		SELECT count(*) FROM pg_stat_activity
		WHERE datname = current_database()
		  AND pid <> pg_backend_pid()
		  AND wait_event_type = 'Lock'
		  AND query LIKE $1`, pattern).Scan(&n)
	if err != nil {
		t.Errorf("pg_stat_activity: %v", err)
		return -1
	}
	return n
}

func count(t *testing.T, pool *pgxpool.Pool, sql string, args ...any) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(ctxT(t), sql, args...).Scan(&n); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	return n
}

type singleResult struct {
	res SingleResult
	err error
}

// startCreateSingle runs CreateSingle in a goroutine; the result arrives on
// the channel.
func startCreateSingle(ctx context.Context, s *Store, key *IdempotencyKey, n NewNotification) <-chan singleResult {
	done := make(chan singleResult, 1)
	go func() {
		r, err := s.CreateSingle(ctx, key, n, respond202)
		done <- singleResult{r, err}
	}()
	return done
}

func receiveSingle(t *testing.T, done <-chan singleResult) singleResult {
	t.Helper()
	var r singleResult
	testutil.Eventually(t, func() bool {
		select {
		case r = <-done:
			return true
		default:
			return false
		}
	}, waitTimeout)
	return r
}

func createSingle(t *testing.T, s *Store, key *IdempotencyKey, n NewNotification) SingleResult {
	t.Helper()
	r, err := s.CreateSingle(ctxT(t), key, n, respond202)
	if err != nil {
		t.Fatalf("create single: %v", err)
	}
	return r
}

type keyRow struct {
	hash      string
	status    *int32
	body      []byte
	createdAt time.Time
	expiresAt time.Time
}

func readKey(t *testing.T, pool *pgxpool.Pool, key string) keyRow {
	t.Helper()
	var r keyRow
	err := pool.QueryRow(ctxT(t), `
		SELECT request_hash, response_status, response_body, created_at, expires_at
		FROM idempotency_keys WHERE key = $1`, key).
		Scan(&r.hash, &r.status, &r.body, &r.createdAt, &r.expiresAt)
	if err != nil {
		t.Fatalf("read key %s: %v", key, err)
	}
	return r
}

// The second request with the same key waits inside the claim statement
// while the first request's transaction is open, and then replays the
// first's response without creating anything (ADR-0006 section 11).
func TestIdempotencyKey_ConcurrentSameKey(t *testing.T) {
	s, pool := newTestStore(t, zeroDelay)
	key := idemKey("key-concurrent", "hash-a")
	ctx := ctxT(t)

	var second <-chan singleResult
	first, err := s.CreateSingle(ctx, key, baseNew(), func(n Notification) (StoredResponse, error) {
		// The key is claimed and the notification inserted; the transaction
		// is open until this function returns.
		second = startCreateSingle(ctx, s, key, baseNew())
		testutil.Eventually(t, func() bool {
			return stmtLockWaiters(ctx, t, pool, claimKeyStmt) == 1
		}, lockWaitTimeout)
		select {
		case r := <-second:
			return StoredResponse{}, fmt.Errorf("second request returned before the first committed: %+v", r)
		default:
		}
		return respond202(n)
	})
	if err != nil {
		t.Fatalf("first: %v", err)
	}
	if first.Outcome != OutcomeCreated || first.Response == nil {
		t.Fatalf("first = %+v", first)
	}

	r := receiveSingle(t, second)
	if r.err != nil {
		t.Fatalf("second: %v", r.err)
	}
	if r.res.Outcome != OutcomeReplayed || r.res.Response == nil {
		t.Fatalf("second = %+v, want a replay", r.res)
	}
	if r.res.Response.Status != first.Response.Status || !jsonEqual(t, r.res.Response.Body, first.Response.Body) {
		t.Errorf("replayed %d %s, want %d %s", r.res.Response.Status, r.res.Response.Body,
			first.Response.Status, first.Response.Body)
	}
	if n := count(t, pool, `SELECT count(*) FROM notifications`); n != 1 {
		t.Errorf("%d notifications, want 1", n)
	}
}

// The same with the first transaction rolling back: the waiting request
// proceeds as new.
func TestIdempotencyKey_ConcurrentSameKeyFirstRollsBack(t *testing.T) {
	s, pool := newTestStore(t, zeroDelay)
	key := idemKey("key-rollback", "hash-a")
	errRender := errors.New("render failed")
	ctx := ctxT(t)

	var second <-chan singleResult
	_, err := s.CreateSingle(ctx, key, baseNew(), func(Notification) (StoredResponse, error) {
		second = startCreateSingle(ctx, s, key, baseNew())
		testutil.Eventually(t, func() bool {
			return stmtLockWaiters(ctx, t, pool, claimKeyStmt) == 1
		}, lockWaitTimeout)
		return StoredResponse{}, errRender
	})
	if !errors.Is(err, errRender) {
		t.Fatalf("first: err = %v, want %v", err, errRender)
	}

	r := receiveSingle(t, second)
	if r.err != nil {
		t.Fatalf("second: %v", r.err)
	}
	if r.res.Outcome != OutcomeCreated || r.res.Response == nil {
		t.Fatalf("second = %+v, want created", r.res)
	}
	if n := count(t, pool, `SELECT count(*) FROM notifications`); n != 1 {
		t.Errorf("%d notifications, want 1", n)
	}
	got := get(t, s, r.res.Notification.ID)
	if got.Status != domain.StatusPending {
		t.Errorf("status = %s", got.Status)
	}
	stored := readKey(t, pool, key.Key)
	if stored.status == nil || int(*stored.status) != r.res.Response.Status ||
		!jsonEqual(t, stored.body, r.res.Response.Body) {
		t.Errorf("stored response %v %s, want the second request's", stored.status, stored.body)
	}
}

func TestIdempotencyKey_Lifecycle(t *testing.T) {
	t.Run("same key and hash replays without creating", func(t *testing.T) {
		s, pool := newTestStore(t, zeroDelay)
		key := idemKey("key-replay", "hash-a")
		first := createSingle(t, s, key, baseNew())
		called := false
		r, err := s.CreateSingle(ctxT(t), key, baseNew(), func(n Notification) (StoredResponse, error) {
			called = true
			return respond202(n)
		})
		if err != nil {
			t.Fatal(err)
		}
		if r.Outcome != OutcomeReplayed || called || r.Response.Status != 202 ||
			!jsonEqual(t, r.Response.Body, first.Response.Body) {
			t.Errorf("replay = %+v (respond called: %v)", r, called)
		}
		if n := count(t, pool, `SELECT count(*) FROM notifications`); n != 1 {
			t.Errorf("%d notifications, want 1", n)
		}
		stored := readKey(t, pool, key.Key)
		if !stored.expiresAt.Equal(stored.createdAt.Add(testKeyRetention)) {
			t.Errorf("expires_at %v, want created_at %v + %s", stored.expiresAt, stored.createdAt, testKeyRetention)
		}
	})

	t.Run("live key with a different hash is refused", func(t *testing.T) {
		s, pool := newTestStore(t, zeroDelay)
		createSingle(t, s, idemKey("key-reused", "hash-a"), baseNew())
		before := readKey(t, pool, "key-reused")
		_, err := s.CreateSingle(ctxT(t), idemKey("key-reused", "hash-b"), baseNew(), respond202)
		if !errors.Is(err, ErrIdempotencyKeyReused) {
			t.Fatalf("err = %v, want ErrIdempotencyKeyReused", err)
		}
		if n := count(t, pool, `SELECT count(*) FROM notifications`); n != 1 {
			t.Errorf("%d notifications, want 1", n)
		}
		if after := readKey(t, pool, "key-reused"); !reflect.DeepEqual(before, after) {
			t.Errorf("key row changed: %+v -> %+v", before, after)
		}
	})

	t.Run("expired key is taken over", func(t *testing.T) {
		s, pool := newTestStore(t, zeroDelay)
		first := createSingle(t, s, idemKey("key-expired", "hash-a"), baseNew())
		exec(t, pool, `UPDATE idempotency_keys SET expires_at = now() - interval '1 millisecond' WHERE key = $1`, "key-expired")

		// A different request: an expired key no longer binds its hash.
		second := createSingle(t, s, idemKey("key-expired", "hash-b"), baseNew())
		if second.Outcome != OutcomeCreated || second.Notification.ID == first.Notification.ID {
			t.Fatalf("second = %+v, want a new notification", second)
		}
		stored := readKey(t, pool, "key-expired")
		if stored.hash != "hash-b" || !jsonEqual(t, stored.body, second.Response.Body) ||
			jsonEqual(t, stored.body, first.Response.Body) {
			t.Errorf("key row %+v: want the second request's hash and response, the old response gone", stored)
		}
		if !stored.expiresAt.After(time.Now()) || !stored.expiresAt.Equal(stored.createdAt.Add(testKeyRetention)) {
			t.Errorf("key not renewed: created_at %v expires_at %v", stored.createdAt, stored.expiresAt)
		}
		if n := count(t, pool, `SELECT count(*) FROM notifications`); n != 2 {
			t.Errorf("%d notifications, want 2", n)
		}
	})

	t.Run("failed create stores nothing", func(t *testing.T) {
		s, pool := newTestStore(t, zeroDelay)
		errRender := errors.New("render failed")
		_, err := s.CreateSingle(ctxT(t), idemKey("key-failed", "hash-a"), baseNew(),
			func(Notification) (StoredResponse, error) { return StoredResponse{}, errRender })
		if !errors.Is(err, errRender) {
			t.Fatalf("err = %v, want %v", err, errRender)
		}
		if n := count(t, pool, `SELECT count(*) FROM notifications`); n != 0 {
			t.Errorf("%d notifications, want 0", n)
		}
		if n := count(t, pool, `SELECT count(*) FROM idempotency_keys`); n != 0 {
			t.Errorf("%d keys, want 0", n)
		}
		// The client can retry with the same key.
		if r := createSingle(t, s, idemKey("key-failed", "hash-a"), baseNew()); r.Outcome != OutcomeCreated {
			t.Errorf("retry = %+v", r)
		}
	})

	t.Run("dedupe conflict stores no key", func(t *testing.T) {
		s, pool := newTestStore(t, zeroDelay)
		held := create(t, s, withDedupe("order-1"))
		n := baseNew()
		n.DedupeKey = ptr("order-1")
		r := createSingle(t, s, idemKey("key-dup", "hash-a"), n)
		if r.Outcome != OutcomeDuplicate || r.ExistingID == nil || *r.ExistingID != held.ID || r.Response != nil {
			t.Fatalf("result = %+v, want a duplicate of %s", r, held.ID)
		}
		if n := count(t, pool, `SELECT count(*) FROM idempotency_keys`); n != 0 {
			t.Errorf("%d keys, want 0", n)
		}
		if n := count(t, pool, `SELECT count(*) FROM notifications`); n != 1 {
			t.Errorf("%d notifications, want 1", n)
		}
	})

	t.Run("invalid input", func(t *testing.T) {
		s, _ := newTestStore(t, zeroDelay)
		if _, err := s.CreateSingle(ctxT(t), idemKey("k", "h"), baseNew(), nil); err == nil {
			t.Error("key without respond: no error")
		}
		if _, err := s.CreateSingle(ctxT(t), &IdempotencyKey{Key: "k", RequestHash: "h"}, baseNew(), respond202); err == nil {
			t.Error("zero retention: no error")
		}
	})
}

func withDedupe(key string) func(*NewNotification) {
	return func(n *NewNotification) { n.DedupeKey = &key }
}

// setStatus puts a row into a state that has no transition yet or that the
// test needs directly. Cancel (transition 9) does not exist yet, so cancelled
// rows are set up with raw SQL.
func setStatus(t *testing.T, pool *pgxpool.Pool, id uuid.UUID, set string) {
	t.Helper()
	exec(t, pool, `UPDATE notifications SET `+set+` WHERE id = $1`, id)
}

// LookupIdempotencyKey reads with the liveness rule of the claim and
// changes nothing: no key, a live key with the same hash (the stored
// response), a live key with another hash (ErrIdempotencyKeyReused), an
// expired key (none), and a key claimed by a transaction that has not
// committed (not seen).
func TestLookupIdempotencyKey(t *testing.T) {
	s, pool := newTestStore(t, zeroDelay)
	if r, err := s.LookupIdempotencyKey(ctxT(t), idemKey("absent", "hash-a")); r != nil || err != nil {
		t.Fatalf("absent key: %v %v", r, err)
	}
	first := createSingle(t, s, idemKey("live", "hash-a"), baseNew())
	before := readKey(t, pool, "live")
	r, err := s.LookupIdempotencyKey(ctxT(t), idemKey("live", "hash-a"))
	if err != nil || r == nil || r.Status != 202 || !jsonEqual(t, r.Body, first.Response.Body) {
		t.Fatalf("same hash: %+v %v", r, err)
	}
	if _, err := s.LookupIdempotencyKey(ctxT(t), idemKey("live", "hash-b")); !errors.Is(err, ErrIdempotencyKeyReused) {
		t.Fatalf("other hash: %v", err)
	}
	if after := readKey(t, pool, "live"); !reflect.DeepEqual(before, after) {
		t.Errorf("key row changed: %+v -> %+v", before, after)
	}

	// Expired: the claim would take it over, so the look-up does not see it.
	exec(t, pool, `UPDATE idempotency_keys SET expires_at = now() - interval '1 millisecond' WHERE key = $1`, "live")
	if r, err := s.LookupIdempotencyKey(ctxT(t), idemKey("live", "hash-b")); r != nil || err != nil {
		t.Fatalf("expired key: %v %v", r, err)
	}

	// A claim in an open transaction is not seen, and the look-up does not
	// wait for it.
	tx, err := pool.Begin(ctxT(t))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if _, err := tx.Exec(ctxT(t), `INSERT INTO idempotency_keys (key, request_hash, created_at, expires_at)
		VALUES ('open', 'hash-a', now(), now() + interval '1 hour')`); err != nil {
		t.Fatal(err)
	}
	if r, err := s.LookupIdempotencyKey(ctxT(t), idemKey("open", "hash-a")); r != nil || err != nil {
		t.Fatalf("uncommitted claim: %v %v", r, err)
	}

	if _, err := s.LookupIdempotencyKey(ctxT(t), &IdempotencyKey{Key: "k"}); err == nil {
		t.Error("key without a hash: no error")
	}
}

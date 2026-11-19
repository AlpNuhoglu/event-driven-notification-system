//go:build integration

package ratelimit

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/ratewindow"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/testutil"
)

func TestMain(m *testing.M) { testutil.Main(m) }

const longHorizon = 60 * time.Second

func newLimiter(t *testing.T, rdb redis.Scripter, perSecond int, horizon time.Duration) *Limiter {
	t.Helper()
	l, err := New(rdb, perSecond, horizon)
	if err != nil {
		t.Fatal(err)
	}
	return l
}

func mustReserve(t *testing.T, l *Limiter, ch domain.Channel) (time.Duration, int64) {
	t.Helper()
	wait, slot, ok, err := l.reserve(context.Background(), ch)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("reservation refused")
	}
	return wait, slot
}

// redisNowUS reads the Redis clock in microseconds.
func redisNowUS(t *testing.T, rdb *redis.Client) int64 {
	t.Helper()
	tm, err := rdb.Time(context.Background()).Result()
	if err != nil {
		t.Fatal(err)
	}
	return tm.UnixMicro()
}

func TestZeroBurstSlotsAreOneIntervalApart(t *testing.T) {
	rdb := testutil.NewRedis(t)
	l := newLimiter(t, rdb, 98, longHorizon)
	prev := int64(-1)
	for i := 0; i < 500; i++ {
		_, slot := mustReserve(t, l, domain.ChannelSMS)
		if prev >= 0 {
			d := slot - prev
			if d < l.intervalUS {
				t.Fatalf("slot %d is %dus after the previous, want >= %dus", i, d, l.intervalUS)
			}
			if d != l.intervalUS {
				t.Fatalf("slot %d is %dus after the previous, want exactly %dus (calls are faster than the interval)", i, d, l.intervalUS)
			}
		}
		prev = slot
	}
}

func TestConcurrentClientsShareOneSchedule(t *testing.T) {
	const (
		clients   = 8
		perClient = 250
		rate      = 98
	)
	rdb := testutil.NewRedis(t)
	var (
		mu    sync.Mutex
		slots []int64
		wg    sync.WaitGroup
	)
	errs := make(chan error, clients)
	for c := 0; c < clients; c++ {
		own := redis.NewClient(rdb.Options())
		t.Cleanup(func() { _ = own.Close() })
		l := newLimiter(t, own, rate, longHorizon)
		wg.Add(1)
		go func() {
			defer wg.Done()
			mine := make([]int64, 0, perClient)
			for i := 0; i < perClient; i++ {
				_, slot, ok, err := l.reserve(context.Background(), domain.ChannelEmail)
				if err != nil || !ok {
					errs <- fmt.Errorf("ok=%v err=%v", ok, err)
					return
				}
				mine = append(mine, slot)
			}
			mu.Lock()
			slots = append(slots, mine...)
			mu.Unlock()
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("reserve failed: %v", err)
	}
	if len(slots) != clients*perClient {
		t.Fatalf("got %d slots, want %d", len(slots), clients*perClient)
	}
	sort.Slice(slots, func(i, j int) bool { return slots[i] < slots[j] })
	interval := intervalMicros(rate)
	times := make([]time.Time, len(slots))
	for i, s := range slots {
		if i > 0 && s-slots[i-1] < interval {
			t.Fatalf("slots %d and %d are %dus apart (want >= %dus; equal means a duplicate)", i-1, i, s-slots[i-1], interval)
		}
		times[i] = time.UnixMicro(s)
	}
	if got := ratewindow.MaxInWindow(times, time.Second); got > rate {
		t.Errorf("MaxInWindow = %d, want <= %d", got, rate)
	}
}

func TestWaitIsRelative(t *testing.T) {
	rdb := testutil.NewRedis(t)
	l := newLimiter(t, rdb, 98, longHorizon)
	for i := 0; i < 20; i++ {
		before := redisNowUS(t, rdb)
		wait, slot := mustReserve(t, l, domain.ChannelSMS)
		after := redisNowUS(t, rdb)
		if i == 0 && wait != 0 {
			t.Errorf("first reservation on an idle key waits %s, want 0", wait)
		}
		w := wait.Microseconds()
		if w < slot-after || w > slot-before {
			t.Errorf("reservation %d: wait %dus not within [%d, %d]", i, w, slot-after, slot-before)
		}
	}
}

func TestIdleKeyDoesNotAccumulateBurst(t *testing.T) {
	rdb := testutil.NewRedis(t)
	l := newLimiter(t, rdb, 98, longHorizon)
	ctx := context.Background()
	past := redisNowUS(t, rdb) - 10*time.Minute.Microseconds()
	if err := rdb.Set(ctx, "rl:sms", past, time.Minute).Err(); err != nil {
		t.Fatal(err)
	}
	before := redisNowUS(t, rdb)
	wait, first := mustReserve(t, l, domain.ChannelSMS)
	if wait != 0 {
		t.Errorf("wait after idle = %s, want 0", wait)
	}
	if first < before {
		t.Errorf("slot %d is before now %d", first, before)
	}
	// The second slot is exactly one interval after the first: the ten idle
	// minutes bought no burst. On a machine so slow that the second call
	// itself arrives later than that, the slot is the call's own time on the
	// Redis clock, which is still at least one interval after the first.
	beforeSecond := redisNowUS(t, rdb)
	_, second := mustReserve(t, l, domain.ChannelSMS)
	afterSecond := redisNowUS(t, rdb)
	want := first + l.intervalUS
	late := second > want && second >= beforeSecond && second <= afterSecond
	if second != want && !late {
		t.Errorf("second slot is %dus after the first, want %dus (or the time of a call later than that, in [%d, %d])",
			second-first, l.intervalUS, beforeSecond-first, afterSecond-first)
	}
}

func TestHorizon(t *testing.T) {
	rdb := testutil.NewRedis(t)
	horizon := 250 * time.Millisecond
	l := newLimiter(t, rdb, 98, horizon)
	ctx := context.Background()

	// Fill the horizon: reserve until the script refuses.
	granted := 0
	var lastWait time.Duration
	for {
		wait, ok, err := l.Reserve(ctx, domain.ChannelSMS)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			break
		}
		granted++
		lastWait = wait
		if granted > 1000 {
			t.Fatal("horizon never reached")
		}
	}
	if granted < 20 {
		t.Errorf("only %d slots granted within %s", granted, horizon)
	}
	if lastWait > horizon {
		t.Errorf("granted a wait of %s beyond the horizon %s", lastWait, horizon)
	}
	// A refusal leaves the key unchanged. The TAT is written far beyond the
	// horizon, so the passing time cannot make a slot fit in between.
	far := redisNowUS(t, rdb) + 10*time.Second.Microseconds()
	if err := rdb.Set(ctx, "rl:sms", far, time.Minute).Err(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if _, ok, err := l.Reserve(ctx, domain.ChannelSMS); err != nil || ok {
			t.Fatalf("reservation beyond the horizon: ok=%v err=%v", ok, err)
		}
	}
	if got, err := rdb.Get(ctx, "rl:sms").Int64(); err != nil || got != far {
		t.Errorf("key after refusals = %d (err %v), want %d", got, err, far)
	}

	// A TAT written at now + horizon is, by the time the script runs, just
	// within the horizon: granted. One second further is beyond it: refused,
	// and the key is not touched.
	long := newLimiter(t, rdb, 98, longHorizon)
	if err := rdb.Set(ctx, "rl:email", redisNowUS(t, rdb)+longHorizon.Microseconds(), time.Minute).Err(); err != nil {
		t.Fatal(err)
	}
	if wait, ok, err := long.Reserve(ctx, domain.ChannelEmail); err != nil || !ok || wait > longHorizon {
		t.Fatalf("slot within the horizon: wait=%s ok=%v err=%v", wait, ok, err)
	}
	beyond := redisNowUS(t, rdb) + longHorizon.Microseconds() + time.Second.Microseconds()
	if err := rdb.Set(ctx, "rl:email", beyond, time.Minute).Err(); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := long.Reserve(ctx, domain.ChannelEmail); err != nil || ok {
		t.Fatalf("slot beyond the horizon: ok=%v err=%v", ok, err)
	}
	if got, err := rdb.Get(ctx, "rl:email").Int64(); err != nil || got != beyond {
		t.Errorf("key after refusal = %d (err %v), want %d", got, err, beyond)
	}
}

func TestHorizonBoundaryIsInclusive(t *testing.T) {
	rdb := testutil.NewRedis(t)
	ctx := context.Background()
	// A zero horizon grants only an immediate slot: the first call on an idle
	// key (wait 0 == horizon) is granted, the next (wait one interval) is not.
	l := newLimiter(t, rdb, 98, 0)
	if _, ok, err := l.Reserve(ctx, domain.ChannelSMS); err != nil || !ok {
		t.Fatalf("slot with wait 0 refused at horizon 0: ok=%v err=%v", ok, err)
	}
}

func TestChannelsAreIndependent(t *testing.T) {
	rdb := testutil.NewRedis(t)
	l := newLimiter(t, rdb, 98, longHorizon)
	for i := 0; i < 50; i++ {
		mustReserve(t, l, domain.ChannelSMS)
	}
	wait, _ := mustReserve(t, l, domain.ChannelEmail)
	if wait != 0 {
		t.Errorf("first email slot waits %s after 50 sms reservations, want 0", wait)
	}
}

func TestKeyExpires(t *testing.T) {
	rdb := testutil.NewRedis(t)
	l := newLimiter(t, rdb, 98, longHorizon)
	mustReserve(t, l, domain.ChannelSMS)
	ttl, err := rdb.PTTL(context.Background(), "rl:sms").Result()
	if err != nil {
		t.Fatal(err)
	}
	if ttl <= 0 {
		t.Errorf("PTTL = %s, want > 0", ttl)
	}
}

func TestClosedClientReturnsError(t *testing.T) {
	rdb := testutil.NewRedis(t)
	own := redis.NewClient(rdb.Options())
	l := newLimiter(t, own, 98, longHorizon)
	if err := own.Close(); err != nil {
		t.Fatal(err)
	}
	wait, ok, err := l.Reserve(context.Background(), domain.ChannelSMS)
	if err == nil {
		t.Error("closed client returned no error")
	}
	if ok || wait != 0 {
		t.Errorf("ok=%v wait=%s on error", ok, wait)
	}
}

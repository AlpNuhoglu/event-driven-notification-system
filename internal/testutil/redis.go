package testutil

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"
)

// RedisImage is the Redis image of the tests. It must equal the image of the
// redis service in docker-compose.yml (checked by a unit test).
const RedisImage = "redis:8.10.3-alpine"

// redisDatabases is the number of logical databases of a default Redis server.
const redisDatabases = 16

var (
	redisOnce sync.Once
	redisOpts *redis.Options
	redisErr  error

	// redisMu guards redisFree, the logical databases no running test holds.
	redisMu   sync.Mutex
	redisFree []int
)

func startRedis() (*redis.Options, error) {
	type started struct {
		container *tcredis.RedisContainer
		opts      *redis.Options
	}
	st, err := startRetrying("redis", func() (started, error) {
		ctx, cancel := context.WithTimeout(context.Background(), startTimeout)
		defer cancel()
		c, err := tcredis.Run(ctx, RedisImage,
			testcontainers.WithCmd("redis-server", "--save", "", "--appendonly", "no"))
		if err != nil {
			if c != nil {
				discard("redis", c)
			}
			return started{}, fmt.Errorf("start redis: %w", err)
		}
		u, err := c.ConnectionString(ctx)
		if err != nil {
			discard("redis", c)
			return started{}, fmt.Errorf("connection string: %w", err)
		}
		opts, err := redis.ParseURL(u)
		if err != nil {
			discard("redis", c)
			return started{}, fmt.Errorf("parse url: %w", err)
		}
		return started{container: c, opts: opts}, nil
	})
	if err != nil {
		return nil, err
	}
	c, opts := st.container, st.opts
	for i := 0; i < redisDatabases; i++ {
		redisFree = append(redisFree, i)
	}
	onExit("redis", c)
	return opts, nil
}

// NewRedisProxy starts a Proxy in front of the server of rdb (a client from
// NewRedis) and returns it with a Redis URL of rdb's logical database through
// the proxy. Break makes Redis unreachable, Pause makes it hang. The proxy
// is closed when the test ends.
func NewRedisProxy(t testing.TB, rdb *redis.Client) (*Proxy, string) {
	t.Helper()
	opts := rdb.Options()
	p := NewProxy(t, "redis://"+opts.Addr)
	return p, fmt.Sprintf("%s/%d", p.URL(), opts.DB)
}

// NewRedis returns a client on a Redis logical database that no other running
// test uses. The database is emptied (FLUSHDB) before the test and again when
// it ends, and the client is closed at cleanup. The container is started on
// first use. A default Redis has 16 logical databases, so at most 16 tests
// that call NewRedis may run at the same time.
func NewRedis(t testing.TB) *redis.Client {
	t.Helper()
	redisOnce.Do(func() { redisOpts, redisErr = startRedis() })
	if redisErr != nil {
		t.Fatalf("redis: %v", redisErr)
	}
	redisMu.Lock()
	if len(redisFree) == 0 {
		redisMu.Unlock()
		t.Fatalf("redis: all %d logical databases are in use", redisDatabases)
	}
	db := redisFree[0]
	redisFree = redisFree[1:]
	redisMu.Unlock()

	opts := *redisOpts
	opts.DB = db
	rdb := redis.NewClient(&opts)
	ctx, cancel := context.WithTimeout(context.Background(), startTimeout)
	defer cancel()
	if err := rdb.FlushDB(ctx).Err(); err != nil {
		_ = rdb.Close()
		redisMu.Lock()
		redisFree = append(redisFree, db)
		redisMu.Unlock()
		t.Fatalf("redis flushdb: %v", err)
	}
	t.Cleanup(func() {
		// A test may have closed its client; flush on a fresh one.
		clean := redis.NewClient(&opts)
		defer func() { _ = clean.Close() }()
		if err := clean.FlushDB(context.Background()).Err(); err != nil {
			t.Errorf("redis flushdb: %v", err)
		}
		_ = rdb.Close()
		redisMu.Lock()
		redisFree = append(redisFree, db)
		redisMu.Unlock()
	})
	return rdb
}

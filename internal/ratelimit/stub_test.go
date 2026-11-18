package ratelimit

import "github.com/redis/go-redis/v9"

// stubScripter satisfies redis.Scripter for tests that never reach Redis.
type stubScripter struct{ redis.Scripter }

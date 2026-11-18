-- GCRA with zero burst (D24, ADR-0007 section 5).
--
-- KEYS[1]  rl:{channel}
-- ARGV[1]  emission interval in microseconds: ceil(1e6 / rate). Rounding up
--          can only make the pace slower, never faster.
-- ARGV[2]  reservation horizon in microseconds
--
-- Returns {wait, slot}. wait is the relative wait in microseconds until the
-- caller's slot (>= 0), and slot is that slot's time in microseconds on the
-- Redis clock (for tests and logs; callers schedule by wait, so their own
-- clocks do not matter). When the next free slot starts beyond the horizon
-- the script returns {-1, 0}: nothing is reserved and nothing is written.
--
-- The stored value is the theoretical arrival time (TAT): the earliest time
-- of the next slot. A slot is max(TAT, now); taking it moves TAT one interval
-- past the slot, so two slots are never closer than one interval.
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000000 + tonumber(t[2])
local interval = tonumber(ARGV[1])
local horizon = tonumber(ARGV[2])

local tat = tonumber(redis.call('GET', KEYS[1]))
if tat == nil or tat < now then
  tat = now
end

local wait = tat - now
if wait > horizon then
  return {-1, 0}
end

-- The key only needs to outlive its own TAT: after that, an absent key and a
-- TAT in the past mean the same thing.
local ttl_ms = math.ceil((wait + interval) / 1000) + 1000
redis.call('SET', KEYS[1], string.format('%.0f', tat + interval), 'PX', ttl_ms)
return {wait, tat}

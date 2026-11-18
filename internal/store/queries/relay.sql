-- name: LockDueLane :many
-- Relay cycle, select (ADR-0002). The row locks are held while the relay publishes.
SELECT id, trace_parent
FROM notifications
WHERE channel = @channel AND priority = @priority
  AND status IN ('pending', 'retrying')
  AND due_at <= now()
ORDER BY due_at
LIMIT @batch_limit
FOR UPDATE SKIP LOCKED;

-- name: MarkQueued :many
-- Transitions 1 and 2.
UPDATE notifications
SET status = 'queued', queued_at = now(), updated_at = now(), version = version + 1
WHERE id = ANY(@ids::uuid[])
  AND status IN ('pending', 'retrying')
RETURNING *;

-- name: CountQueuedByLane :many
-- High-water-mark headroom (ADR-0003 section 1).
SELECT channel, priority, count(*) AS queued
FROM notifications
WHERE status = 'queued'
GROUP BY channel, priority;

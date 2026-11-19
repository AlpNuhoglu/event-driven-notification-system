-- name: SummaryQueuedByLane :many
-- Notifications in state queued, per lane (the metrics summary). The
-- partial index notifications_queued_lane_idx holds exactly these rows; on a
-- small table the planner may read another partial index of the waiting
-- states instead.
SELECT channel, priority, count(*)::bigint AS n
FROM notifications
WHERE status = 'queued'
GROUP BY channel, priority;

-- name: SummaryBacklogDueByLane :many
-- Notifications waiting for the relay whose due time has passed, per lane:
-- the same predicate as the relay's lane selection, which
-- notifications_relay_lane_idx serves. The count grows with the backlog,
-- which is why the summary caches it longer than the rest.
SELECT channel, priority, count(*)::bigint AS n
FROM notifications
WHERE status IN ('pending', 'retrying') AND due_at <= @now::timestamptz
GROUP BY channel, priority;

-- name: SummaryAttemptsByOutcome :many
-- Attempts that finished since @since, per channel and outcome. The channel
-- list lets the planner use notification_attempts_channel_finished_idx on
-- both of its columns; an in-flight attempt has no finished_at and is not
-- counted.
SELECT channel, outcome, count(*)::bigint AS n
FROM notification_attempts
WHERE channel = ANY(@channels::text[]) AND finished_at >= @since::timestamptz
GROUP BY channel, outcome;

-- name: SummaryProviderLatency :many
-- Median and 95th percentile of the provider latency of the attempts that
-- finished since @since and recorded one, in milliseconds, per channel.
SELECT channel,
       percentile_cont(0.5) WITHIN GROUP (ORDER BY latency_ms)::float8 AS p50,
       percentile_cont(0.95) WITHIN GROUP (ORDER BY latency_ms)::float8 AS p95
FROM notification_attempts
WHERE channel = ANY(@channels::text[]) AND finished_at >= @since::timestamptz
  AND latency_ms IS NOT NULL
GROUP BY channel;

-- name: SummaryEndToEndLatency :many
-- Median and 95th percentile, in milliseconds, of the time from the moment a
-- notification was meant to be sent (scheduled_at, else created_at) to
-- sent_at, over the notifications that have a successful attempt finished
-- since @since; a notification counts once however many such attempts it has.
SELECT n.channel,
       percentile_cont(0.5) WITHIN GROUP (ORDER BY e2e.ms)::float8 AS p50,
       percentile_cont(0.95) WITHIN GROUP (ORDER BY e2e.ms)::float8 AS p95
FROM (
    SELECT DISTINCT a.notification_id
    FROM notification_attempts a
    WHERE a.channel = ANY(@channels::text[]) AND a.finished_at >= @since::timestamptz
      AND a.outcome = 'success'
) w
JOIN notifications n ON n.id = w.notification_id AND n.sent_at IS NOT NULL
CROSS JOIN LATERAL (
    SELECT extract(epoch FROM n.sent_at - COALESCE(n.scheduled_at, n.created_at)) * 1000 AS ms
) e2e
GROUP BY n.channel;

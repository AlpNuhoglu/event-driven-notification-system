-- name: QueuedLaneGauges :many
-- The relay's gauges lane_queued and lane_oldest_queued_age_seconds
-- (ADR-0009 section 1). It reads only queued rows, through a partial index
-- of the unfinished rows (the queued-lane index, or the expiry index when
-- the planner judges it cheaper). The age is measured on the database
-- clock; lanes without queued rows are absent.
SELECT channel, priority, count(*) AS queued,
       COALESCE(EXTRACT(EPOCH FROM now() - min(queued_at)), 0)::float8 AS oldest_age_seconds
FROM notifications
WHERE status = 'queued'
GROUP BY channel, priority;

-- name: BacklogLaneGauges :many
-- The relay's gauges backlog_due and scheduled_future (ADR-0009 section 1):
-- the pending and retrying rows that are due now and those due later
-- (scheduled, or waiting for a retry's back-off). It reads only those rows,
-- through a partial index of the unfinished rows (the relay-lane index, or
-- the expiry index when the planner judges it cheaper). Lanes without such
-- rows are absent.
SELECT channel, priority,
       count(*) FILTER (WHERE due_at <= now()) AS due,
       count(*) FILTER (WHERE due_at > now()) AS future
FROM notifications
WHERE status IN ('pending', 'retrying')
GROUP BY channel, priority;

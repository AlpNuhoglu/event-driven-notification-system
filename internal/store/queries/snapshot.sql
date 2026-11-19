-- name: SnapshotByIDs :many
-- The gateway's snapshot on subscribe (ADR-0005 "Gateway"): the current
-- state of each named notification, with its version. An id without a row
-- has no result row. The columns are those of a status event; nothing
-- personal (no recipient, no content).
SELECT id, batch_id, channel, priority, status, version, failure_reason, attempt_count, updated_at
FROM notifications
WHERE id = ANY(@ids::uuid[]);

-- name: SnapshotByBatch :many
-- The gateway's snapshot of a batch subscription: the current state of every
-- notification of the batch (at most 1,000), through notifications_batch_idx.
SELECT id, batch_id, channel, priority, status, version, failure_reason, attempt_count, updated_at
FROM notifications
WHERE batch_id = @batch_id::uuid;

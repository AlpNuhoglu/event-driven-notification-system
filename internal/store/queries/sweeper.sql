-- name: LockExpiredLeases :many
-- Lease sweeper, select (ADR-0003 section 4).
SELECT id, attempt_count, priority, failures, max_failures, expires_at, now()::timestamptz AS db_now
FROM notifications
WHERE status = 'processing' AND lease_expires_at < now()
ORDER BY lease_expires_at
LIMIT @batch_limit
FOR UPDATE SKIP LOCKED;

-- name: MarkAttemptUnknown :execrows
-- Lease sweeper.
UPDATE notification_attempts
SET outcome = 'unknown', finished_at = now()
WHERE notification_id = @notification_id AND attempt_no = @attempt_no AND outcome = 'in_flight';

-- name: ClaimNotification :one
-- Transition 3 (ADR-0004 section 3, verbatim).
WITH claimed AS (
  UPDATE notifications
  SET status           = CASE WHEN expires_at <= now() THEN 'failed' ELSE 'processing' END,
      failure_reason   = CASE WHEN expires_at <= now() THEN 'expired' END,
      attempt_count    = attempt_count + CASE WHEN expires_at <= now() THEN 0 ELSE 1 END,
      lease_expires_at = CASE WHEN expires_at <= now() THEN NULL ELSE now() + @lease::interval END,
      updated_at = now(), version = version + 1
  WHERE id = @id
    AND status IN ('pending', 'retrying', 'queued')
    AND due_at <= now()
  RETURNING *
), attempt AS (
  INSERT INTO notification_attempts (notification_id, attempt_no, channel, provider, outcome, started_at)
  SELECT id, attempt_count, channel, @provider, 'in_flight', now()
  FROM claimed
  WHERE status = 'processing'
)
SELECT * FROM claimed;

-- name: MarkSent :one
-- Transition 4; fenced by attempt_count.
UPDATE notifications
SET status = 'sent', provider_message_id = @provider_message_id, provider = @provider, sent_at = now(),
    lease_expires_at = NULL, updated_at = now(), version = version + 1
WHERE id = @id AND status = 'processing' AND attempt_count = @attempt_no
RETURNING *;

-- name: LockProcessing :one
-- Read for transitions 5 and 6; same fencing guard.
SELECT now()::timestamptz AS db_now, priority, failures, max_failures, expires_at
FROM notifications
WHERE id = @id AND status = 'processing' AND attempt_count = @attempt_no
FOR UPDATE;

-- name: MarkRetrying :one
-- Transitions 5 and 7; fenced.
UPDATE notifications
SET status = 'retrying',
    failures = failures + @failures_delta::int,
    due_at = now() + @delay::interval,
    queued_at = NULL, lease_expires_at = NULL,
    updated_at = now(), version = version + 1
WHERE id = @id AND status = 'processing' AND attempt_count = @attempt_no
RETURNING *;

-- name: MarkFailed :one
-- Transitions 6 and 7; fenced.
UPDATE notifications
SET status = 'failed', failure_reason = @failure_reason,
    failures = failures + @failures_delta::int,
    lease_expires_at = NULL,
    updated_at = now(), version = version + 1
WHERE id = @id AND status = 'processing' AND attempt_count = @attempt_no
RETURNING *;

-- name: FinishAttempt :execrows
-- Worker; overwrites 'unknown' with the true outcome.
UPDATE notification_attempts
SET outcome = @outcome, http_status = @http_status, latency_ms = @latency_ms, error = @error,
    provider_message_id = @provider_message_id, finished_at = now()
WHERE notification_id = @notification_id AND attempt_no = @attempt_no
  AND outcome IN ('in_flight', 'unknown');

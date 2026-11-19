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

-- name: MarkSentLate :one
-- Transition 12, late success (ADR-0004 sections 3 and 4, D14, D18). Run
-- only after MarkSent matched zero rows, in the same transaction.
--
-- The guard is entirely in the UPDATE's WHERE clause:
--   attempt_count = @attempt_no: no newer attempt has started (only a claim
--     increments it), so this result belongs to the row's latest attempt;
--   status IN (...): every state except processing (a newer attempt holds
--     the row, since MarkSent did not match) and sent (nothing to do).
-- A row version that fails this guard is never followed by one that passes
-- it: attempt_count never decreases, sent is final, and processing with
-- this attempt_count is over once MarkSent has missed. So a statement
-- snapshot that predates a concurrent commit cannot hide a row that should
-- be updated.
--
-- The CTE locks the row by id and does nothing else: the status the row
-- leaves (from_status) is read from the locked, latest row version, which
-- is the version the UPDATE then changes. When another transaction holds
-- the row, the lock waits for it. One row, by id, as in every single-row
-- transition: no lock-order concern.
--
-- SET, beyond what transition 4 sets:
--   failure_reason = NULL: the row may come from failed, and the CHECK
--     requires failed <=> failure_reason;
--   queued_at = NULL: on a queued, cancelled or failed row it may record a
--     publish made after the attempt that succeeded, a wrong time; NULL
--     means "not known", as for a direct claim.
-- lease_expires_at = NULL as in transition 4; it is already NULL in the
-- five source states. attempt_count (the fencing token), failures (the
-- budget consumed; only redrive resets it), due_at and expires_at are left
-- as they are.
WITH cur AS (
  SELECT l.id, l.status
  FROM notifications AS l
  WHERE l.id = @id
  FOR UPDATE
)
UPDATE notifications AS n
SET status = 'sent', provider_message_id = @provider_message_id, provider = @provider, sent_at = now(),
    failure_reason = NULL, lease_expires_at = NULL, queued_at = NULL,
    updated_at = now(), version = n.version + 1
FROM cur
WHERE n.id = cur.id
  AND n.attempt_count = @attempt_no
  AND n.status IN ('pending', 'queued', 'retrying', 'failed', 'cancelled')
RETURNING sqlc.embed(n), cur.status AS from_status;

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

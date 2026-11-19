-- name: CancelNotification :one
-- Transition 9 (ADR-0004 section 3, D15). The guard is the whole rule: a row
-- that a worker holds (processing) or that is finished (sent, failed) is not
-- matched, and a claim (transition 3) that runs after this statement finds
-- 'cancelled' and affects zero rows, so cancel and claim never both win.
--
-- Only the status changes. Nothing is set that a late success (transition
-- 12) would have to undo, attempt_count is untouched (it decides whether the
-- dedupe_key is released), and "may have been sent" is not stored: it is
-- read from the attempts (HasUnknownAttempt).
UPDATE notifications
SET status = 'cancelled', updated_at = now(), version = version + 1
WHERE id = @id AND status IN ('pending', 'retrying', 'queued')
RETURNING *;

-- name: CancelBatchNotifications :many
-- Transition 9 for every cancellable notification of a batch, in one
-- statement (ADR-0006 section 7). Same guard and same SET as
-- CancelNotification.
--
-- Lock order: the rows are locked in id order first (the locking clause runs
-- above the sort) and updated afterwards. A plain UPDATE locks rows in the
-- order its scan meets them, which nothing fixes, so two transactions that
-- each update several rows of one batch could wait for each other; with one
-- global order they cannot. A row that leaves the cancellable states while
-- the lock is waited for is dropped by the re-check of the SELECT, and the
-- UPDATE repeats the guard.
WITH locked AS (
  SELECT c.id
  FROM notifications AS c
  WHERE c.batch_id = @batch_id AND c.status IN ('pending', 'retrying', 'queued')
  ORDER BY c.id
  FOR UPDATE
)
UPDATE notifications AS n
SET status = 'cancelled', updated_at = now(), version = n.version + 1
FROM locked
WHERE n.id = locked.id AND n.status IN ('pending', 'retrying', 'queued')
RETURNING n.*;

-- name: HasUnknownAttempt :one
-- may_have_been_sent (D15): an attempt ended without a known result.
SELECT EXISTS (
  SELECT 1 FROM notification_attempts
  WHERE notification_id = @notification_id AND outcome = 'unknown'
) AS may_have_been_sent;

-- name: BatchCancelCounts :one
-- The counts of a batch after a cancel (D69): its notifications, the
-- cancelled ones, and the cancelled ones with an attempt of unknown outcome.
SELECT count(*) AS total,
       count(*) FILTER (WHERE n.status = 'cancelled') AS cancelled,
       count(*) FILTER (WHERE n.status = 'cancelled' AND EXISTS (
         SELECT 1 FROM notification_attempts a
         WHERE a.notification_id = n.id AND a.outcome = 'unknown'
       )) AS may_have_been_sent
FROM notifications n
WHERE n.batch_id = @batch_id;

-- name: RedriveNotification :one
-- Transition 11 (ADR-0004 section 3, D16): failed -> pending with a new
-- retry budget, due at once. attempt_count is never reset. One clock, the
-- database's, decides about the expiry in the statement itself:
--   no new expires_at: applies only while the stored one is in the future,
--     and keeps it;
--   a new expires_at: applies only when the stored one has passed and the
--     new one is at least min_lead ahead.
-- So a redriven row is never already expired, and a stored expiry that is
-- still valid is never replaced.
UPDATE notifications
SET status = 'pending', failure_reason = NULL, failures = 0, due_at = now(),
    expires_at = COALESCE(sqlc.narg(new_expires_at)::timestamptz, expires_at),
    queued_at = NULL,
    updated_at = now(), version = version + 1
WHERE id = @id AND status = 'failed'
  AND CASE WHEN sqlc.narg(new_expires_at)::timestamptz IS NULL
           THEN expires_at > now()
           ELSE expires_at <= now()
                AND sqlc.narg(new_expires_at)::timestamptz >= now() + @min_lead::interval
      END
RETURNING *;

-- name: TransitionState :one
-- Why a cancel or a redrive matched no row: the row's state on the database
-- clock.
SELECT status, expires_at, now()::timestamptz AS db_now
FROM notifications
WHERE id = @id;

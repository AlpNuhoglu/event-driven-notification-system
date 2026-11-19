-- name: ExpireDue :many
-- Transition 10 (ADR-0002 "Expiry", ADR-0004 section 3): at most
-- batch_limit pending, retrying or queued rows whose expires_at has passed
-- become failed/expired, oldest expires_at first.
--
-- The rows are locked first, with SKIP LOCKED (the locking clause runs below
-- the LIMIT, so a skipped row does not count against it), and updated
-- afterwards; the statement never waits for a row lock. A row another
-- transaction holds (the relay's lane transaction, a claim, a cancel, a
-- redrive) is left to the next pass, or to the claim's own expiry branch. A
-- row that changed after this statement's snapshot is re-checked against
-- the WHERE clause when it is locked, and the UPDATE repeats the guard.
--
-- Only status, failure_reason, version and updated_at change, as in the
-- claim's expiry branch: attempt_count, failures and the attempt rows are
-- untouched, and lease_expires_at is already NULL in these three states.
WITH due AS (
  SELECT e.id
  FROM notifications AS e
  WHERE e.status IN ('pending', 'retrying', 'queued')
    AND e.expires_at <= now()
  ORDER BY e.expires_at
  LIMIT @batch_limit
  FOR UPDATE SKIP LOCKED
)
UPDATE notifications AS n
SET status = 'failed', failure_reason = 'expired', updated_at = now(), version = n.version + 1
FROM due
WHERE n.id = due.id
  AND n.status IN ('pending', 'retrying', 'queued')
  AND n.expires_at <= now()
RETURNING n.*;

-- name: DeleteExpiredIdempotencyKeys :execrows
-- Idempotency-key cleanup (D78, ADR-0006 section 11): deletes at most
-- batch_limit expired keys, oldest expires_at first. A key that a request
-- holds (a take-over in an open transaction, or a live key being read for a
-- replay) is skipped, so the delete never waits for a request. A key taken
-- over after this statement's snapshot is re-checked when it is locked and,
-- now live, is not deleted. A take-over that runs while this statement's
-- transaction holds the lock waits for it and then inserts the key as new.
WITH doomed AS (
  SELECT k.key
  FROM idempotency_keys AS k
  WHERE k.expires_at <= now()
  ORDER BY k.expires_at
  LIMIT @batch_limit
  FOR UPDATE SKIP LOCKED
)
DELETE FROM idempotency_keys AS i
USING doomed
WHERE i.key = doomed.key
  AND i.expires_at <= now();

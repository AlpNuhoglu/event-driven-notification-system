-- name: ReconcileQueued :many
-- Transition 8, reconciliation (ADR-0003 section 3, ADR-0004 section 3,
-- D5): every queued row returns to pending, so that the relay publishes it
-- again. It never publishes. Only status, queued_at, version and updated_at
-- change: queued_at is cleared like on every transition back to a state the
-- relay publishes from; dedupe_key, attempt_count, failures, due_at and
-- expires_at stay as they are (due_at is already past, so the row is due).
--
-- Rows another transaction holds are waited for, never skipped: a row
-- skipped while a cancel or a claim holds it, whose transaction then rolls
-- back, would stay queued with no copy in the broker. After the wait the
-- locking clause re-checks the row's latest version: a row that the holder
-- moved out of queued (claimed, cancelled, expired) is left alone, a row
-- whose holder rolled back is still queued and is taken.
--
-- Lock order: the rows are locked in id order first (the locking clause
-- runs above the sort) and updated afterwards, as in the batch cancel
-- (CancelBatchNotifications), the only other statement that waits for
-- several rows. Two statements that lock several rows in one global order
-- cannot wait for each other in a cycle. Every other writer of the table
-- holds at most one notifications row while it waits (the claim, the
-- single cancel, the redrive, the worker's final writes), or never waits
-- for a row lock (the relay's lane transaction, the expiry pass and the
-- lease sweeper select with SKIP LOCKED and update only the rows they
-- locked). Two reconciliations (two relays) also lock in id order; the
-- later one finds the rows pending and leaves them.
--
-- RETURNING lists only the columns of the status event, which keeps the
-- result of a large backlog small.
WITH locked AS (
  SELECT q.id
  FROM notifications AS q
  WHERE q.status = 'queued'
  ORDER BY q.id
  FOR UPDATE
)
UPDATE notifications AS n
SET status = 'pending', queued_at = NULL, updated_at = now(), version = n.version + 1
FROM locked
WHERE n.id = locked.id
  AND n.status = 'queued'
RETURNING n.id, n.batch_id, n.channel, n.priority, n.status, n.version, n.failure_reason,
          n.attempt_count, n.updated_at;

-- name: RequeueStale :many
-- Transition 8 for the manual requeue-stale command (ADR-0003 section 3,
-- D64): the queued rows whose queued_at is older than older_than. The same
-- statement as ReconcileQueued with one more condition, repeated in the
-- UPDATE: a row republished while its lock was waited for has a new
-- queued_at and is left alone. Same lock order (id), same waiting, same
-- columns changed.
WITH locked AS (
  SELECT q.id
  FROM notifications AS q
  WHERE q.status = 'queued'
    AND q.queued_at < now() - @older_than::interval
  ORDER BY q.id
  FOR UPDATE
)
UPDATE notifications AS n
SET status = 'pending', queued_at = NULL, updated_at = now(), version = n.version + 1
FROM locked
WHERE n.id = locked.id
  AND n.status = 'queued'
  AND n.queued_at < now() - @older_than::interval
RETURNING n.id, n.batch_id, n.channel, n.priority, n.status, n.version, n.failure_reason,
          n.attempt_count, n.updated_at;

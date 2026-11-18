-- name: InsertBatch :exec
INSERT INTO batches (id, total, accepted, rejected, correlation_id)
VALUES (@id, @total, @accepted, @rejected, @correlation_id);

-- name: SetBatchCounts :execrows
UPDATE batches SET accepted = @accepted, rejected = @rejected WHERE id = @id;

-- name: InsertBatchNotifications :many
-- Batch create (ADR-0006 section 6): one statement for every item; RETURNING
-- lists the inserted rows only. The ON CONFLICT target repeats the predicate
-- of notifications_dedupe_key_uidx verbatim, so that Postgres infers that
-- partial index.
--
-- Lock order: the rows are inserted in dedupe_key order. An insert that meets
-- another transaction's uncommitted entry for the same key waits for that
-- transaction, so two batches whose keys overlap in different orders could
-- each wait for the other (a deadlock); in one global order they cannot.
--
-- The arrays are expanded by unnest in the select list (they have equal
-- lengths, so they are zipped row by row); sqlc does not know the
-- multi-argument unnest of a FROM clause. The nullable columns are passed as
-- _text, _int2, _int4 and _timestamptz (the same array types): sqlc types a
-- text[] parameter as []string, which cannot hold a NULL element, and for
-- these spellings it emits interface{}, so the store passes pointer slices.
INSERT INTO notifications (
  id, batch_id, dedupe_key, channel, priority, recipient, content,
  sms_encoding, sms_segments, template_name, template_version, template_vars,
  due_at, scheduled_at, expires_at, max_failures, correlation_id, trace_parent
)
SELECT u.id, @batch_id::uuid, u.dedupe_key, u.channel, u.priority, u.recipient, u.content,
       u.sms_encoding, u.sms_segments, u.template_name, u.template_version, u.template_vars,
       COALESCE(u.scheduled_at, now()),
       u.scheduled_at,
       COALESCE(u.expires_at, COALESCE(u.scheduled_at, now()) + u.ttl),
       u.max_failures, u.correlation_id, u.trace_parent
FROM (
  SELECT unnest(@ids::uuid[]) AS id,
         unnest(@dedupe_keys::_text) AS dedupe_key,
         unnest(@channels::text[]) AS channel,
         unnest(@priorities::smallint[]) AS priority,
         unnest(@recipients::text[]) AS recipient,
         unnest(@contents::text[]) AS content,
         unnest(@sms_encodings::_text) AS sms_encoding,
         unnest(@sms_segments::_int2) AS sms_segments,
         unnest(@template_names::_text) AS template_name,
         unnest(@template_versions::_int4) AS template_version,
         unnest(@template_vars::jsonb[]) AS template_vars,
         unnest(@scheduled_ats::_timestamptz) AS scheduled_at,
         unnest(@expires_ats::_timestamptz) AS expires_at,
         unnest(@ttls::interval[]) AS ttl,
         unnest(@max_failures::int[]) AS max_failures,
         unnest(@correlation_ids::text[]) AS correlation_id,
         unnest(@trace_parents::text[]) AS trace_parent
) AS u
ORDER BY u.dedupe_key
ON CONFLICT (dedupe_key) WHERE dedupe_key IS NOT NULL AND NOT (status = 'cancelled' AND attempt_count = 0)
DO NOTHING
RETURNING id, dedupe_key;

-- name: GetBatch :one
SELECT * FROM batches WHERE id = @id;

-- name: CountBatchByStatus :many
-- Per-status progress of a batch (ADR-0006 section 6).
SELECT status, count(*) AS count
FROM notifications
WHERE batch_id = @batch_id
GROUP BY status;

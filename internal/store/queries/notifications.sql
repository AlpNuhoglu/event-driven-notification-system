-- name: CreateNotification :one
-- Single create with the record layer (ADR-0006 section 11): on a dedupe_key
-- conflict no row is inserted and none is returned (DO NOTHING). due_at and
-- expires_at are computed from the database clock (one clock). The ON
-- CONFLICT target repeats the predicate of notifications_dedupe_key_uidx
-- verbatim, so that Postgres infers that partial index.
INSERT INTO notifications (
  id, batch_id, dedupe_key, channel, priority, recipient, content,
  sms_encoding, sms_segments, template_name, template_version, template_vars,
  due_at, scheduled_at, expires_at, max_failures, correlation_id, trace_parent
) VALUES (
  @id, sqlc.narg(batch_id), sqlc.narg(dedupe_key), @channel, @priority, @recipient, @content,
  sqlc.narg(sms_encoding), sqlc.narg(sms_segments), sqlc.narg(template_name),
  sqlc.narg(template_version), sqlc.narg(template_vars),
  COALESCE(sqlc.narg(scheduled_at)::timestamptz, now()),
  sqlc.narg(scheduled_at)::timestamptz,
  COALESCE(sqlc.narg(expires_at)::timestamptz,
           COALESCE(sqlc.narg(scheduled_at)::timestamptz, now()) + @ttl::interval),
  @max_failures, @correlation_id, @trace_parent
)
ON CONFLICT (dedupe_key) WHERE dedupe_key IS NOT NULL AND NOT (status = 'cancelled' AND attempt_count = 0)
DO NOTHING
RETURNING *;

-- name: FindDedupeConflicts :many
-- Conflict look-up of the record layer (ADR-0006 sections 6 and 11): the row
-- that holds each key in notifications_dedupe_key_uidx. A key whose row left
-- the index since the insert has no result row.
SELECT id, dedupe_key
FROM notifications
WHERE dedupe_key = ANY(@dedupe_keys::text[])
  AND NOT (status = 'cancelled' AND attempt_count = 0);

-- name: GetNotification :one
SELECT * FROM notifications WHERE id = @id;

-- name: ListAttempts :many
SELECT * FROM notification_attempts
WHERE notification_id = @notification_id
ORDER BY attempt_no;

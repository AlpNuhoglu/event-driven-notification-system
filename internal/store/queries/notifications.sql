-- name: CreateNotification :one
-- due_at and expires_at are computed from the database clock (one clock).
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
RETURNING *;

-- name: GetNotification :one
SELECT * FROM notifications WHERE id = @id;

-- name: ListAttempts :many
SELECT * FROM notification_attempts
WHERE notification_id = @notification_id
ORDER BY attempt_no;

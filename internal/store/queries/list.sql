-- The listing: newest first, keyset pagination on (created_at, id).
--
-- The cursor and the date bounds use COALESCE rather than "IS NULL OR" so
-- that they stay index conditions also in a generic plan (a prepared
-- statement that is planned without its parameter values). A missing cursor
-- compares against the largest possible row, a missing bound against
-- -infinity / infinity. The other filters are residual filters on the index
-- scan.

-- name: ListNotifications :many
-- Served by notifications_created_idx (created_at DESC, id DESC): an index
-- scan in index order, no sort.
SELECT * FROM notifications
WHERE (created_at, id) < (COALESCE(sqlc.narg(cursor_created_at)::timestamptz, 'infinity'::timestamptz),
                          COALESCE(sqlc.narg(cursor_id)::uuid, 'ffffffff-ffff-ffff-ffff-ffffffffffff'::uuid))
  AND created_at >= COALESCE(sqlc.narg(created_from)::timestamptz, '-infinity'::timestamptz)
  AND created_at <  COALESCE(sqlc.narg(created_to)::timestamptz, 'infinity'::timestamptz)
  AND (sqlc.narg(channel)::text IS NULL OR channel = sqlc.narg(channel)::text)
  AND (sqlc.narg(priority)::smallint IS NULL OR priority = sqlc.narg(priority)::smallint)
  AND (sqlc.narg(batch_id)::uuid IS NULL OR batch_id = sqlc.narg(batch_id)::uuid)
ORDER BY created_at DESC, id DESC
LIMIT @page_limit;

-- name: ListNotificationsByStatus :many
-- Served by notifications_status_created_idx (status, created_at DESC,
-- id DESC): the status is an equality on the first column, so the scan is in
-- (created_at, id) order, no sort.
SELECT * FROM notifications
WHERE status = @status
  AND (created_at, id) < (COALESCE(sqlc.narg(cursor_created_at)::timestamptz, 'infinity'::timestamptz),
                          COALESCE(sqlc.narg(cursor_id)::uuid, 'ffffffff-ffff-ffff-ffff-ffffffffffff'::uuid))
  AND created_at >= COALESCE(sqlc.narg(created_from)::timestamptz, '-infinity'::timestamptz)
  AND created_at <  COALESCE(sqlc.narg(created_to)::timestamptz, 'infinity'::timestamptz)
  AND (sqlc.narg(channel)::text IS NULL OR channel = sqlc.narg(channel)::text)
  AND (sqlc.narg(priority)::smallint IS NULL OR priority = sqlc.narg(priority)::smallint)
  AND (sqlc.narg(batch_id)::uuid IS NULL OR batch_id = sqlc.narg(batch_id)::uuid)
ORDER BY created_at DESC, id DESC
LIMIT @page_limit;

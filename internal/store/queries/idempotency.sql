-- name: ClaimIdempotencyKey :one
-- Request layer (ADR-0006 section 11, verbatim except that the retention is
-- a parameter): claims a new key, or takes over an expired one atomically.
-- No row: the key is live. Its row is then locked by this transaction (ON
-- CONFLICT DO UPDATE locks the conflicting row even when its WHERE is false).
-- A concurrent uncommitted claim of the same key makes this statement wait.
INSERT INTO idempotency_keys (key, request_hash, created_at, expires_at)
VALUES (@key, @request_hash, now(), now() + @retention::interval)
ON CONFLICT (key) DO UPDATE
  SET request_hash = EXCLUDED.request_hash,
      created_at = EXCLUDED.created_at,
      expires_at = EXCLUDED.expires_at,
      response_status = NULL,
      response_body = NULL
  WHERE idempotency_keys.expires_at <= now()
RETURNING key;

-- name: GetIdempotencyKey :one
SELECT request_hash, response_status, response_body
FROM idempotency_keys
WHERE key = @key;

-- name: GetLiveIdempotencyKey :one
-- Read-only look-up outside the create transaction, with the liveness rule
-- of ClaimIdempotencyKey: a key is live while expires_at > now() (the claim
-- takes over a key whose expires_at <= now()). It takes no lock and does not
-- see a claim that has not committed.
SELECT request_hash, response_status, response_body
FROM idempotency_keys
WHERE key = @key AND expires_at > now();

-- name: StoreIdempotencyResponse :execrows
-- Stores the response on the key claimed in the same transaction.
UPDATE idempotency_keys
SET response_status = @response_status, response_body = @response_body
WHERE key = @key;

CREATE TABLE batches (
    id             uuid PRIMARY KEY,
    total          int NOT NULL,
    accepted       int NOT NULL,
    rejected       int NOT NULL,
    correlation_id text NOT NULL,
    created_at     timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE notifications (
    id                  uuid PRIMARY KEY,
    batch_id            uuid NULL REFERENCES batches (id),
    dedupe_key          text NULL,
    channel             text NOT NULL CHECK (channel IN ('sms', 'email', 'push')),
    priority            smallint NOT NULL CHECK (priority BETWEEN 0 AND 2),
    recipient           text NOT NULL,
    content             text NOT NULL,
    sms_encoding        text NULL CHECK (sms_encoding IN ('GSM-7', 'UCS-2')),
    sms_segments        smallint NULL,
    template_name       text NULL,
    template_version    int NULL,
    template_vars       jsonb NULL,
    status              text NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'queued', 'processing', 'retrying', 'sent', 'failed', 'cancelled')),
    failure_reason      text NULL
        CHECK (failure_reason IN ('provider_rejected', 'max_failures', 'max_failures_unknown_outcome', 'expired', 'internal_error')),
    due_at              timestamptz NOT NULL,
    scheduled_at        timestamptz NULL,
    expires_at          timestamptz NOT NULL,
    attempt_count       int NOT NULL DEFAULT 0,
    failures            int NOT NULL DEFAULT 0,
    max_failures        int NOT NULL,
    queued_at           timestamptz NULL,
    lease_expires_at    timestamptz NULL,
    provider            text NULL,
    provider_message_id text NULL,
    correlation_id      text NOT NULL,
    trace_parent        text NOT NULL DEFAULT '',
    version             bigint NOT NULL DEFAULT 1,
    created_at          timestamptz NOT NULL DEFAULT now(),
    updated_at          timestamptz NOT NULL DEFAULT now(),
    sent_at             timestamptz NULL,
    CONSTRAINT notifications_failed_has_reason CHECK ((status = 'failed') = (failure_reason IS NOT NULL)),
    CONSTRAINT notifications_sent_has_sent_at CHECK ((status = 'sent') = (sent_at IS NOT NULL))
);

CREATE TABLE notification_attempts (
    notification_id     uuid NOT NULL REFERENCES notifications (id),
    attempt_no          int NOT NULL,
    channel             text NOT NULL,
    provider            text NOT NULL,
    outcome             text NOT NULL
        CHECK (outcome IN ('in_flight', 'success', 'retryable', 'permanent', 'provider_fatal', 'throttled', 'unknown', 'unreachable', 'aborted', 'internal_error')),
    http_status         int NULL,
    latency_ms          int NULL,
    error               text NULL,
    provider_message_id text NULL,
    started_at          timestamptz NOT NULL,
    finished_at         timestamptz NULL,
    PRIMARY KEY (notification_id, attempt_no),
    CONSTRAINT notification_attempts_finished_unless_in_flight CHECK ((outcome = 'in_flight') = (finished_at IS NULL))
);

CREATE TABLE idempotency_keys (
    key             text PRIMARY KEY,
    request_hash    text NOT NULL,
    response_status int NULL,
    response_body   jsonb NULL,
    created_at      timestamptz NOT NULL,
    expires_at      timestamptz NOT NULL
);

-- Relay lane selection.
CREATE INDEX notifications_relay_lane_idx ON notifications (channel, priority, due_at)
    WHERE status IN ('pending', 'retrying');

-- HWM count, oldest-queued age, reconciliation.
CREATE INDEX notifications_queued_lane_idx ON notifications (channel, priority, queued_at)
    WHERE status = 'queued';

-- Lease sweeper.
CREATE INDEX notifications_lease_idx ON notifications (lease_expires_at)
    WHERE status = 'processing';

-- Expiry pass.
CREATE INDEX notifications_expiry_idx ON notifications (expires_at)
    WHERE status IN ('pending', 'retrying', 'queued');

-- Listing and keyset pagination.
CREATE INDEX notifications_created_idx ON notifications (created_at DESC, id DESC);

-- Listing filtered by status.
CREATE INDEX notifications_status_created_idx ON notifications (status, created_at DESC, id DESC);

-- Batch queries and batch cancel.
CREATE INDEX notifications_batch_idx ON notifications (batch_id)
    WHERE batch_id IS NOT NULL;

-- Record-level idempotency; every ON CONFLICT repeats this predicate.
CREATE UNIQUE INDEX notifications_dedupe_key_uidx ON notifications (dedupe_key)
    WHERE dedupe_key IS NOT NULL AND NOT (status = 'cancelled' AND attempt_count = 0);

-- At most one open attempt.
CREATE UNIQUE INDEX notification_attempts_one_in_flight_uidx ON notification_attempts (notification_id)
    WHERE outcome = 'in_flight';

-- Metrics summary.
CREATE INDEX notification_attempts_channel_finished_idx ON notification_attempts (channel, finished_at);

-- Cleanup.
CREATE INDEX idempotency_keys_expires_idx ON idempotency_keys (expires_at);

ALTER TABLE notifications SET (autovacuum_vacuum_scale_factor = 0.02);
ALTER TABLE notification_attempts SET (autovacuum_vacuum_scale_factor = 0.02);

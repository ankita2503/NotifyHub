-- Required for gen_random_uuid()
CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE notification_request (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    idempotency_key VARCHAR(255) NOT NULL UNIQUE,

    request_hash VARCHAR(64),

    user_id VARCHAR(64) NOT NULL,

    category VARCHAR(32) NOT NULL,

    channels VARCHAR(128) NOT NULL,

    template_id VARCHAR(64) NOT NULL,

    payload JSONB,

    status VARCHAR(32) NOT NULL,

-- use TIMESTAMPTZ as timestamp has no timezone
    scheduled_at TIMESTAMPTZ,

    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_notification_request_user_id
    ON notification_request(user_id);

CREATE INDEX idx_notification_request_status
    ON notification_request(status,created_at);

CREATE INDEX idx_notification_request_scheduled_at
    ON notification_request(scheduled_at);
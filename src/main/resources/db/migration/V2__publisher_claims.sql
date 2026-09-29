ALTER TABLE notification_request
    ADD COLUMN publisher_claim_token UUID,
    ADD COLUMN publisher_claim_until TIMESTAMPTZ,
    ADD CONSTRAINT publisher_claim_pair CHECK (
        (publisher_claim_token IS NULL) = (publisher_claim_until IS NULL)
    );

CREATE INDEX idx_notification_request_claims
    ON notification_request(publisher_claim_until, created_at, id)
    WHERE status = 'ACCEPTED';

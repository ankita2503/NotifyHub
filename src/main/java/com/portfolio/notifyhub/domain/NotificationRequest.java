package com.portfolio.notifyhub.domain;

import jakarta.persistence.*;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "notification_request")
public class NotificationRequest {

    @Id
    @GeneratedValue
    private UUID id;

    @Column(name = "idempotency_key", nullable = false, unique = true, length = 255)
    private String idempotencyKey;

    @Column(name = "request_hash", length = 64)
    private String requestHash;

    @Column(name = "user_id", nullable = false, length = 64)
    private String userId;

    @Enumerated(EnumType.STRING)
    @Column(name = "category", nullable = false, length = 32)
    private Category category;

    @Column(name = "channels", nullable = false, length = 128)
    private String channels;

    @Column(name = "template_id", nullable = false, length = 64)
    private String templateId;

    @Column(name = "payload", columnDefinition = "jsonb")
    private String payload;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false, length = 32)
    private Status status;

    @Column(name = "scheduled_at")
    private Instant scheduledAt;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    protected NotificationRequest() {
        // Required by JPA
    }

    public NotificationRequest(
            String idempotencyKey,
            String requestHash,
            String userId,
            Category category,
            String channels,
            String templateId,
            String payload,
            Status status,
            Instant scheduledAt
    ) {
        this.id = UUID.randomUUID();
        this.idempotencyKey = idempotencyKey;
        this.requestHash = requestHash;
        this.userId = userId;
        this.category = category;
        this.channels = channels;
        this.templateId = templateId;
        this.payload = payload;
        this.status = status;
        this.scheduledAt = scheduledAt;
        this.createdAt = Instant.now();
    }

    // getters only
}
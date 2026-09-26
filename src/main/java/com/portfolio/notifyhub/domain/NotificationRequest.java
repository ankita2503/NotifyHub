package com.portfolio.notifyhub.domain;

import jakarta.persistence.*;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "notification_request")
public class NotificationRequest {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
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

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "payload")
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

    public UUID getId() {
        return id;
    }

    public String getUserId() {
        return userId;
    }

    public Category getCategory() {
        return category;
    }

    public String getChannels() {
        return channels;
    }

    public String getTemplateId() {
        return templateId;
    }

    public String getPayload() {
        return payload;
    }

    public Instant getScheduledAt() {
        return scheduledAt;
    }

    public Status getStatus() {
        return status;
    }
}

// getters only

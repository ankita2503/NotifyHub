package com.portfolio.notifyhub.messaging;

import com.portfolio.notifyhub.domain.Category;

import java.time.Instant;
import java.util.UUID;


public record NotificationRequestedEvent(
        UUID requestId,
        String userId,
        Category category,
        String channels,
        String templateId,
        String payload,
        Instant scheduledAt
) {
}

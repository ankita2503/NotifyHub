package com.portfolio.notifyhub.api;

import com.portfolio.notifyhub.domain.Category;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;

import java.nio.channels.Channel;
import java.time.Instant;
import java.util.Map;
import java.util.Set;

public record CreateNotificationRequest(
        @NotBlank String userId,
        @NotNull Category category,
        @NotBlank String channels,
        @NotBlank String templateId,
        String payload,
        Instant scheduledAt
) {}

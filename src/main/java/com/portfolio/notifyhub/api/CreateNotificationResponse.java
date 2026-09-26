package com.portfolio.notifyhub.api;

import com.portfolio.notifyhub.domain.Category;
import com.portfolio.notifyhub.domain.Status;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;

import java.nio.channels.Channel;
import java.time.Instant;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

public record CreateNotificationResponse(
        UUID requestId,
        Status status,
        boolean duplicate
) {}

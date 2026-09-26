package com.portfolio.notifyhub.idempotency;

import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.UUID;

@Service
public class IdempotencyService {

    private static final Duration TTL = Duration.ofHours(24);

    private final RedisTemplate<String, String> redisTemplate;

    public IdempotencyService(RedisTemplate<String, String> redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    public boolean claim(String key, UUID requestId) {

        Boolean claimed = redisTemplate
                .opsForValue()
                .setIfAbsent(
                        key,
                        requestId.toString(),
                        TTL
                );

        return Boolean.TRUE.equals(claimed);
    }

    public UUID getExistingRequestId(String key) {

        String value = redisTemplate
                .opsForValue()
                .get(key);

        if (value == null) {
            return null;
        }

        return UUID.fromString(value);
    }
}
package com.portfolio.notifyhub.messaging;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Service
public class NotificationSweeper {
    private static final Logger log = LoggerFactory.getLogger(NotificationSweeper.class);
    private final NotificationClaims claims;
    private final NotificationProducer producer;
    private final int batchSize;
    private final long acknowledgmentWaitMs;
    private final long leaseMs;
    private UUID token;
    private final Map<UUID, CompletableFuture<Void>> inFlight = new LinkedHashMap<>();

    public NotificationSweeper(NotificationClaims claims, NotificationProducer producer,
            @Value("${notifyhub.publisher.batch-size:500}") int batchSize,
            @Value("${notifyhub.publisher.acknowledgment-wait-ms:1000}") long acknowledgmentWaitMs,
            @Value("${notifyhub.publisher.lease-ms:300000}") long leaseMs) {
        if (batchSize < 1 || batchSize > 10000 || acknowledgmentWaitMs < 1
                || leaseMs < 3 || acknowledgmentWaitMs >= leaseMs / 3) {
            throw new IllegalArgumentException("Batch size must be 1..10000; positive acknowledgment wait must be below one third of lease");
        }
        this.claims = claims;
        this.producer = producer;
        this.batchSize = batchSize;
        this.acknowledgmentWaitMs = acknowledgmentWaitMs;
        this.leaseMs = leaseMs;
    }

    @Scheduled(fixedDelayString = "${notifyhub.publisher.poll-delay-ms:100}")
    public synchronized void publishAcceptedNotifications() {
        try {
            if (inFlight.isEmpty()) dispatchClaimedBatch();
            else renewOwnership();
            if (inFlight.isEmpty()) return;

            CompletableFuture<?>[] settled = inFlight.values().stream()
                    .map(future -> future.handle((result, error) -> null))
                    .toArray(CompletableFuture<?>[]::new);
            try {
                CompletableFuture.allOf(settled).get(acknowledgmentWaitMs, TimeUnit.MILLISECONDS);
            } catch (TimeoutException e) {
                log.debug("Batch acknowledgment wait elapsed; retaining unresolved sends");
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (ExecutionException e) {
                throw new IllegalStateException("Unexpected batch completion failure", e);
            }

            List<UUID> failed = inFlight.entrySet().stream()
                    .filter(entry -> entry.getValue().isCompletedExceptionally())
                    .map(Map.Entry::getKey).toList();
            if (!failed.isEmpty()) {
                claims.release(token, failed);
                failed.forEach(inFlight::remove);
            }
            List<UUID> acknowledged = inFlight.entrySet().stream()
                    .filter(entry -> entry.getValue().isDone() && !entry.getValue().isCompletedExceptionally())
                    .map(Map.Entry::getKey).toList();
            if (!acknowledged.isEmpty()) {
                claims.published(token, acknowledged);
                acknowledged.forEach(inFlight::remove);
            }
        } catch (RuntimeException e) {
            // Keep in-flight outcomes on database failure; the next tick checks lease ownership first.
            log.warn("Publisher batch could not progress; will retry with ownership check", e);
        }
    }

    private void renewOwnership() {
        List<UUID> owned = claims.renew(token, List.copyOf(inFlight.keySet()), leaseMs);
        inFlight.keySet().retainAll(owned);
    }

    private void dispatchClaimedBatch() {
        token = UUID.randomUUID();
        long renewedAt = System.nanoTime();
        List<NotificationRequestedEvent> events = claims.claim(batchSize, token, leaseMs);
        // Placeholders make unsent rows releasable if dispatch is interrupted or a lease is lost.
        events.forEach(event -> inFlight.put(event.requestId(),
                CompletableFuture.failedFuture(new IllegalStateException("Not submitted"))));
        for (NotificationRequestedEvent event : events) {
            if (System.nanoTime() - renewedAt >= TimeUnit.MILLISECONDS.toNanos(leaseMs / 3)) {
                renewedAt = System.nanoTime();
                renewOwnership();
            }
            if (!inFlight.containsKey(event.requestId())) continue;
            try {
                inFlight.put(event.requestId(), producer.publish(event));
            } catch (RuntimeException e) {
                log.warn("Send rejected for request {}; claim will be released", event.requestId(), e);
            }
        }
    }
}

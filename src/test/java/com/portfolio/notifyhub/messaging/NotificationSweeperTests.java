package com.portfolio.notifyhub.messaging;

import com.portfolio.notifyhub.domain.Category;
import com.portfolio.notifyhub.domain.NotificationRequest;
import com.portfolio.notifyhub.domain.Status;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.test.util.ReflectionTestUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class NotificationSweeperTests {
    private final NotificationClaims claims = mock(NotificationClaims.class);
    private final NotificationProducer producer = mock(NotificationProducer.class);
    private final NotificationSweeper sweeper = new NotificationSweeper(claims, producer, 100, 20, 30000);

    private NotificationRequest request() {
        NotificationRequest request = new NotificationRequest(UUID.randomUUID().toString(), null,
                "user", Category.TRANSACTIONAL, "EMAIL", "welcome", "{}", Status.ACCEPTED, null);
        ReflectionTestUtils.setField(request, "id", UUID.randomUUID());
        return request;
    }

    @BeforeEach
    void renewAllClaims() {
        when(claims.renew(any(), anyList(), anyLong())).thenAnswer(call -> call.getArgument(1));
    }

    private NotificationRequestedEvent event(NotificationRequest row) {
        return new NotificationRequestedEvent(row.getId(), row.getUserId(), row.getCategory(),
                row.getChannels(), row.getTemplateId(), row.getPayload(), row.getScheduledAt());
    }

    private void rows(NotificationRequest... requests) {
        when(claims.claim(anyInt(), any(), anyLong()))
                .thenReturn(java.util.Arrays.stream(requests).map(this::event).toList());
    }

    @Test
    void dispatchesEntireBatchBeforeAnyAcknowledgment() throws Exception {
        NotificationRequest first = request();
        NotificationRequest second = request();
        rows(first, second);
        CompletableFuture<Void> acknowledgment = new CompletableFuture<>();
        CountDownLatch sent = new CountDownLatch(2);
        when(producer.publish(any())).thenAnswer(call -> { sent.countDown(); return acknowledgment; });
        var waitingSweeper = new NotificationSweeper(claims, producer, 100, 5000, 30000);
        try (var executor = Executors.newSingleThreadExecutor()) {
            var execution = executor.submit(waitingSweeper::publishAcceptedNotifications);
            try {
                assertTrue(sent.await(2, TimeUnit.SECONDS), "Both sends must start before either is acknowledged");
                verify(claims, never()).published(any(), anyList());
            } finally {
                acknowledgment.complete(null);
            }
            execution.get(2, TimeUnit.SECONDS);
        }
        verify(claims).published(any(), eq(List.of(first.getId(), second.getId())));
    }

    @Test
    void mixedFailuresOnlyUpdateAcknowledgedIds() {
        NotificationRequest rejected = request();
        NotificationRequest failed = request();
        NotificationRequest successful = request();
        rows(rejected, failed, successful);
        when(producer.publish(any())).thenThrow(new IllegalStateException("serialization failed"))
                .thenReturn(CompletableFuture.failedFuture(new IllegalStateException("broker failed")))
                .thenReturn(CompletableFuture.completedFuture(null));
        sweeper.publishAcceptedNotifications();
        verify(claims).published(any(), eq(List.of(successful.getId())));
        verify(producer, times(3)).publish(any());
        verify(claims).release(any(), eq(List.of(rejected.getId(), failed.getId())));
    }

    @Test
    void timeoutRecordsSuccessAndRetainsPendingSendWithoutResubmission() {
        NotificationRequest pending = request();
        NotificationRequest success = request();
        rows(pending, success);
        CompletableFuture<Void> acknowledgment = new CompletableFuture<>();
        when(producer.publish(any())).thenReturn(acknowledgment)
                .thenReturn(CompletableFuture.completedFuture(null));
        sweeper.publishAcceptedNotifications();
        verify(claims).published(any(), eq(List.of(success.getId())));
        sweeper.publishAcceptedNotifications();
        verify(producer, times(2)).publish(any());
        verify(claims, times(1)).claim(anyInt(), any(), anyLong());
        acknowledgment.complete(null);
        sweeper.publishAcceptedNotifications();
        verify(claims).published(any(), eq(List.of(pending.getId())));
    }

    @Test
    void subsequentSweepsReachRequestsBeyondBatchLimit() {
        List<NotificationRequest> pending = new ArrayList<>();
        for (int i = 0; i < 201; i++) pending.add(request());
        when(claims.claim(anyInt(), any(), anyLong()))
                .thenAnswer(call -> pending.stream().limit((Integer) call.getArgument(0)).map(this::event).toList());
        when(producer.publish(any())).thenReturn(CompletableFuture.completedFuture(null));
        when(claims.published(any(), anyList()))
                .thenAnswer(call -> {
                    List<UUID> ids = call.getArgument(1);
                    pending.removeIf(row -> ids.contains(row.getId()));
                    return ids.size();
                });
        sweeper.publishAcceptedNotifications();
        assertEquals(101, pending.size());
        sweeper.publishAcceptedNotifications();
        assertEquals(1, pending.size());
        sweeper.publishAcceptedNotifications();
        sweeper.publishAcceptedNotifications();
        assertTrue(pending.isEmpty());
        verify(producer, times(201)).publish(any());
        verify(claims, times(3)).published(any(), anyList());
    }

    @Test
    void databaseFailureRetriesPersistenceWithoutResending() {
        NotificationRequest request = request();
        rows(request);
        when(producer.publish(any())).thenReturn(CompletableFuture.completedFuture(null));
        when(claims.published(any(), anyList()))
                .thenThrow(new IllegalStateException("database unavailable")).thenReturn(1);
        sweeper.publishAcceptedNotifications();
        sweeper.publishAcceptedNotifications();
        verify(producer, times(1)).publish(any());
        verify(claims, times(2)).published(any(), eq(List.of(request.getId())));
    }

    @Test
    void failedBatchCanRetryOnNextSweep() {
        NotificationRequest request = request();
        rows(request);
        when(producer.publish(any()))
                .thenReturn(CompletableFuture.failedFuture(new IllegalStateException("broker failed")))
                .thenReturn(CompletableFuture.completedFuture(null));
        sweeper.publishAcceptedNotifications();
        verify(claims, never()).published(any(), anyList());
        sweeper.publishAcceptedNotifications();
        verify(claims).published(any(), eq(List.of(request.getId())));
    }
    @Test
    void lostLeaseDiscardsLateAcknowledgmentWithoutUpdatingAnotherOwner() {
        NotificationRequest request = request();
        rows(request);
        CompletableFuture<Void> acknowledgment = new CompletableFuture<>();
        when(producer.publish(any())).thenReturn(acknowledgment);
        sweeper.publishAcceptedNotifications();
        when(claims.renew(any(), anyList(), anyLong())).thenReturn(List.of());
        acknowledgment.complete(null);
        sweeper.publishAcceptedNotifications();
        verify(claims, never()).published(any(), anyList());
        verify(claims, never()).release(any(), anyList());
        verify(producer, times(1)).publish(any());
    }

}

package com.portfolio.notifyhub.messaging;

import com.portfolio.notifyhub.domain.Category;
import com.portfolio.notifyhub.domain.NotificationRequest;
import com.portfolio.notifyhub.domain.Status;
import com.portfolio.notifyhub.repo.NotificationRequestRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.HashSet;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

@DataJpaTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Import(NotificationClaims.class)
@Transactional(propagation = Propagation.NOT_SUPPORTED)
@Testcontainers
class NotificationClaimsTests {
    @Container
    static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine");

    @DynamicPropertySource
    static void database(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
    }

    @Autowired NotificationClaims claims;
    @Autowired NotificationRequestRepository repository;
    @Autowired JdbcTemplate jdbc;
    @Autowired PlatformTransactionManager transactions;

    @BeforeEach
    void clearDatabase() { repository.deleteAll(); }

    private UUID insert() {
        return repository.saveAndFlush(new NotificationRequest(UUID.randomUUID().toString(), null,
                "user", Category.TRANSACTIONAL, "EMAIL", "welcome", "{}", Status.ACCEPTED, null)).getId();
    }

    private List<UUID> ids(List<NotificationRequestedEvent> events) {
        return events.stream().map(NotificationRequestedEvent::requestId).toList();
    }

    private void expire(UUID id) {
        jdbc.update("UPDATE notification_request SET publisher_claim_until = clock_timestamp() - interval '1 second' WHERE id = ?", id);
    }

    @Test
    void concurrentPublishersClaimDisjointBatches() throws Exception {
        for (int i = 0; i < 6; i++) insert();
        CyclicBarrier barrier = new CyclicBarrier(2);
        try (var executor = Executors.newFixedThreadPool(2)) {
            var a = executor.submit(() -> { barrier.await(5, TimeUnit.SECONDS); return claims.claim(3, UUID.randomUUID(), 30000); });
            var b = executor.submit(() -> { barrier.await(5, TimeUnit.SECONDS); return claims.claim(3, UUID.randomUUID(), 30000); });
            var first = ids(a.get(5, TimeUnit.SECONDS));
            var second = ids(b.get(5, TimeUnit.SECONDS));
            assertEquals(3, first.size());
            assertEquals(3, second.size());
            var combined = new HashSet<>(first);
            combined.addAll(second);
            assertEquals(6, combined.size());
        }
        assertTrue(claims.claim(3, UUID.randomUUID(), 30000).isEmpty());
    }

    @Test
    void skipsRowLockedByAnotherTransactionWithoutWaiting() throws Exception {
        UUID lockedId = insert();
        UUID freeId = insert();
        CountDownLatch locked = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        try (var executor = Executors.newFixedThreadPool(2)) {
            var holder = executor.submit(() -> new TransactionTemplate(transactions).execute(status -> {
                jdbc.queryForObject("SELECT id FROM notification_request WHERE id = ? FOR UPDATE", UUID.class, lockedId);
                locked.countDown();
                try { assertTrue(release.await(10, TimeUnit.SECONDS)); }
                catch (InterruptedException e) { Thread.currentThread().interrupt(); throw new IllegalStateException(e); }
                return null;
            }));
            try {
                assertTrue(locked.await(5, TimeUnit.SECONDS));
                var claimant = executor.submit(() -> claims.claim(2, UUID.randomUUID(), 30000));
                assertEquals(List.of(freeId), ids(claimant.get(3, TimeUnit.SECONDS)));
            } finally { release.countDown(); }
            holder.get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void expiredClaimCanBeRecoveredAndOldOwnerCannotMutateIt() {
        UUID id = insert();
        UUID oldToken = UUID.randomUUID();
        UUID newToken = UUID.randomUUID();
        assertEquals(List.of(id), ids(claims.claim(1, oldToken, 30000)));
        expire(id);
        assertTrue(claims.renew(oldToken, List.of(id), 30000).isEmpty());
        assertEquals(0, claims.published(oldToken, List.of(id)));
        assertEquals(List.of(id), ids(claims.claim(1, newToken, 30000)));
        assertTrue(claims.renew(oldToken, List.of(id), 30000).isEmpty());
        assertEquals(0, claims.release(oldToken, List.of(id)));
        assertEquals(0, claims.published(oldToken, List.of(id)));
        assertEquals(1, claims.published(newToken, List.of(id)));
        assertEquals(Status.PUBLISHED, repository.findById(id).orElseThrow().getStatus());
        assertNull(jdbc.queryForObject("SELECT publisher_claim_token FROM notification_request WHERE id = ?", UUID.class, id));
    }

    @Test
    void renewalExtendsLeaseAndFailedRowsCanBeReleased() {
        UUID id = insert();
        UUID token = UUID.randomUUID();
        claims.claim(1, token, 30000);
        var before = jdbc.queryForObject("SELECT publisher_claim_until FROM notification_request WHERE id = ?", java.sql.Timestamp.class, id);
        assertEquals(List.of(id), claims.renew(token, List.of(id), 60000));
        var after = jdbc.queryForObject("SELECT publisher_claim_until FROM notification_request WHERE id = ?", java.sql.Timestamp.class, id);
        assertTrue(after.after(before));
        assertTrue(claims.claim(1, UUID.randomUUID(), 30000).isEmpty());
        assertEquals(1, claims.release(token, List.of(id)));
        assertEquals(List.of(id), ids(claims.claim(1, UUID.randomUUID(), 30000)));
    }

    @Test
    void batchAcknowledgmentCannotUpdateAnotherOwnersRows() {
        UUID a = insert();
        UUID b = insert();
        UUID token = UUID.randomUUID();
        var owned = ids(claims.claim(1, token, 30000));
        claims.claim(1, UUID.randomUUID(), 30000);
        assertEquals(1, claims.published(token, List.of(a, b)));
        assertEquals(Status.PUBLISHED, repository.findById(owned.getFirst()).orElseThrow().getStatus());
        assertEquals(1, jdbc.queryForObject("SELECT count(*) FROM notification_request WHERE status = 'ACCEPTED'", Integer.class));
    }
}

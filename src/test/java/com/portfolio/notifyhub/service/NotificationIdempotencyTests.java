package com.portfolio.notifyhub.service;

import com.portfolio.notifyhub.api.CreateNotificationRequest;
import com.portfolio.notifyhub.domain.Category;
import com.portfolio.notifyhub.domain.NotificationRequest;
import com.portfolio.notifyhub.idempotency.IdempotencyConflictException;
import java.time.Instant;
import com.portfolio.notifyhub.domain.Status;
import com.portfolio.notifyhub.idempotency.IdempotencyService;
import com.portfolio.notifyhub.repo.NotificationRequestRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.context.annotation.Import;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.QueryTimeoutException;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@DataJpaTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Import({NotificationService.class, NotificationPersistence.class})
@Transactional(propagation = Propagation.NOT_SUPPORTED)
@Testcontainers
class NotificationIdempotencyTests {
    @Container
    static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine");

    @DynamicPropertySource
    static void database(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
    }

    @Autowired NotificationService service;
    @Autowired NotificationRequestRepository repository;
    @MockitoBean IdempotencyService idempotency;

    @BeforeEach
    void clearDatabase() {
        repository.deleteAll();
    }

    private CreateNotificationRequest request(String user) {
        return new CreateNotificationRequest(user, Category.TRANSACTIONAL, "EMAIL", "welcome", "{}", null);
    }

    @Test
    void expiredRedisEntryRecoversOriginalCommittedRequest() {
        var original = service.create(request("user"), "same-key");
        repository.transitionStatuses(List.of(original.requestId()), Status.ACCEPTED, Status.PUBLISHED);
        // Redis mock always misses, as with an expired cache entry.
        var replay = service.create(request("user"), "same-key");
        assertTrue(replay.duplicate());
        assertEquals(original.requestId(), replay.requestId());
        assertEquals(Status.PUBLISHED, replay.status());
        assertEquals(1, repository.count());
    }

    @Test
    void concurrentCacheMissesReturnOneRequestId() throws Exception {
        CyclicBarrier barrier = new CyclicBarrier(2);
        when(idempotency.getExistingRequestId("race-key")).thenAnswer(call -> {
            barrier.await(5, TimeUnit.SECONDS);
            return null;
        });
        try (var executor = Executors.newFixedThreadPool(2)) {
            var first = executor.submit(() -> service.create(request("user"), "race-key"));
            var second = executor.submit(() -> service.create(request("user"), "race-key"));
            var a = first.get(10, TimeUnit.SECONDS);
            var b = second.get(10, TimeUnit.SECONDS);
            assertEquals(a.requestId(), b.requestId());
            assertNotEquals(a.duplicate(), b.duplicate());
        }
        assertEquals(1, repository.count());
        verify(idempotency, times(1)).claim(eq("race-key"), any());
    }

    @Test
    void unrelatedIntegrityFailureIsNotReportedAsDuplicate() {
        assertThrows(DataIntegrityViolationException.class,
                () -> service.create(request("x".repeat(65)), "invalid-key"));
        assertEquals(0, repository.count());
        verify(idempotency, never()).claim(anyString(), any());
    }

    @Test
    void existingKeyDoesNotHideUnrelatedIntegrityFailure() {
        service.create(request("user"), "existing-key");
        assertThrows(DataIntegrityViolationException.class,
                () -> service.create(request("x".repeat(65)), "existing-key"));
        assertEquals(1, repository.count());
        verify(idempotency, times(1)).claim(eq("existing-key"), any());
    }

    @Test
    void cacheIsWrittenOnlyAfterDatabaseCommit() {
        when(idempotency.claim(eq("commit-key"), any())).thenAnswer(call -> {
            assertFalse(TransactionSynchronizationManager.isActualTransactionActive());
            UUID id = call.getArgument(1);
            assertTrue(repository.existsById(id));
            return true;
        });
        var created = service.create(request("user"), "commit-key");
        assertFalse(created.duplicate());
        verify(idempotency).claim("commit-key", created.requestId());
    }
    @Test
    void redisReadOutageUsesDatabaseForAcceptanceAndDuplicateRecovery() {
        when(idempotency.getExistingRequestId("offline-key"))
                .thenThrow(new RedisConnectionFailureException("Redis unavailable"));
        var created = service.create(request("user"), "offline-key");
        var replay = service.create(request("user"), "offline-key");
        assertFalse(created.duplicate());
        assertTrue(replay.duplicate());
        assertEquals(created.requestId(), replay.requestId());
        assertEquals(1, repository.count());
        verify(idempotency, never()).claim(anyString(), any());
    }

    @Test
    void redisReadTimeoutUsesDatabase() {
        when(idempotency.getExistingRequestId("timeout-key"))
                .thenThrow(new QueryTimeoutException("Redis command timed out"));
        var created = service.create(request("user"), "timeout-key");
        assertFalse(created.duplicate());
        assertTrue(repository.existsById(created.requestId()));
        verify(idempotency, never()).claim(anyString(), any());
    }

    @Test
    void redisWriteConnectionFailureReturnsCommittedResult() {
        when(idempotency.claim(eq("write-key"), any()))
                .thenThrow(new RedisConnectionFailureException("Redis unavailable"));
        var created = service.create(request("user"), "write-key");
        assertFalse(created.duplicate());
        assertTrue(repository.existsById(created.requestId()));
        var replay = service.create(request("user"), "write-key");
        assertTrue(replay.duplicate());
        assertEquals(created.requestId(), replay.requestId());
        assertEquals(1, repository.count());
    }

    @Test
    void redisWriteTimeoutReturnsCommittedResult() {
        when(idempotency.claim(eq("write-timeout-key"), any()))
                .thenThrow(new QueryTimeoutException("Redis command timed out"));
        var created = service.create(request("user"), "write-timeout-key");
        assertFalse(created.duplicate());
        assertTrue(repository.existsById(created.requestId()));
    }

    @Test
    void databaseFailureStillPropagatesDuringRedisOutage() {
        when(idempotency.getExistingRequestId("invalid-offline-key"))
                .thenThrow(new RedisConnectionFailureException("Redis unavailable"));
        assertThrows(DataIntegrityViolationException.class,
                () -> service.create(request("x".repeat(65)), "invalid-offline-key"));
        assertEquals(0, repository.count());
    }

    @Test
    void unexpectedCacheErrorsAreNotSilentlyIgnored() {
        when(idempotency.getExistingRequestId("bug-key"))
                .thenThrow(new IllegalArgumentException("invalid cached UUID"));
        assertThrows(IllegalArgumentException.class,
                () -> service.create(request("user"), "bug-key"));
        assertEquals(0, repository.count());
    }

    @Test
    void changedFieldsConflictOnCacheMissAndCacheHit() {
        var original = request("user");
        var created = service.create(original, "content-key");
        assertEquals(64, repository.findByIdempotencyKey("content-key").orElseThrow().getRequestHash().length());
        var changed = List.of(
                request("another-user"),
                new CreateNotificationRequest("user", Category.MARKETING, "EMAIL", "welcome", "{}", null),
                new CreateNotificationRequest("user", Category.TRANSACTIONAL, "SMS", "welcome", "{}", null),
                new CreateNotificationRequest("user", Category.TRANSACTIONAL, "EMAIL", "other-template", "{}", null),
                new CreateNotificationRequest("user", Category.TRANSACTIONAL, "EMAIL", "welcome", "{\"a\":1}", null),
                new CreateNotificationRequest("user", Category.TRANSACTIONAL, "EMAIL", "welcome", "{}", Instant.parse("2030-01-01T00:00:00Z")));
        for (var value : changed) {
            assertThrows(IdempotencyConflictException.class, () -> service.create(value, "content-key"));
        }
        when(idempotency.getExistingRequestId("content-key")).thenReturn(created.requestId());
        for (var value : changed) {
            assertThrows(IdempotencyConflictException.class, () -> service.create(value, "content-key"));
        }
        assertEquals(1, repository.count());
        assertEquals(created.requestId(), service.create(original, "content-key").requestId());
    }

    @Test
    void jsonObjectOrderAndWhitespaceDoNotCreateConflicts() {
        var first = new CreateNotificationRequest("user", Category.TRANSACTIONAL, "EMAIL", "welcome",
                "{\"b\":2,\"a\":{\"y\":1,\"x\":0}}", null);
        var second = new CreateNotificationRequest("user", Category.TRANSACTIONAL, "EMAIL", "welcome",
                "{ \"a\": {\"x\":0,\"y\":1}, \"b\":2 }", null);
        var created = service.create(first, "json-key");
        var replay = service.create(second, "json-key");
        assertTrue(replay.duplicate());
        assertEquals(created.requestId(), replay.requestId());
    }

    @Test
    void legacyRowsWithoutHashComparePersistedContent() {
        var row = repository.saveAndFlush(new NotificationRequest("legacy-key", null, "user",
                Category.TRANSACTIONAL, "EMAIL", "welcome", "{}", Status.ACCEPTED, null));
        assertEquals(row.getId(), service.create(request("user"), "legacy-key").requestId());
        assertThrows(IdempotencyConflictException.class,
                () -> service.create(request("other-user"), "legacy-key"));
    }

    @Test
    void concurrentDifferentContentProducesOneConflict() throws Exception {
        CyclicBarrier barrier = new CyclicBarrier(2);
        when(idempotency.getExistingRequestId("content-race")).thenAnswer(call -> {
            barrier.await(5, TimeUnit.SECONDS);
            return null;
        });
        try (var executor = Executors.newFixedThreadPool(2)) {
            var a = executor.submit(() -> service.create(request("user-a"), "content-race"));
            var b = executor.submit(() -> service.create(request("user-b"), "content-race"));
            int successes = 0;
            int conflicts = 0;
            for (var result : List.of(a, b)) {
                try {
                    assertFalse(result.get(10, TimeUnit.SECONDS).duplicate());
                    successes++;
                } catch (java.util.concurrent.ExecutionException e) {
                    assertInstanceOf(IdempotencyConflictException.class, e.getCause());
                    conflicts++;
                }
            }
            assertEquals(1, successes);
            assertEquals(1, conflicts);
        }
        assertEquals(1, repository.count());
    }

}

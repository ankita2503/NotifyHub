package com.portfolio.notifyhub.service;

import com.portfolio.notifyhub.api.CreateNotificationRequest;
import com.portfolio.notifyhub.api.CreateNotificationResponse;
import com.portfolio.notifyhub.domain.NotificationRequest;
import com.portfolio.notifyhub.domain.Status;
import com.portfolio.notifyhub.idempotency.IdempotencyService;
import com.portfolio.notifyhub.repo.NotificationRequestRepository;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

@Service
public class NotificationService {

    private final NotificationRequestRepository repository;
    private final IdempotencyService idempotencyService;

    public NotificationService(
            NotificationRequestRepository repository,
            IdempotencyService idempotencyService) {

        this.repository = repository;
        this.idempotencyService = idempotencyService;
    }

    @Transactional
    public CreateNotificationResponse create(
            CreateNotificationRequest request,
            String idempotencyKey) {

        // 1. Fast path: check Redis
        UUID existingRequestId =
                idempotencyService.getExistingRequestId(idempotencyKey);

        if (existingRequestId != null) {
            return new CreateNotificationResponse(
                    existingRequestId,
                    Status.ACCEPTED,
                    true
            );
        }

        try {
            // 2. Create notification
            NotificationRequest notification =
                    new NotificationRequest(
                            idempotencyKey,
                            null, // requestHash - calculate later
                            request.userId(),
                            request.category(),
                            request.channels(),
                            request.templateId(),
                            request.payload(),
                            Status.ACCEPTED,
                            request.scheduledAt()
                    );

            // 3. PostgreSQL is the source of truth
            NotificationRequest saved =
                    repository.saveAndFlush(notification);

            // 4. Populate Redis after successful DB insert
            idempotencyService.claim(
                    idempotencyKey,
                    saved.getId()
            );

            // 5. DO NOT publish Kafka here.
            //
            // The sweeper will find ACCEPTED rows
            // and publish them to Kafka.

            return new CreateNotificationResponse(
                    saved.getId(),
                    saved.getStatus(),
                    false
            );

        } catch (DataIntegrityViolationException e) {

            /*
             * Another request may have inserted the same
             * idempotency key concurrently.
             *
             * PostgreSQL UNIQUE constraint is the final arbiter.
             */

            NotificationRequest existing =
                    repository
                            .findByIdempotencyKey(idempotencyKey)
                            .orElseThrow(() ->
                                    new IllegalStateException(
                                            "Idempotency conflict but existing request not found"
                                    )
                            );

            return new CreateNotificationResponse(
                    existing.getId(),
                    existing.getStatus(),
                    true
            );
        }
    }
}
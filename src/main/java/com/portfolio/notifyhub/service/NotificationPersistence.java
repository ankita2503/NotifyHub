package com.portfolio.notifyhub.service;

import com.portfolio.notifyhub.domain.NotificationRequest;
import com.portfolio.notifyhub.repo.NotificationRequestRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.Optional;

@Service
public class NotificationPersistence {
    private final NotificationRequestRepository repository;

    public NotificationPersistence(NotificationRequestRepository repository) {
        this.repository = repository;
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public NotificationRequest insert(NotificationRequest request) {
        return repository.saveAndFlush(request);
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public Optional<NotificationRequest> findByIdempotencyKey(String key) {
        return repository.findByIdempotencyKey(key);
    }
}

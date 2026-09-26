package com.portfolio.notifyhub.repo;

import com.portfolio.notifyhub.domain.NotificationRequest;
import com.portfolio.notifyhub.domain.Status;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface NotificationRequestRepository extends JpaRepository<NotificationRequest, UUID> {

    Optional<NotificationRequest> findByIdempotencyKey(String key);

    List<NotificationRequest> findTop100ByStatusOrderByCreatedAtAsc(
            Status status
    );
}

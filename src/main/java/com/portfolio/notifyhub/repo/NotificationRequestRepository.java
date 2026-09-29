package com.portfolio.notifyhub.repo;

import com.portfolio.notifyhub.domain.NotificationRequest;
import com.portfolio.notifyhub.domain.Status;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface NotificationRequestRepository extends JpaRepository<NotificationRequest, UUID> {

    @Transactional
    @Modifying
    @Query("update NotificationRequest n set n.status = :next where n.id in :ids and n.status = :expected")
    int transitionStatuses(@Param("ids") List<UUID> ids, @Param("expected") Status expected,
                         @Param("next") Status next);

    Optional<NotificationRequest> findByIdempotencyKey(String key);

    List<NotificationRequest> findByStatusOrderByCreatedAtAscIdAsc(
            Status status, Pageable pageable
    );
}

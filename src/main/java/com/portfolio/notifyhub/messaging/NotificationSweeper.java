package com.portfolio.notifyhub.messaging;

import com.portfolio.notifyhub.domain.NotificationRequest;
import com.portfolio.notifyhub.domain.Status;
import com.portfolio.notifyhub.repo.NotificationRequestRepository;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class NotificationSweeper {

    private final NotificationRequestRepository repository;
    private final NotificationProducer producer;

    public NotificationSweeper(
            NotificationRequestRepository repository,
            NotificationProducer producer) {
        this.repository = repository;
        this.producer = producer;
    }

    @Scheduled(fixedDelay = 5000)
    public void publishAcceptedNotifications() {

        List<NotificationRequest> requests =
                repository.findTop100ByStatusOrderByCreatedAtAsc(
                        Status.ACCEPTED
                );

        for (NotificationRequest request : requests) {

            NotificationRequestedEvent event =
                    new NotificationRequestedEvent(
                            request.getId(),
                            request.getUserId(),
                            request.getCategory(),
                            request.getChannels(),
                            request.getTemplateId(),
                            request.getPayload(),
                            request.getScheduledAt()
                    );

            producer.publish(event);
        }
    }
}

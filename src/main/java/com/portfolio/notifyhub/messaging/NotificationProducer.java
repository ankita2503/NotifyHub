package com.portfolio.notifyhub.messaging;

import com.portfolio.notifyhub.config.KafkaTopicConfig;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class NotificationProducer {

    private final KafkaTemplate<String, NotificationRequestedEvent> kafkaTemplate;

    public NotificationProducer(
            KafkaTemplate<String, NotificationRequestedEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publish(NotificationRequestedEvent event) {

        kafkaTemplate.send(
                KafkaTopicConfig.NOTIFICATIONS_REQUESTED,
                event.userId(),     // Kafka message key
                event
        );
    }
}

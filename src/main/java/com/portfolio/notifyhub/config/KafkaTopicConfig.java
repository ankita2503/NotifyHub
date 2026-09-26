package com.portfolio.notifyhub.config;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class KafkaTopicConfig {

    public static final String NOTIFICATIONS_REQUESTED =
            "notifications.requested";

    @Bean
    public NewTopic notificationsRequestedTopic() {
        return new NewTopic(
                NOTIFICATIONS_REQUESTED,
                12,
                (short) 1
        );
    }
}

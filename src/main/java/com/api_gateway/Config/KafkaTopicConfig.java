package com.api_gateway.Config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {
    @Bean
    public NewTopic rateLimitAlertsTopic() {
        return TopicBuilder.name("rate-limit-alerts")
                .partitions(1)
                .replicas(1)
                .build();
    }
}

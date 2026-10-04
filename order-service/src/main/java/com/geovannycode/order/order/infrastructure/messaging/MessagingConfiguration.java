package com.geovannycode.order.order.infrastructure.messaging;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.config.TopicBuilder;

@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(OutboxProperties.class)
class MessagingConfiguration {

    /**
     * Only where Order owns a throwaway broker. In Kubernetes and beyond, topics are platform resources
     * (partitions, replication, retention) and the broker has auto-creation disabled.
     */
    @Bean
    @Profile({"local", "docker", "test"})
    NewTopic orderEventsTopic(OutboxProperties outbox) {
        // Three partitions so Notification can scale to three consumers; the key (order id) keeps per-order order.
        return TopicBuilder.name(outbox.topic()).partitions(3).replicas(1).build();
    }
}

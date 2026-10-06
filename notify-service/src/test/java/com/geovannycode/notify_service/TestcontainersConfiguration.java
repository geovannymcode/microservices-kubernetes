package com.geovannycode.notify_service;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.TopicBuilder;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.mongodb.MongoDBContainer;

// @ServiceConnection replaces spring.mongodb.* and spring.kafka.bootstrap-servers with the containers' addresses.
// Kafka too: every full context starts the order-events listener, which needs a real broker.
@TestConfiguration(proxyBeanMethods = false)
public final class TestcontainersConfiguration {

    @Bean
    @ServiceConnection
    MongoDBContainer mongoContainer() {
        // mongo:8.0.32, same digest as docker-compose.yaml. Testcontainers rejects "name:tag@digest", so no tag.
        return new MongoDBContainer("mongo@sha256:d0d926f94df099bff534b7ee5b5986458131a22489dfff8664509af0c1e2ca9c");
    }

    @Bean
    @ServiceConnection
    KafkaContainer kafkaContainer() {
        // apache/kafka:4.3.1 in KRaft mode, same digest as docker-compose.yaml.
        return new KafkaContainer("apache/kafka@sha256:77e3df9054047a88b520d0cc46e16696d3b22022e1d580aeccd2632df6532837");
    }

    // Created by Order in the real system; 3 partitions as there (and as the DLT).
    @Bean
    NewTopic orderEventsTopic() {
        return TopicBuilder.name(OrderEventsKafka.TOPIC).partitions(3).replicas(1).build();
    }
}

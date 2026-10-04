package com.geovannycode.notify_service.notify.infrastructure.messaging;

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.config.ContainerCustomizer;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;

/**
 * Error handling of the order-events consumer (ADR 0007). Boot's listener container factory picks up the single
 * CommonErrorHandler and ContainerCustomizer beans declared here.
 *
 * <ul>
 *   <li>deserialization errors (ErrorHandlingDeserializer) and invalid events: straight to the DLT, never retried;</li>
 *   <li>any other failure (sending, MongoDB, timeout): redelivered with exponential backoff, then the DLT.</li>
 * </ul>
 */
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(NotifyKafkaProperties.class)
class KafkaConsumerConfiguration {

    @Bean
    DefaultErrorHandler orderEventsErrorHandler(DeadLetterPublishingRecoverer deadLetters, NotifyKafkaProperties notify) {
        var retry = notify.retry();
        var backOff = new ExponentialBackOffWithMaxRetries(retry.maxAttempts() - 1);
        backOff.setInitialInterval(retry.initialInterval().toMillis());
        backOff.setMultiplier(retry.multiplier());
        backOff.setMaxInterval(retry.maxInterval().toMillis());
        var handler = new DefaultErrorHandler(deadLetters, backOff);
        // DeserializationException is already not retryable by default.
        handler.addNotRetryableExceptions(InvalidOrderEventException.class);
        return handler;
    }

    /**
     * Same partition in orders.events.v1.dlt, with the kafka_dlt-exception-* headers. A record that failed to
     * deserialize is republished with its original bytes; a deserialized one (invalid or undeliverable) as JSON.
     */
    @Bean
    DeadLetterPublishingRecoverer orderEventsDeadLetterRecoverer(KafkaProperties kafka, NotifyKafkaProperties notify) {
        Map<String, Object> producer = kafka.buildProducerProperties();
        var json = new JacksonJsonSerializer<Object>();
        json.setAddTypeInfo(false);
        Map<Class<?>, KafkaOperations<?, ?>> templates = new LinkedHashMap<>();
        templates.put(byte[].class, new KafkaTemplate<>(
                new DefaultKafkaProducerFactory<>(producer, new StringSerializer(), new ByteArraySerializer())));
        templates.put(Object.class, new KafkaTemplate<>(
                new DefaultKafkaProducerFactory<>(producer, new StringSerializer(), json)));
        String deadLetterTopic = notify.topics().orderEventsDlt();
        return new DeadLetterPublishingRecoverer(templates,
                (record, error) -> new TopicPartition(deadLetterTopic, record.partition()));
    }

    // Exposes the delivery attempt (1, 2, 3...) to the listener, which marks the notification FAILED on the last one.
    @Bean
    ContainerCustomizer<Object, Object, ConcurrentMessageListenerContainer<Object, Object>> deliveryAttemptHeader() {
        return container -> container.getContainerProperties().setDeliveryAttemptHeader(true);
    }

    /** Only where the broker is ours (local, docker, test); elsewhere topics are platform resources. */
    @Bean
    @Profile({"local", "docker", "test"})
    NewTopic orderEventsDeadLetterTopic(NotifyKafkaProperties notify) {
        // As many partitions as orders.events.v1 (3): the DLT keeps each record's partition.
        return TopicBuilder.name(notify.topics().orderEventsDlt()).partitions(3).replicas(1).build();
    }
}

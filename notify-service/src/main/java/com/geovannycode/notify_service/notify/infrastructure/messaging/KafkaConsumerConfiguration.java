package com.geovannycode.notify_service.notify.infrastructure.messaging;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import com.geovannycode.notify_service.notify.domain.NotificationDeliveryException;
import com.geovannycode.notify_service.notify.domain.NotificationRejectedException;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.kafka.config.ContainerCustomizer;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;
import org.springframework.util.backoff.ExponentialBackOff;

/**
 * Error handling of the order-events consumer (ADR 0007). Boot's listener container factory picks up the single
 * CommonErrorHandler and ContainerCustomizer beans declared here.
 *
 * <ul>
 *   <li>deserialization errors (ErrorHandlingDeserializer), invalid events and notifications the receiver rejected
 *       (NotificationRejectedException): straight to the DLT, never retried;</li>
 *   <li>MongoDB down or not answering (processing timeout): redelivered with the same backoff and no attempt limit.
 *       The event is fine, so it waits for the storage instead of going to the DLT;</li>
 *   <li>any other failure (mainly the channel): redelivered with exponential backoff, then the DLT.</li>
 * </ul>
 */
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(NotifyKafkaProperties.class)
class KafkaConsumerConfiguration {

    @Bean
    DefaultErrorHandler orderEventsErrorHandler(DeadLetterPublishingRecoverer deadLetters, NotifyKafkaProperties notify,
                                                MeterRegistry meterRegistry) {
        var retry = notify.retry();
        var backOff = new ExponentialBackOffWithMaxRetries(retry.maxAttempts() - 1);
        backOff.setInitialInterval(retry.initialInterval().toMillis());
        backOff.setMultiplier(retry.multiplier());
        backOff.setMaxInterval(retry.maxInterval().toMillis());
        var handler = new DefaultErrorHandler(countingDeadLetters(deadLetters, meterRegistry), backOff);
        // Unbounded: the partition waits (keeping its order) and the consumer keeps polling, so the group does not
        // evict it. Each attempt is bounded by processing-timeout plus at most max-interval of backoff.
        handler.setBackOffFunction((record, error) -> storageUnavailable(error) ? untilAvailable(retry) : null);
        // A different exception starts a new count: a channel failure after an outage gets its own 3 attempts.
        handler.setResetStateOnExceptionChange(true);
        // DeserializationException is already not retryable by default.
        // Matched along the cause chain (the listener's exception arrives wrapped in ListenerExecutionFailedException).
        handler.addNotRetryableExceptions(InvalidOrderEventException.class, NotificationRejectedException.class);
        return handler;
    }

    /**
     * notifications.dlt (reason = invalid | rejected | exhausted), counted only once the record is in the DLT: the
     * recoverer waits for the send and throws if it fails, and then the record is retried, not counted.
     */
    private static ConsumerRecordRecoverer countingDeadLetters(DeadLetterPublishingRecoverer deadLetters,
                                                               MeterRegistry meterRegistry) {
        // Registered up front so the dashboard shows 0 instead of "no data" until the first record arrives.
        Map<String, Counter> counters = new LinkedHashMap<>();
        for (String reason : new String[]{"invalid", "rejected", "exhausted"}) {
            counters.put(reason, Counter.builder("notifications.dlt")
                    .description("Order events published to the dead-letter topic")
                    .tag("reason", reason).register(meterRegistry));
        }
        return (record, error) -> {
            deadLetters.accept(record, error);
            counters.get(deadLetterReason(error)).increment();
        };
    }

    static String deadLetterReason(Throwable error) {
        for (Throwable cause = error; cause != null; cause = cause.getCause()) {
            if (cause instanceof DeserializationException || cause instanceof InvalidOrderEventException) {
                return "invalid";
            }
            if (cause instanceof NotificationRejectedException) {
                return "rejected";
            }
        }
        return "exhausted";
    }

    /**
     * MongoDB unreachable (resource failure) or not answering (the listener's processing timeout). A channel
     * failure is never storage, even if its cause is a timeout: the channel decides its own outcome.
     */
    static boolean storageUnavailable(Throwable error) {
        for (Throwable cause = error; cause != null; cause = cause.getCause()) {
            if (cause instanceof NotificationDeliveryException) {
                return false;
            }
            if (cause instanceof DataAccessResourceFailureException || cause instanceof TimeoutException) {
                return true;
            }
        }
        return false;
    }

    private static ExponentialBackOff untilAvailable(NotifyKafkaProperties.Retry retry) {
        var backOff = new ExponentialBackOff(retry.initialInterval().toMillis(), retry.multiplier());
        backOff.setMaxInterval(retry.maxInterval().toMillis());
        return backOff;
    }

    /**
     * Same partition in orders.events.v1.dlt, with the kafka_dlt-exception-* headers. A record that failed to
     * deserialize is republished with its original bytes; a deserialized one (invalid or undeliverable) as JSON.
     */
    @Bean
    DeadLetterPublishingRecoverer orderEventsDeadLetterRecoverer(ProducerFactory<?, ?> bootProducerFactory,
                                                                 NotifyKafkaProperties notify) {
        // Boot's factory, not KafkaProperties: its configuration already includes the connection details
        // (bootstrap servers from spring.kafka.*, a @ServiceConnection or a cloud binding).
        Map<String, Object> producer = bootProducerFactory.getConfigurationProperties();
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
    ContainerCustomizer<Object, Object, ConcurrentMessageListenerContainer<Object, Object>> deliveryAttemptHeader(
            @Value("${notify.shutdown-timeout:10s}") Duration shutdownTimeout) {
        return container -> {
            container.getContainerProperties().setDeliveryAttemptHeader(true);
            // k8s allows the bounded in-flight processing and offset commit to finish before termination.
            container.getContainerProperties().setShutdownTimeout(shutdownTimeout.toMillis());
        };
    }

    /** Only where the broker is ours (local, docker, test); elsewhere topics are platform resources. */
    @Bean
    @Profile({"local", "docker", "test"})
    NewTopic orderEventsDeadLetterTopic(NotifyKafkaProperties notify) {
        // As many partitions as orders.events.v1 (3): the DLT keeps each record's partition.
        return TopicBuilder.name(notify.topics().orderEventsDlt()).partitions(3).replicas(1).build();
    }
}

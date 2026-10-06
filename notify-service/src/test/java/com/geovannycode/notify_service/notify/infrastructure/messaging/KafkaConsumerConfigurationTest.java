package com.geovannycode.notify_service.notify.infrastructure.messaging;

import java.util.concurrent.TimeoutException;

import com.geovannycode.notify_service.notify.domain.NotificationDeliveryException;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataAccessResourceFailureException;
import com.geovannycode.notify_service.notify.domain.NotificationRejectedException;
import org.springframework.kafka.listener.ListenerExecutionFailedException;
import org.springframework.kafka.support.serializer.DeserializationException;

import static org.assertj.core.api.Assertions.assertThat;

// Which failures wait for MongoDB without limit (never to the DLT) and which use the normal 3 attempts.
final class KafkaConsumerConfigurationTest {

    @Test
    void processingTimeoutIsStorageUnavailable() {
        // What block(Duration) throws, wrapped as the container hands it to the error handler.
        var timeout = new IllegalStateException("Timeout on blocking read", new TimeoutException());
        assertThat(KafkaConsumerConfiguration.storageUnavailable(new ListenerExecutionFailedException("x", timeout))).isTrue();
    }

    @Test
    void mongoResourceFailureIsStorageUnavailable() {
        var down = new DataAccessResourceFailureException("Timed out while waiting for a server");
        assertThat(KafkaConsumerConfiguration.storageUnavailable(new ListenerExecutionFailedException("x", down))).isTrue();
    }

    @Test
    void channelFailureIsNotStorageEvenWithATimeoutCause() {
        var channel = new NotificationDeliveryException("e1", new TimeoutException("webhook lento"));
        assertThat(KafkaConsumerConfiguration.storageUnavailable(new ListenerExecutionFailedException("x", channel))).isFalse();
    }

    @Test
    void anyOtherFailureIsNotStorage() {
        assertThat(KafkaConsumerConfiguration.storageUnavailable(new IllegalArgumentException("x"))).isFalse();
    }

    @Test
    void deadLetterReasonTellsInvalidRejectedAndExhaustedApart() {
        assertThat(KafkaConsumerConfiguration.deadLetterReason(
                new DeserializationException("json", new byte[0], false, new IllegalStateException()))).isEqualTo("invalid");
        assertThat(KafkaConsumerConfiguration.deadLetterReason(
                new ListenerExecutionFailedException("x", new InvalidOrderEventException("sin codeProduct")))).isEqualTo("invalid");
        assertThat(KafkaConsumerConfiguration.deadLetterReason(
                new ListenerExecutionFailedException("x", new NotificationRejectedException("e1", "HTTP 400")))).isEqualTo("rejected");
        assertThat(KafkaConsumerConfiguration.deadLetterReason(
                new ListenerExecutionFailedException("x", new NotificationDeliveryException("e1", "HTTP 500")))).isEqualTo("exhausted");
    }

    @Test
    void appliesShutdownBudgetWithoutDisablingDeliveryAttemptHeaders() {
        org.springframework.kafka.listener.ConcurrentMessageListenerContainer<Object, Object> container = org.mockito.Mockito.mock();
        var properties = new org.springframework.kafka.listener.ContainerProperties("orders.events.v1");
        org.mockito.Mockito.when(container.getContainerProperties()).thenReturn(properties);
        new KafkaConsumerConfiguration().deliveryAttemptHeader(java.time.Duration.ofSeconds(40)).configure(container);
        assertThat(properties.getShutdownTimeout()).isEqualTo(40_000);
        assertThat(properties.isDeliveryAttemptHeader()).isTrue();
    }
}

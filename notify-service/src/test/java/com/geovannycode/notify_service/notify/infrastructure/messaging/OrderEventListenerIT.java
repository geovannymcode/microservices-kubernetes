package com.geovannycode.notify_service.notify.infrastructure.messaging;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

import com.geovannycode.notify_service.OrderEventsKafka;
import com.geovannycode.notify_service.TestcontainersConfiguration;
import com.geovannycode.notify_service.notify.domain.NotificationSender;
import com.geovannycode.notify_service.notify.domain.NotifyStatus;
import com.geovannycode.notify_service.notify.infrastructure.persistence.NotificationDocument;
import com.geovannycode.notify_service.notify.infrastructure.persistence.NotificationRepository;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.bean.override.convention.TestBean;
import org.testcontainers.kafka.KafkaContainer;

import static com.geovannycode.notify_service.OrderEventsKafka.event;
import static com.geovannycode.notify_service.OrderEventsKafka.header;
import static com.geovannycode.notify_service.OrderEventsKafka.newOrderId;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

// The real listener, error handler and DLT against Kafka and MongoDB (Testcontainers). A scripted channel replaces
// the log sender (@TestBean: still a single NotificationSender) so failures can be injected per event.
@SpringBootTest
@Import(TestcontainersConfiguration.class)
@ActiveProfiles("test")
final class OrderEventListenerIT {

    private static final Duration TIMEOUT = Duration.ofSeconds(30);
    private static final ScriptedSender SENDER = new ScriptedSender();

    @TestBean
    private NotificationSender notificationSender;

    @Autowired private KafkaContainer kafka;
    @Autowired private NotificationRepository repository;
    private OrderEventsKafka orders;

    static NotificationSender notificationSender() {
        return SENDER;
    }

    @BeforeEach
    void connect() {
        repository.deleteAll().block(TIMEOUT);
        orders = new OrderEventsKafka(kafka.getBootstrapServers());
    }

    @AfterEach
    void disconnect() {
        orders.close();
    }

    @Test
    void completedEventCreatesOneSentNotificationWithTheMessage() {
        long orderId = newOrderId();
        String eventId = orders.publish(orderId, event(UUID.randomUUID().toString(), "OrderCompleted", orderId, "completed", null));

        var notification = awaitStatus(eventId, NotifyStatus.SENT);
        assertThat(notification.message()).isEqualTo("La orden " + orderId + " (AC-1550 x2) fue completada.");
        assertThat(notification.channel()).isEqualTo("test");
        assertThat(notification.attempts()).isEqualTo(1);
        assertThat(notification.sentAt()).isNotNull();
        assertThat(SENDER.deliveries(eventId)).isEqualTo(1);
    }

    @Test
    void canceledEventMessageCarriesTheReason() {
        long orderId = newOrderId();
        String eventId = orders.publish(orderId, event(UUID.randomUUID().toString(), "OrderCanceled", orderId, "canceled",
                "PRODUCT_NOT_FOUND"));

        var notification = awaitStatus(eventId, NotifyStatus.SENT);
        assertThat(notification.message())
                .isEqualTo("La orden " + orderId + " (AC-1550 x2) fue cancelada: el producto no existe.");
        assertThat(notification.cancelReason()).isEqualTo("PRODUCT_NOT_FOUND");
    }

    @Test
    void sameEventPublishedTwiceCreatesOneNotification() {
        long orderId = newOrderId();
        String eventId = UUID.randomUUID().toString();
        orders.publish(orderId, event(eventId, "OrderCompleted", orderId, "completed", null));
        orders.publish(orderId, event(eventId, "OrderCompleted", orderId, "completed", null));
        // A marker on the same key (same partition, processed in order) proves both copies were consumed.
        String marker = orders.publish(orderId, event(UUID.randomUUID().toString(), "OrderCompleted", orderId, "completed", null));
        awaitStatus(marker, NotifyStatus.SENT);

        assertThat(notificationsOf(orderId)).filteredOn(n -> n.eventId().equals(eventId)).hasSize(1);
        assertThat(SENDER.deliveries(eventId)).isEqualTo(1);
    }

    @Test
    void invalidJsonGoesToTheDltAndTheConsumerKeepsGoing() {
        long orderId = newOrderId();
        String garbage = "{esto no es json " + orderId;
        orders.send(orderId, garbage, Map.of());
        String next = orders.publish(orderId, event(UUID.randomUUID().toString(), "OrderCompleted", orderId, "completed", null));

        awaitStatus(next, NotifyStatus.SENT);
        var dead = orders.awaitDeadLetter(record -> garbage.equals(record.value()));
        assertThat(header(dead, "kafka_dlt-exception-fqcn")).contains("DeserializationException");
    }

    @Test
    void eventMissingRequiredFieldsGoesToTheDltWithoutRetries() {
        long orderId = newOrderId();
        String eventId = UUID.randomUUID().toString();
        String withoutCode = event(eventId, "OrderCompleted", orderId, "completed", null).replace("\"codeProduct\":\"AC-1550\",", "");
        orders.send(orderId, withoutCode, Map.of("eventId", eventId));

        var dead = orders.awaitDeadLetter(record -> record.value() != null && record.value().contains(eventId));
        assertThat(header(dead, "kafka_dlt-exception-message")).contains("data.codeProduct");
        assertThat(repository.findByEventId(eventId).blockOptional(TIMEOUT)).isEmpty();
        assertThat(SENDER.deliveries(eventId)).isZero();
    }

    @Test
    void unknownEventTypeIsIgnoredAndNotSentToTheDlt() {
        long orderId = newOrderId();
        String unknown = orders.publish(orderId, event(UUID.randomUUID().toString(), "OrderShipped", orderId, "shipped", null));
        String marker = orders.publish(orderId, event(UUID.randomUUID().toString(), "OrderCompleted", orderId, "completed", null));
        awaitStatus(marker, NotifyStatus.SENT);

        assertThat(repository.findByEventId(unknown).blockOptional(TIMEOUT)).isEmpty();
        assertThat(orders.deadLetters()).noneMatch(record -> record.value() != null && record.value().contains(unknown));
    }

    @Test
    void senderThatAlwaysFailsEndsFailedAfterThreeAttemptsAndInTheDlt() {
        long orderId = newOrderId();
        String eventId = UUID.randomUUID().toString();
        SENDER.failAlways(eventId);
        orders.publish(orderId, event(eventId, "OrderCompleted", orderId, "completed", null));

        var failed = awaitStatus(eventId, NotifyStatus.FAILED);
        assertThat(failed.attempts()).isEqualTo(3);
        assertThat(failed.sentAt()).isNull();
        var dead = orders.awaitDeadLetter(record -> record.value() != null && record.value().contains(eventId));
        assertThat(header(dead, "kafka_dlt-exception-fqcn")).contains("ListenerExecutionFailedException");
        assertThat(header(dead, "kafka_dlt-exception-cause-fqcn")).contains("NotificationDeliveryException");
        assertThat(SENDER.deliveries(eventId)).isEqualTo(3);
    }

    @Test
    void rejectedNotificationEndsFailedAtOnceAndInTheDlt() {
        long orderId = newOrderId();
        String eventId = UUID.randomUUID().toString();
        SENDER.reject(eventId);
        orders.publish(orderId, event(eventId, "OrderCompleted", orderId, "completed", null));

        var failed = awaitStatus(eventId, NotifyStatus.FAILED);
        assertThat(failed.attempts()).isEqualTo(1);
        var dead = orders.awaitDeadLetter(record -> record.value() != null && record.value().contains(eventId));
        assertThat(header(dead, "kafka_dlt-exception-cause-fqcn")).contains("NotificationRejectedException");
        assertThat(SENDER.deliveries(eventId)).isEqualTo(1);
    }

    @Test
    void senderThatFailsOnceIsSentOnTheRetry() {
        long orderId = newOrderId();
        String eventId = UUID.randomUUID().toString();
        SENDER.failTimes(eventId, 1);
        orders.publish(orderId, event(eventId, "OrderCompleted", orderId, "completed", null));

        var sent = awaitStatus(eventId, NotifyStatus.SENT);
        assertThat(sent.attempts()).isEqualTo(2);
        assertThat(notificationsOf(orderId)).hasSize(1);
        assertThat(SENDER.deliveries(eventId)).isEqualTo(2);
    }

    // --- helpers -------------------------------------------------------------------------------------------------

    private NotificationDocument awaitStatus(String eventId, NotifyStatus status) {
        return await().atMost(TIMEOUT).until(() -> repository.findByEventId(eventId).block(TIMEOUT),
                notification -> notification != null && notification.status() == status);
    }

    private List<NotificationDocument> notificationsOf(long orderId) {
        return Objects.requireNonNull(repository.findAll().filter(n -> n.orderId() == orderId).collectList().block(TIMEOUT));
    }
}

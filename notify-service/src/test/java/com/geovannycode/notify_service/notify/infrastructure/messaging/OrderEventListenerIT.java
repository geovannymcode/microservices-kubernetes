package com.geovannycode.notify_service.notify.infrastructure.messaging;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

import com.geovannycode.notify_service.TestcontainersConfiguration;
import com.geovannycode.notify_service.notify.domain.NotificationMessage;
import com.geovannycode.notify_service.notify.domain.NotificationSender;
import com.geovannycode.notify_service.notify.domain.NotifyStatus;
import com.geovannycode.notify_service.notify.infrastructure.persistence.NotificationDocument;
import com.geovannycode.notify_service.notify.infrastructure.persistence.NotificationRepository;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.test.context.ActiveProfiles;
import org.testcontainers.kafka.KafkaContainer;
import reactor.core.publisher.Mono;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

// The real listener, error handler and DLT against Kafka and MongoDB (Testcontainers). A scripted channel replaces
// the log sender (notification.sender.type=test disables it) so failures can be injected per event.
@SpringBootTest(properties = "notification.sender.type=test")
@Import({TestcontainersConfiguration.class, OrderEventListenerIT.ScriptedChannel.class})
@ActiveProfiles("test")
final class OrderEventListenerIT {

    private static final String TOPIC = "orders.events.v1";
    private static final String DLT = "orders.events.v1.dlt";
    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    @Autowired private KafkaContainer kafka;
    @Autowired private NotificationRepository repository;
    @Autowired private ScriptedSender sender;
    private KafkaProducer<String, String> producer;

    @BeforeEach
    void createProducer() {
        var settings = new Properties();
        settings.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        producer = new KafkaProducer<>(settings, new StringSerializer(), new StringSerializer());
    }

    @AfterEach
    void closeProducer() {
        producer.close();
    }

    @Test
    void completedEventCreatesOneSentNotificationWithTheMessage() {
        long orderId = newOrderId();
        String eventId = publish(orderId, event(UUID.randomUUID().toString(), "OrderCompleted", orderId, "completed", null));

        var notification = awaitStatus(eventId, NotifyStatus.SENT);
        assertThat(notification.message()).isEqualTo("La orden " + orderId + " (AC-1550 x2) fue completada.");
        assertThat(notification.channel()).isEqualTo("test");
        assertThat(notification.attempts()).isEqualTo(1);
        assertThat(notification.sentAt()).isNotNull();
        assertThat(sender.deliveries(eventId)).isEqualTo(1);
    }

    @Test
    void canceledEventMessageCarriesTheReason() {
        long orderId = newOrderId();
        String eventId = publish(orderId, event(UUID.randomUUID().toString(), "OrderCanceled", orderId, "canceled",
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
        publish(orderId, event(eventId, "OrderCompleted", orderId, "completed", null));
        publish(orderId, event(eventId, "OrderCompleted", orderId, "completed", null));
        // A marker on the same key (same partition, processed in order) proves both copies were consumed.
        String marker = publish(orderId, event(UUID.randomUUID().toString(), "OrderCompleted", orderId, "completed", null));
        awaitStatus(marker, NotifyStatus.SENT);

        assertThat(notificationsOf(orderId)).filteredOn(n -> n.eventId().equals(eventId)).hasSize(1);
        assertThat(sender.deliveries(eventId)).isEqualTo(1);
    }

    @Test
    void invalidJsonGoesToTheDltAndTheConsumerKeepsGoing() {
        long orderId = newOrderId();
        String garbage = "{esto no es json " + orderId;
        send(orderId, garbage, Map.of());
        String next = publish(orderId, event(UUID.randomUUID().toString(), "OrderCompleted", orderId, "completed", null));

        awaitStatus(next, NotifyStatus.SENT);
        var dead = awaitDeadLetter(record -> garbage.equals(record.value()));
        assertThat(header(dead, "kafka_dlt-exception-fqcn")).contains("DeserializationException");
    }

    @Test
    void eventMissingRequiredFieldsGoesToTheDltWithoutRetries() {
        long orderId = newOrderId();
        String eventId = UUID.randomUUID().toString();
        String withoutCode = event(eventId, "OrderCompleted", orderId, "completed", null).replace("\"codeProduct\":\"AC-1550\",", "");
        send(orderId, withoutCode, Map.of("eventId", eventId));

        var dead = awaitDeadLetter(record -> record.value() != null && record.value().contains(eventId));
        assertThat(header(dead, "kafka_dlt-exception-message")).contains("data.codeProduct");
        assertThat(repository.findByEventId(eventId).blockOptional(TIMEOUT)).isEmpty();
        assertThat(sender.deliveries(eventId)).isZero();
    }

    @Test
    void unknownEventTypeIsIgnoredAndNotSentToTheDlt() {
        long orderId = newOrderId();
        String unknown = publish(orderId, event(UUID.randomUUID().toString(), "OrderShipped", orderId, "shipped", null));
        String marker = publish(orderId, event(UUID.randomUUID().toString(), "OrderCompleted", orderId, "completed", null));
        awaitStatus(marker, NotifyStatus.SENT);

        assertThat(repository.findByEventId(unknown).blockOptional(TIMEOUT)).isEmpty();
        assertThat(deadLetters()).noneMatch(record -> record.value() != null && record.value().contains(unknown));
    }

    @Test
    void senderThatAlwaysFailsEndsFailedAfterThreeAttemptsAndInTheDlt() {
        long orderId = newOrderId();
        String eventId = UUID.randomUUID().toString();
        sender.failAlways(eventId);
        publish(orderId, event(eventId, "OrderCompleted", orderId, "completed", null));

        var failed = awaitStatus(eventId, NotifyStatus.FAILED);
        assertThat(failed.attempts()).isEqualTo(3);
        assertThat(failed.sentAt()).isNull();
        var dead = awaitDeadLetter(record -> record.value() != null && record.value().contains(eventId));
        assertThat(header(dead, "kafka_dlt-exception-fqcn")).contains("ListenerExecutionFailedException");
        assertThat(header(dead, "kafka_dlt-exception-cause-fqcn")).contains("NotificationDeliveryException");
        assertThat(sender.deliveries(eventId)).isEqualTo(3);
    }

    @Test
    void senderThatFailsOnceIsSentOnTheRetry() {
        long orderId = newOrderId();
        String eventId = UUID.randomUUID().toString();
        sender.failTimes(eventId, 1);
        publish(orderId, event(eventId, "OrderCompleted", orderId, "completed", null));

        var sent = awaitStatus(eventId, NotifyStatus.SENT);
        assertThat(sent.attempts()).isEqualTo(2);
        assertThat(notificationsOf(orderId)).hasSize(1);
        assertThat(sender.deliveries(eventId)).isEqualTo(2);
    }

    // --- helpers -------------------------------------------------------------------------------------------------

    private static long newOrderId() {
        return ThreadLocalRandom.current().nextLong(1_000_000, Long.MAX_VALUE / 2);
    }

    /** The JSON Order publishes, plus a field this consumer does not know (compatible additions are ignored). */
    private static String event(String eventId, String eventType, long orderId, String status, String cancelReason) {
        return "{\"eventId\":\"" + eventId + "\",\"eventType\":\"" + eventType + "\",\"occurredAt\":\""
                + Instant.now() + "\",\"version\":1,\"futureField\":true,\"data\":{\"orderId\":" + orderId
                + ",\"codeProduct\":\"AC-1550\",\"quantity\":2,\"status\":\"" + status + "\""
                + (cancelReason == null ? "" : ",\"cancelReason\":\"" + cancelReason + "\"") + "}}";
    }

    private String publish(long orderId, String json) {
        String eventId = json.substring(json.indexOf("\"eventId\":\"") + 11, json.indexOf("\"", json.indexOf("\"eventId\":\"") + 11));
        send(orderId, json, Map.of("eventId", eventId));
        return eventId;
    }

    private void send(long orderId, String value, Map<String, String> headers) {
        var record = new ProducerRecord<>(TOPIC, String.valueOf(orderId), value);
        headers.forEach((name, headerValue) -> record.headers().add(name, headerValue.getBytes(StandardCharsets.UTF_8)));
        try {
            producer.send(record).get();
        } catch (Exception error) {
            throw new IllegalStateException(error);
        }
    }

    private NotificationDocument awaitStatus(String eventId, NotifyStatus status) {
        return await().atMost(TIMEOUT).until(() -> repository.findByEventId(eventId).block(TIMEOUT),
                notification -> notification != null && notification.status() == status);
    }

    private List<NotificationDocument> notificationsOf(long orderId) {
        return Objects.requireNonNull(repository.findAll().filter(n -> n.orderId() == orderId).collectList().block(TIMEOUT));
    }

    private ConsumerRecord<String, String> awaitDeadLetter(java.util.function.Predicate<ConsumerRecord<String, String>> match) {
        return await().atMost(TIMEOUT).until(() -> deadLetters().stream().filter(match).findFirst(),
                java.util.Optional::isPresent).orElseThrow();
    }

    /** Every record currently in the DLT, read from the beginning up to its end offsets. */
    private List<ConsumerRecord<String, String>> deadLetters() {
        var settings = new Properties();
        settings.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        settings.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        try (var consumer = new KafkaConsumer<>(settings, new StringDeserializer(), new StringDeserializer())) {
            var partitions = consumer.partitionsFor(DLT).stream()
                    .map(info -> new TopicPartition(info.topic(), info.partition())).toList();
            consumer.assign(partitions);
            consumer.seekToBeginning(partitions);
            var end = consumer.endOffsets(partitions);
            var found = new ArrayList<ConsumerRecord<String, String>>();
            var deadline = Instant.now().plus(TIMEOUT);
            while (partitions.stream().anyMatch(partition -> consumer.position(partition) < end.get(partition))) {
                assertThat(Instant.now()).as("lectura de %s", DLT).isBefore(deadline);
                consumer.poll(Duration.ofMillis(200)).forEach(found::add);
            }
            return found;
        }
    }

    private static String header(ConsumerRecord<String, String> record, String name) {
        var header = record.headers().lastHeader(name);
        return header == null ? "" : new String(header.value(), StandardCharsets.UTF_8);
    }

    @TestConfiguration(proxyBeanMethods = false)
    static class ScriptedChannel {

        @Bean
        ScriptedSender scriptedSender() {
            return new ScriptedSender();
        }

        // Created by Order in the real system; 3 partitions as there (and as the DLT).
        @Bean
        NewTopic orderEventsTopic() {
            return TopicBuilder.name(TOPIC).partitions(3).replicas(1).build();
        }
    }

    /** Channel whose failures are scripted per eventId; counts every delivery attempt. */
    static final class ScriptedSender implements NotificationSender {

        private final Map<String, AtomicInteger> attempts = new ConcurrentHashMap<>();
        private final Map<String, AtomicInteger> failuresLeft = new ConcurrentHashMap<>();
        private final Set<String> alwaysFailing = ConcurrentHashMap.newKeySet();

        void failAlways(String eventId) {
            alwaysFailing.add(eventId);
        }

        void failTimes(String eventId, int times) {
            failuresLeft.put(eventId, new AtomicInteger(times));
        }

        int deliveries(String eventId) {
            var count = attempts.get(eventId);
            return count == null ? 0 : count.get();
        }

        @Override
        public Mono<Void> send(NotificationMessage message) {
            return Mono.defer(() -> {
                attempts.computeIfAbsent(message.eventId(), id -> new AtomicInteger()).incrementAndGet();
                if (alwaysFailing.contains(message.eventId()) || consumeScriptedFailure(message.eventId())) {
                    return Mono.error(new IllegalStateException("canal de prueba caído"));
                }
                return Mono.empty();
            });
        }

        private boolean consumeScriptedFailure(String eventId) {
            var left = failuresLeft.get(eventId);
            return left != null && left.getAndUpdate(remaining -> Math.max(remaining - 1, 0)) > 0;
        }

        @Override
        public String channel() {
            return "test";
        }
    }
}

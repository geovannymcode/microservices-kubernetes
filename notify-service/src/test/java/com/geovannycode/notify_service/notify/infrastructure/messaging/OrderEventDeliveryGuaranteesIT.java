package com.geovannycode.notify_service.notify.infrastructure.messaging;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.geovannycode.notify_service.OrderEventsKafka;
import com.geovannycode.notify_service.TestcontainersConfiguration;
import com.geovannycode.notify_service.notify.domain.NotificationSender;
import com.geovannycode.notify_service.notify.domain.NotifyStatus;
import com.geovannycode.notify_service.notify.infrastructure.persistence.NotificationDocument;
import com.geovannycode.notify_service.notify.infrastructure.persistence.NotificationRepository;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.listener.RetryListener;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.bean.override.convention.TestBean;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.mongodb.MongoDBContainer;

import static com.geovannycode.notify_service.OrderEventsKafka.event;
import static com.geovannycode.notify_service.OrderEventsKafka.newOrderId;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Delivery guarantees of the consumer under ordering, concurrency, rebalancing and a MongoDB outage, against real
 * Kafka and MongoDB. Own consumer group and a 3s processing timeout (short, so an outage exceeds the 3 normal
 * attempts quickly, but room for the concurrency test's hold); its own context, so its own containers.
 */
@SpringBootTest(properties = {"spring.kafka.consumer.group-id=notify-delivery-guarantees-it",
        "notify.processing-timeout=3s"})
@Import(TestcontainersConfiguration.class)
@ActiveProfiles("test")
final class OrderEventDeliveryGuaranteesIT {

    private static final Duration TIMEOUT = Duration.ofSeconds(60);
    private static final ScriptedSender SENDER = new ScriptedSender();

    @TestBean
    private NotificationSender notificationSender;

    @Autowired private KafkaContainer kafka;
    @Autowired private MongoDBContainer mongo;
    @Autowired private NotificationRepository repository;
    @Autowired private KafkaListenerEndpointRegistry listeners;
    @Autowired private DefaultErrorHandler errorHandler;
    @Autowired private NotifyKafkaProperties notify;
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
    void eventsOfOneOrderAreProcessedInPublicationOrder() {
        long orderId = newOrderId();
        List<String> eventIds = IntStream.range(0, 5).mapToObj(i -> UUID.randomUUID().toString()).toList();
        // The first one is the slowest: if the partition were processed in parallel, the others would overtake it.
        SENDER.delay(eventIds.getFirst(), Duration.ofMillis(500));
        eventIds.forEach(eventId -> orders.publish(orderId, event(eventId, "OrderCompleted", orderId, "completed", null)));

        eventIds.forEach(eventId -> awaitStatus(eventId, NotifyStatus.SENT));

        assertThat(SENDER.deliveryOrder()).filteredOn(eventIds::contains).containsExactlyElementsOf(eventIds);
    }

    @Test
    void sameEventTenTimesOnThreeConsumerThreadsCreatesOneNotification() {
        long orderId = newOrderId();
        String eventId = UUID.randomUUID().toString();
        String json = event(eventId, "OrderCompleted", orderId, "completed", null);
        // The first copy stays PENDING in the channel until a second thread reaches the channel with another copy:
        // without it the first one is SENT before any other thread reads it, and the race (two threads sending the
        // same PENDING notification) depends on timing. Below the 3s processing timeout.
        SENDER.holdUntilConcurrent(eventId, Duration.ofMillis(2_500));
        // Spread over the 3 partitions, so the 3 consumer threads race on the same eventId.
        IntStream.range(0, 10).forEach(copy -> orders.publishToPartition(copy % 3, orderId, json));
        // One marker per partition, after the copies: once they are sent, every copy was consumed.
        List<String> markers = IntStream.range(0, 3)
                .mapToObj(partition -> orders.publishToPartition(partition, orderId,
                        event(UUID.randomUUID().toString(), "OrderCompleted", orderId, "completed", null)))
                .toList();
        markers.forEach(marker -> awaitStatus(marker, NotifyStatus.SENT));

        assertThat(notificationsOf(Set.of(eventId))).singleElement()
                .satisfies(notification -> assertThat(notification.status()).isEqualTo(NotifyStatus.SENT));
        // The race did happen: more than one thread reached the channel. One notification, but the channel may
        // receive it more than once (at least once), which is why the webhook sends Idempotency-Key.
        assertThat(SENDER.deliveries(eventId)).as("envíos al canal del evento repetido").isGreaterThan(1);
        assertThat(orders.deadLetters()).noneMatch(record -> record.value() != null && record.value().contains(eventId));
    }

    @Test
    void listenerRestartedInTheMiddleOfFiftyEventsLosesAndDuplicatesNothing() {
        List<String> eventIds = IntStream.range(0, 50).mapToObj(i -> UUID.randomUUID().toString()).toList();
        // ~100ms per event over 3 threads: the batch takes well over a second, so the stop lands in the middle.
        eventIds.forEach(eventId -> SENDER.delay(eventId, Duration.ofMillis(100)));
        eventIds.forEach(eventId -> {
            long orderId = newOrderId();
            orders.publish(orderId, event(eventId, "OrderCompleted", orderId, "completed", null));
        });
        Set<String> batch = Set.copyOf(eventIds);
        await().atMost(TIMEOUT).until(() -> sent(batch) >= 10);

        MessageListenerContainer container = Objects.requireNonNull(listeners.getListenerContainer("order-events"));
        container.stop();
        try {
            assertThat(container.isRunning()).isFalse();
            assertThat(sent(batch)).as("la parada llega a mitad del lote").isLessThan(50);
        } finally {
            container.start();
        }

        await().atMost(TIMEOUT).until(() -> sent(batch) == 50);
        var notifications = notificationsOf(batch);
        assertThat(notifications).hasSize(50);
        assertThat(notifications).extracting(NotificationDocument::eventId).doesNotHaveDuplicates();
    }

    @Test
    void mongoUnavailableWhileConsumingDelaysTheEventButNeverLosesIt() {
        Map<String, Integer> failedAttempts = new ConcurrentHashMap<>();
        Map<String, Exception> failures = new ConcurrentHashMap<>();
        errorHandler.setRetryListeners(new RetryListener() {
            @Override
            public void failedDelivery(ConsumerRecord<?, ?> record, Exception error, int deliveryAttempt) {
                failedAttempts.merge(String.valueOf(record.key()), deliveryAttempt, Math::max);
                failures.put(String.valueOf(record.key()), error);
            }
        });
        long orderId = newOrderId();
        String eventId = UUID.randomUUID().toString();

        var docker = mongo.getDockerClient();
        docker.pauseContainerCmd(mongo.getContainerId()).exec();
        try {
            orders.publish(orderId, event(eventId, "OrderCompleted", orderId, "completed", null));
            // More failed deliveries than the normal limit: an ordinary failure would be in the DLT by now.
            await().atMost(TIMEOUT).until(() -> failedAttempts.getOrDefault(String.valueOf(orderId), 0)
                    > notify.retry().maxAttempts());
        } finally {
            docker.unpauseContainerCmd(mongo.getContainerId()).exec();
            errorHandler.setRetryListeners();
        }

        assertThat(KafkaConsumerConfiguration.storageUnavailable(failures.get(String.valueOf(orderId))))
                .as("el fallo se clasifica como almacenamiento no disponible").isTrue();
        var sent = awaitStatus(eventId, NotifyStatus.SENT);
        assertThat(sent.attempts()).isEqualTo(1);
        assertThat(notificationsOf(Set.of(eventId))).hasSize(1);
        assertThat(orders.deadLetters()).noneMatch(record -> record.value() != null && record.value().contains(eventId));
    }

    // --- helpers -------------------------------------------------------------------------------------------------

    private NotificationDocument awaitStatus(String eventId, NotifyStatus status) {
        return await().atMost(TIMEOUT).until(() -> repository.findByEventId(eventId).block(TIMEOUT),
                notification -> notification != null && notification.status() == status);
    }

    private List<NotificationDocument> notificationsOf(Set<String> eventIds) {
        return Objects.requireNonNull(repository.findAll().filter(n -> eventIds.contains(n.eventId()))
                .collectList().block(TIMEOUT));
    }

    private long sent(Set<String> eventIds) {
        return notificationsOf(eventIds).stream().filter(n -> n.status() == NotifyStatus.SENT)
                .collect(Collectors.counting());
    }
}

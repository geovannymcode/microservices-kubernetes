package com.geovannycode.order.order.infrastructure.messaging;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import com.geovannycode.order.TestcontainersConfiguration;
import com.geovannycode.order.generated.dto.OrderResponse;
import com.geovannycode.order.order.domain.OrderStatus;
import com.geovannycode.order.order.infrastructure.inventory.InventoryStubs;
import com.geovannycode.order.order.infrastructure.persistence.OrderEntity;
import com.geovannycode.order.order.infrastructure.persistence.OrderRepository;
import com.github.tomakehurst.wiremock.junit5.WireMockExtension;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import org.springframework.test.web.reactive.server.WebTestClient;
import org.springframework.transaction.reactive.TransactionalOperator;
import org.springframework.web.reactive.function.client.WebClient;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.kafka.KafkaContainer;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import tools.jackson.databind.json.JsonMapper;

import static com.geovannycode.order.order.infrastructure.inventory.InventoryStubs.decreasePath;
import static com.github.tomakehurst.wiremock.client.WireMock.put;
import static com.github.tomakehurst.wiremock.client.WireMock.putRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;

// Order with PostgreSQL and Kafka (Testcontainers); WireMock plays Inventory. The relay polls every 200ms.
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Import(TestcontainersConfiguration.class)
@ActiveProfiles("test")
final class OutboxIT {

    @RegisterExtension
    static final WireMockExtension INVENTORY = WireMockExtension.newInstance()
            .options(wireMockConfig().dynamicPort()).build();

    private static final String ORDERS = "/services-order/orders";
    private static final Duration TIMEOUT = Duration.ofSeconds(30);
    private static final String CODE = "AC-1550";

    @DynamicPropertySource
    static void inventoryBaseUrl(DynamicPropertyRegistry registry) {
        registry.add("inventory.client.base-url", () -> INVENTORY.baseUrl() + InventoryStubs.BASE_PATH);
    }

    @MockitoSpyBean private OutboxWriter outboxWriter;
    @Autowired private KafkaContainer kafka;
    @Autowired private DatabaseClient database;
    @Autowired private OrderRepository orders;
    @Autowired private OutboxRelay relay;
    @Autowired private TransactionalOperator transactions;
    @Autowired private KafkaTemplate<String, String> kafkaTemplate;
    @Autowired private OutboxProperties outbox;
    @Autowired private CircuitBreakerRegistry circuitBreakers;
    @Autowired private JsonMapper json;
    @Value("${local.server.port}") private int port;

    private WebTestClient client() {
        return WebTestClient.bindToServer().baseUrl("http://localhost:" + port).responseTimeout(TIMEOUT).build();
    }

    @BeforeEach
    void reset() {
        INVENTORY.resetAll();
        circuitBreakers.circuitBreaker("inventory").reset();
        StepVerifier.create(database.sql("DELETE FROM outbox_event").then().then(orders.deleteAll()))
                .expectComplete().verify(TIMEOUT);
    }

    @Test
    void completedOrderPublishesExactlyOneEventWithKeyAndHeaders() {
        inventoryAnswers(200);
        long id = create();
        client().put().uri(ORDERS + "/" + id).exchange().expectStatus().isOk();

        var event = awaitPublished(id);
        var records = recordsWithKey(String.valueOf(id));
        assertThat(records).hasSize(1);
        var record = records.getFirst();
        assertThat(header(record, "eventType")).isEqualTo("OrderCompleted");
        assertThat(header(record, "eventId")).isEqualTo(event.get("id").toString());

        var envelope = json.readTree(record.value());
        assertThat(envelope.path("eventId").asString()).isEqualTo(event.get("id").toString());
        assertThat(envelope.path("eventType").asString()).isEqualTo("OrderCompleted");
        assertThat(envelope.path("version").asInt()).isEqualTo(1);
        assertThat(Instant.parse(envelope.path("occurredAt").asString())).isNotNull();
        assertThat(envelope.path("data").path("orderId").asLong()).isEqualTo(id);
        assertThat(envelope.path("data").path("codeProduct").asString()).isEqualTo(CODE);
        assertThat(envelope.path("data").path("quantity").asInt()).isEqualTo(2);
        assertThat(envelope.path("data").path("status").asString()).isEqualTo("completed");
        assertThat(envelope.path("data").has("cancelReason")).isFalse();
    }

    @Test
    void rejectedOrderPublishesOrderCanceledWithTheReason() {
        inventoryAnswers(409);
        long id = create();
        client().put().uri(ORDERS + "/" + id).exchange().expectStatus().isEqualTo(409);

        awaitPublished(id);
        var record = recordsWithKey(String.valueOf(id)).getFirst();
        assertThat(header(record, "eventType")).isEqualTo("OrderCanceled");
        var data = json.readTree(record.value()).path("data");
        assertThat(data.path("status").asString()).isEqualTo("canceled");
        assertThat(data.path("cancelReason").asString()).isEqualTo("INSUFFICIENT_STOCK");
    }

    @Test
    void kafkaOutageNeitherBlocksConfirmationsNorLosesTheEvent() {
        inventoryAnswers(200);
        long id = create();
        var docker = DockerClientFactory.instance().client();
        docker.pauseContainerCmd(kafka.getContainerId()).exec();
        try {
            client().put().uri(ORDERS + "/" + id).exchange().expectStatus().isOk()
                    .expectBody().jsonPath("$.status").isEqualTo("completed");
            // Kafka is not part of readiness: the pod keeps receiving traffic while events wait in the outbox.
            client().get().uri("/services-order/actuator/health/readiness").exchange().expectStatus().isOk()
                    .expectBody().jsonPath("$.status").isEqualTo("UP");
            // Several relay cycles fail (delivery.timeout.ms is 3s in tests) and the event stays pending.
            await().during(Duration.ofSeconds(4)).atMost(Duration.ofSeconds(6))
                    .until(() -> outboxRows(id).size() == 1 && outboxRows(id).getFirst().get("published_at") == null);
        } finally {
            docker.unpauseContainerCmd(kafka.getContainerId()).exec();
        }

        var event = awaitPublished(id);
        // At least once: a send that timed out may still have reached the broker, so duplicates share the eventId.
        assertThat(recordsWithKey(String.valueOf(id))).isNotEmpty()
                .allSatisfy(record -> assertThat(header(record, "eventId")).isEqualTo(event.get("id").toString()));
    }

    @Test
    void failedOrderSaveLeavesNoEvent() throws Exception {
        INVENTORY.stubFor(put(urlEqualTo(decreasePath(CODE))).willReturn(InventoryStubs.decreased(CODE, 9).withFixedDelay(300)));
        long id = create();

        var status = WebClient.create("http://localhost:" + port).put().uri(ORDERS + "/" + id)
                .exchangeToMono(response -> response.releaseBody().thenReturn(response.statusCode().value()))
                .toFuture();
        // While Inventory answers, the order disappears: the save of the completed order fails (no row with that
        // version), the transaction rolls back and the re-read finds nothing.
        await().atMost(TIMEOUT).until(() -> !INVENTORY.findAll(putRequestedFor(urlEqualTo(decreasePath(CODE)))).isEmpty());
        StepVerifier.create(orders.deleteById(id)).expectComplete().verify(TIMEOUT);

        assertThat(status.get(TIMEOUT.toSeconds(), java.util.concurrent.TimeUnit.SECONDS)).isEqualTo(404);
        assertThat(outboxRows(id)).isEmpty();
    }

    @Test
    void failedOutboxInsertRollsBackTheOrderChange() {
        inventoryAnswers(200);
        long id = create();
        doReturn(Mono.error(new IllegalStateException("outbox no disponible"))).when(outboxWriter).append(any(OrderEntity.class));

        client().put().uri(ORDERS + "/" + id).exchange().expectStatus().isEqualTo(500);

        // The completed state was saved inside the same transaction and rolled back with the failed insert.
        client().get().uri(ORDERS + "/" + id).exchange().expectBody().jsonPath("$.status").isEqualTo("pending");
        assertThat(outboxRows(id)).isEmpty();
    }

    @Test
    void twoRelaysInParallelPublishEachEventOnce() {
        long firstOrder = ThreadLocalRandom.current().nextLong(1_000_000, 9_000_000);
        var ids = LongStream.range(firstOrder, firstOrder + 120).boxed().toList();
        StepVerifier.create(Flux.fromIterable(ids).concatMap(orderId -> outboxWriter.append(
                        new OrderEntity(orderId, CODE, 1, OrderStatus.COMPLETED, null, Instant.now(), Instant.now(), 1L))))
                .expectComplete().verify(TIMEOUT);

        // A second replica next to this context's own relay (which keeps polling too): three relays competing.
        var otherReplica = new OutboxRelay(database, transactions, kafkaTemplate, outbox);
        StepVerifier.create(Flux.merge(relay.publishPending(), otherReplica.publishPending(),
                        relay.publishPending(), otherReplica.publishPending()).then())
                .expectComplete().verify(TIMEOUT);
        await().atMost(TIMEOUT).until(() -> pendingCount() == 0);

        Set<String> keys = ids.stream().map(String::valueOf).collect(Collectors.toSet());
        var eventIds = records().stream().filter(record -> keys.contains(record.key()))
                .map(record -> header(record, "eventId")).toList();
        assertThat(eventIds).hasSize(ids.size()).doesNotHaveDuplicates();
    }

    @Test
    void cleanupDeletesOnlyPublishedEventsOlderThanTheRetention() {
        long base = ThreadLocalRandom.current().nextLong(1_000_000, 9_000_000);
        StepVerifier.create(Flux.range(0, 3).concatMap(offset -> outboxWriter.append(
                        new OrderEntity(base + offset, CODE, 1, OrderStatus.COMPLETED, null, Instant.now(), Instant.now(), 1L)))
                .then(database.sql("UPDATE outbox_event SET published_at = now() - INTERVAL '8 days' WHERE aggregate_id = :id")
                        .bind("id", String.valueOf(base)).then())
                .then(database.sql("UPDATE outbox_event SET published_at = now() - INTERVAL '1 day' WHERE aggregate_id = :id")
                        .bind("id", String.valueOf(base + 1)).then()))
                .expectComplete().verify(TIMEOUT);

        StepVerifier.create(relay.deletePublished()).expectNext(1L).expectComplete().verify(TIMEOUT);

        assertThat(outboxRows(base)).isEmpty();
        assertThat(outboxRows(base + 1)).hasSize(1);
        // Pending events are never deleted, however old (here the relay may already have published it).
        assertThat(outboxRows(base + 2)).hasSize(1);
    }

    private long create() {
        var order = client().post().uri(ORDERS).contentType(MediaType.APPLICATION_JSON)
                .bodyValue("{\"codeProduct\":\"" + CODE + "\",\"quantity\":2}").exchange()
                .expectStatus().isCreated().expectBody(OrderResponse.class).returnResult().getResponseBody();
        return Objects.requireNonNull(order).getId();
    }

    private static void inventoryAnswers(int status) {
        INVENTORY.stubFor(put(urlEqualTo(decreasePath(CODE)))
                .willReturn(status == 200 ? InventoryStubs.decreased(CODE, 9) : InventoryStubs.problem(status, CODE)));
    }

    private Map<String, Object> awaitPublished(long orderId) {
        await().atMost(TIMEOUT).until(() -> outboxRows(orderId).stream().anyMatch(row -> row.get("published_at") != null));
        var rows = outboxRows(orderId);
        assertThat(rows).as("eventos de la orden %s", orderId).hasSize(1);
        return rows.getFirst();
    }

    private List<Map<String, Object>> outboxRows(long orderId) {
        return Objects.requireNonNull(database.sql("SELECT id, event_type, published_at FROM outbox_event WHERE aggregate_id = :id")
                .bind("id", String.valueOf(orderId)).fetch().all().collectList().block(TIMEOUT));
    }

    private long pendingCount() {
        return Objects.requireNonNull(database.sql("SELECT count(*) AS pending FROM outbox_event WHERE published_at IS NULL")
                .map(row -> Objects.requireNonNull(row.get("pending", Long.class))).one().block(TIMEOUT));
    }

    private List<ConsumerRecord<String, String>> recordsWithKey(String key) {
        return records().stream().filter(record -> key.equals(record.key())).toList();
    }

    /** Every record currently in the topic, read from the beginning up to the end offsets taken at the start. */
    private List<ConsumerRecord<String, String>> records() {
        var settings = new Properties();
        settings.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        settings.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        try (var consumer = new KafkaConsumer<>(settings, new StringDeserializer(), new StringDeserializer())) {
            var partitions = consumer.partitionsFor(outbox.topic()).stream()
                    .map(info -> new TopicPartition(info.topic(), info.partition())).toList();
            consumer.assign(partitions);
            consumer.seekToBeginning(partitions);
            var end = consumer.endOffsets(partitions);
            var found = new ArrayList<ConsumerRecord<String, String>>();
            var deadline = Instant.now().plus(TIMEOUT);
            while (partitions.stream().anyMatch(partition -> consumer.position(partition) < end.get(partition))) {
                assertThat(Instant.now()).as("lectura del topic").isBefore(deadline);
                consumer.poll(Duration.ofMillis(200)).forEach(found::add);
            }
            return found;
        }
    }

    private static String header(ConsumerRecord<String, String> record, String name) {
        return new String(Objects.requireNonNull(record.headers().lastHeader(name)).value(), StandardCharsets.UTF_8);
    }
}

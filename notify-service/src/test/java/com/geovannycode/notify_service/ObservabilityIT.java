package com.geovannycode.notify_service;

import java.time.Duration;
import java.util.Collection;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.regex.Pattern;

import com.geovannycode.notify_service.notify.domain.NotifyStatus;
import com.geovannycode.notify_service.notify.infrastructure.persistence.NotificationRepository;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SpanExporter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.micrometer.metrics.test.autoconfigure.AutoConfigureMetrics;
import org.springframework.boot.micrometer.tracing.test.autoconfigure.AutoConfigureTracing;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.reactive.server.WebTestClient;
import org.testcontainers.kafka.KafkaContainer;

import static com.geovannycode.notify_service.OrderEventsKafka.event;
import static com.geovannycode.notify_service.OrderEventsKafka.newOrderId;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

// Spans go to an in-memory exporter instead of OTLP (no collector needed). Console logs in ECS, as in the docker
// and k8s profiles, to check the fields each consumer line carries.
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "management.tracing.export.otlp.enabled=false",
        "management.opentelemetry.tracing.export.schedule-delay=50ms",
        "logging.structured.format.console=ecs",
        "logging.structured.json.rename.traceId=trace.id",
        "logging.structured.json.rename.spanId=span.id"})
@AutoConfigureMetrics
@AutoConfigureTracing
@Import({TestcontainersConfiguration.class, ObservabilityIT.SpanCapture.class})
@ActiveProfiles("test")
@ExtendWith(OutputCaptureExtension.class)
final class ObservabilityIT {

    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    private final WebTestClient client;
    private final CapturingSpanExporter spans;
    private final NotificationRepository repository;
    private final OrderEventsKafka orders;

    @Autowired
    ObservabilityIT(@Value("${local.server.port}") int port, CapturingSpanExporter spans,
                    NotificationRepository repository, KafkaContainer kafka) {
        this.client = WebTestClient.bindToServer().baseUrl("http://127.0.0.1:" + port + "/services-notify")
                .responseTimeout(TIMEOUT).build();
        this.spans = spans;
        this.repository = repository;
        this.orders = new OrderEventsKafka(kafka.getBootstrapServers());
    }

    @BeforeEach
    void cleanCollection() {
        repository.deleteAll().block(TIMEOUT);
    }

    @AfterEach
    void closeProducer() {
        orders.close();
    }

    @Test
    void prometheusExposesBusinessKafkaAndListenerMetricsAfterProcessing() {
        long orderId = newOrderId();
        String eventId = orders.publish(orderId, event(UUID.randomUUID().toString(), "OrderCompleted", orderId, "completed", null));
        awaitSent(eventId);
        orders.publish(orderId, event(eventId, "OrderCompleted", orderId, "completed", null));
        orders.publish(orderId, event(UUID.randomUUID().toString(), "OrderShipped", orderId, "shipped", null));
        orders.send(orderId, "{no es json", Map.of());
        String marker = orders.publish(orderId, event(UUID.randomUUID().toString(), "OrderCompleted", orderId, "completed", null));
        awaitSent(marker);

        // The DLT counter and the client metrics may lag a moment behind the notification itself.
        await().atMost(TIMEOUT).untilAsserted(() -> {
            String scrape = scrape();
            assertThat(counter(scrape, business("notifications_processed_total", "eventType=\"OrderCompleted\",result=\"sent\"")))
                    .isGreaterThanOrEqualTo(2);
            assertThat(counter(scrape, business("notifications_processed_total", "eventType=\"OrderCompleted\",result=\"duplicate\"")))
                    .isGreaterThanOrEqualTo(1);
            assertThat(counter(scrape, business("notifications_processed_total", "eventType=\"other\",result=\"ignored\"")))
                    .isGreaterThanOrEqualTo(1);
            assertThat(counter(scrape, business("notifications_dlt_total", "reason=\"invalid\""))).isGreaterThanOrEqualTo(1);
            assertThat(scrape)
                    .contains("notifications_delivery_seconds_bucket{application=\"service-notify\",channel=\"log\"")
                    .contains("notifications_delivery_seconds_count{application=\"service-notify\",channel=\"log\",outcome=\"success\"}")
                    // Consumer lag (KafkaClientMetrics) and the listener timer (observation). The per-partition
                    // records_lag series appear at the binder's next refresh (60s); records_lag_max is there at once.
                    .contains("kafka_consumer_fetch_manager_records_lag_max{application=\"service-notify\"")
                    .contains("spring_kafka_listener_seconds_count{")
                    .contains("spring_kafka_listener_seconds_bucket{");
        });
    }

    @Test
    void consumerSpanContinuesTheProducerTraceAndMongoWritesBelongToIt() {
        String traceId = randomHex(16);
        long orderId = newOrderId();
        String eventId = UUID.randomUUID().toString();
        // What Order's KafkaTemplate injects: the traceparent of its producer span.
        orders.send(orderId, event(eventId, "OrderCompleted", orderId, "completed", null),
                Map.of("eventId", eventId, "traceparent", "00-" + traceId + "-" + randomHex(8) + "-01"));
        awaitSent(eventId);

        await().atMost(TIMEOUT).untilAsserted(() -> {
            List<SpanData> trace = spans.inTrace(traceId);
            assertThat(trace).anySatisfy(span -> {
                assertThat(span.getKind()).isEqualTo(SpanKind.CONSUMER);
                assertThat(span.getName()).contains("orders.events.v1");
            });
            // The insert of the PENDING notification and the update to SENT, both inside the same trace.
            assertThat(trace).filteredOn(span -> span.getKind() == SpanKind.CLIENT)
                    .extracting(SpanData::getName)
                    .anyMatch(name -> name.contains("insert"))
                    .anyMatch(name -> name.contains("update"));
        });
    }

    @Test
    void everyConsumerLogLineCarriesTraceIdEventIdAndOrderId(CapturedOutput output) {
        long orderId = newOrderId();
        String eventId = orders.publish(orderId, event(UUID.randomUUID().toString(), "OrderCompleted", orderId, "completed", null));
        awaitSent(eventId);

        // "Notificación enviada" is logged on a MongoDB driver thread, not the consumer's: the MDC keys and the
        // trace id reached it through Reactor's context propagation.
        String sentLine = await().atMost(TIMEOUT).until(() -> output.getOut().lines()
                .filter(line -> line.contains("Notificación enviada") && line.contains(eventId))
                .findFirst().orElse(null), Objects::nonNull);
        assertThat(sentLine)
                .contains("\"eventId\":\"" + eventId + "\"")
                .contains("\"orderId\":\"" + orderId + "\"")
                .containsPattern("\"trace\\.id\":\"[0-9a-f]{32}\"");
    }

    @Test
    void infoExposesBuildInformation() {
        client.get().uri("/actuator/info").exchange().expectStatus().isOk()
                .expectBody().jsonPath("$.build.artifact").isEqualTo("notify-service");
    }

    private void awaitSent(String eventId) {
        await().atMost(TIMEOUT).until(() -> repository.findByEventId(eventId).block(TIMEOUT),
                notification -> notification != null && notification.status() == NotifyStatus.SENT);
    }

    private String scrape() {
        return Objects.requireNonNull(client.get().uri("/actuator/prometheus").exchange().expectStatus().isOk()
                .expectBody(String.class).returnResult().getResponseBody());
    }

    private static String business(String name, String labels) {
        // Prometheus writes labels in alphabetical order, application first.
        return name + "{application=\"service-notify\"," + labels + "}";
    }

    private static double counter(String scrape, String series) {
        var matcher = Pattern.compile("^" + Pattern.quote(series) + " (\\S+)$", Pattern.MULTILINE).matcher(scrape);
        assertThat(matcher.find()).as("series %s", series).isTrue();
        return Double.parseDouble(matcher.group(1));
    }

    private static String randomHex(int bytes) {
        var value = new byte[bytes];
        ThreadLocalRandom.current().nextBytes(value);
        return HexFormat.of().formatHex(value);
    }

    @TestConfiguration(proxyBeanMethods = false)
    static class SpanCapture {

        @Bean
        CapturingSpanExporter capturingSpanExporter() {
            return new CapturingSpanExporter();
        }
    }

    static final class CapturingSpanExporter implements SpanExporter {

        private final Collection<SpanData> exported = new ConcurrentLinkedQueue<>();

        List<SpanData> inTrace(String traceId) {
            return exported.stream().filter(span -> span.getTraceId().equals(traceId)).toList();
        }

        @Override
        public CompletableResultCode export(Collection<SpanData> batch) {
            exported.addAll(batch);
            return CompletableResultCode.ofSuccess();
        }

        @Override
        public CompletableResultCode flush() {
            return CompletableResultCode.ofSuccess();
        }

        @Override
        public CompletableResultCode shutdown() {
            return CompletableResultCode.ofSuccess();
        }
    }
}

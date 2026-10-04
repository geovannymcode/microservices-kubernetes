package com.geovannycode.order;

import java.time.Duration;
import java.util.Collection;
import java.util.HexFormat;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.regex.Pattern;

import com.geovannycode.order.generated.dto.OrderResponse;
import com.geovannycode.order.order.infrastructure.inventory.InventoryStubs;
import com.github.tomakehurst.wiremock.junit5.WireMockExtension;
import com.github.tomakehurst.wiremock.stubbing.Scenario;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SpanExporter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.micrometer.metrics.test.autoconfigure.AutoConfigureMetrics;
import org.springframework.boot.micrometer.tracing.test.autoconfigure.AutoConfigureTracing;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.reactive.server.WebTestClient;
import org.testcontainers.kafka.KafkaContainer;

import static com.geovannycode.order.order.infrastructure.inventory.InventoryStubs.decreasePath;
import static com.github.tomakehurst.wiremock.client.WireMock.put;
import static com.github.tomakehurst.wiremock.client.WireMock.putRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

// Spans go to an in-memory exporter instead of OTLP so the test needs no collector; WireMock plays Inventory.
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "management.tracing.export.otlp.enabled=false",
        "management.opentelemetry.tracing.export.schedule-delay=50ms"})
@AutoConfigureMetrics
@AutoConfigureTracing
@Import({TestcontainersConfiguration.class, ObservabilityIT.SpanCapture.class})
@ActiveProfiles("test")
final class ObservabilityIT {

    @RegisterExtension
    static final WireMockExtension INVENTORY = WireMockExtension.newInstance()
            .options(wireMockConfig().dynamicPort()).build();

    private static final String BASE = "/services-order";
    private static final String ORDERS = BASE + "/orders";
    private static final Duration TIMEOUT = Duration.ofSeconds(20);

    @DynamicPropertySource
    static void inventoryBaseUrl(DynamicPropertyRegistry registry) {
        registry.add("inventory.client.base-url", () -> INVENTORY.baseUrl() + InventoryStubs.BASE_PATH);
    }

    private final WebTestClient client;
    private final CapturingSpanExporter spans;
    private final CircuitBreaker circuitBreaker;
    private final KafkaContainer kafka;

    @Autowired
    ObservabilityIT(@Value("${local.server.port}") int port, CapturingSpanExporter spans,
                    CircuitBreakerRegistry circuitBreakers, KafkaContainer kafka) {
        this.client = WebTestClient.bindToServer().baseUrl("http://localhost:" + port).responseTimeout(TIMEOUT).build();
        this.spans = spans;
        this.circuitBreaker = circuitBreakers.circuitBreaker("inventory");
        this.kafka = kafka;
    }

    @BeforeEach
    void reset() {
        INVENTORY.resetAll();
        circuitBreaker.reset();
    }

    @Test
    void prometheusExposesResilienceAndBusinessMetricsAfterConfirmations() {
        inventoryAnswers("OBS-OK", 200);
        inventoryAnswers("OBS-LOW", 409);
        inventoryAnswers("OBS-DOWN", 500);
        client.put().uri(ORDERS + "/" + create("OBS-OK")).exchange().expectStatus().isOk();
        client.put().uri(ORDERS + "/" + create("OBS-LOW")).exchange().expectStatus().isEqualTo(409);
        client.put().uri(ORDERS + "/" + create("OBS-DOWN")).exchange().expectStatus().isEqualTo(503);

        String scrape = scrape();
        // Prometheus writes labels in alphabetical order: application, kind, name, state.
        assertThat(scrape)
                .contains("resilience4j_circuitbreaker_state{application=\"service-order\",name=\"inventory\",state=\"closed\"} 1.0")
                .contains(inventory("resilience4j_circuitbreaker_calls_seconds_count", "successful"))
                .contains(inventory("resilience4j_circuitbreaker_calls_seconds_count", "failed"))
                .contains(inventory("resilience4j_retry_calls_total", "failed_with_retry"))
                .contains("resilience4j_ratelimiter_available_permissions{application=\"service-order\",name=\"inventory\"}")
                .contains(inventory("resilience4j_timelimiter_calls_total", "successful"))
                .contains("http_server_requests_seconds_bucket{", "le=\"0.1\"", "le=\"0.3\"", "le=\"1.0\"")
                .contains("http_client_requests_seconds_bucket{")
                .contains("outbox_pending{application=\"service-order\"}");
        assertThat(counter(scrape, "orders_registered_total{application=\"service-order\"}")).isGreaterThanOrEqualTo(3);
        for (String result : new String[]{"completed", "canceled", "unavailable"}) {
            assertThat(counter(scrape, "orders_confirmed_total{application=\"service-order\",result=\"" + result + "\"}"))
                    .as(result).isGreaterThanOrEqualTo(1);
        }
    }

    @Test
    void oneTraceCrossesToInventoryWithEachRetryAsItsOwnClientSpan() {
        // Two 500s and a 200: three attempts of the same confirmation.
        String path = decreasePath("OBS-RETRY");
        INVENTORY.stubFor(put(urlEqualTo(path)).inScenario("flaky").whenScenarioStateIs(Scenario.STARTED)
                .willReturn(InventoryStubs.problem(500, "OBS-RETRY")).willSetStateTo("second"));
        INVENTORY.stubFor(put(urlEqualTo(path)).inScenario("flaky").whenScenarioStateIs("second")
                .willReturn(InventoryStubs.problem(500, "OBS-RETRY")).willSetStateTo("third"));
        INVENTORY.stubFor(put(urlEqualTo(path)).inScenario("flaky").whenScenarioStateIs("third")
                .willReturn(InventoryStubs.decreased("OBS-RETRY", 9)));
        long id = create("OBS-RETRY");
        String traceId = randomHex(16);
        String parentSpanId = randomHex(8);

        client.put().uri(ORDERS + "/" + id).header("traceparent", "00-" + traceId + "-" + parentSpanId + "-01")
                .exchange().expectStatus().isOk();

        // What Inventory received: three requests, all in the caller's trace, each from a different client span.
        var sent = INVENTORY.findAll(putRequestedFor(urlEqualTo(path))).stream()
                .map(request -> request.getHeader("traceparent")).toList();
        assertThat(sent).hasSize(3).allSatisfy(header -> assertThat(header).startsWith("00-" + traceId + "-"));
        assertThat(sent).doesNotHaveDuplicates();

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            List<SpanData> trace = spans.inTrace(traceId);
            assertThat(trace).anySatisfy(span -> {
                assertThat(span.getKind()).isEqualTo(SpanKind.SERVER);
                assertThat(span.getParentSpanId()).isEqualTo(parentSpanId);
            });
            assertThat(trace).filteredOn(span -> span.getKind() == SpanKind.CLIENT
                    && span.getName().toLowerCase().startsWith("http put")).hasSize(3);
            assertThat(trace).as("R2DBC query spans in trace %s", trace).anySatisfy(span ->
                    assertThat(span.getAttributes().asMap().keySet())
                            .anySatisfy(key -> assertThat(key.getKey()).startsWith("r2dbc.query")));
        });
    }

    @Test
    void orderEventCarriesTheTraceOfTheConfirmation() {
        inventoryAnswers("OBS-EVT", 200);
        long id = create("OBS-EVT");
        String traceId = randomHex(16);

        client.put().uri(ORDERS + "/" + id).header("traceparent", "00-" + traceId + "-" + randomHex(8) + "-01")
                .exchange().expectStatus().isOk();

        await().atMost(TIMEOUT).untilAsserted(() -> {
            var event = KafkaTopicReader.records(kafka.getBootstrapServers(), "orders.events.v1").stream()
                    .filter(record -> record.key().equals(String.valueOf(id))).findFirst();
            assertThat(event).isPresent();
            assertThat(KafkaTopicReader.header(event.get(), "traceparent")).startsWith("00-" + traceId + "-");
        });
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> assertThat(spans.inTrace(traceId))
                .anySatisfy(span -> assertThat(span.getKind()).isEqualTo(SpanKind.PRODUCER)));
    }

    @Test
    void openCircuitIsVisibleInHealthButReadinessStaysUp() {
        circuitBreaker.transitionToOpenState();

        client.get().uri(BASE + "/actuator/health/readiness").exchange().expectStatus().isOk()
                .expectBody().jsonPath("$.status").isEqualTo("UP");
        client.get().uri(BASE + "/actuator/health/liveness").exchange().expectStatus().isOk();
        // allowHealthIndicatorToFail=false: an open circuit is UNKNOWN, never DOWN, so Order's own health holds.
        client.get().uri(BASE + "/actuator/health").exchange().expectStatus().isOk()
                .expectBody().jsonPath("$.status").isEqualTo("UP")
                .jsonPath("$.components.circuitBreakers.status").isEqualTo("UNKNOWN");
        client.get().uri(BASE + "/actuator/health/resilience").exchange().expectStatus().isOk()
                .expectBody().jsonPath("$.components.circuitBreakers.details.inventory.details.state").isEqualTo("OPEN");
        client.get().uri(BASE + "/actuator/circuitbreakers").exchange().expectStatus().isOk()
                .expectBody().jsonPath("$.circuitBreakers.inventory.state").isEqualTo("OPEN");
    }

    @Test
    void resilienceAndInfoEndpointsAreExposedAndSensitiveOnesAreNot() {
        client.get().uri(BASE + "/actuator/retries").exchange().expectStatus().isOk()
                .expectBody().jsonPath("$.retries").value(names -> assertThat(names.toString()).contains("inventory"));
        client.get().uri(BASE + "/actuator/ratelimiters").exchange().expectStatus().isOk()
                .expectBody().jsonPath("$.rateLimiters").value(names -> assertThat(names.toString()).contains("inventory"));
        client.get().uri(BASE + "/actuator/info").exchange().expectStatus().isOk()
                .expectBody().jsonPath("$.build.artifact").isEqualTo("order-service");
        for (String endpoint : new String[]{"env", "beans", "configprops"}) {
            client.get().uri(BASE + "/actuator/" + endpoint).exchange().expectStatus().isNotFound();
        }
    }

    private long create(String code) {
        var order = client.post().uri(ORDERS).contentType(MediaType.APPLICATION_JSON)
                .bodyValue("{\"codeProduct\":\"" + code + "\",\"quantity\":1}").exchange()
                .expectStatus().isCreated().expectBody(OrderResponse.class).returnResult().getResponseBody();
        return Objects.requireNonNull(order).getId();
    }

    private static void inventoryAnswers(String code, int status) {
        INVENTORY.stubFor(put(urlEqualTo(decreasePath(code)))
                .willReturn(status == 200 ? InventoryStubs.decreased(code, 9) : InventoryStubs.problem(status, code)));
    }

    private String scrape() {
        return Objects.requireNonNull(client.get().uri(BASE + "/actuator/prometheus").exchange()
                .expectStatus().isOk().expectBody(String.class).returnResult().getResponseBody());
    }

    private static String inventory(String metric, String kind) {
        return metric + "{application=\"service-order\",kind=\"" + kind + "\",name=\"inventory\"}";
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

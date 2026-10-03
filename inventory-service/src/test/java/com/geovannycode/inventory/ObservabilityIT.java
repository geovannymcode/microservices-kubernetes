package com.geovannycode.inventory;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.Collection;
import java.util.HexFormat;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.regex.Pattern;

import com.geovannycode.inventory.inventory.api.dto.InventoryRequest;
import com.geovannycode.inventory.inventory.api.dto.OrderInvRequest;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SpanExporter;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.micrometer.metrics.test.autoconfigure.AutoConfigureMetrics;
import org.springframework.boot.micrometer.tracing.test.autoconfigure.AutoConfigureTracing;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.test.web.reactive.server.WebTestClient;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

// Spans go to an in-memory exporter instead of OTLP so the test needs no collector.
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "management.tracing.export.otlp.enabled=false",
        "management.opentelemetry.tracing.export.schedule-delay=50ms"})
@AutoConfigureMetrics
@AutoConfigureTracing
@Import({TestcontainersConfiguration.class, ObservabilityIT.SpanCapture.class})
final class ObservabilityIT {

    private static final String BASE = "/services-inventory";
    private static final String PATH = BASE + "/inventories";
    private final WebTestClient client;
    private final CapturingSpanExporter spans;

    @Autowired
    ObservabilityIT(@Value("${local.server.port}") int port, CapturingSpanExporter spans) {
        this.client = WebTestClient.bindToServer().baseUrl("http://localhost:" + port)
                .responseTimeout(Duration.ofSeconds(15)).build();
        this.spans = spans;
    }

    @Test
    void prometheusExposesHttpLatencyAndBusinessMetrics() {
        String code = register(1);
        decrease(code, 1).expectStatus().isOk();
        decrease(code, 1).expectStatus().isEqualTo(409);

        String scrape = client.get().uri(BASE + "/actuator/prometheus").exchange()
                .expectStatus().isOk().expectBody(String.class).returnResult().getResponseBody();

        assertThat(scrape)
                .contains("http_server_requests_seconds_bucket{")
                .contains("le=\"0.1\"", "le=\"0.3\"", "le=\"1.0\"")
                .contains("application=\"service-inventory\"");
        assertThat(counter(scrape, "inventory_product_registered_total{application=\"service-inventory\"}"))
                .isGreaterThanOrEqualTo(1);
        assertThat(counter(scrape, "inventory_stock_decrease_total{application=\"service-inventory\",result=\"ok\"}"))
                .isGreaterThanOrEqualTo(1);
        assertThat(counter(scrape, "inventory_stock_decrease_total{application=\"service-inventory\",result=\"insufficient\"}"))
                .isGreaterThanOrEqualTo(1);
        assertThat(scrape).contains("inventory_stock_decrease_total{application=\"service-inventory\",result=\"not_found\"}");
    }

    @Test
    void continuesIncomingW3cTraceAndRecordsDatabaseSpans() {
        String code = register(5);
        String traceId = randomHex(16);
        String parentSpanId = randomHex(8);

        client.put().uri(PATH + "/" + code).header("traceparent", "00-" + traceId + "-" + parentSpanId + "-01")
                .bodyValue(new OrderInvRequest(1)).exchange().expectStatus().isOk();

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            List<SpanData> trace = spans.inTrace(traceId);
            assertThat(trace).anySatisfy(span -> {
                assertThat(span.getKind()).isEqualTo(SpanKind.SERVER);
                assertThat(span.getParentSpanId()).isEqualTo(parentSpanId);
            });
            assertThat(trace).as("R2DBC query spans in trace %s", trace).anySatisfy(span ->
                    assertThat(span.getAttributes().asMap().keySet())
                            .anySatisfy(key -> assertThat(key.getKey()).startsWith("r2dbc.query")));
        });
    }

    @Test
    void infoExposesBuildAndSensitiveEndpointsStayHidden() {
        client.get().uri(BASE + "/actuator/info").exchange().expectStatus().isOk()
                .expectBody().jsonPath("$.build.artifact").isEqualTo("service-inventory");
        for (String endpoint : new String[]{"env", "beans", "configprops"}) {
            client.get().uri(BASE + "/actuator/" + endpoint).exchange().expectStatus().isNotFound();
        }
    }

    private WebTestClient.ResponseSpec decrease(String code, int count) {
        return client.put().uri(PATH + "/" + code).bodyValue(new OrderInvRequest(count)).exchange();
    }

    private String register(int stock) {
        String code = "OBS-" + UUID.randomUUID();
        client.post().uri(PATH).bodyValue(new InventoryRequest(code, "Observabilidad", new BigDecimal("1.00"), stock))
                .exchange().expectStatus().isCreated();
        return code;
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

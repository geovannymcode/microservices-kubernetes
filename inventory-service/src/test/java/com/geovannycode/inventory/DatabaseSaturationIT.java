package com.geovannycode.inventory;

import java.time.Duration;

import io.micrometer.core.instrument.MeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.http.HttpHeaders;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Import(TestcontainersConfiguration.class)
final class DatabaseSaturationIT {

    private static final String PRODUCT = "/services-inventory/inventories/AC-1550";
    private static final int POOL_SIZE = 10;
    private final WebTestClient client;
    private final DatabaseClient database;
    private final MeterRegistry meterRegistry;

    @Autowired
    DatabaseSaturationIT(@Value("${local.server.port}") int port, DatabaseClient database, MeterRegistry meterRegistry) {
        this.client = WebTestClient.bindToServer().baseUrl("http://localhost:" + port)
                .responseTimeout(Duration.ofSeconds(15)).build();
        this.database = database;
        this.meterRegistry = meterRegistry;
    }

    @Test
    void exhaustedPoolFailsFastWithServiceUnavailableAndRecovers() {
        Disposable sleepers = Flux.range(0, POOL_SIZE)
                .flatMap(ignored -> database.sql("SELECT SLEEP(6)").fetch().rowsUpdated(), POOL_SIZE)
                .subscribe();
        try {
            await().atMost(Duration.ofSeconds(10)).until(() -> acquiredConnections() == POOL_SIZE);

            long started = System.nanoTime();
            client.get().uri(PRODUCT).exchange()
                    .expectStatus().isEqualTo(503)
                    .expectHeader().valueEquals(HttpHeaders.RETRY_AFTER, "5")
                    .expectBody().jsonPath("$.type").isEqualTo("https://codearti.com/problems/database-unavailable");
            // max-acquire-time 1 s x (1 + acquire-retry) stays under the readiness probe's 3 s timeout.
            assertThat(Duration.ofNanos(System.nanoTime() - started)).isLessThan(Duration.ofSeconds(3));
        } finally {
            sleepers.dispose();
        }
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
                client.get().uri(PRODUCT).exchange().expectStatus().isOk());
    }

    private double acquiredConnections() {
        var gauge = meterRegistry.find("r2dbc.pool.acquired").gauge();
        return gauge == null ? -1 : gauge.value();
    }
}

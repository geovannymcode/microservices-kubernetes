package com.geovannycode.inventory;

import java.time.Duration;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.web.reactive.server.WebTestClient;
import org.testcontainers.mysql.MySQLContainer;

import static org.awaitility.Awaitility.await;

// Stops the context's MySQL container, so the context must not be reused by other test classes.
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Import(TestcontainersConfiguration.class)
@DirtiesContext
final class HealthProbesIT {

    private static final String HEALTH = "/services-inventory/actuator/health";
    private final WebTestClient client;
    private final MySQLContainer mysql;

    @Autowired
    HealthProbesIT(@Value("${local.server.port}") int port, MySQLContainer mysql) {
        this.client = WebTestClient.bindToServer().baseUrl("http://localhost:" + port)
                .responseTimeout(Duration.ofSeconds(30)).build();
        this.mysql = mysql;
    }

    @Test
    void databaseOutageTakesPodOutOfServiceWithoutRestartingIt() {
        expectStatus("/readiness", 200, "UP");
        expectStatus("/liveness", 200, "UP");

        mysql.stop();

        await().atMost(Duration.ofSeconds(60)).untilAsserted(() -> expectStatus("/readiness", 503, "DOWN"));
        expectStatus("/liveness", 200, "UP");
    }

    private void expectStatus(String probe, int httpStatus, String status) {
        client.get().uri(HEALTH + probe).exchange().expectStatus().isEqualTo(httpStatus)
                .expectBody().jsonPath("$.status").isEqualTo(status);
    }
}

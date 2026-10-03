package com.geovannycode.inventory;

import java.time.Duration;

import com.geovannycode.inventory.inventory.api.dto.OrderInvRequest;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
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
    private static final String INVENTORIES = "/services-inventory/inventories";
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

        expectDatabaseUnavailable(client.get().uri(INVENTORIES + "/AC-1550").exchange());
        expectDatabaseUnavailable(client.put().uri(INVENTORIES + "/AC-1550")
                .bodyValue(new OrderInvRequest(1)).exchange());
    }

    private static void expectDatabaseUnavailable(WebTestClient.ResponseSpec response) {
        response.expectStatus().isEqualTo(503)
                .expectHeader().valueEquals(HttpHeaders.RETRY_AFTER, "5")
                .expectHeader().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON)
                .expectBody().jsonPath("$.type").isEqualTo("https://codearti.com/problems/database-unavailable")
                .jsonPath("$.detail").value(detail -> org.assertj.core.api.Assertions.assertThat((String) detail)
                        .doesNotContain("Exception"));
    }

    private void expectStatus(String probe, int httpStatus, String status) {
        client.get().uri(HEALTH + probe).exchange().expectStatus().isEqualTo(httpStatus)
                .expectBody().jsonPath("$.status").isEqualTo(status);
    }
}

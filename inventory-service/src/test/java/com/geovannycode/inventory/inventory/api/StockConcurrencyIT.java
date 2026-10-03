package com.geovannycode.inventory.inventory.api;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.UUID;
import com.geovannycode.inventory.TestcontainersConfiguration;
import com.geovannycode.inventory.inventory.api.dto.*;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.http.ProblemDetail;
import org.springframework.test.web.reactive.server.WebTestClient;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;
import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Import(TestcontainersConfiguration.class)
final class StockConcurrencyIT {
    private static final String PATH = "/services-inventory/inventories";
    private final WebTestClient client;
    private final WebClient concurrentClient;
    @Autowired StockConcurrencyIT(@Value("${local.server.port}") int port) {
        String baseUrl = "http://localhost:" + port;
        client = WebTestClient.bindToServer().baseUrl(baseUrl).responseTimeout(Duration.ofSeconds(15)).build();
        concurrentClient = WebClient.builder().baseUrl(baseUrl).build();
    }
    @Test void fiftyConcurrentOrdersCannotOversellTenUnits() {
        String code = "RACE-" + UUID.randomUUID();
        client.post().uri(PATH).bodyValue(new InventoryRequest(code, "Concurrencia", new BigDecimal("1.00"), 10))
                .exchange().expectStatus().isCreated();
        var requests = Flux.range(0, 50).flatMap(ignored -> concurrentClient.put().uri(PATH + "/" + code)
                .bodyValue(new OrderInvRequest(1)).exchangeToMono(response -> {
                    int status = response.statusCode().value();
                    if (status == 200) {
                        return response.bodyToMono(InventoryResponse.class).map(product -> {
                            assertThat(product.stock()).isBetween(0, 9);
                            return status;
                        });
                    }
                    assertThat(status).isEqualTo(409);
                    return response.bodyToMono(ProblemDetail.class).map(problem -> {
                        assertThat(problem.getStatus()).isEqualTo(409);
                        assertThat(problem.getType().toString()).endsWith("/insufficient-stock");
                        return status;
                    });
                }), 50).collectList();
        StepVerifier.create(requests).assertNext(statuses -> {
            assertThat(statuses).hasSize(50);
            assertThat(statuses.stream().filter(status -> status == 200).count()).isEqualTo(10);
            assertThat(statuses.stream().filter(status -> status == 409).count()).isEqualTo(40);
        }).expectComplete().verify(Duration.ofSeconds(30));
        client.get().uri(PATH + "/" + code).exchange().expectStatus().isOk()
                .expectBody().jsonPath("$.stock").isEqualTo(0);
    }
}

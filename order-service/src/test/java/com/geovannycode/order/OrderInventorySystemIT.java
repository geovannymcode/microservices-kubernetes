package com.geovannycode.order;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import com.geovannycode.order.generated.dto.OrderResponse;
import com.github.dockerjava.api.exception.NotFoundException;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.web.reactive.function.client.WebClient;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Order (in this JVM, PostgreSQL in Testcontainers) against the real service-inventory image and its MySQL.
 * Runs only with -Psystem-tests. Proves end to end that duplicated, concurrent confirmations never decrease
 * stock twice and that stock never goes below zero.
 */
// Production time limiter (2s) instead of the test profile's 500ms: the real Inventory under 30 concurrent
// requests is slower than WireMock, and a timeout here would only add idempotent retries, not change the result.
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = "resilience4j.timelimiter.instances.inventory.timeout-duration=2s")
@Import(TestcontainersConfiguration.class)
@ActiveProfiles("test")
@Testcontainers
final class OrderInventorySystemIT {

    private static final String INVENTORY_IMAGE =
            System.getProperty("inventory.image", "geovannycode/service-inventory:0.0.1-SNAPSHOT");
    private static final String PRODUCT = "SYS-0001";
    private static final int STOCK = 10;
    private static final int ORDERS = 15;
    private static final Duration TIMEOUT = Duration.ofMinutes(1);

    private static final Network NETWORK = Network.newNetwork();

    // GenericContainer instead of MySQLContainer: the latter checks readiness over JDBC and would add the MySQL
    // driver to Order only for this test. Inventory's own readiness probe (r2dbc) gates the test anyway.
    @Container
    static final GenericContainer<?> MYSQL = new GenericContainer<>(
            // mysql:8.4.11, same digest as docker-compose.yaml.
            "mysql@sha256:6ea90827b1100f8f2ae306a539f86d2c264a26ed435a2a9f75551dd5c3aeb242")
            .withNetwork(NETWORK).withNetworkAliases("mysql")
            .withEnv(Map.of(
                    "MYSQL_ROOT_PASSWORD", "root",
                    "MYSQL_DATABASE", "inventory",
                    "MYSQL_USER", "inventory",
                    "MYSQL_PASSWORD", "inventory"))
            // The entrypoint first starts a temporary server without networking (port: 0); wait for the real one.
            .waitingFor(Wait.forLogMessage(".*ready for connections.*port: 3306.*", 1)
                    .withStartupTimeout(Duration.ofMinutes(2)));

    @Container
    static final GenericContainer<?> INVENTORY = new GenericContainer<>(requireInventoryImage())
            .withNetwork(NETWORK)
            .dependsOn(MYSQL)
            .withEnv(Map.of(
                    "SPRING_PROFILES_ACTIVE", "docker",
                    "DB_HOST", "mysql",
                    "DB_NAME", "inventory",
                    "DB_USER", "inventory",
                    "DB_PASSWORD", "inventory",
                    "MANAGEMENT_TRACING_EXPORT_ENABLED", "false"))
            .withExposedPorts(8080)
            .waitingFor(Wait.forHttp("/services-inventory/actuator/health/readiness").forStatusCode(200)
                    .withStartupTimeout(Duration.ofMinutes(2)));

    @DynamicPropertySource
    static void inventoryBaseUrl(DynamicPropertyRegistry registry) {
        registry.add("inventory.client.base-url", OrderInventorySystemIT::inventoryUrl);
    }

    private static String inventoryUrl() {
        return "http://" + INVENTORY.getHost() + ":" + INVENTORY.getMappedPort(8080) + "/services-inventory";
    }

    /**
     * The Dockerfile needs BuildKit (named build context for the contract and cache mounts), which Testcontainers'
     * ImageFromDockerfile does not support, so the image must be built beforehand with the Docker CLI.
     */
    private static String requireInventoryImage() {
        try {
            DockerClientFactory.instance().client().inspectImageCmd(INVENTORY_IMAGE).exec();
            return INVENTORY_IMAGE;
        } catch (NotFoundException missing) {
            throw new IllegalStateException("No existe la imagen " + INVENTORY_IMAGE + ". Constrúyela desde la raíz del repo: "
                    + "docker build --build-context contracts=contracts -t " + INVENTORY_IMAGE + " inventory-service",
                    missing);
        }
    }

    private final WebClient order;
    private final WebClient inventory = WebClient.create(inventoryUrl());

    OrderInventorySystemIT(@Value("${local.server.port}") int port) {
        this.order = WebClient.create("http://localhost:" + port + "/services-order");
    }

    @Test
    void duplicatedConcurrentConfirmationsNeverDecreaseStockTwice() {
        StepVerifier.create(inventory.post().uri("/inventories").contentType(MediaType.APPLICATION_JSON)
                        .bodyValue(Map.of("idProduct", PRODUCT, "nameProduct", "Producto de sistema", "price", 10.00,
                                "stock", STOCK))
                        .retrieve().toBodilessEntity())
                .assertNext(created -> assertThat(created.getStatusCode().value()).isEqualTo(201))
                .expectComplete().verify(TIMEOUT);

        List<Long> ids = Flux.range(0, ORDERS)
                .concatMap(ignored -> order.post().uri("/orders").contentType(MediaType.APPLICATION_JSON)
                        .bodyValue(Map.of("codeProduct", PRODUCT, "quantity", 1))
                        .retrieve().bodyToMono(OrderResponse.class).map(OrderResponse::getId))
                .collectList().block(TIMEOUT);
        assertThat(ids).hasSize(ORDERS);

        // Each order confirmed twice, all 30 PUTs in flight at once: the duplicates race on the same order
        // (optimistic locking) and reach Inventory with the same Idempotency-Key.
        var statuses = Flux.fromIterable(Objects.requireNonNull(ids)).concatMap(id -> Flux.just(id, id))
                .flatMap(id -> order.put().uri("/orders/{id}", id)
                        .exchangeToMono(response -> response.releaseBody().thenReturn(response.statusCode().value())),
                        2 * ORDERS)
                .collectList();
        StepVerifier.create(statuses)
                // 200 completed, or 409: rejected (INSUFFICIENT_STOCK) or the duplicate saw it already canceled.
                .assertNext(codes -> assertThat(codes).hasSize(2 * ORDERS).containsOnly(200, 409))
                .expectComplete().verify(TIMEOUT);

        var orders = Flux.fromIterable(ids)
                .flatMap(id -> order.get().uri("/orders/{id}", id).retrieve().bodyToMono(OrderResponse.class))
                .collectList().block(TIMEOUT);
        Map<String, Long> byStatus = Objects.requireNonNull(orders).stream()
                .collect(Collectors.groupingBy(found -> found.getStatus().getValue(), Collectors.counting()));
        assertThat(byStatus).containsOnly(Map.entry("completed", (long) STOCK), Map.entry("canceled", (long) ORDERS - STOCK));
        assertThat(orders).filteredOn(found -> found.getStatus().getValue().equals("canceled"))
                .allSatisfy(canceled -> assertThat(Objects.requireNonNull(canceled.getCancelReason()).getValue())
                        .isEqualTo("INSUFFICIENT_STOCK"));

        StepVerifier.create(inventory.get().uri("/inventories/{code}", PRODUCT).retrieve()
                        .bodyToMono(new org.springframework.core.ParameterizedTypeReference<Map<String, Object>>() { })
                        .map(product -> product.get("stock")))
                .expectNext(0)
                .expectComplete().verify(TIMEOUT);
    }
}

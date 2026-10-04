package com.geovannycode.order.order.api;

import java.time.Duration;
import java.util.Objects;

import com.geovannycode.order.TestcontainersConfiguration;
import com.github.tomakehurst.wiremock.junit5.WireMockExtension;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.anyRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.anyUrl;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.put;
import static com.github.tomakehurst.wiremock.client.WireMock.putRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.assertj.core.api.Assertions.assertThat;

// End to end over HTTP: Order on a random port with PostgreSQL (Testcontainers); WireMock plays Inventory.
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Import(TestcontainersConfiguration.class)
@ActiveProfiles("test")
final class OrderApiIT {

    @RegisterExtension
    static final WireMockExtension INVENTORY = WireMockExtension.newInstance()
            .options(wireMockConfig().dynamicPort()).build();

    private static final String ORDERS = "/services-order/orders";

    @DynamicPropertySource
    static void inventoryBaseUrl(DynamicPropertyRegistry registry) {
        registry.add("inventory.client.base-url", () -> INVENTORY.baseUrl() + "/services-inventory");
    }

    private final WebTestClient client;
    private final org.springframework.web.reactive.function.client.WebClient concurrentClient;
    private final CircuitBreakerRegistry circuitBreakers;

    @Autowired
    OrderApiIT(@Value("${local.server.port}") int port, CircuitBreakerRegistry circuitBreakers) {
        this.client = WebTestClient.bindToServer().baseUrl("http://localhost:" + port)
                .responseTimeout(Duration.ofSeconds(20)).build();
        this.concurrentClient = org.springframework.web.reactive.function.client.WebClient.create("http://localhost:" + port);
        this.circuitBreakers = circuitBreakers;
    }

    @BeforeEach
    void reset() {
        INVENTORY.resetAll();
        circuitBreakers.circuitBreaker("inventory").reset();
    }

    @Test
    void createConfirmAndRepeatTheConfirmationWithoutDecreasingTwice() {
        inventoryAnswers("AC-1550", 200);
        long id = create("AC-1550", 2);

        confirm(id).expectStatus().isOk().expectBody().jsonPath("$.status").isEqualTo("completed");
        confirm(id).expectStatus().isOk().expectBody().jsonPath("$.status").isEqualTo("completed");

        INVENTORY.verify(1, putRequestedFor(urlEqualTo("/services-inventory/inventories/AC-1550"))
                .withHeader("Idempotency-Key", equalTo("order-" + id)));
    }

    @Test
    void lowercaseCodeIsStoredAndSentInTheUppercaseFormInventoryAccepts() {
        inventoryAnswers("AC-1551", 200);
        long id = create("ac-1551", 1);
        client.get().uri(ORDERS + "/" + id).exchange().expectBody().jsonPath("$.codeProduct").isEqualTo("AC-1551");
        confirm(id).expectStatus().isOk();
        INVENTORY.verify(1, putRequestedFor(urlEqualTo("/services-inventory/inventories/AC-1551")));
    }

    @Test
    void rejectionCancelsTheOrderPersistentlyAndLaterConfirmationsAreOrderCanceled() {
        inventoryAnswers("NO-EXISTE", 404);
        long id = create("NO-EXISTE", 1);

        problem(confirm(id), 409, "order-rejected").jsonPath("$.reason").isEqualTo("PRODUCT_NOT_FOUND");
        client.get().uri(ORDERS + "/" + id).exchange().expectStatus().isOk().expectBody()
                .jsonPath("$.status").isEqualTo("canceled").jsonPath("$.cancelReason").isEqualTo("PRODUCT_NOT_FOUND");

        problem(confirm(id), 409, "order-canceled").jsonPath("$.reason").isEqualTo("ORDER_CANCELED");
        INVENTORY.verify(1, anyRequestedFor(anyUrl()));
    }

    @Test
    void insufficientStockIs409AndCanceled() {
        inventoryAnswers("LOW-1", 409);
        long id = create("LOW-1", 5);
        problem(confirm(id), 409, "order-rejected").jsonPath("$.reason").isEqualTo("INSUFFICIENT_STOCK");
        client.get().uri(ORDERS + "/" + id).exchange().expectBody().jsonPath("$.cancelReason").isEqualTo("INSUFFICIENT_STOCK");
    }

    @Test
    void unavailableInventoryAnswers503AndKeepsTheOrderPending() {
        inventoryAnswers("AC-1552", 500);
        long id = create("AC-1552", 1);
        var response = confirm(id);
        response.expectHeader().valueEquals("Retry-After", "30");
        problem(response, 503, "inventory-unavailable");
        client.get().uri(ORDERS + "/" + id).exchange().expectBody().jsonPath("$.status").isEqualTo("pending");

        inventoryAnswers("AC-1552", 200);
        confirm(id).expectStatus().isOk().expectBody().jsonPath("$.status").isEqualTo("completed");
    }

    @Test
    void concurrentConfirmationsBothSucceedWithASingleLogicalDecrease() {
        INVENTORY.stubFor(put(urlEqualTo("/services-inventory/inventories/AC-1553"))
                .willReturn(json(200).withFixedDelay(200)));
        long id = create("AC-1553", 1);

        // WebClient (non-blocking) so both PUTs are really in flight together; one of them loses the version race.
        var statuses = Flux.range(0, 2).flatMap(ignored -> concurrentClient.put().uri(ORDERS + "/" + id)
                .exchangeToMono(response -> response.releaseBody().thenReturn(response.statusCode().value())), 2)
                .collectList();
        StepVerifier.create(statuses).assertNext(codes -> assertThat(codes).containsExactly(200, 200))
                .expectComplete().verify(Duration.ofSeconds(20));

        client.get().uri(ORDERS + "/" + id).exchange().expectBody().jsonPath("$.status").isEqualTo("completed");
        // Every call Inventory saw carried the same key, so it decreased the stock once.
        INVENTORY.findAll(putRequestedFor(urlEqualTo("/services-inventory/inventories/AC-1553")))
                .forEach(request -> assertThat(request.getHeader("Idempotency-Key")).isEqualTo("order-" + id));
    }

    @Test
    void missingOrderIs404Problem() {
        problem(client.get().uri(ORDERS + "/999999").exchange(), 404, "order-not-found")
                .jsonPath("$.instance").isEqualTo(ORDERS + "/999999");
        problem(client.put().uri(ORDERS + "/999999").exchange(), 404, "order-not-found");
    }

    @Test
    void listFiltersByStatusInEveryRepresentation() {
        inventoryAnswers("AC-1554", 200);
        long pending = create("AC-1554", 1);
        long completed = create("AC-1554", 1);
        confirm(completed).expectStatus().isOk();

        var pendingIds = client.get().uri(ORDERS + "?status=pending").accept(MediaType.APPLICATION_JSON).exchange()
                .expectStatus().isOk().expectBodyList(com.geovannycode.order.generated.dto.OrderResponse.class)
                .returnResult().getResponseBody().stream().map(order -> {
                    assertThat(order.getStatus().getValue()).isEqualTo("pending");
                    return order.getId();
                }).toList();
        assertThat(pendingIds).contains(pending).doesNotContain(completed);

        client.get().uri(ORDERS + "?status=completed").accept(MediaType.APPLICATION_NDJSON).exchange()
                .expectStatus().isOk().expectHeader().contentTypeCompatibleWith(MediaType.APPLICATION_NDJSON);
        client.get().uri(ORDERS).accept(MediaType.TEXT_EVENT_STREAM).exchange()
                .expectStatus().isOk().expectHeader().contentTypeCompatibleWith(MediaType.TEXT_EVENT_STREAM);
    }

    @Test
    void swaggerUiServesTheStaticContract() {
        client.get().uri("/services-order/v3/api-docs/swagger-config").exchange().expectStatus().isOk()
                .expectBody().jsonPath("$.url").isEqualTo("/services-order/openapi/services-order.yaml");
        var contract = client.get().uri("/services-order/openapi/services-order.yaml").exchange().expectStatus().isOk()
                .expectBody(String.class).returnResult().getResponseBody();
        assertThat(Objects.requireNonNull(contract)).contains("title: Servicio de Órdenes", "version: 1.1.0");
    }

    private long create(String codeProduct, int quantity) {
        var result = client.post().uri(ORDERS).contentType(MediaType.APPLICATION_JSON)
                .bodyValue("{\"codeProduct\":\"" + codeProduct + "\",\"quantity\":" + quantity + "}").exchange()
                .expectStatus().isCreated()
                .expectBody(com.geovannycode.order.generated.dto.OrderResponse.class).returnResult();
        var order = Objects.requireNonNull(result.getResponseBody());
        assertThat(result.getResponseHeaders().getLocation()).hasToString(ORDERS + "/" + order.getId());
        assertThat(order.getStatus().getValue()).isEqualTo("pending");
        return order.getId();
    }

    private WebTestClient.ResponseSpec confirm(long id) {
        return client.put().uri(ORDERS + "/" + id).exchange();
    }

    private static void inventoryAnswers(String code, int status) {
        INVENTORY.stubFor(put(urlEqualTo("/services-inventory/inventories/" + code)).willReturn(json(status)));
    }

    private static com.github.tomakehurst.wiremock.client.ResponseDefinitionBuilder json(int status) {
        String body = status == 200
                ? "{\"idProduct\":\"X\",\"nameProduct\":\"X\",\"price\":1.00,\"stock\":1}"
                : "{\"type\":\"https://codearti.com/problems/test\",\"title\":\"test\",\"status\":" + status + "}";
        return aResponse().withStatus(status).withHeader("Content-Type",
                status == 200 ? "application/json" : "application/problem+json").withBody(body);
    }

    private static WebTestClient.BodyContentSpec problem(WebTestClient.ResponseSpec response, int status, String slug) {
        return response.expectStatus().isEqualTo(status)
                .expectHeader().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON)
                .expectBody().jsonPath("$.type").isEqualTo("https://codearti.com/problems/" + slug)
                .jsonPath("$.status").isEqualTo(status).jsonPath("$.timestamp").exists();
    }
}

package com.geovannycode.inventory.inventory.api;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.stream.IntStream;

import com.geovannycode.inventory.TestcontainersConfiguration;
import com.geovannycode.inventory.inventory.api.dto.InventoryRequest;
import com.geovannycode.inventory.inventory.api.dto.InventoryResponse;
import com.geovannycode.inventory.inventory.api.dto.OrderInvRequest;
import com.geovannycode.inventory.inventory.application.InventoryService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Import;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("local")
@Import(TestcontainersConfiguration.class)
final class InventoryApiIT {

    private static final String PATH = "/services-inventory/inventories";
    private final WebTestClient client;
    private final InventoryService service;

    @Autowired
    InventoryApiIT(@Value("${local.server.port}") int port, InventoryService service) {
        this.client = WebTestClient.bindToServer().baseUrl("http://localhost:" + port)
                .responseTimeout(Duration.ofSeconds(15)).build();
        this.service = service;
    }

    @Test
    void readsSeedAndReturnsProblemForMissingProduct() {
        client.get().uri(PATH + "/AC-1550").exchange().expectStatus().isOk()
                .expectBody().jsonPath("$.idProduct").isEqualTo("AC-1550")
                .jsonPath("$.price").isEqualTo(123.50).jsonPath("$.stock").isEqualTo(50)
                .jsonPath("$.id").doesNotExist();
        client.get().uri(PATH + "/NOEXISTE").exchange().expectStatus().isNotFound()
                .expectHeader().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON)
                .expectBody().jsonPath("$.status").isEqualTo(404)
                .jsonPath("$.type").isEqualTo("https://geovannycode.com/problems/product-not-found")
                .jsonPath("$.instance").isEqualTo(PATH + "/NOEXISTE")
                .jsonPath("$.timestamp").value(value -> Instant.parse((String) value));
    }

    @Test
    void registersAndRejectsDuplicateCode() {
        String code = code();
        var request = new InventoryRequest(code, "Lentes", new BigDecimal("123.5"), 50);
        client.post().uri(PATH).bodyValue(request).exchange().expectStatus().isCreated()
                .expectHeader().valueEquals(HttpHeaders.LOCATION, PATH + "/" + code)
                .expectBody().jsonPath("$.idProduct").isEqualTo(code)
                .jsonPath("$.price").isEqualTo(123.5).jsonPath("$.stock").isEqualTo(50);
        client.post().uri(PATH).bodyValue(request).exchange().expectStatus().isEqualTo(409)
                .expectHeader().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON)
                .expectBody().jsonPath("$.type").isEqualTo("https://geovannycode.com/problems/duplicate-product")
                .jsonPath("$.status").isEqualTo(409).jsonPath("$.timestamp").exists();
    }

    @Test
    void rejectsInvalidStockAndPriceWithFieldErrors() {
        client.post().uri(PATH).bodyValue(new InventoryRequest(code(), "Lentes", new BigDecimal("123.456"), 0))
                .exchange().expectStatus().isBadRequest()
                .expectHeader().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON)
                .expectBody().jsonPath("$.errors[*].field").value(fields ->
                        assertThat(((java.util.List<?>) fields).stream().map(Object::toString).toList())
                                .contains("stock", "price"))
                .jsonPath("$.type").isEqualTo("https://geovannycode.com/problems/validation-error");
    }

    @Test
    void rejectsInvalidPathAndDemoDelay() {
        client.get().uri(PATH + "/INVALID_CODE").exchange().expectStatus().isBadRequest()
                .expectBody().jsonPath("$.errors[0].field").isEqualTo("productId");
        for (String accept : new String[]{"application/json", "application/x-ndjson", "text/event-stream"}) {
            client.get().uri(PATH + "?delayMs=2001").header(HttpHeaders.ACCEPT, accept)
                    .exchange().expectStatus().isBadRequest()
                    .expectBody().jsonPath("$.errors[0].field").isEqualTo("delayMs");
        }
    }

    @Test
    void rejectsMalformedJsonAndMissingBody() {
        client.post().uri(PATH).contentType(MediaType.APPLICATION_JSON).bodyValue("{\"stock\":")
                .exchange().expectStatus().isBadRequest().expectBody()
                .jsonPath("$.type").isEqualTo("https://geovannycode.com/problems/invalid-request")
                .jsonPath("$.timestamp").exists().jsonPath("$.trace").doesNotExist();
        client.put().uri(PATH + "/AC-1550").contentType(MediaType.APPLICATION_JSON)
                .exchange().expectStatus().isBadRequest();
    }

    @Test
    void validatesQuantityAndDistinguishesMissingProduct() {
        client.put().uri(PATH + "/AC-1550").bodyValue(new OrderInvRequest(6))
                .exchange().expectStatus().isBadRequest().expectBody()
                .jsonPath("$.errors[0].field").isEqualTo("orderCount");
        client.put().uri(PATH + "/NOEXISTE").bodyValue(new OrderInvRequest(1))
                .exchange().expectStatus().isNotFound().expectBody().jsonPath("$.status").isEqualTo(404);
    }

    @Test
    void decreasesToZeroThenReturnsConflictWithoutChangingStock() {
        String code = register(50);
        client.get().uri(PATH + "/" + code).exchange().expectStatus().isOk()
                .expectBody().jsonPath("$.stock").isEqualTo(50);
        for (int expected = 45; expected >= 0; expected -= 5) {
            client.put().uri(PATH + "/" + code).bodyValue(new OrderInvRequest(5)).exchange()
                    .expectStatus().isOk().expectBody().jsonPath("$.stock").isEqualTo(expected);
        }
        client.put().uri(PATH + "/" + code).bodyValue(new OrderInvRequest(1)).exchange()
                .expectStatus().isEqualTo(409).expectBody()
                .jsonPath("$.type").isEqualTo("https://geovannycode.com/problems/insufficient-stock")
                .jsonPath("$.timestamp").exists();
        client.get().uri(PATH + "/" + code).exchange().expectStatus().isOk()
                .expectBody().jsonPath("$.stock").isEqualTo(0);
    }

    @Test
    void transactionKeepsEachReadConsistentWithItsConcurrentDecrement() {
        String code = register(10);
        StepVerifier.create(Flux.range(0, 10).flatMap(ignored -> service.decreaseStock(code, 1, null), 10)
                        .map(InventoryResponse::stock).collectList())
                .assertNext(stocks -> assertThat(stocks).containsExactlyInAnyOrderElementsOf(
                        IntStream.range(0, 10).boxed().toList()))
                .expectComplete().verify(Duration.ofSeconds(10));
    }

    @Test
    void rejectsLowercaseProductCodesInsteadOfTreatingThemAsAliases() {
        client.get().uri(PATH + "/ac-1550").exchange().expectStatus().isBadRequest()
                .expectBody().jsonPath("$.errors[0].field").isEqualTo("productId");
        client.put().uri(PATH + "/ac-1550").bodyValue(new OrderInvRequest(1)).exchange().expectStatus().isBadRequest();
        client.post().uri(PATH).bodyValue(new InventoryRequest("prd-" + UUID.randomUUID(), "Lentes", BigDecimal.ONE, 1))
                .exchange().expectStatus().isBadRequest()
                .expectBody().jsonPath("$.errors[0].field").isEqualTo("idProduct");
        client.get().uri(PATH + "/AC-1550").exchange().expectStatus().isOk()
                .expectBody().jsonPath("$.stock").isEqualTo(50);
    }

    @Test
    void retryWithSameIdempotencyKeyDecreasesOnlyOnce() {
        String code = register(10);
        String key = "it-" + UUID.randomUUID();
        for (int attempt = 0; attempt < 2; attempt++) {
            decrease(code, 2, key).expectStatus().isOk().expectBody().jsonPath("$.stock").isEqualTo(8);
        }
        client.get().uri(PATH + "/" + code).exchange().expectBody().jsonPath("$.stock").isEqualTo(8);
        decrease(code, 3, key).expectStatus().isEqualTo(422)
                .expectHeader().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON)
                .expectBody().jsonPath("$.type").isEqualTo("https://geovannycode.com/problems/idempotency-key-reused");
        client.get().uri(PATH + "/" + code).exchange().expectBody().jsonPath("$.stock").isEqualTo(8);
    }

    @Test
    void failedDecreaseDoesNotConsumeIdempotencyKey() {
        String code = register(1);
        String key = "it-" + UUID.randomUUID();
        decrease(code, 2, key).expectStatus().isEqualTo(409);
        decrease(code, 1, key).expectStatus().isOk().expectBody().jsonPath("$.stock").isEqualTo(0);
    }

    @Test
    void concurrentRetriesWithSameIdempotencyKeyDecreaseOnlyOnce() {
        String code = register(10);
        String key = "it-" + UUID.randomUUID();
        StepVerifier.create(Flux.range(0, 10).flatMap(ignored -> service.decreaseStock(code, 1, key), 10)
                        .map(InventoryResponse::stock).collectList())
                .assertNext(stocks -> assertThat(stocks).hasSize(10).containsOnly(9))
                .expectComplete().verify(Duration.ofSeconds(15));
        client.get().uri(PATH + "/" + code).exchange().expectBody().jsonPath("$.stock").isEqualTo(9);
    }

    @Test
    void paginatesTheCatalogInCodeOrder() {
        var firstPage = client.get().uri(PATH + "?page=0&size=2").accept(MediaType.APPLICATION_JSON).exchange()
                .expectStatus().isOk().expectBodyList(InventoryResponse.class).returnResult().getResponseBody();
        var secondPage = client.get().uri(PATH + "?page=1&size=2").accept(MediaType.APPLICATION_JSON).exchange()
                .expectStatus().isOk().expectBodyList(InventoryResponse.class).returnResult().getResponseBody();
        assertThat(firstPage).extracting(InventoryResponse::idProduct).hasSize(2).isSorted();
        assertThat(secondPage).extracting(InventoryResponse::idProduct).hasSize(2).isSorted();
        assertThat(secondPage.getFirst().idProduct()).isGreaterThan(firstPage.getLast().idProduct());
        client.get().uri(PATH + "?size=101").exchange().expectStatus().isBadRequest()
                .expectBody().jsonPath("$.errors[0].field").isEqualTo("size");
    }

    @Test
    void returnsJsonArraySortedByCode() {
        client.get().uri(PATH).accept(MediaType.APPLICATION_JSON).exchange().expectStatus().isOk()
                .expectBodyList(InventoryResponse.class).value(products -> {
                    assertThat(products).isNotEmpty();
                    assertThat(products).extracting(InventoryResponse::idProduct).isSorted();
                });
    }

    @Test
    void streamsNdjsonIncrementally() {
        var result = client.get().uri(PATH + "?delayMs=100").accept(MediaType.APPLICATION_NDJSON)
                .exchange().expectStatus().isOk().expectHeader().contentTypeCompatibleWith(MediaType.APPLICATION_NDJSON)
                .returnResult(InventoryResponse.class);
        StepVerifier.create(result.getResponseBody().take(3).elapsed())
                .assertNext(first -> assertThat(first.getT2().idProduct()).isEqualTo("AC-1550"))
                .assertNext(next -> assertThat(next.getT1()).isGreaterThanOrEqualTo(50L))
                .assertNext(next -> assertThat(next.getT1()).isGreaterThanOrEqualTo(50L))
                .expectComplete().verify(Duration.ofSeconds(5));
    }

    @Test
    void streamsNamedEventsWithProductId() {
        var type = new ParameterizedTypeReference<ServerSentEvent<InventoryResponse>>() { };
        var result = client.get().uri(PATH + "?delayMs=100").accept(MediaType.TEXT_EVENT_STREAM)
                .exchange().expectStatus().isOk().returnResult(type);
        StepVerifier.create(result.getResponseBody().take(2).elapsed())
                .assertNext(first -> {
                    assertThat(first.getT2().id()).isEqualTo("AC-1550");
                    assertThat(first.getT2().event()).isEqualTo("inventory");
                    assertThat(first.getT2().data()).isNotNull();
                })
                .assertNext(second -> {
                    assertThat(second.getT1()).isGreaterThanOrEqualTo(50L);
                    assertThat(second.getT2().id()).isEqualTo("AC-1551");
                }).expectComplete().verify(Duration.ofSeconds(5));
    }

    @Test
    void allowsLocalViewerOriginsAndRejectsOthers() {
        for (String origin : new String[]{"http://localhost:5500", "http://127.0.0.1:8000"}) {
            client.options().uri(PATH).header(HttpHeaders.ORIGIN, origin)
                    .header(HttpHeaders.ACCESS_CONTROL_REQUEST_METHOD, "GET")
                    .exchange().expectStatus().isOk()
                    .expectHeader().valueEquals(HttpHeaders.ACCESS_CONTROL_ALLOW_ORIGIN, origin);
        }
        client.options().uri(PATH).header(HttpHeaders.ORIGIN, "https://example.com")
                .header(HttpHeaders.ACCESS_CONTROL_REQUEST_METHOD, "GET")
                .exchange().expectStatus().isForbidden()
                .expectHeader().doesNotExist(HttpHeaders.ACCESS_CONTROL_ALLOW_ORIGIN);
    }

    @Test
    void exposesSwaggerAndOpenApiUnderServicePrefix() {
        client.get().uri("/services-inventory/swagger-ui.html").exchange().expectStatus().is3xxRedirection();
        client.get().uri("/services-inventory/v3/api-docs").exchange().expectStatus().isOk()
                .expectBody().jsonPath("$.info.version").isEqualTo("2.1.0")
                .jsonPath("$.servers[0].url").isEqualTo("/services-inventory")
                .jsonPath("$.paths['/inventories'].post.responses['201']").exists()
                .jsonPath("$.paths['/inventories/{productId}'].put.responses['409']").exists();
    }

    private WebTestClient.ResponseSpec decrease(String code, int count, String idempotencyKey) {
        return client.put().uri(PATH + "/" + code).header("Idempotency-Key", idempotencyKey)
                .bodyValue(new OrderInvRequest(count)).exchange();
    }

    private String register(int stock) {
        String code = code();
        client.post().uri(PATH).bodyValue(new InventoryRequest(code, "Producto HTTP", new BigDecimal("1.00"), stock))
                .exchange().expectStatus().isCreated();
        return code;
    }

    private String code() {
        return "HTTP-" + UUID.randomUUID().toString().toUpperCase();
    }
}

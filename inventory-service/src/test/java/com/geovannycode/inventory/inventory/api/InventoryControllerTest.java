package com.geovannycode.inventory.inventory.api;

import java.math.BigDecimal;
import java.time.Duration;
import com.geovannycode.inventory.inventory.api.dto.*;
import com.geovannycode.inventory.inventory.application.InventoryService;
import com.geovannycode.inventory.inventory.domain.*;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webflux.test.autoconfigure.WebFluxTest;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.MediaType;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

// El cliente del slice no atraviesa el servidor que aplica el base-path; InventoryApiIT lo verifica.
@WebFluxTest(controllers = InventoryController.class, properties = "spring.webflux.base-path=")
final class InventoryControllerTest {
    @Autowired private WebTestClient client;
    @MockitoBean private InventoryService service;
    private static final String PATH = "/inventories";
    private static final InventoryResponse FIRST = new InventoryResponse("PRD-1", "Lentes", new BigDecimal("123.50"), 10);
    private static final InventoryResponse SECOND = new InventoryResponse("PRD-2", "Otro", new BigDecimal("1.00"), 0);
    private static InventoryRequest request(int stock) {
        return new InventoryRequest("PRD-1", "Lentes", new BigDecimal("123.50"), stock);
    }
    @Test void listsJsonArray() {
        when(service.findAll()).thenReturn(Flux.just(FIRST, SECOND));
        client.get().uri(PATH).accept(MediaType.APPLICATION_JSON).exchange().expectStatus().isOk()
                .expectHeader().contentTypeCompatibleWith(MediaType.APPLICATION_JSON)
                .expectBodyList(InventoryResponse.class).contains(FIRST, SECOND).hasSize(2);
    }
    @Test void createsProductWithLocation() {
        when(service.register(request(10))).thenReturn(Mono.just(FIRST));
        client.post().uri(PATH).bodyValue(request(10)).exchange().expectStatus().isCreated()
                .expectHeader().valueEquals("Location", PATH + "/PRD-1")
                .expectHeader().contentTypeCompatibleWith(MediaType.APPLICATION_JSON)
                .expectBody(InventoryResponse.class).isEqualTo(FIRST);
    }
    @Test void readsProduct() {
        when(service.findByCode("PRD-1")).thenReturn(Mono.just(FIRST));
        client.get().uri(PATH + "/PRD-1").exchange().expectStatus().isOk()
                .expectHeader().contentTypeCompatibleWith(MediaType.APPLICATION_JSON)
                .expectBody(InventoryResponse.class).isEqualTo(FIRST);
    }
    @Test void decreasesStock() {
        when(service.decreaseStock("PRD-1", 1)).thenReturn(Mono.just(FIRST));
        client.put().uri(PATH + "/PRD-1").bodyValue(new OrderInvRequest(1)).exchange().expectStatus().isOk()
                .expectHeader().contentTypeCompatibleWith(MediaType.APPLICATION_JSON)
                .expectBody(InventoryResponse.class).isEqualTo(FIRST);
    }
    @Test void reportsMissingProduct() {
        when(service.findByCode("PRD-1")).thenReturn(Mono.error(new ProductNotFoundException("PRD-1")));
        assertProblem(client.get().uri(PATH + "/PRD-1").exchange(), 404, "product-not-found", "Producto no encontrado");
    }
    @Test void reportsDuplicateProduct() {
        when(service.register(request(10))).thenReturn(Mono.error(new DuplicateProductException("PRD-1")));
        assertProblem(client.post().uri(PATH).bodyValue(request(10)).exchange(), 409, "duplicate-product", "Producto duplicado");
    }
    @Test void reportsInsufficientStock() {
        when(service.decreaseStock("PRD-1", 1)).thenReturn(Mono.error(new InsufficientStockException("PRD-1", 1)));
        assertProblem(client.put().uri(PATH + "/PRD-1").bodyValue(new OrderInvRequest(1)).exchange(), 409,
                "insufficient-stock", "Stock insuficiente");
    }
    @Test void rejectsInvalidStockBeforeCallingService() {
        assertProblem(client.post().uri(PATH).bodyValue(request(0)).exchange(), 400, "validation-error", "Solicitud inválida")
                .jsonPath("$.errors[0].field").isEqualTo("stock").jsonPath("$.errors[0].message").isNotEmpty();
        verifyNoInteractions(service);
    }
    @Test void rejectsInvalidOrderCount() {
        assertProblem(client.put().uri(PATH + "/PRD-1").bodyValue(new OrderInvRequest(6)).exchange(), 400,
                "validation-error", "Solicitud inválida").jsonPath("$.errors[0].field").isEqualTo("orderCount");
        verifyNoInteractions(service);
    }
    @Test void rejectsMalformedJson() {
        assertProblem(client.post().uri(PATH).contentType(MediaType.APPLICATION_JSON).bodyValue("{").exchange(),
                400, "invalid-request", "Solicitud inválida");
    }
    @Test void rejectsUnsupportedHttpMethod() {
        assertProblem(client.delete().uri(PATH + "/PRD-1").exchange(), 405, "http-405", "Solicitud rechazada");
    }
    @Test void streamsAllNdjsonElements() {
        when(service.findAll()).thenReturn(Flux.just(FIRST, SECOND));
        var result = client.get().uri(PATH).accept(MediaType.APPLICATION_NDJSON).exchange().expectStatus().isOk()
                .expectHeader().contentTypeCompatibleWith(MediaType.APPLICATION_NDJSON).returnResult(InventoryResponse.class);
        StepVerifier.create(result.getResponseBody()).expectNext(FIRST, SECOND).expectComplete().verify(Duration.ofSeconds(5));
    }
    @Test void streamsEventIdsNamesAndData() {
        when(service.findAll()).thenReturn(Flux.just(FIRST, SECOND));
        var type = new ParameterizedTypeReference<ServerSentEvent<InventoryResponse>>() { };
        var result = client.get().uri(PATH).accept(MediaType.TEXT_EVENT_STREAM).exchange().expectStatus().isOk()
                .expectHeader().contentTypeCompatibleWith(MediaType.TEXT_EVENT_STREAM).returnResult(type);
        StepVerifier.create(result.getResponseBody())
                .assertNext(event -> assertEvent(event, FIRST)).assertNext(event -> assertEvent(event, SECOND))
                .expectComplete().verify(Duration.ofSeconds(5));
    }
    private static void assertEvent(ServerSentEvent<InventoryResponse> event, InventoryResponse expected) {
        assertThat(event.id()).isEqualTo(expected.idProduct());
        assertThat(event.event()).isEqualTo("inventory");
        assertThat(event.data()).isEqualTo(expected);
    }
    private static WebTestClient.BodyContentSpec assertProblem(WebTestClient.ResponseSpec result, int status, String slug, String title) {
        return result.expectStatus().isEqualTo(status).expectHeader().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON)
                .expectBody().jsonPath("$.type").isEqualTo("https://codearti.com/problems/" + slug)
                .jsonPath("$.title").isEqualTo(title).jsonPath("$.status").isEqualTo(status)
                .jsonPath("$.instance").exists().jsonPath("$.timestamp").exists();
    }
}

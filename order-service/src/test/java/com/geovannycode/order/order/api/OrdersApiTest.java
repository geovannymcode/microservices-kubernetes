package com.geovannycode.order.order.api;

import java.lang.reflect.Method;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;

import com.geovannycode.order.generated.api.OrdersApiDelegate;
import com.geovannycode.order.generated.config.EnumConverterConfiguration;
import com.geovannycode.order.generated.dto.OrderRequest;
import com.geovannycode.order.generated.dto.OrderResponse;
import com.geovannycode.order.generated.dto.OrderStatus;
import com.geovannycode.order.order.application.OrderService;
import com.geovannycode.order.order.domain.CancelReason;
import com.geovannycode.order.order.domain.IllegalOrderStateException;
import com.geovannycode.order.order.domain.InventoryUnavailableException;
import com.geovannycode.order.order.domain.OrderNotFoundException;
import com.geovannycode.order.order.domain.OrderRejectedException;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webflux.test.autoconfigure.WebFluxTest;
import org.springframework.context.annotation.Import;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.MediaType;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

// The slice client does not go through the server's base path; OrderApiIT covers /services-order.
@WebFluxTest(properties = "spring.webflux.base-path=")
// The generated EnumConverterConfiguration is a @Configuration, which slices do not pick up on their own.
@Import({OrdersApiDelegateImpl.class, EnumConverterConfiguration.class})
final class OrdersApiTest {

    private static final OffsetDateTime CREATED = OffsetDateTime.of(2026, 10, 3, 19, 0, 0, 0, ZoneOffset.UTC);
    private static final OrderResponse FIRST = response(1L, OrderStatus.PENDING);
    private static final OrderResponse SECOND = response(2L, OrderStatus.COMPLETED);

    @Autowired private WebTestClient client;
    @MockitoBean private OrderService orders;

    private static OrderResponse response(long id, OrderStatus status) {
        return new OrderResponse(id, "AC-1550", 2, status, CREATED);
    }

    @Test
    void overridesEveryGeneratedOperation() throws NoSuchMethodException {
        // skipDefaultInterface must stay false with delegatePattern, so the compiler cannot enforce this.
        for (Method operation : OrdersApiDelegate.class.getMethods()) {
            if (operation.isDefault() && !operation.getName().equals("getRequest")) {
                var implementation = OrdersApiDelegateImpl.class.getMethod(operation.getName(), operation.getParameterTypes());
                assertThat(implementation.getDeclaringClass()).as(operation.getName()).isEqualTo(OrdersApiDelegateImpl.class);
            }
        }
        assertThat(Arrays.stream(OrdersApiDelegate.class.getMethods()).map(Method::getName))
                .contains("listOrders", "createOrder", "getOrder", "updateOrder");
    }

    @Test
    void createAnswers201WithLocation() {
        when(orders.create(any(OrderRequest.class))).thenReturn(Mono.just(FIRST));
        client.post().uri("/orders").contentType(MediaType.APPLICATION_JSON)
                .bodyValue("{\"codeProduct\":\"AC-1550\",\"quantity\":2}").exchange()
                .expectStatus().isCreated()
                .expectHeader().valueEquals("Location", "/orders/1")
                .expectHeader().contentTypeCompatibleWith(MediaType.APPLICATION_JSON)
                .expectBody().jsonPath("$.status").isEqualTo("pending").jsonPath("$.id").isEqualTo(1);
    }

    @Test
    void listsJsonArrayAndFiltersByStatus() {
        when(orders.findAll(null)).thenReturn(Flux.just(FIRST, SECOND));
        when(orders.findAll(com.geovannycode.order.order.domain.OrderStatus.COMPLETED)).thenReturn(Flux.just(SECOND));
        client.get().uri("/orders").accept(MediaType.APPLICATION_JSON).exchange().expectStatus().isOk()
                .expectHeader().contentTypeCompatibleWith(MediaType.APPLICATION_JSON)
                .expectBodyList(OrderResponse.class).hasSize(2);
        client.get().uri("/orders?status=completed").accept(MediaType.APPLICATION_JSON).exchange().expectStatus().isOk()
                .expectBody().jsonPath("$.length()").isEqualTo(1).jsonPath("$[0].status").isEqualTo("completed");
    }

    @Test
    void streamsNdjson() {
        when(orders.findAll(null)).thenReturn(Flux.just(FIRST, SECOND));
        var result = client.get().uri("/orders").accept(MediaType.APPLICATION_NDJSON).exchange().expectStatus().isOk()
                .expectHeader().contentTypeCompatibleWith(MediaType.APPLICATION_NDJSON).returnResult(OrderResponse.class);
        StepVerifier.create(result.getResponseBody().map(OrderResponse::getId)).expectNext(1L, 2L)
                .expectComplete().verify(Duration.ofSeconds(5));
    }

    @Test
    void streamsNamedServerSentEventsWithOrderIds() {
        when(orders.findAll(null)).thenReturn(Flux.just(FIRST, SECOND));
        var type = new ParameterizedTypeReference<ServerSentEvent<OrderResponse>>() { };
        var result = client.get().uri("/orders").accept(MediaType.TEXT_EVENT_STREAM).exchange().expectStatus().isOk()
                .expectHeader().contentTypeCompatibleWith(MediaType.TEXT_EVENT_STREAM).returnResult(type);
        StepVerifier.create(result.getResponseBody())
                .assertNext(event -> {
                    assertThat(event.id()).isEqualTo("1");
                    assertThat(event.event()).isEqualTo("order");
                    assertThat(event.data().getCodeProduct()).isEqualTo("AC-1550");
                })
                .assertNext(event -> assertThat(event.id()).isEqualTo("2"))
                .expectComplete().verify(Duration.ofSeconds(5));
    }

    @Test
    void getAnswersTheOrderAsJson() {
        when(orders.findById(2L)).thenReturn(Mono.just(SECOND));
        client.get().uri("/orders/2").exchange().expectStatus().isOk()
                .expectHeader().contentTypeCompatibleWith(MediaType.APPLICATION_JSON)
                .expectBody().jsonPath("$.id").isEqualTo(2).jsonPath("$.codeProduct").isEqualTo("AC-1550")
                .jsonPath("$.quantity").isEqualTo(2).jsonPath("$.status").isEqualTo("completed")
                .jsonPath("$.createdAt").isEqualTo("2026-10-03T19:00:00Z");
    }

    @Test
    void missingOrderIsProblem404() {
        when(orders.findById(999999L)).thenReturn(Mono.error(new OrderNotFoundException(999999)));
        problem(client.get().uri("/orders/999999").exchange(), 404, "order-not-found")
                .jsonPath("$.instance").isEqualTo("/orders/999999");
    }

    @Test
    void confirmAnswersCompletedOrder() {
        when(orders.confirm(2L)).thenReturn(Mono.just(SECOND));
        client.put().uri("/orders/2").exchange().expectStatus().isOk()
                .expectHeader().contentTypeCompatibleWith(MediaType.APPLICATION_JSON)
                .expectBody().jsonPath("$.status").isEqualTo("completed");
    }

    @Test
    void rejectedConfirmationIs409WithReason() {
        when(orders.confirm(1L)).thenReturn(Mono.error(new OrderRejectedException(1, "AC-1550", CancelReason.INSUFFICIENT_STOCK)));
        problem(client.put().uri("/orders/1").exchange(), 409, "order-rejected").jsonPath("$.reason").isEqualTo("INSUFFICIENT_STOCK");
    }

    @Test
    void canceledOrderIs409OrderCanceled() {
        when(orders.confirm(1L)).thenReturn(Mono.error(new IllegalOrderStateException(1L,
                com.geovannycode.order.order.domain.OrderStatus.CANCELED, "confirmar")));
        problem(client.put().uri("/orders/1").exchange(), 409, "order-canceled").jsonPath("$.reason").isEqualTo("ORDER_CANCELED");
    }

    @Test
    void unavailableInventoryIs503WithRetryAfterFromCircuitBreakerWait() {
        when(orders.confirm(1L)).thenReturn(Mono.error(new InventoryUnavailableException(new RuntimeException("down"))));
        var response = client.put().uri("/orders/1").exchange();
        response.expectHeader().valueEquals("Retry-After", "30");
        problem(response, 503, "inventory-unavailable");
    }

    @Test
    void invalidBodyIs400WithFieldErrors() {
        problem(client.post().uri("/orders").contentType(MediaType.APPLICATION_JSON).bodyValue("{\"codeProduct\":null}")
                .exchange(), 400, "validation-error").jsonPath("$.errors[0].field").isEqualTo("codeProduct");
        problem(client.post().uri("/orders").contentType(MediaType.APPLICATION_JSON)
                .bodyValue("{\"codeProduct\":\"AC-1550\",\"quantity\":1.9}").exchange(), 400, "invalid-request");
        problem(client.post().uri("/orders").contentType(MediaType.APPLICATION_JSON).bodyValue("{").exchange(),
                400, "invalid-request");
        verifyNoInteractions(orders);
    }

    @Test
    void invalidPathOrStatusIs400() {
        problem(client.get().uri("/orders/0").exchange(), 400, "validation-error").jsonPath("$.errors[0].field").isEqualTo("orderId");
        problem(client.get().uri("/orders?status=otro").exchange(), 400, "invalid-request");
        verifyNoInteractions(orders);
    }

    @Test
    void unexpectedErrorsAreSanitized500() {
        when(orders.findById(5L)).thenReturn(Mono.error(new IllegalStateException("secret-password")));
        problem(client.get().uri("/orders/5").exchange(), 500, "internal-error")
                .jsonPath("$.detail").value(detail -> assertThat((String) detail).doesNotContain("secret-password"));
    }

    private static WebTestClient.BodyContentSpec problem(WebTestClient.ResponseSpec response, int status, String slug) {
        return response.expectStatus().isEqualTo(status)
                .expectHeader().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON)
                .expectBody().jsonPath("$.type").isEqualTo("https://codearti.com/problems/" + slug)
                .jsonPath("$.status").isEqualTo(status).jsonPath("$.title").exists().jsonPath("$.detail").exists()
                .jsonPath("$.timestamp").exists();
    }
}

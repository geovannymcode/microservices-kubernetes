package com.geovannycode.order.order.api;

import com.geovannycode.order.generated.api.OrdersApiDelegate;
import com.geovannycode.order.generated.dto.OrderRequest;
import com.geovannycode.order.generated.dto.OrderResponse;
import com.geovannycode.order.generated.dto.OrderStatus;
import com.geovannycode.order.order.application.OrderService;
import org.jspecify.annotations.Nullable;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.util.UriComponentsBuilder;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/** HTTP adapter only: the generated OrdersApiController routes and validates; OrderService decides. */
@Service
public class OrdersApiDelegateImpl implements OrdersApiDelegate {

    private final OrderService orders;

    public OrdersApiDelegateImpl(OrderService orders) {
        this.orders = orders;
    }

    @Override
    public Mono<ResponseEntity<Flux<OrderResponse>>> listOrders(@Nullable OrderStatus status, ServerWebExchange exchange) {
        var domainStatus = status == null ? null : com.geovannycode.order.order.domain.OrderStatus.fromValue(status.getValue());
        var found = orders.findAll(domainStatus);
        return Mono.just(ResponseEntity.ok(acceptsEventStream(exchange) ? asNamedEvents(found) : found));
    }

    @Override
    public Mono<ResponseEntity<OrderResponse>> createOrder(Mono<OrderRequest> orderRequest, ServerWebExchange exchange) {
        return orderRequest.flatMap(orders::create).map(order -> ResponseEntity.created(
                UriComponentsBuilder.fromPath(exchange.getRequest().getPath().contextPath().value())
                        .pathSegment("orders", String.valueOf(order.getId())).build().toUri()).body(order));
    }

    @Override
    public Mono<ResponseEntity<OrderResponse>> getOrder(Long orderId, ServerWebExchange exchange) {
        return orders.findById(orderId).map(ResponseEntity::ok);
    }

    @Override
    public Mono<ResponseEntity<OrderResponse>> updateOrder(Long orderId, ServerWebExchange exchange) {
        return orders.confirm(orderId).map(ResponseEntity::ok);
    }

    private static boolean acceptsEventStream(ServerWebExchange exchange) {
        return exchange.getRequest().getHeaders().getAccept().stream()
                .anyMatch(MediaType.TEXT_EVENT_STREAM::equalsTypeAndSubtype);
    }

    /**
     * The generator emits one signature (Flux<OrderResponse>) for JSON, NDJSON and SSE. WebFlux's SSE writer
     * checks each element at runtime and writes ServerSentEvent instances as-is (id, event) while encoding
     * their data as OrderResponse, so the events are passed through this declared type.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private static Flux<OrderResponse> asNamedEvents(Flux<OrderResponse> found) {
        Flux events = found.map(order -> ServerSentEvent.builder(order)
                .id(String.valueOf(order.getId())).event("order").build());
        return (Flux<OrderResponse>) events;
    }
}

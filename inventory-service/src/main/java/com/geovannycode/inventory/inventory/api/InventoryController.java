package com.geovannycode.inventory.inventory.api;

import java.time.Duration;

import com.geovannycode.inventory.inventory.api.dto.InventoryRequest;
import com.geovannycode.inventory.inventory.api.dto.InventoryResponse;
import com.geovannycode.inventory.inventory.api.dto.OrderInvRequest;
import com.geovannycode.inventory.inventory.api.generated.InventoriesApi;
import com.geovannycode.inventory.inventory.application.InventoryService;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import org.jspecify.annotations.Nullable;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.util.UriComponentsBuilder;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/inventories")
@Validated
public class InventoryController implements InventoriesApi {

    private final InventoryService service;

    public InventoryController(InventoryService service) {
        this.service = service;
    }

    @Override
    @GetMapping(produces = MediaType.APPLICATION_JSON_VALUE)
    public Mono<ResponseEntity<Flux<InventoryResponse>>> listInventories(Integer page, Integer size, Integer delayMs,
                                                                         ServerWebExchange exchange) {
        return Mono.just(ResponseEntity.ok(products(page, size, delayMs)));
    }

    @GetMapping(produces = MediaType.APPLICATION_NDJSON_VALUE)
    public Flux<InventoryResponse> streamInventories(
            @RequestParam(defaultValue = "0") @Min(0) int page,
            @RequestParam(defaultValue = "20") @Min(1) @Max(100) int size,
            @RequestParam(defaultValue = "0") @Min(0) @Max(2000) int delayMs) {
        return products(page, size, delayMs);
    }

    @GetMapping(produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<ServerSentEvent<InventoryResponse>> streamInventoryEvents(
            @RequestParam(defaultValue = "0") @Min(0) int page,
            @RequestParam(defaultValue = "20") @Min(1) @Max(100) int size,
            @RequestParam(defaultValue = "0") @Min(0) @Max(2000) int delayMs) {
        return products(page, size, delayMs).map(product -> ServerSentEvent.builder(product)
                .id(product.idProduct()).event("inventory").build());
    }

    @Override
    @PostMapping(consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public Mono<ResponseEntity<InventoryResponse>> createInventory(Mono<InventoryRequest> inventoryRequest,
                                                                   ServerWebExchange exchange) {
        // @Valid y @RequestBody, como @Pattern del path, se heredan de InventoriesApi.
        return inventoryRequest.flatMap(service::register).map(product -> {
            var location = UriComponentsBuilder.fromPath(exchange.getRequest().getPath().contextPath().value())
                    .pathSegment("inventories", product.idProduct()).build().encode().toUri();
            return ResponseEntity.created(location).body(product);
        });
    }

    @Override
    @GetMapping(value = "/{productId}", produces = MediaType.APPLICATION_JSON_VALUE)
    public Mono<ResponseEntity<InventoryResponse>> getInventory(String productId, ServerWebExchange exchange) {
        return service.findByCode(productId).map(ResponseEntity::ok);
    }

    @Override
    @PutMapping(value = "/{productId}", consumes = MediaType.APPLICATION_JSON_VALUE,
            produces = MediaType.APPLICATION_JSON_VALUE)
    public Mono<ResponseEntity<InventoryResponse>> decreaseInventory(String productId, Mono<OrderInvRequest> orderInvRequest,
                                                                    @Nullable String idempotencyKey,
                                                                    ServerWebExchange exchange) {
        return orderInvRequest.flatMap(request -> service.decreaseStock(productId, request.orderCount(), idempotencyKey))
                .map(ResponseEntity::ok);
    }

    private Flux<InventoryResponse> products(int page, int size, int delayMs) {
        var products = service.findAll(page, size);
        // Demo pacing: read the rows first so a slow client never holds a pooled connection for the whole stream.
        return delayMs == 0 ? products
                : products.collectList().flatMapMany(Flux::fromIterable).delayElements(Duration.ofMillis(delayMs));
    }
}

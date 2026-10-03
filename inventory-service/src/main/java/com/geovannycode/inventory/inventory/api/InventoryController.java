package com.geovannycode.inventory.inventory.api;

import java.time.Duration;

import com.geovannycode.inventory.inventory.api.dto.InventoryRequest;
import com.geovannycode.inventory.inventory.api.dto.InventoryResponse;
import com.geovannycode.inventory.inventory.api.dto.OrderInvRequest;
import com.geovannycode.inventory.inventory.api.generated.InventoriesApi;
import com.geovannycode.inventory.inventory.application.InventoryService;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
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
    public Mono<ResponseEntity<Flux<InventoryResponse>>> listInventories(Integer delayMs, ServerWebExchange exchange) {
        return Mono.just(ResponseEntity.ok(products(delayMs)));
    }

    @GetMapping(produces = MediaType.APPLICATION_NDJSON_VALUE)
    public Flux<InventoryResponse> streamInventories(
            @RequestParam(defaultValue = "0")
            @Min(value = 0, message = "La demora debe ser como mínimo 0.")
            @Max(value = 2000, message = "La demora no puede superar 2000 ms.") int delayMs) {
        return products(delayMs);
    }

    @GetMapping(produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<ServerSentEvent<InventoryResponse>> streamInventoryEvents(
            @RequestParam(defaultValue = "0")
            @Min(value = 0, message = "La demora debe ser como mínimo 0.")
            @Max(value = 2000, message = "La demora no puede superar 2000 ms.") int delayMs) {
        return products(delayMs).map(product -> ServerSentEvent.builder(product)
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
                                                                    ServerWebExchange exchange) {
        return orderInvRequest.flatMap(request -> service.decreaseStock(productId, request.orderCount()))
                .map(ResponseEntity::ok);
    }

    private Flux<InventoryResponse> products(int delayMs) {
        var products = service.findAll();
        return delayMs == 0 ? products : products.delayElements(Duration.ofMillis(delayMs));
    }
}

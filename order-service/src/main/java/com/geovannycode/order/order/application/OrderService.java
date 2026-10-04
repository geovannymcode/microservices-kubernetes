package com.geovannycode.order.order.application;

import com.geovannycode.order.generated.dto.OrderRequest;
import com.geovannycode.order.generated.dto.OrderResponse;
import com.geovannycode.order.order.domain.IllegalOrderStateException;
import com.geovannycode.order.order.domain.InventoryRejectedException;
import com.geovannycode.order.order.domain.InventoryUnavailableException;
import com.geovannycode.order.order.domain.OrderNotFoundException;
import com.geovannycode.order.order.domain.OrderRejectedException;
import com.geovannycode.order.order.domain.OrderStatus;
import com.geovannycode.order.order.infrastructure.inventory.InventoryGateway;
import com.geovannycode.order.order.infrastructure.messaging.OutboxWriter;
import com.geovannycode.order.order.infrastructure.persistence.OrderEntity;
import com.geovannycode.order.order.infrastructure.persistence.OrderRepository;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

/**
 * Order use cases. No @Transactional on the methods: a database transaction must never stay open while
 * waiting for Inventory, and a cancellation has to persist even though the request then fails with 409.
 * Only the state change and its outbox event share a (short) transaction, after Inventory has answered.
 */
@Service
public class OrderService {

    private static final Logger LOG = LoggerFactory.getLogger(OrderService.class);
    // Two concurrent confirmations: the loser re-reads and re-applies the decision table (Inventory replays the
    // decrease thanks to the Idempotency-Key, so it ends as completed).
    private static final int MAX_REREADS_ON_CONFLICT = 2;

    private final OrderRepository repository;
    private final InventoryGateway inventory;
    private final OrderMapper mapper;
    private final OutboxWriter outbox;
    private final TransactionalOperator transactions;
    private final Counter ordersCreated;
    private final Counter confirmedCompleted;
    private final Counter confirmedCanceled;
    private final Counter confirmedUnavailable;

    public OrderService(OrderRepository repository, InventoryGateway inventory, OrderMapper mapper, OutboxWriter outbox,
                        TransactionalOperator transactions, MeterRegistry meterRegistry) {
        this.repository = repository;
        this.inventory = inventory;
        this.mapper = mapper;
        this.outbox = outbox;
        this.transactions = transactions;
        // Not "orders.created": the Prometheus client strips the OpenMetrics-reserved "_created" suffix and the
        // series would be exported as orders_total.
        this.ordersCreated = Counter.builder("orders.registered").description("Orders registered through the API")
                .register(meterRegistry);
        // Registered eagerly so every result series exists (as 0) before the first confirmation. Only state
        // changes count: confirming an already completed order again is not a new confirmation.
        this.confirmedCompleted = confirmedCounter(meterRegistry, "completed");
        this.confirmedCanceled = confirmedCounter(meterRegistry, "canceled");
        this.confirmedUnavailable = confirmedCounter(meterRegistry, "unavailable");
    }

    public Mono<OrderResponse> create(OrderRequest request) {
        return repository.save(mapper.toEntity(request))
                .doOnNext(order -> {
                    ordersCreated.increment();
                    LOG.info("Orden registrada: orderId={}, codeProduct={}, quantity={}",
                            order.id(), order.codeProduct(), order.quantity());
                })
                .map(mapper::toResponse);
    }

    public Mono<OrderResponse> findById(long id) {
        return load(id).map(mapper::toResponse);
    }

    public Flux<OrderResponse> findAll(@Nullable OrderStatus status) {
        var orders = status == null ? repository.findAllByOrderByIdAsc() : repository.findAllByStatusOrderByIdAsc(status);
        return orders.map(mapper::toResponse);
    }

    /**
     * Confirms an order: completed stays completed (200, Inventory untouched), canceled is a 409, and a pending
     * order decreases stock in Inventory and becomes completed, or canceled if Inventory rejects it. When
     * Inventory is unavailable nothing is saved and InventoryUnavailableException propagates (503).
     */
    public Mono<OrderResponse> confirm(long id) {
        return Mono.defer(() -> confirmOnce(id))
                .retryWhen(Retry.max(MAX_REREADS_ON_CONFLICT)
                        .filter(OptimisticLockingFailureException.class::isInstance)
                        .doBeforeRetry(signal -> LOG.info("Conflicto de versión al confirmar, se relee: orderId={}", id))
                        .onRetryExhaustedThrow((spec, signal) -> signal.failure()))
                .map(mapper::toResponse);
    }

    private Mono<OrderEntity> confirmOnce(long id) {
        return load(id).flatMap(order -> switch (order.status()) {
            case COMPLETED -> Mono.just(order);
            case CANCELED -> Mono.error(new IllegalOrderStateException(id, order.status(), "confirmar"));
            case PENDING -> reserveAndComplete(id, order);
        });
    }

    private Mono<OrderEntity> reserveAndComplete(long id, OrderEntity order) {
        return inventory.reserveStock(id, order.codeProduct(), order.quantity())
                .onErrorResume(InventoryRejectedException.class, rejection -> cancel(id, order, rejection))
                .doOnError(InventoryUnavailableException.class, unavailable -> {
                    confirmedUnavailable.increment();
                    LOG.error("Inventario no disponible, la orden sigue pendiente: orderId={}, codeProduct={}",
                            id, order.codeProduct(), unavailable);
                })
                .then(Mono.defer(() -> saveWithEvent(order.complete())))
                .doOnNext(completed -> {
                    confirmedCompleted.increment();
                    LOG.info("Orden completada: orderId={}, codeProduct={}, quantity={}",
                            id, completed.codeProduct(), completed.quantity());
                });
    }

    private Mono<Void> cancel(long id, OrderEntity order, InventoryRejectedException rejection) {
        LOG.warn("Inventario rechazó la orden: orderId={}, codeProduct={}, reason={}",
                id, order.codeProduct(), rejection.reason());
        return saveWithEvent(order.cancel(rejection.reason()))
                .doOnNext(canceled -> {
                    confirmedCanceled.increment();
                    LOG.info("Orden cancelada: orderId={}, codeProduct={}, reason={}",
                            id, canceled.codeProduct(), rejection.reason());
                })
                .then(Mono.error(new OrderRejectedException(id, order.codeProduct(), rejection.reason())));
    }

    /**
     * The new state and its event are committed together or not at all: never an event for a change that was
     * not saved (e.g. a version conflict rolls both back), never a saved change without its event.
     */
    private Mono<OrderEntity> saveWithEvent(OrderEntity changed) {
        return transactions.transactional(repository.save(changed)
                .flatMap(saved -> outbox.append(saved).thenReturn(saved)));
    }

    private static Counter confirmedCounter(MeterRegistry meterRegistry, String result) {
        return Counter.builder("orders.confirmed").description("Order confirmations by outcome")
                .tag("result", result).register(meterRegistry);
    }

    private Mono<OrderEntity> load(long id) {
        return repository.findById(id).switchIfEmpty(Mono.error(() -> new OrderNotFoundException(id)));
    }
}

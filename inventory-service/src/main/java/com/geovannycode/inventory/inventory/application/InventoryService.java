package com.geovannycode.inventory.inventory.application;

import com.geovannycode.inventory.inventory.api.dto.InventoryRequest;
import com.geovannycode.inventory.inventory.api.dto.InventoryResponse;
import com.geovannycode.inventory.inventory.domain.DuplicateProductException;
import com.geovannycode.inventory.inventory.domain.IdempotencyKeyReusedException;
import com.geovannycode.inventory.inventory.domain.InsufficientStockException;
import com.geovannycode.inventory.inventory.domain.ProductNotFoundException;
import com.geovannycode.inventory.inventory.infrastructure.persistence.ProductRepository;
import com.geovannycode.inventory.inventory.infrastructure.persistence.StockMovementRepository;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Service;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Service
public class InventoryService {

    private static final Logger LOG = LoggerFactory.getLogger(InventoryService.class);
    private final ProductRepository repository;
    private final StockMovementRepository movements;
    private final TransactionalOperator transactions;
    private final Counter productsRegistered;
    private final Counter decreasesOk;
    private final Counter decreasesInsufficient;
    private final Counter decreasesNotFound;
    private final Counter decreasesReplayed;

    public InventoryService(ProductRepository repository, StockMovementRepository movements,
                            TransactionalOperator transactions, MeterRegistry meterRegistry) {
        this.repository = repository;
        this.movements = movements;
        this.transactions = transactions;
        this.productsRegistered = Counter.builder("inventory.product.registered")
                .description("Products created through the API").register(meterRegistry);
        // Registered eagerly so every result series exists (as 0) before the first order arrives.
        this.decreasesOk = decreaseCounter(meterRegistry, "ok");
        this.decreasesInsufficient = decreaseCounter(meterRegistry, "insufficient");
        this.decreasesNotFound = decreaseCounter(meterRegistry, "not_found");
        this.decreasesReplayed = decreaseCounter(meterRegistry, "replayed");
    }

    public Flux<InventoryResponse> findAll(int page, int size) {
        return repository.findAllByOrderByCodeAsc(PageRequest.of(page, size)).map(InventoryMapper::toResponse);
    }

    public Mono<InventoryResponse> findByCode(String code) {
        return repository.findByCode(code)
                .switchIfEmpty(Mono.error(new ProductNotFoundException(code)))
                .map(InventoryMapper::toResponse);
    }

    public Mono<InventoryResponse> register(InventoryRequest request) {
        return Mono.defer(() -> repository.save(InventoryMapper.toEntity(request)))
                .onErrorMap(DuplicateKeyException.class,
                        error -> new DuplicateProductException(request.idProduct()))
                .map(InventoryMapper::toResponse)
                .doOnNext(product -> {
                    productsRegistered.increment();
                    LOG.info("Producto registrado: code={}, stock={}", product.idProduct(), product.stock());
                });
    }

    /**
     * Decreases stock atomically. With an idempotency key, the key is recorded in the same transaction as
     * the UPDATE: a retry with the same key and request returns the current product without decreasing
     * again, and a failed decrease (404/409) rolls the key back so the client may retry with it.
     * Metrics and logs run after the transaction ends, so a rolled-back decrease is never counted as ok.
     */
    public Mono<InventoryResponse> decreaseStock(String code, int quantity, @Nullable String idempotencyKey) {
        if (quantity <= 0) {
            // Defence in depth: the DTO already enforces 1..5, but "stock - :quantity" would add stock if negative.
            return Mono.error(new IllegalArgumentException("quantity must be positive: " + quantity));
        }
        return transactions.transactional(decreaseInTransaction(code, quantity, idempotencyKey))
                .doOnNext(decrease -> {
                    if (decrease.replayed()) {
                        decreasesReplayed.increment();
                        LOG.info("Descuento repetido ignorado: code={}, quantity={}, key={}", code, quantity, idempotencyKey);
                    } else {
                        decreasesOk.increment();
                        LOG.info("Stock descontado: code={}, quantity={}, stock={}", code, quantity, decrease.product().stock());
                    }
                })
                .map(Decrease::product)
                .doOnError(ProductNotFoundException.class, notFound -> decreasesNotFound.increment())
                .doOnError(InsufficientStockException.class, insufficient -> {
                    decreasesInsufficient.increment();
                    LOG.warn("Stock insuficiente: code={}, quantity={}", code, quantity);
                });
    }

    private Mono<Decrease> decreaseInTransaction(String code, int quantity, @Nullable String idempotencyKey) {
        if (idempotencyKey == null) {
            return applyDecrease(code, quantity);
        }
        return movements.insert(idempotencyKey, code, quantity)
                .map(inserted -> true)
                .onErrorResume(DuplicateKeyException.class, duplicate -> Mono.just(false))
                .flatMap(firstAttempt -> firstAttempt
                        ? applyDecrease(code, quantity)
                        : replay(idempotencyKey, code, quantity));
    }

    private Mono<Decrease> replay(String idempotencyKey, String code, int quantity) {
        return movements.findById(idempotencyKey).flatMap(movement -> {
            if (!movement.productCode().equals(code) || movement.quantity() != quantity) {
                return Mono.error(new IdempotencyKeyReusedException(idempotencyKey));
            }
            return findByCode(code).map(product -> new Decrease(product, true));
        });
    }

    private Mono<Decrease> applyDecrease(String code, int quantity) {
        return repository.decreaseStock(code, quantity)
                .flatMap(rows -> {
                    if (rows == 1) {
                        return findByCode(code).map(product -> new Decrease(product, false));
                    }
                    return repository.existsByCode(code).flatMap(exists -> Mono.error(exists
                            ? new InsufficientStockException(code, quantity)
                            : new ProductNotFoundException(code)));
                });
    }

    private record Decrease(InventoryResponse product, boolean replayed) {
    }

    private static Counter decreaseCounter(MeterRegistry meterRegistry, String result) {
        return Counter.builder("inventory.stock.decrease").description("Stock decrease attempts by outcome")
                .tag("result", result).register(meterRegistry);
    }
}

package com.geovannycode.inventory.inventory.application;

import com.geovannycode.inventory.inventory.api.dto.InventoryRequest;
import com.geovannycode.inventory.inventory.api.dto.InventoryResponse;
import com.geovannycode.inventory.inventory.domain.DuplicateProductException;
import com.geovannycode.inventory.inventory.domain.InsufficientStockException;
import com.geovannycode.inventory.inventory.domain.ProductNotFoundException;
import com.geovannycode.inventory.inventory.infrastructure.persistence.ProductRepository;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Service
public class InventoryService {

    private static final Logger LOG = LoggerFactory.getLogger(InventoryService.class);
    private final ProductRepository repository;
    private final Counter productsRegistered;
    private final Counter decreasesOk;
    private final Counter decreasesInsufficient;
    private final Counter decreasesNotFound;

    public InventoryService(ProductRepository repository, MeterRegistry meterRegistry) {
        this.repository = repository;
        this.productsRegistered = Counter.builder("inventory.product.registered")
                .description("Products created through the API").register(meterRegistry);
        // Registered eagerly so every result series exists (as 0) before the first order arrives.
        this.decreasesOk = decreaseCounter(meterRegistry, "ok");
        this.decreasesInsufficient = decreaseCounter(meterRegistry, "insufficient");
        this.decreasesNotFound = decreaseCounter(meterRegistry, "not_found");
    }

    public Flux<InventoryResponse> findAll() {
        return repository.findAllByOrderByCodeAsc().map(InventoryMapper::toResponse);
    }

    public Mono<InventoryResponse> findByCode(String code) {
        return repository.findByCode(code)
                .switchIfEmpty(Mono.error(new ProductNotFoundException(code)))
                .map(InventoryMapper::toResponse);
    }

    public Mono<InventoryResponse> register(InventoryRequest request) {
        return Mono.defer(() -> repository.save(InventoryMapper.toEntity(request)))
                .onErrorMap(DataIntegrityViolationException.class,
                        error -> new DuplicateProductException(request.idProduct()))
                .map(InventoryMapper::toResponse)
                .doOnNext(product -> {
                    productsRegistered.increment();
                    LOG.info("Producto registrado: code={}, stock={}", product.idProduct(), product.stock());
                });
    }

    @Transactional(transactionManager = "inventoryTransactionManager")
    public Mono<InventoryResponse> decreaseStock(String code, int quantity) {
        return repository.decreaseStock(code, quantity)
                .flatMap(rows -> {
                    if (rows == 1) {
                        return findByCode(code).doOnNext(product -> {
                            decreasesOk.increment();
                            LOG.info("Stock descontado: code={}, quantity={}, stock={}", code, quantity, product.stock());
                        });
                    }
                    return repository.existsByCode(code).flatMap(exists -> {
                        if (!exists) {
                            decreasesNotFound.increment();
                            return Mono.error(new ProductNotFoundException(code));
                        }
                        decreasesInsufficient.increment();
                        LOG.warn("Stock insuficiente: code={}, quantity={}", code, quantity);
                        return Mono.error(new InsufficientStockException(code, quantity));
                    });
                });
    }

    private static Counter decreaseCounter(MeterRegistry meterRegistry, String result) {
        return Counter.builder("inventory.stock.decrease").description("Stock decrease attempts by outcome")
                .tag("result", result).register(meterRegistry);
    }
}

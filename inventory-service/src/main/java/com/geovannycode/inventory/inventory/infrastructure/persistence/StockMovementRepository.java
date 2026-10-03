package com.geovannycode.inventory.inventory.infrastructure.persistence;

import org.springframework.data.r2dbc.repository.Modifying;
import org.springframework.data.r2dbc.repository.Query;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import reactor.core.publisher.Mono;

public interface StockMovementRepository extends ReactiveCrudRepository<StockMovementEntity, String> {

    /**
     * Records the key before the decrease. The id is client-assigned, so save() would issue an UPDATE;
     * an explicit INSERT fails with DuplicateKeyException when the key was already used. A concurrent
     * request with the same key blocks on the primary key until the first one commits or rolls back.
     */
    @Modifying
    @Query("INSERT INTO stock_movements (idempotency_key, product_code, quantity) VALUES (:key, :code, :quantity)")
    Mono<Integer> insert(String key, String code, int quantity);
}

package com.geovannycode.inventory.inventory.infrastructure.persistence;

import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Column;
import org.springframework.data.relational.core.mapping.Table;

@Table("stock_movements")
public record StockMovementEntity(
        @Id @Column("idempotency_key") String idempotencyKey,
        @Column("product_code") String productCode,
        Integer quantity) {
}

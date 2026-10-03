package com.geovannycode.inventory.inventory.infrastructure.persistence;

import java.math.BigDecimal;

import org.jspecify.annotations.Nullable;
import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Column;
import org.springframework.data.relational.core.mapping.Table;

@Table("products")
public record ProductEntity(
        @Id @Nullable Long id,
        String code,
        @Column("name_product") String nameProduct,
        BigDecimal price,
        Integer stock) {
}

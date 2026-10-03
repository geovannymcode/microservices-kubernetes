package com.geovannycode.inventory.inventory.application;

import com.geovannycode.inventory.inventory.api.dto.InventoryRequest;
import com.geovannycode.inventory.inventory.api.dto.InventoryResponse;
import com.geovannycode.inventory.inventory.infrastructure.persistence.ProductEntity;

final class InventoryMapper {

    private InventoryMapper() {
    }

    static ProductEntity toEntity(InventoryRequest request) {
        return new ProductEntity(null, request.idProduct(), request.nameProduct(),
                request.price().setScale(2), request.stock());
    }

    static InventoryResponse toResponse(ProductEntity entity) {
        return new InventoryResponse(entity.code(), entity.nameProduct(),
                entity.price().setScale(2), entity.stock());
    }
}

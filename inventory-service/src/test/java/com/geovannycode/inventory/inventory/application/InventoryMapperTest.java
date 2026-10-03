package com.geovannycode.inventory.inventory.application;

import java.math.BigDecimal;

import com.geovannycode.inventory.inventory.api.dto.InventoryRequest;
import com.geovannycode.inventory.inventory.infrastructure.persistence.ProductEntity;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class InventoryMapperTest {

    @Test
    void mapsPublicCodeAndLeavesInternalIdUnset() {
        var entity = InventoryMapper.toEntity(new InventoryRequest("PRD-123", "Lentes", new BigDecimal("1"), 5));
        assertThat(entity.id()).isNull();
        assertThat(entity.code()).isEqualTo("PRD-123");
        assertThat(entity.nameProduct()).isEqualTo("Lentes");
        assertThat(entity.price()).isEqualTo(new BigDecimal("1.00"));
        assertThat(entity.stock()).isEqualTo(5);
    }

    @Test
    void mapsCodeInsteadOfInternalIdAndIncludesPriceAndZeroStock() {
        var response = InventoryMapper.toResponse(new ProductEntity(99L, "AC-1550", "Lentes", new BigDecimal("123.50"), 0));
        assertThat(response.idProduct()).isEqualTo("AC-1550");
        assertThat(response.nameProduct()).isEqualTo("Lentes");
        assertThat(response.price()).isEqualTo(new BigDecimal("123.50"));
        assertThat(response.stock()).isZero();
        assertThat(response.getClass().getRecordComponents()).extracting(component -> component.getName())
                .containsExactly("idProduct", "nameProduct", "price", "stock");
    }

    @Test
    void doesNotSilentlyRoundInvalidPrice() {
        assertThatThrownBy(() -> InventoryMapper.toEntity(
                new InventoryRequest("PRD-123", "Lentes", new BigDecimal("123.456"), 1)))
                .isInstanceOf(ArithmeticException.class);
    }
}

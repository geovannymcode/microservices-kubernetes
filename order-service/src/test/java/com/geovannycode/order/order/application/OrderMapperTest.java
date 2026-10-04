package com.geovannycode.order.order.application;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

import com.geovannycode.order.generated.dto.OrderRequest;
import com.geovannycode.order.generated.dto.OrderResponse;
import com.geovannycode.order.order.domain.CancelReason;
import com.geovannycode.order.order.domain.OrderStatus;
import com.geovannycode.order.order.infrastructure.persistence.OrderEntity;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

final class OrderMapperTest {

    private final OrderMapper mapper = new OrderMapperImpl();

    @Test
    void newOrderIsPendingAndDefaultsQuantityToOne() {
        var request = new OrderRequest("AC-1550");
        request.setQuantity(null);
        var entity = mapper.toEntity(request);
        assertThat(entity.codeProduct()).isEqualTo("AC-1550");
        assertThat(entity.quantity()).isEqualTo(1);
        assertThat(entity.status()).isEqualTo(OrderStatus.PENDING);
        assertThat(entity.id()).isNull();
        assertThat(entity.version()).isNull();
        assertThat(mapper.toEntity(new OrderRequest("AC-1550").quantity(4)).quantity()).isEqualTo(4);
    }

    @Test
    void storesTheCanonicalUppercaseCodeThatInventoryAccepts() {
        assertThat(mapper.toEntity(new OrderRequest("ac-1550")).codeProduct()).isEqualTo("AC-1550");
    }

    @Test
    void responseUsesUtcOffsetsAndContractEnums() {
        var created = Instant.parse("2026-10-03T19:00:00Z");
        var entity = new OrderEntity(9L, "AC-1550", 2, OrderStatus.CANCELED, CancelReason.INSUFFICIENT_STOCK,
                created, created.plusSeconds(5), 1L);
        var response = mapper.toResponse(entity);
        assertThat(response.getId()).isEqualTo(9L);
        assertThat(response.getStatus()).isEqualTo(com.geovannycode.order.generated.dto.OrderStatus.CANCELED);
        assertThat(response.getStatus().getValue()).isEqualTo("canceled");
        assertThat(response.getCancelReason()).isEqualTo(OrderResponse.CancelReasonEnum.INSUFFICIENT_STOCK);
        assertThat(response.getCreatedAt()).isEqualTo(OffsetDateTime.of(2026, 10, 3, 19, 0, 0, 0, ZoneOffset.UTC));
        assertThat(response.getUpdatedAt().getOffset()).isEqualTo(ZoneOffset.UTC);
    }
}

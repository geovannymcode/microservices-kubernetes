package com.geovannycode.order.order.application;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

import com.geovannycode.order.generated.dto.OrderRequest;
import com.geovannycode.order.generated.dto.OrderResponse;
import com.geovannycode.order.order.infrastructure.persistence.OrderEntity;
import org.jspecify.annotations.Nullable;
import org.mapstruct.InjectionStrategy;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;

@Mapper(componentModel = "spring", injectionStrategy = InjectionStrategy.CONSTRUCTOR)
public interface OrderMapper {

    @Mapping(target = "id", ignore = true)
    // Inventory only accepts uppercase codes (its contract v2); Order's contract also allows lowercase, so the
    // order stores the canonical code instead of failing later, at confirmation, with a 400 from Inventory.
    @Mapping(target = "codeProduct", expression = "java(request.getCodeProduct().toUpperCase(java.util.Locale.ROOT))")
    @Mapping(target = "quantity", source = "quantity", defaultValue = "1")
    @Mapping(target = "status", constant = "PENDING")
    @Mapping(target = "cancelReason", ignore = true)
    @Mapping(target = "createdAt", ignore = true)
    @Mapping(target = "updatedAt", ignore = true)
    @Mapping(target = "version", ignore = true)
    OrderEntity toEntity(OrderRequest request);

    OrderResponse toResponse(OrderEntity entity);

    // The contract exposes date-time in UTC; the database and domain keep Instant.
    default @Nullable OffsetDateTime toUtc(@Nullable Instant instant) {
        return instant == null ? null : instant.atOffset(ZoneOffset.UTC);
    }
}

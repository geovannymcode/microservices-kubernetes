package com.geovannycode.order.order.infrastructure.messaging;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.geovannycode.order.order.infrastructure.persistence.OrderEntity;
import org.jspecify.annotations.Nullable;

/** Envelope published to orders.events.v1, as described in contracts/events/order-events.yaml. */
public record OrderEvent(UUID eventId, String eventType, Instant occurredAt, int version, Data data) {

    public static final String COMPLETED = "OrderCompleted";
    public static final String CANCELED = "OrderCanceled";
    static final int SCHEMA_VERSION = 1;

    public static OrderEvent of(OrderEntity order) {
        String eventType = switch (order.status()) {
            case COMPLETED -> COMPLETED;
            case CANCELED -> CANCELED;
            case PENDING -> throw new IllegalArgumentException("Una orden pendiente no genera eventos: " + order.id());
        };
        var data = new Data(Objects.requireNonNull(order.id(), "id"), order.codeProduct(), order.quantity(),
                order.status().value(), order.cancelReason() == null ? null : order.cancelReason().name());
        // updatedAt is the audit timestamp of the very save that changed the state.
        return new OrderEvent(UUID.randomUUID(), eventType, Objects.requireNonNullElseGet(order.updatedAt(), Instant::now),
                SCHEMA_VERSION, data);
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record Data(long orderId, String codeProduct, int quantity, String status, @Nullable String cancelReason) {
    }
}

package com.geovannycode.notify_service.notify.domain;

import java.time.Instant;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import org.jspecify.annotations.Nullable;

/**
 * Envelope Order publishes to orders.events.v1 (contracts/events/order-events.yaml). Fields are nullable at the
 * Java level on purpose: the JSON may be incomplete, and the listener validates it before anything uses it.
 * Unknown properties are ignored (Jackson 3 default), so compatible additions to version 1 do not break the consumer.
 */
public record OrderEvent(
        @NotBlank @Nullable String eventId,
        @NotBlank @Nullable String eventType,
        @NotNull @Nullable Instant occurredAt,
        @NotNull @Nullable Integer version,
        @NotNull @Valid @Nullable Data data) {

    public static final String COMPLETED = "OrderCompleted";
    public static final String CANCELED = "OrderCanceled";

    public record Data(
            @NotNull @Positive @Nullable Long orderId,
            @NotBlank @Nullable String codeProduct,
            @NotNull @Positive @Nullable Integer quantity,
            @Nullable String status,
            @Nullable String cancelReason) {
    }
}

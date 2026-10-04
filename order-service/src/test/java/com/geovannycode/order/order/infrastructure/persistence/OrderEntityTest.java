package com.geovannycode.order.order.infrastructure.persistence;

import java.time.Instant;

import com.geovannycode.order.order.domain.CancelReason;
import com.geovannycode.order.order.domain.IllegalOrderStateException;
import com.geovannycode.order.order.domain.OrderStatus;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class OrderEntityTest {

    private static final Instant CREATED = Instant.parse("2026-10-03T19:00:00Z");
    private static final OrderEntity PENDING =
            new OrderEntity(7L, "AC-1550", 2, OrderStatus.PENDING, null, CREATED, CREATED, 3L);

    @Test
    void completeReturnsCopyAndKeepsIdentityAndVersion() {
        var completed = PENDING.complete();
        assertThat(completed.status()).isEqualTo(OrderStatus.COMPLETED);
        assertThat(completed.cancelReason()).isNull();
        assertThat(completed).usingRecursiveComparison().ignoringFields("status").isEqualTo(PENDING);
        assertThat(PENDING.status()).isEqualTo(OrderStatus.PENDING);
    }

    @Test
    void cancelRecordsTheReason() {
        var canceled = PENDING.cancel(CancelReason.INSUFFICIENT_STOCK);
        assertThat(canceled.status()).isEqualTo(OrderStatus.CANCELED);
        assertThat(canceled.cancelReason()).isEqualTo(CancelReason.INSUFFICIENT_STOCK);
        assertThat(canceled.version()).isEqualTo(3L);
    }

    @Test
    void onlyPendingOrdersCanTransition() {
        var completed = PENDING.complete();
        var canceled = PENDING.cancel(CancelReason.PRODUCT_NOT_FOUND);
        assertThatThrownBy(completed::complete).isInstanceOf(IllegalOrderStateException.class)
                .hasMessageContaining("completar").hasMessageContaining("completed");
        assertThatThrownBy(() -> completed.cancel(CancelReason.PRODUCT_NOT_FOUND)).isInstanceOf(IllegalOrderStateException.class);
        assertThatThrownBy(canceled::complete).isInstanceOf(IllegalOrderStateException.class)
                .hasMessageContaining("canceled");
        assertThatThrownBy(() -> canceled.cancel(CancelReason.INSUFFICIENT_STOCK)).isInstanceOf(IllegalOrderStateException.class);
    }

    @Test
    void newOrdersStartPendingWithoutPersistenceFields() {
        var order = OrderEntity.pending("AC-1550", 1);
        assertThat(order.status()).isEqualTo(OrderStatus.PENDING);
        assertThat(order.id()).isNull();
        assertThat(order.version()).isNull();
        assertThat(order.createdAt()).isNull();
    }

    @Test
    void statusRoundTripsThroughItsLowercaseValue() {
        for (OrderStatus status : OrderStatus.values()) {
            assertThat(OrderStatus.fromValue(status.value())).isEqualTo(status);
            assertThat(status.value()).isEqualTo(status.name().toLowerCase());
        }
        assertThatThrownBy(() -> OrderStatus.fromValue("PENDING")).isInstanceOf(IllegalArgumentException.class);
    }
}

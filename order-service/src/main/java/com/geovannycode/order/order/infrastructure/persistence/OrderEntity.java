package com.geovannycode.order.order.infrastructure.persistence;

import java.time.Instant;

import com.geovannycode.order.order.domain.CancelReason;
import com.geovannycode.order.order.domain.IllegalOrderStateException;
import com.geovannycode.order.order.domain.OrderStatus;
import org.jspecify.annotations.Nullable;
import org.springframework.data.annotation.CreatedDate;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.LastModifiedDate;
import org.springframework.data.annotation.Version;
import org.springframework.data.relational.core.mapping.Column;
import org.springframework.data.relational.core.mapping.Table;

/**
 * Immutable row of order_shop. The reference column is deliberately not mapped: it only exists in the
 * cert and prod Liquibase contexts. Transitions return a new copy; only PENDING orders can move.
 */
@Table("order_shop")
public record OrderEntity(
        @Id @Nullable Long id,
        @Column("code_product") String codeProduct,
        int quantity,
        @Column("status_order") OrderStatus status,
        @Column("cancel_reason") @Nullable CancelReason cancelReason,
        @Column("created_at") @CreatedDate @Nullable Instant createdAt,
        @Column("updated_at") @LastModifiedDate @Nullable Instant updatedAt,
        @Version @Nullable Long version) {

    public static OrderEntity pending(String codeProduct, int quantity) {
        return new OrderEntity(null, codeProduct, quantity, OrderStatus.PENDING, null, null, null, null);
    }

    public OrderEntity complete() {
        requirePending("completar");
        return new OrderEntity(id, codeProduct, quantity, OrderStatus.COMPLETED, null, createdAt, updatedAt, version);
    }

    public OrderEntity cancel(CancelReason reason) {
        requirePending("cancelar");
        return new OrderEntity(id, codeProduct, quantity, OrderStatus.CANCELED, reason, createdAt, updatedAt, version);
    }

    private void requirePending(String attemptedAction) {
        if (status != OrderStatus.PENDING) {
            throw new IllegalOrderStateException(id, status, attemptedAction);
        }
    }
}

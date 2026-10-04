package com.geovannycode.order.order.domain;

/** Inventory rejected the order, which is now persisted as canceled; answered as 409 with the reason. */
public final class OrderRejectedException extends RuntimeException {

    private final long orderId;
    private final CancelReason reason;

    public OrderRejectedException(long orderId, String codeProduct, CancelReason reason) {
        super(switch (reason) {
            case PRODUCT_NOT_FOUND -> "La orden " + orderId + " se canceló: el producto " + codeProduct + " no existe.";
            case INSUFFICIENT_STOCK -> "La orden " + orderId + " se canceló: stock insuficiente para el producto " + codeProduct + ".";
        });
        this.orderId = orderId;
        this.reason = reason;
    }

    public long orderId() {
        return orderId;
    }

    public CancelReason reason() {
        return reason;
    }
}

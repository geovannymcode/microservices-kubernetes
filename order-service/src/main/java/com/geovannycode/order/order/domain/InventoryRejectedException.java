package com.geovannycode.order.order.domain;

/** Inventory answered with a business rejection; retrying will not change the outcome. */
public final class InventoryRejectedException extends RuntimeException {

    private final String codeProduct;
    private final CancelReason reason;

    public InventoryRejectedException(String codeProduct, CancelReason reason) {
        super(switch (reason) {
            case PRODUCT_NOT_FOUND -> "El producto " + codeProduct + " no existe en el inventario.";
            case INSUFFICIENT_STOCK -> "Stock insuficiente para el producto " + codeProduct + ".";
        });
        this.codeProduct = codeProduct;
        this.reason = reason;
    }

    public String codeProduct() {
        return codeProduct;
    }

    public CancelReason reason() {
        return reason;
    }
}

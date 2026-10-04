package com.geovannycode.order.order.domain;

/** Why Inventory rejected an order; stored by name, as in the contract. */
public enum CancelReason {
    PRODUCT_NOT_FOUND,
    INSUFFICIENT_STOCK
}

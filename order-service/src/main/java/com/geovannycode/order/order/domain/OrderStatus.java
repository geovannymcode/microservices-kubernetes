package com.geovannycode.order.order.domain;

import java.util.Arrays;

/** Order lifecycle; {@code value} is the lowercase form used by the contract and the status_order column. */
public enum OrderStatus {

    PENDING("pending"),
    COMPLETED("completed"),
    CANCELED("canceled");

    private final String value;

    OrderStatus(String value) {
        this.value = value;
    }

    public String value() {
        return value;
    }

    public static OrderStatus fromValue(String value) {
        return Arrays.stream(values()).filter(status -> status.value.equals(value)).findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Estado de orden desconocido: " + value));
    }
}

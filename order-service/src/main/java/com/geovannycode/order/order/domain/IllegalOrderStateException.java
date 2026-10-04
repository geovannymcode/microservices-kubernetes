package com.geovannycode.order.order.domain;

import org.jspecify.annotations.Nullable;

public final class IllegalOrderStateException extends RuntimeException {

    public IllegalOrderStateException(@Nullable Long id, OrderStatus currentStatus, String attemptedAction) {
        super("No se puede " + attemptedAction + " la orden " + id + " en estado " + currentStatus.value() + ".");
    }
}

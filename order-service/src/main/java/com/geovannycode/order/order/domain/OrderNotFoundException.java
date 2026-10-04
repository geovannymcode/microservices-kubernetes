package com.geovannycode.order.order.domain;

public final class OrderNotFoundException extends RuntimeException {

    public OrderNotFoundException(long id) {
        super("No existe la orden " + id + ".");
    }
}

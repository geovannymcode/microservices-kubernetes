package com.geovannycode.inventory.inventory.domain;

public final class InsufficientStockException extends RuntimeException {

    public InsufficientStockException(String code, int requested) {
        super("Stock insuficiente para el producto " + code + ": cantidad solicitada " + requested + ".");
    }
}

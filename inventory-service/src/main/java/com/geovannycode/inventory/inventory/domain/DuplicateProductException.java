package com.geovannycode.inventory.inventory.domain;

public final class DuplicateProductException extends RuntimeException {

    public DuplicateProductException(String code) {
        super("Ya existe un producto con código " + code + ".");
    }
}

package com.geovannycode.inventory.inventory.domain;

public final class ProductNotFoundException extends RuntimeException {

    public ProductNotFoundException(String code) {
        super("No se encontró el producto con código " + code + ".");
    }
}

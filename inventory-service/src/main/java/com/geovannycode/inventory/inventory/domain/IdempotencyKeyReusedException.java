package com.geovannycode.inventory.inventory.domain;

public final class IdempotencyKeyReusedException extends RuntimeException {

    public IdempotencyKeyReusedException(String key) {
        super("La clave " + key + " ya se usó con otra solicitud.");
    }
}

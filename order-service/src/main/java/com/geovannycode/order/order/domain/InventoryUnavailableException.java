package com.geovannycode.order.order.domain;

/** Inventory could not be reached (timeout, connection, 5xx, open circuit, rate limit): a technical failure. */
public final class InventoryUnavailableException extends RuntimeException {

    public InventoryUnavailableException(Throwable cause) {
        super("El servicio de inventario no está disponible.", cause);
    }
}

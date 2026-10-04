package com.geovannycode.notify_service.notify.infrastructure.messaging;

/** The record deserialized but does not meet the event contract: retrying cannot fix it, it goes to the DLT. */
public final class InvalidOrderEventException extends RuntimeException {

    public InvalidOrderEventException(String message) {
        super(message);
    }
}

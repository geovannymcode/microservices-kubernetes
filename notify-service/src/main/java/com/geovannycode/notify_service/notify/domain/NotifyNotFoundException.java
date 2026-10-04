package com.geovannycode.notify_service.notify.domain;

/** No notification with that id: always a 404, never an empty 200. */
public final class NotifyNotFoundException extends RuntimeException {

    public NotifyNotFoundException(String id) {
        super("No existe la notificación " + id);
    }
}

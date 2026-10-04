package com.geovannycode.notify_service.notify.domain;

/** The channel could not deliver the notification; Kafka redelivers the event until the attempts run out. */
public final class NotificationDeliveryException extends RuntimeException {

    public NotificationDeliveryException(String eventId, Throwable cause) {
        super("No se pudo enviar la notificación del evento " + eventId + ": " + cause.getMessage(), cause);
    }
}

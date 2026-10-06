package com.geovannycode.notify_service.notify.domain;

/**
 * The channel could not deliver the notification. Retryable: Kafka redelivers the event until the attempts run
 * out. The non-retryable case is {@link NotificationRejectedException}.
 */
public sealed class NotificationDeliveryException extends RuntimeException permits NotificationRejectedException {

    public NotificationDeliveryException(String eventId, Throwable cause) {
        super(describe(eventId, String.valueOf(cause.getMessage())), cause);
    }

    /** Without a cause: for channels whose underlying errors may carry secrets (a URL with a token, a header). */
    public NotificationDeliveryException(String eventId, String reason) {
        super(describe(eventId, reason));
    }

    private static String describe(String eventId, String reason) {
        return "No se pudo enviar la notificación del evento " + eventId + ": " + reason;
    }
}

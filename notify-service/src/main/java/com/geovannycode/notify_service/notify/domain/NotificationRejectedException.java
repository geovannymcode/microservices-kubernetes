package com.geovannycode.notify_service.notify.domain;

/**
 * The receiver refused the content (a webhook answering 4xx): sending the same notification again cannot succeed,
 * so it ends FAILED and goes to the dead-letter topic without using up the retries.
 */
public final class NotificationRejectedException extends NotificationDeliveryException {

    public NotificationRejectedException(String eventId, String reason) {
        super(eventId, reason);
    }
}

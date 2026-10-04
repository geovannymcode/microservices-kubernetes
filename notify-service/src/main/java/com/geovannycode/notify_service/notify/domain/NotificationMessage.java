package com.geovannycode.notify_service.notify.domain;

/** What a channel delivers: the text plus the identifiers a channel may need (webhook payload, logs). */
public record NotificationMessage(String eventId, long orderId, String message) {
}

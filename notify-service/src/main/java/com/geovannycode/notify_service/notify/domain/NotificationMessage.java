package com.geovannycode.notify_service.notify.domain;

import java.time.Instant;

import org.jspecify.annotations.Nullable;

/** What a channel delivers: the text plus the order data a channel may forward (webhook payload, logs). */
public record NotificationMessage(String notificationId, String eventId, String eventType, long orderId,
                                  String codeProduct, int quantity, @Nullable String cancelReason, String message,
                                  Instant createdAt) {
}

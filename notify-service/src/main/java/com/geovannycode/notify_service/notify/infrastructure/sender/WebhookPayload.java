package com.geovannycode.notify_service.notify.infrastructure.sender;

import java.time.Instant;

import com.geovannycode.notify_service.notify.domain.NotificationMessage;
import org.jspecify.annotations.Nullable;

/** Body POSTed by the webhook channel. cancelReason is null except in OrderCanceled. */
record WebhookPayload(String notificationId, String eventId, String eventType, long orderId, String codeProduct,
                      int quantity, @Nullable String cancelReason, String message, Instant createdAt) {

    static WebhookPayload of(NotificationMessage notification) {
        return new WebhookPayload(notification.notificationId(), notification.eventId(), notification.eventType(),
                notification.orderId(), notification.codeProduct(), notification.quantity(),
                notification.cancelReason(), notification.message(), notification.createdAt());
    }
}

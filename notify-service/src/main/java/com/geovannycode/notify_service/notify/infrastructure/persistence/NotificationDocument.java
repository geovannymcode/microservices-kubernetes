package com.geovannycode.notify_service.notify.infrastructure.persistence;

import java.time.Instant;

import com.geovannycode.notify_service.notify.domain.NotifyStatus;
import org.jspecify.annotations.Nullable;
import org.springframework.data.annotation.CreatedDate;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Version;
import org.springframework.data.mongodb.core.mapping.Document;

/**
 * One notification per order event (eventId is unique, see MongoIndexInitializer). Immutable: every transition
 * returns a new copy, and @Version makes a concurrent second write fail instead of overwriting the first.
 */
@Document("notify_orders")
public record NotificationDocument(
        @Id @Nullable String id,
        String eventId,
        String eventType,
        long orderId,
        String codeProduct,
        int quantity,
        @Nullable String cancelReason,
        String message,
        String channel,
        NotifyStatus status,
        int attempts,
        @CreatedDate @Nullable Instant createdAt,
        @Nullable Instant sentAt,
        @Version @Nullable Long version) {

    public NotificationDocument markSent(Instant at) {
        return new NotificationDocument(id, eventId, eventType, orderId, codeProduct, quantity, cancelReason, message,
                channel, NotifyStatus.SENT, attempts, createdAt, at, version);
    }

    public NotificationDocument markFailed() {
        return new NotificationDocument(id, eventId, eventType, orderId, codeProduct, quantity, cancelReason, message,
                channel, NotifyStatus.FAILED, attempts, createdAt, sentAt, version);
    }

    public NotificationDocument withAttempt() {
        return new NotificationDocument(id, eventId, eventType, orderId, codeProduct, quantity, cancelReason, message,
                channel, status, attempts + 1, createdAt, sentAt, version);
    }
}

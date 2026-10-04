package com.geovannycode.notify_service.notify.domain;

import reactor.core.publisher.Mono;

/** Delivery channel (port). Exactly one implementation is active, chosen by notification.sender.type. */
public interface NotificationSender {

    /** Completes when the channel accepted the message; errors with NotificationDeliveryException otherwise. */
    Mono<Void> send(NotificationMessage message);

    /** Stored in the notification ("log", "webhook"), as in the API contract. */
    String channel();
}

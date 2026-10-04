package com.geovannycode.notify_service.notify.infrastructure.sender;

import com.geovannycode.notify_service.notify.domain.NotificationMessage;
import com.geovannycode.notify_service.notify.domain.NotificationSender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

/** Default channel: the notification is "sent" by writing it to the log (INFO). */
@Component
@ConditionalOnProperty(name = "notification.sender.type", havingValue = "log", matchIfMissing = true)
public class LogNotificationSender implements NotificationSender {

    private static final Logger LOG = LoggerFactory.getLogger(LogNotificationSender.class);

    @Override
    public Mono<Void> send(NotificationMessage message) {
        return Mono.fromRunnable(() -> LOG.info("Notificación enviada: orderId={}, eventId={}, mensaje=\"{}\"",
                message.orderId(), message.eventId(), message.message()));
    }

    @Override
    public String channel() {
        return "log";
    }
}

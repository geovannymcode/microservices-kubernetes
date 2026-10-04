package com.geovannycode.notify_service.notify.infrastructure.messaging;

import java.util.Objects;
import java.util.stream.Collectors;

import com.geovannycode.notify_service.notify.application.NotificationService;
import com.geovannycode.notify_service.notify.domain.NotificationDeliveryException;
import com.geovannycode.notify_service.notify.domain.OrderEvent;
import jakarta.validation.Validator;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

/**
 * Consumes orders.events.v1 one record at a time (ack-mode RECORD): the offset is committed only after the
 * notification was stored and delivered, and an exception hands the record to the DefaultErrorHandler
 * (retries, then the DLT). See ADR 0007.
 */
@Component
public class OrderEventListener {

    private static final Logger LOG = LoggerFactory.getLogger(OrderEventListener.class);

    private final NotificationService notifications;
    private final Validator validator;
    private final NotifyKafkaProperties notify;

    public OrderEventListener(NotificationService notifications, Validator validator, NotifyKafkaProperties notify) {
        this.notifications = notifications;
        this.validator = validator;
        this.notify = notify;
    }

    @KafkaListener(id = "order-events", idIsGroup = false, topics = "${notify.topics.order-events}")
    public void onOrderEvent(@Payload OrderEvent event, @Header(KafkaHeaders.DELIVERY_ATTEMPT) int deliveryAttempt,
                             @Header(name = KafkaHeaders.RECEIVED_KEY, required = false) @Nullable String key) {
        validate(event);
        LOG.debug("Evento recibido: eventId={}, eventType={}, key={}, intento={}", event.eventId(), event.eventType(),
                key, deliveryAttempt);
        try {
            // The only blocking call in the service: Kafka's consumer thread, never a WebFlux one (ADR 0007).
            notifications.handle(event).block(notify.processingTimeout());
        } catch (NotificationDeliveryException failure) {
            if (deliveryAttempt >= notify.retry().maxAttempts()) {
                notifications.markFailed(Objects.requireNonNull(event.eventId())).block(notify.processingTimeout());
            }
            throw failure;
        }
    }

    private void validate(OrderEvent event) {
        var violations = validator.validate(event);
        if (!violations.isEmpty()) {
            String detail = violations.stream()
                    .map(violation -> violation.getPropertyPath() + " " + violation.getMessage())
                    .sorted().collect(Collectors.joining(", "));
            throw new InvalidOrderEventException("Evento de orden inválido (eventId=" + event.eventId() + "): " + detail);
        }
    }
}

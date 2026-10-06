package com.geovannycode.notify_service.notify.infrastructure.messaging;

import java.util.Objects;
import java.util.stream.Collectors;

import com.geovannycode.notify_service.notify.application.NotificationService;
import com.geovannycode.notify_service.notify.domain.NotificationDeliveryException;
import com.geovannycode.notify_service.notify.domain.NotificationRejectedException;
import com.geovannycode.notify_service.notify.domain.OrderEvent;
import io.micrometer.context.ContextRegistry;
import io.micrometer.context.integration.Slf4jThreadLocalAccessor;
import jakarta.validation.Validator;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
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

    /** MDC keys of every log line while an event is processed; ECS writes them as top-level fields. */
    public static final String MDC_EVENT_ID = "eventId";
    public static final String MDC_ORDER_ID = "orderId";

    private static final Logger LOG = LoggerFactory.getLogger(OrderEventListener.class);

    static {
        // The reactive chain the listener waits for runs on Netty and Reactor threads: with automatic context
        // propagation these MDC keys follow it there, like the trace ids do.
        ContextRegistry.getInstance().registerThreadLocalAccessor(new Slf4jThreadLocalAccessor(MDC_EVENT_ID, MDC_ORDER_ID));
    }

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
        try (var eventId = MDC.putCloseable(MDC_EVENT_ID, Objects.requireNonNull(event.eventId()));
             var orderId = MDC.putCloseable(MDC_ORDER_ID, String.valueOf(Objects.requireNonNull(event.data()).orderId()))) {
            process(event, deliveryAttempt, key);
        }
    }

    private void process(OrderEvent event, int deliveryAttempt, @Nullable String key) {
        LOG.debug("Evento recibido: eventId={}, eventType={}, key={}, intento={}", event.eventId(), event.eventType(),
                key, deliveryAttempt);
        try {
            // The only blocking call in the service: Kafka's consumer thread, never a WebFlux one (ADR 0007).
            notifications.handle(event).block(notify.processingTimeout());
        } catch (NotificationDeliveryException failure) {
            // A rejection is final (not retryable, see KafkaConsumerConfiguration): FAILED now, not after 3 attempts.
            if (failure instanceof NotificationRejectedException || deliveryAttempt >= notify.retry().maxAttempts()) {
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

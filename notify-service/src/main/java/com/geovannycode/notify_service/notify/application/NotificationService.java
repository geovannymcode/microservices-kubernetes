package com.geovannycode.notify_service.notify.application;

import java.time.Clock;
import java.util.Objects;

import com.geovannycode.notify_service.notify.domain.NotificationDeliveryException;
import com.geovannycode.notify_service.notify.domain.NotificationMessage;
import com.geovannycode.notify_service.notify.domain.NotificationSender;
import com.geovannycode.notify_service.notify.domain.NotifyStatus;
import com.geovannycode.notify_service.notify.domain.OrderEvent;
import com.geovannycode.notify_service.notify.infrastructure.persistence.MongoIndexInitializer;
import com.geovannycode.notify_service.notify.infrastructure.persistence.NotificationDocument;
import com.geovannycode.notify_service.notify.infrastructure.persistence.NotificationRepository;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

/**
 * Turns an order event into exactly one notification and delivers it.
 *
 * <ul>
 *   <li>unknown eventType: ignored (WARN), not an error;</li>
 *   <li>no notification yet: insert PENDING, send, SENT;</li>
 *   <li>notification already SENT: duplicate (at-least-once delivery), ignored;</li>
 *   <li>notification PENDING or FAILED: a redelivery, send again, SENT;</li>
 *   <li>send fails: the attempt is counted, the document stays PENDING and the error propagates, so Kafka
 *       redelivers; when the attempts run out the listener calls {@link #markFailed}.</li>
 * </ul>
 * The unique eventId index is what prevents duplicates: insert first and read the existing one on
 * DuplicateKeyException, never "find, then insert" (two deliveries could both find nothing).
 */
@Service
public class NotificationService {

    private static final Logger LOG = LoggerFactory.getLogger(NotificationService.class);

    private final NotificationRepository repository;
    private final NotificationSender sender;
    private final MongoIndexInitializer indexes;
    private final Clock clock;

    public NotificationService(NotificationRepository repository, NotificationSender sender,
                               MongoIndexInitializer indexes, Clock clock) {
        this.repository = repository;
        this.sender = sender;
        this.indexes = indexes;
        this.clock = clock;
    }

    /** Expects an event already validated by the listener (all required fields present). */
    public Mono<Void> handle(OrderEvent event) {
        String eventType = Objects.requireNonNull(event.eventType());
        if (!OrderEvent.COMPLETED.equals(eventType) && !OrderEvent.CANCELED.equals(eventType)) {
            LOG.warn("Evento ignorado: tipo desconocido eventType={}, eventId={}", eventType, event.eventId());
            return Mono.empty();
        }
        // Without the unique index a redelivered event could create a second notification.
        return indexes.ready().then(Mono.defer(() -> repository.insert(pending(event))))
                .onErrorResume(DuplicateKeyException.class,
                        duplicate -> repository.findByEventId(Objects.requireNonNull(event.eventId())))
                .flatMap(notification -> {
                    if (notification.status() == NotifyStatus.SENT) {
                        LOG.info("Evento duplicado ignorado: eventId={}, orderId={}", notification.eventId(),
                                notification.orderId());
                        return Mono.empty();
                    }
                    return deliver(notification);
                });
    }

    /** Retries exhausted: the notification ends FAILED (a SENT one is never downgraded). */
    public Mono<Void> markFailed(String eventId) {
        return repository.findByEventId(eventId)
                .filter(notification -> notification.status() != NotifyStatus.SENT)
                .flatMap(notification -> repository.save(notification.markFailed()))
                .doOnNext(failed -> LOG.error("Notificación fallida tras {} intentos: eventId={}, orderId={}",
                        failed.attempts(), failed.eventId(), failed.orderId()))
                .then();
    }

    private Mono<Void> deliver(NotificationDocument notification) {
        var attempted = notification.withAttempt();
        var message = new NotificationMessage(notification.eventId(), notification.orderId(), notification.message());
        return sender.send(message)
                // Only channel errors are delivery failures; persistence errors below propagate unchanged.
                .onErrorMap(error -> !(error instanceof NotificationDeliveryException),
                        error -> new NotificationDeliveryException(notification.eventId(), error))
                .onErrorResume(NotificationDeliveryException.class, failure -> {
                    LOG.warn("Fallo al enviar la notificación: eventId={}, intento={}: {}", notification.eventId(),
                            attempted.attempts(), failure.getMessage());
                    return repository.save(attempted).then(Mono.error(failure));
                })
                .then(Mono.defer(() -> repository.save(attempted.markSent(clock.instant()))))
                .doOnNext(sent -> LOG.info("Notificación enviada por {}: eventId={}, orderId={}, intento={}",
                        sent.channel(), sent.eventId(), sent.orderId(), sent.attempts()))
                .then();
    }

    private NotificationDocument pending(OrderEvent event) {
        var data = Objects.requireNonNull(event.data());
        long orderId = Objects.requireNonNull(data.orderId());
        String codeProduct = Objects.requireNonNull(data.codeProduct());
        int quantity = Objects.requireNonNull(data.quantity());
        return new NotificationDocument(null, Objects.requireNonNull(event.eventId()),
                Objects.requireNonNull(event.eventType()), orderId, codeProduct, quantity, data.cancelReason(),
                message(event.eventType(), orderId, codeProduct, quantity, data.cancelReason()), sender.channel(),
                NotifyStatus.PENDING, 0, null, null, null);
    }

    static String message(String eventType, long orderId, String codeProduct, int quantity,
                          @Nullable String cancelReason) {
        String order = "La orden " + orderId + " (" + codeProduct + " x" + quantity + ")";
        return OrderEvent.COMPLETED.equals(eventType)
                ? order + " fue completada."
                : order + " fue cancelada: " + readableReason(cancelReason) + ".";
    }

    private static String readableReason(@Nullable String cancelReason) {
        if (cancelReason == null) {
            return "motivo no indicado";
        }
        return switch (cancelReason) {
            case "PRODUCT_NOT_FOUND" -> "el producto no existe";
            case "INSUFFICIENT_STOCK" -> "no hay stock suficiente";
            default -> cancelReason;
        };
    }
}

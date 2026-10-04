package com.geovannycode.notify_service.notify.api;

import com.geovannycode.notify_service.generated.api.NotifyApiDelegate;
import com.geovannycode.notify_service.generated.dto.NotifyResponse;
import com.geovannycode.notify_service.generated.dto.NotifyStatus;
import com.geovannycode.notify_service.notify.application.NotificationService;
import org.jspecify.annotations.Nullable;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/** HTTP adapter only: the generated NotifyApiController routes and validates; NotificationService answers. */
@Component
public class NotifyApiDelegateImpl implements NotifyApiDelegate {

    private final NotificationService notifications;

    public NotifyApiDelegateImpl(NotificationService notifications) {
        this.notifications = notifications;
    }

    @Override
    public Mono<ResponseEntity<Flux<NotifyResponse>>> findAllNotify(@Nullable Long orderId, @Nullable NotifyStatus status,
                                                                    Integer limit, ServerWebExchange exchange) {
        var domainStatus = status == null ? null
                : com.geovannycode.notify_service.notify.domain.NotifyStatus.fromValue(status.getValue());
        var found = notifications.findAll(orderId, domainStatus, limit);
        return Mono.just(ResponseEntity.ok(acceptsEventStream(exchange) ? asNamedEvents(found) : found));
    }

    @Override
    public Mono<ResponseEntity<NotifyResponse>> getNotifyById(String notifyId, ServerWebExchange exchange) {
        return notifications.findById(notifyId).map(ResponseEntity::ok);
    }

    private static boolean acceptsEventStream(ServerWebExchange exchange) {
        return exchange.getRequest().getHeaders().getAccept().stream()
                .anyMatch(MediaType.TEXT_EVENT_STREAM::equalsTypeAndSubtype);
    }

    /**
     * The generator emits one signature (Flux<NotifyResponse>) for JSON, NDJSON and SSE. WebFlux's SSE writer
     * checks each element at runtime and writes ServerSentEvent instances as-is (id, event) while encoding their
     * data as NotifyResponse, so the events are passed through this declared type (same as order-service).
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private static Flux<NotifyResponse> asNamedEvents(Flux<NotifyResponse> found) {
        Flux events = found.map(notification -> ServerSentEvent.builder(notification)
                .id(notification.getId()).event("notify").build());
        return (Flux<NotifyResponse>) events;
    }
}

package com.geovannycode.notify_service.notify.api;

import com.geovannycode.notify_service.generated.api.NotifyApiDelegate;
import com.geovannycode.notify_service.generated.dto.NotifyResponse;
import com.geovannycode.notify_service.generated.dto.NotifyStatus;
import org.jspecify.annotations.Nullable;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/** Read-only API over the stored notifications. The generated NotifyApiController delegates here. */
@Component
public class NotifyApiDelegateImpl implements NotifyApiDelegate {

    // TODO(phase 5): list through NotificationQueryRepository (filters orderId/status, limit) as JSON, NDJSON or SSE.
    @Override
    public Mono<ResponseEntity<Flux<NotifyResponse>>> findAllNotify(@Nullable Long orderId, @Nullable NotifyStatus status,
                                                                    Integer limit, ServerWebExchange exchange) {
        return Mono.just(ResponseEntity.status(HttpStatus.NOT_IMPLEMENTED).build());
    }

    // TODO(phase 5): find by id, 404 ProblemDetail (notify-not-found) when it does not exist.
    @Override
    public Mono<ResponseEntity<NotifyResponse>> getNotifyById(String notifyId, ServerWebExchange exchange) {
        return Mono.just(ResponseEntity.status(HttpStatus.NOT_IMPLEMENTED).build());
    }
}

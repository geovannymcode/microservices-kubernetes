package com.geovannycode.notify_service.notify.infrastructure.persistence;

import org.springframework.data.mongodb.repository.ReactiveMongoRepository;
import reactor.core.publisher.Mono;

public interface NotificationRepository extends ReactiveMongoRepository<NotificationDocument, String> {

    Mono<NotificationDocument> findByEventId(String eventId);
}

package com.geovannycode.notify_service.notify.infrastructure.persistence;

import com.geovannycode.notify_service.notify.domain.NotifyStatus;
import org.jspecify.annotations.Nullable;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;

/**
 * Listing with optional filters: derived query methods would need one method per combination (none, orderId,
 * status, both), so the Criteria are built here instead. The status/createdAt index serves the status filter.
 */
@Repository
public class NotificationQueryRepository {

    private final ReactiveMongoTemplate mongo;

    public NotificationQueryRepository(ReactiveMongoTemplate mongo) {
        this.mongo = mongo;
    }

    /** Newest first (createdAt, then _id to break ties within the same millisecond), at most {@code limit}. */
    public Flux<NotificationDocument> find(@Nullable Long orderId, @Nullable NotifyStatus status, int limit) {
        var criteria = new Criteria();
        if (orderId != null) {
            criteria = criteria.and("orderId").is(orderId);
        }
        if (status != null) {
            criteria = criteria.and("status").is(status);
        }
        var query = Query.query(criteria)
                .with(Sort.by(Sort.Order.desc("createdAt"), Sort.Order.desc("id")))
                .limit(limit);
        return mongo.find(query, NotificationDocument.class);
    }
}

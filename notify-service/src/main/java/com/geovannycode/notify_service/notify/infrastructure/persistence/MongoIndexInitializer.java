package com.geovannycode.notify_service.notify.infrastructure.persistence;

import java.time.Duration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationStartedEvent;
import org.springframework.boot.health.contributor.AbstractReactiveHealthIndicator;
import org.springframework.boot.health.contributor.Health;
import org.springframework.context.event.EventListener;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.index.Index;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Creates the notify_orders indexes explicitly at startup (spring.data.mongodb.auto-index-creation is off, so the
 * schema is reviewed here and not derived from annotations). createIndex is idempotent for an identical definition,
 * so restarting is safe.
 *
 * <p>The unique index on eventId is what makes a redelivered event unable to create a second notification, so the
 * service must not take traffic before it exists. Creation is non-blocking and runs when the application has
 * started; this class is also the "mongoIndexes" health indicator, part of the readiness group, which stays DOWN
 * until the indexes are in place. Callers that must wait (the Kafka listener, later) can chain on {@link #ready()}.
 */
@Component("mongoIndexes")
public class MongoIndexInitializer extends AbstractReactiveHealthIndicator {

    private static final Logger LOG = LoggerFactory.getLogger(MongoIndexInitializer.class);
    static final String COLLECTION = "notify_orders";
    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    private final Mono<Void> indexes;
    private volatile boolean created;

    public MongoIndexInitializer(ReactiveMongoTemplate mongo) {
        super("No se pudo comprobar el estado de los índices de MongoDB");
        var operations = mongo.indexOps(COLLECTION);
        // cache(): every caller shares one creation; an error is not cached, so a later call retries it.
        this.indexes = Flux.just(
                        new Index().on("eventId", Sort.Direction.ASC).unique().named("ux_event_id"),
                        new Index().on("orderId", Sort.Direction.ASC).named("ix_order_id"),
                        new Index().on("status", Sort.Direction.ASC).on("createdAt", Sort.Direction.DESC)
                                .named("ix_status_created_at"))
                .concatMap(operations::createIndex)
                .then()
                .timeout(TIMEOUT)
                .doOnSuccess(done -> {
                    created = true;
                    LOG.info("Índices de {} verificados: ux_event_id, ix_order_id, ix_status_created_at", COLLECTION);
                })
                .doOnError(error -> LOG.error("No se pudieron crear los índices de {}", COLLECTION, error))
                .cache(done -> Duration.ofMillis(Long.MAX_VALUE), error -> Duration.ZERO, () -> Duration.ofMillis(Long.MAX_VALUE));
    }

    /** Completes once the indexes exist (creating them on the first subscription). */
    public Mono<Void> ready() {
        return indexes;
    }

    @EventListener(ApplicationStartedEvent.class)
    void createOnStartup() {
        ready().onErrorComplete().subscribe();
    }

    @Override
    protected Mono<Health> doHealthCheck(Health.Builder builder) {
        if (created) {
            return Mono.just(builder.up().build());
        }
        // Not yet created (startup still running or a previous attempt failed): try now and report the outcome.
        return ready().thenReturn(builder.up().build())
                .onErrorResume(error -> Mono.just(builder.down(error).build()));
    }
}

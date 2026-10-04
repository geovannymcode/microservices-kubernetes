package com.geovannycode.order.order.infrastructure.messaging;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.tracing.Span;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Component;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

/**
 * Publishes pending outbox events to Kafka: at least once, in creation order, safe with several replicas.
 *
 * <p>Each cycle locks a batch with FOR UPDATE SKIP LOCKED inside a transaction, sends it, marks what Kafka
 * acknowledged and commits. Another replica skips the locked rows instead of waiting for them, so in normal
 * conditions no event is sent twice. If a send fails, the batch stops there: that event and the rest stay
 * pending for the next cycle. A crash between the ack and the commit re-sends events, so consumers
 * deduplicate by eventId.
 */
@Component
public class OutboxRelay implements SmartLifecycle {

    private static final Logger LOG = LoggerFactory.getLogger(OutboxRelay.class);

    private static final String LOCK_PENDING = """
            SELECT id, aggregate_id, event_type, payload::text AS payload, trace_parent
            FROM outbox_event
            WHERE published_at IS NULL
            ORDER BY created_at
            LIMIT :limit
            FOR UPDATE SKIP LOCKED""";
    private static final String MARK_PUBLISHED = "UPDATE outbox_event SET published_at = now() WHERE id IN (:ids)";
    private static final String DELETE_PUBLISHED = "DELETE FROM outbox_event WHERE published_at < :threshold";
    private static final String COUNT_PENDING = "SELECT count(*) AS pending FROM outbox_event WHERE published_at IS NULL";

    private final DatabaseClient database;
    private final TransactionalOperator transactions;
    private final KafkaTemplate<String, String> kafka;
    private final OutboxProperties outbox;
    private final OutboxTracing tracing;
    private final AtomicLong pending = new AtomicLong();
    private volatile @Nullable Disposable loops;

    OutboxRelay(DatabaseClient database, TransactionalOperator transactions, KafkaTemplate<String, String> kafka,
                OutboxProperties outbox, OutboxTracing tracing, MeterRegistry meterRegistry) {
        this.database = database;
        this.transactions = transactions;
        this.kafka = kafka;
        this.outbox = outbox;
        this.tracing = tracing;
        // Refreshed after every cycle: a growing value means Kafka is down or the relay cannot keep up.
        Gauge.builder("outbox.pending", pending, AtomicLong::get)
                .description("Outbox events not yet published to Kafka").register(meterRegistry);
    }

    /** Publishes batches until one comes back incomplete; emits the number of events published. */
    public Mono<Long> publishPending() {
        return publishBatch()
                .expand(published -> published == outbox.batchSize() ? publishBatch() : Mono.empty())
                .reduce(0L, Long::sum);
    }

    /** Counts unpublished events (partial index) and updates the outbox.pending gauge. */
    public Mono<Long> refreshPendingGauge() {
        return database.sql(COUNT_PENDING)
                .map(row -> Objects.requireNonNull(row.get("pending", Long.class)))
                .one()
                .doOnNext(pending::set);
    }

    /** Deletes published events older than the retention; emits the number of rows deleted. */
    public Mono<Long> deletePublished() {
        return database.sql(DELETE_PUBLISHED)
                .bind("threshold", Instant.now().minus(outbox.retention()))
                .fetch().rowsUpdated()
                .doOnNext(deleted -> {
                    if (deleted > 0) {
                        LOG.info("Eventos publicados eliminados de la outbox: {}", deleted);
                    }
                });
    }

    private Mono<Long> publishBatch() {
        return transactions.transactional(lockPending().flatMap(this::sendInOrder).flatMap(this::markPublished));
    }

    private Mono<List<PendingEvent>> lockPending() {
        return database.sql(LOCK_PENDING)
                .bind("limit", outbox.batchSize())
                .map(row -> new PendingEvent(row.get("id", UUID.class), row.get("aggregate_id", String.class),
                        row.get("event_type", String.class), row.get("payload", String.class),
                        row.get("trace_parent", String.class)))
                .all()
                // The whole batch is read before the first send: rows are not streamed while waiting for acks.
                .collectList();
    }

    /** Emits the ids Kafka acknowledged, in order, up to the first failure. */
    private Mono<List<UUID>> sendInOrder(List<PendingEvent> events) {
        return Flux.fromIterable(events)
                .concatMap(event -> send(event).thenReturn(event.id()))
                // Marking only the acknowledged prefix keeps the creation order: the failed event is retried
                // first in the next cycle.
                .onErrorResume(error -> {
                    LOG.warn("No se pudo publicar en Kafka; los eventos siguen pendientes: {}", error.toString());
                    return Flux.empty();
                })
                .collectList();
    }

    private Mono<Void> send(PendingEvent event) {
        var message = new ProducerRecord<String, String>(outbox.topic(), event.aggregateId(), event.payload());
        message.headers()
                .add("eventType", event.eventType().getBytes(StandardCharsets.UTF_8))
                .add("eventId", event.id().toString().getBytes(StandardCharsets.UTF_8));
        // send() may block up to max.block.ms while fetching metadata (Kafka down): never on a Netty or parallel thread.
        // Inside a child span of the original request, KafkaTemplate's producer observation joins that trace.
        return Mono.using(() -> tracing.startRelaySpan(event.traceParent(), outbox.topic()),
                span -> Mono.fromFuture(() -> {
                            try (var scope = tracing.inScope(span)) {
                                return kafka.send(message);
                            }
                        })
                        .subscribeOn(Schedulers.boundedElastic())
                        .doOnError(span::error)
                        .then(),
                Span::end);
    }

    private Mono<Long> markPublished(List<UUID> ids) {
        if (ids.isEmpty()) {
            return Mono.just(0L);
        }
        return database.sql(MARK_PUBLISHED).bind("ids", ids).fetch().rowsUpdated()
                .doOnNext(published -> LOG.debug("Eventos publicados en {}: {}", outbox.topic(), published));
    }

    @Override
    public void start() {
        // onBackpressureDrop: a slow cycle (Kafka timing out) skips ticks instead of queueing them; concatMap
        // keeps a single cycle in flight per replica.
        var relay = Flux.interval(outbox.pollInterval())
                .onBackpressureDrop()
                .concatMap(tick -> publishPending().then(refreshPendingGauge()).onErrorResume(error -> {
                    LOG.error("Fallo inesperado en el ciclo del relay de la outbox", error);
                    return Mono.empty();
                }), 0)
                .subscribe();
        var cleanup = Flux.interval(outbox.cleanupInterval())
                .onBackpressureDrop()
                .concatMap(tick -> deletePublished().onErrorResume(error -> {
                    LOG.error("Fallo al limpiar la outbox", error);
                    return Mono.empty();
                }), 0)
                .subscribe();
        loops = Disposables.composite(relay, cleanup);
        LOG.info("Relay de la outbox iniciado: topic={}, intervalo={}, lote={}",
                outbox.topic(), outbox.pollInterval(), outbox.batchSize());
    }

    @Override
    public void stop() {
        var running = loops;
        if (running != null) {
            running.dispose();
            loops = null;
        }
    }

    @Override
    public boolean isRunning() {
        return loops != null;
    }

    private record PendingEvent(UUID id, String aggregateId, String eventType, String payload,
                                @Nullable String traceParent) {
    }
}

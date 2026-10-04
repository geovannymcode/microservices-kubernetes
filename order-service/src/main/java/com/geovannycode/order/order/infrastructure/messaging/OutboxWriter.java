package com.geovannycode.order.order.infrastructure.messaging;

import com.geovannycode.order.order.infrastructure.persistence.OrderEntity;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import tools.jackson.databind.json.JsonMapper;

/**
 * Appends the event of an order state change to outbox_event. It must run in the same transaction as the
 * save of the order (OrderService does that), so both are committed or rolled back together.
 */
@Component
public class OutboxWriter {

    private static final String INSERT = """
            INSERT INTO outbox_event (id, aggregate_type, aggregate_id, event_type, payload)
            VALUES (:id, 'order', :aggregateId, :eventType, CAST(:payload AS JSONB))""";

    private final DatabaseClient database;
    private final JsonMapper json;

    public OutboxWriter(DatabaseClient database, JsonMapper json) {
        this.database = database;
        this.json = json;
    }

    public Mono<Void> append(OrderEntity order) {
        return Mono.fromCallable(() -> OrderEvent.of(order))
                // Serialized once, here, with Jackson 3. JSONB normalizes key order and spacing, so the relay
                // publishes the same content, not necessarily the same bytes.
                .flatMap(event -> database.sql(INSERT)
                        .bind("id", event.eventId())
                        .bind("aggregateId", String.valueOf(event.data().orderId()))
                        .bind("eventType", event.eventType())
                        .bind("payload", json.writeValueAsString(event))
                        .then());
    }
}

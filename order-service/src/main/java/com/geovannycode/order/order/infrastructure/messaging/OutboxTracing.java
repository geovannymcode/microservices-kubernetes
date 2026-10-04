package com.geovannycode.order.order.infrastructure.messaging;

import java.util.HashMap;
import java.util.Map;

import io.micrometer.tracing.Span;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.propagation.Propagator;
import org.jspecify.annotations.Nullable;
import org.springframework.stereotype.Component;

/**
 * Carries the trace of the request that produced an event across the outbox: the writer stores its W3C
 * traceparent and the relay publishes inside a child span, so KafkaTemplate's producer span (and the
 * traceparent header it injects) belongs to the same trace as the HTTP confirmation.
 */
@Component
class OutboxTracing {

    private static final String TRACEPARENT = "traceparent";

    private final Tracer tracer;
    private final Propagator propagator;

    OutboxTracing(Tracer tracer, Propagator propagator) {
        this.tracer = tracer;
        this.propagator = propagator;
    }

    @Nullable String currentTraceParent() {
        Span current = tracer.currentSpan();
        if (current == null) {
            return null;
        }
        Map<String, String> carrier = new HashMap<>();
        propagator.inject(current.context(), carrier, Map::put);
        return carrier.get(TRACEPARENT);
    }

    /** A started span, child of the stored context when there is one (a new trace otherwise). */
    Span startRelaySpan(@Nullable String traceParent, String topic) {
        Span.Builder builder = traceParent == null
                ? tracer.spanBuilder()
                : propagator.extract(Map.of(TRACEPARENT, traceParent), (carrier, key) -> carrier.get(key));
        return builder.name("outbox relay " + topic).start();
    }

    Tracer.SpanInScope inScope(Span span) {
        return tracer.withSpan(span);
    }
}

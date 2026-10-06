package com.geovannycode.notify_service.notify.infrastructure.messaging;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import com.geovannycode.notify_service.notify.domain.NotificationMessage;
import com.geovannycode.notify_service.notify.domain.NotificationRejectedException;
import com.geovannycode.notify_service.notify.domain.NotificationSender;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

/**
 * Test channel scripted per eventId: failures, rejections and slow deliveries. Counts every attempt and keeps the
 * order of successful deliveries. Installed with @TestBean, so it is still the single NotificationSender.
 */
final class ScriptedSender implements NotificationSender {

    private final Map<String, AtomicInteger> attempts = new ConcurrentHashMap<>();
    private final Map<String, AtomicInteger> failuresLeft = new ConcurrentHashMap<>();
    private final Map<String, Duration> delays = new ConcurrentHashMap<>();
    private final Set<String> alwaysFailing = ConcurrentHashMap.newKeySet();
    private final Set<String> rejected = ConcurrentHashMap.newKeySet();
    private final List<String> delivered = new CopyOnWriteArrayList<>();
    private final Map<String, Sinks.Empty<Void>> concurrentArrivals = new ConcurrentHashMap<>();
    private final Map<String, Duration> holds = new ConcurrentHashMap<>();

    void failAlways(String eventId) {
        alwaysFailing.add(eventId);
    }

    void failTimes(String eventId, int times) {
        failuresLeft.put(eventId, new AtomicInteger(times));
    }

    void reject(String eventId) {
        rejected.add(eventId);
    }

    /** The channel takes this long (without blocking) before accepting the notification. */
    void delay(String eventId, Duration delay) {
        delays.put(eventId, delay);
    }

    /**
     * The first delivery of this event waits (without blocking) until a second delivery of the same event reaches
     * the channel, at most {@code max}: makes two consumer threads overlap on purpose instead of by timing.
     */
    void holdUntilConcurrent(String eventId, Duration max) {
        concurrentArrivals.put(eventId, Sinks.empty());
        holds.put(eventId, max);
    }

    int deliveries(String eventId) {
        var count = attempts.get(eventId);
        return count == null ? 0 : count.get();
    }

    /** eventIds of the successful deliveries, in the order they completed. */
    List<String> deliveryOrder() {
        return List.copyOf(delivered);
    }

    @Override
    public Mono<Void> send(NotificationMessage message) {
        String eventId = message.eventId();
        return Mono.defer(() -> {
            int attempt = attempts.computeIfAbsent(eventId, id -> new AtomicInteger()).incrementAndGet();
            if (rejected.contains(eventId)) {
                return Mono.error(new NotificationRejectedException(eventId, "contenido rechazado"));
            }
            if (alwaysFailing.contains(eventId) || consumeScriptedFailure(eventId)) {
                return Mono.error(new IllegalStateException("canal de prueba caído"));
            }
            Sinks.Empty<Void> arrivals = concurrentArrivals.get(eventId);
            if (arrivals != null) {
                if (attempt > 1) {
                    arrivals.tryEmitEmpty();
                    return Mono.<Void>empty().doOnSuccess(done -> delivered.add(eventId));
                }
                return arrivals.asMono().timeout(holds.get(eventId), Mono.empty())
                        .doOnSuccess(done -> delivered.add(eventId));
            }
            Duration delay = delays.getOrDefault(eventId, Duration.ZERO);
            return Mono.delay(delay).doOnNext(done -> delivered.add(eventId)).then();
        });
    }

    private boolean consumeScriptedFailure(String eventId) {
        var left = failuresLeft.get(eventId);
        return left != null && left.getAndUpdate(remaining -> Math.max(remaining - 1, 0)) > 0;
    }

    @Override
    public String channel() {
        return "test";
    }
}

package com.geovannycode.order.order.infrastructure.inventory;

import com.geovannycode.order.generated.inventory.api.InventoriesApi;
import com.geovannycode.order.generated.inventory.dto.OrderInvRequest;
import com.geovannycode.order.order.domain.CancelReason;
import com.geovannycode.order.order.domain.InventoryRejectedException;
import com.geovannycode.order.order.domain.InventoryUnavailableException;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.ratelimiter.RateLimiter;
import io.github.resilience4j.ratelimiter.RateLimiterRegistry;
import io.github.resilience4j.reactor.circuitbreaker.operator.CircuitBreakerOperator;
import io.github.resilience4j.reactor.ratelimiter.operator.RateLimiterOperator;
import io.github.resilience4j.reactor.retry.RetryOperator;
import io.github.resilience4j.reactor.timelimiter.TimeLimiterOperator;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryRegistry;
import io.github.resilience4j.timelimiter.TimeLimiter;
import io.github.resilience4j.timelimiter.TimeLimiterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import reactor.core.publisher.Mono;

/**
 * The only way Order talks to Inventory: one business operation that hides the HTTP client, the
 * resilience policy and the translation of Inventory's answers into domain exceptions.
 */
@Component
public class InventoryGateway {

    private static final Logger LOG = LoggerFactory.getLogger(InventoryGateway.class);
    static final String INSTANCE = "inventory";

    private final InventoriesApi inventory;
    private final TimeLimiter timeLimiter;
    private final RateLimiter rateLimiter;
    private final CircuitBreaker circuitBreaker;
    private final Retry retry;

    public InventoryGateway(InventoriesApi inventory, TimeLimiterRegistry timeLimiters, RateLimiterRegistry rateLimiters,
                            CircuitBreakerRegistry circuitBreakers, RetryRegistry retries) {
        this.inventory = inventory;
        this.timeLimiter = timeLimiters.timeLimiter(INSTANCE);
        this.rateLimiter = rateLimiters.rateLimiter(INSTANCE);
        this.circuitBreaker = circuitBreakers.circuitBreaker(INSTANCE);
        this.retry = retries.retry(INSTANCE);
        // One line per transition (CLOSED -> OPEN -> HALF_OPEN -> CLOSED): the timeline of an Inventory outage.
        circuitBreaker.getEventPublisher().onStateTransition(event -> LOG.info("Circuito hacia Inventory: {} -> {}",
                event.getStateTransition().getFromState(), event.getStateTransition().getToState()));
    }

    /**
     * Decreases stock for the order. The Idempotency-Key is derived from the order id, so every retry (here or
     * from a later confirmation) is the same request for Inventory and stock is never decreased twice.
     *
     * @return empty on success; InventoryRejectedException for a business rejection; InventoryUnavailableException
     *         for any technical failure
     */
    public Mono<Void> reserveStock(long orderId, String codeProduct, int quantity) {
        String idempotencyKey = "order-" + orderId;
        return Mono.defer(() -> inventory.decreaseInventory(codeProduct,
                        Mono.just(new OrderInvRequest().orderCount(quantity)), idempotencyKey))
                // Business answers become domain exceptions before resilience, so they are neither retried
                // nor counted by the circuit breaker (both ignore them).
                .onErrorMap(WebClientResponseException.class, error -> translate(error, codeProduct))
                // transformDeferred applies inside-out: the first operator wraps the call, the last wraps them all.
                // TimeLimiter bounds each attempt; RateLimiter and CircuitBreaker see every attempt; Retry is
                // outermost, so each retry is admitted again by the circuit breaker and the rate limiter.
                .transformDeferred(TimeLimiterOperator.of(timeLimiter))
                .transformDeferred(RateLimiterOperator.of(rateLimiter))
                .transformDeferred(CircuitBreakerOperator.of(circuitBreaker))
                .transformDeferred(RetryOperator.of(retry))
                .onErrorMap(InventoryGateway::isTechnicalFailure, InventoryUnavailableException::new)
                .then();
    }

    private static Throwable translate(WebClientResponseException error, String codeProduct) {
        if (error.getStatusCode().is5xxServerError()) {
            return error;
        }
        if (error.getStatusCode().isSameCodeAs(HttpStatus.NOT_FOUND)) {
            return new InventoryRejectedException(codeProduct, CancelReason.PRODUCT_NOT_FOUND);
        }
        if (error.getStatusCode().isSameCodeAs(HttpStatus.CONFLICT)) {
            return new InventoryRejectedException(codeProduct, CancelReason.INSUFFICIENT_STOCK);
        }
        // Any other 4xx (400, 422...) means Order sent an invalid request: an integration bug, never retried.
        return new IllegalStateException("Inventory rechazó la solicitud con " + error.getStatusCode().value()
                + ": " + error.getResponseBodyAsString(), error);
    }

    private static boolean isTechnicalFailure(Throwable error) {
        return !(error instanceof InventoryRejectedException) && !(error instanceof IllegalStateException);
    }
}

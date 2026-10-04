package com.geovannycode.order.order.infrastructure.inventory;

import java.time.Duration;

import com.geovannycode.order.TestcontainersConfiguration;
import com.geovannycode.order.order.domain.CancelReason;
import com.geovannycode.order.order.domain.InventoryRejectedException;
import com.geovannycode.order.order.domain.InventoryUnavailableException;
import com.github.tomakehurst.wiremock.junit5.WireMockExtension;
import com.github.tomakehurst.wiremock.stubbing.Scenario;
import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import reactor.test.StepVerifier;

import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.equalToJson;
import static com.github.tomakehurst.wiremock.client.WireMock.put;
import static com.github.tomakehurst.wiremock.client.WireMock.putRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.assertj.core.api.Assertions.assertThat;

// WireMock plays Inventory; application-test.yml shortens the time limiter (500ms) and the retry backoff.
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@Import(TestcontainersConfiguration.class)
@ActiveProfiles("test")
final class InventoryGatewayIT {

    @RegisterExtension
    static final WireMockExtension INVENTORY = WireMockExtension.newInstance()
            .options(wireMockConfig().dynamicPort()).build();

    private static final String CODE = "AC-1550";
    private static final String DECREASE = InventoryStubs.decreasePath(CODE);
    private static final Duration TIMEOUT = Duration.ofSeconds(15);

    @DynamicPropertySource
    static void inventoryBaseUrl(DynamicPropertyRegistry registry) {
        registry.add("inventory.client.base-url", () -> INVENTORY.baseUrl() + InventoryStubs.BASE_PATH);
    }

    private final InventoryGateway gateway;
    private final CircuitBreaker circuitBreaker;

    @Autowired
    InventoryGatewayIT(InventoryGateway gateway, CircuitBreakerRegistry circuitBreakers) {
        this.gateway = gateway;
        this.circuitBreaker = circuitBreakers.circuitBreaker(InventoryGateway.INSTANCE);
    }

    @BeforeEach
    void resetInventoryAndCircuit() {
        INVENTORY.resetAll();
        circuitBreaker.reset();
    }

    @Test
    void successSendsTheOrderIdempotencyKeyAndQuantity() {
        stubDecrease(InventoryStubs.decreased(CODE, 48));

        StepVerifier.create(gateway.reserveStock(42, "AC-1550", 2)).expectComplete().verify(TIMEOUT);

        INVENTORY.verify(1, putRequestedFor(urlEqualTo(DECREASE))
                .withHeader("Idempotency-Key", equalTo("order-42"))
                .withRequestBody(equalToJson("{\"orderCount\":2}")));
    }

    @Test
    void notFoundIsABusinessRejectionAndIsNotRetried() {
        stubDecrease(problem(404));
        StepVerifier.create(gateway.reserveStock(1, "AC-1550", 1))
                .expectErrorSatisfies(error -> assertThat(error).isInstanceOf(InventoryRejectedException.class)
                        .extracting(e -> ((InventoryRejectedException) e).reason()).isEqualTo(CancelReason.PRODUCT_NOT_FOUND))
                .verify(TIMEOUT);
        INVENTORY.verify(1, putRequestedFor(urlEqualTo(DECREASE)));
    }

    @Test
    void conflictIsInsufficientStockAndIsNotRetried() {
        stubDecrease(problem(409));
        StepVerifier.create(gateway.reserveStock(1, "AC-1550", 1))
                .expectErrorSatisfies(error -> assertThat(error).isInstanceOf(InventoryRejectedException.class)
                        .extracting(e -> ((InventoryRejectedException) e).reason()).isEqualTo(CancelReason.INSUFFICIENT_STOCK))
                .verify(TIMEOUT);
        INVENTORY.verify(1, putRequestedFor(urlEqualTo(DECREASE)));
    }

    @Test
    void otherClientErrorsAreIntegrationBugsAndAreNotRetried() {
        stubDecrease(problem(422));
        StepVerifier.create(gateway.reserveStock(1, "AC-1550", 1))
                .expectError(IllegalStateException.class).verify(TIMEOUT);
        INVENTORY.verify(1, putRequestedFor(urlEqualTo(DECREASE)));
    }

    @Test
    void persistentServerErrorBecomesUnavailableAfterThreeAttempts() {
        stubDecrease(problem(500));
        StepVerifier.create(gateway.reserveStock(1, "AC-1550", 1))
                .expectError(InventoryUnavailableException.class).verify(TIMEOUT);
        INVENTORY.verify(3, putRequestedFor(urlEqualTo(DECREASE)));
    }

    @Test
    void transientServerErrorsAreRetriedWithTheSameIdempotencyKey() {
        INVENTORY.stubFor(put(urlEqualTo(DECREASE)).inScenario("flaky").whenScenarioStateIs(Scenario.STARTED)
                .willReturn(problem(500)).willSetStateTo("second"));
        INVENTORY.stubFor(put(urlEqualTo(DECREASE)).inScenario("flaky").whenScenarioStateIs("second")
                .willReturn(problem(500)).willSetStateTo("third"));
        INVENTORY.stubFor(put(urlEqualTo(DECREASE)).inScenario("flaky").whenScenarioStateIs("third")
                .willReturn(InventoryStubs.decreased(CODE, 9)));

        StepVerifier.create(gateway.reserveStock(7, "AC-1550", 1)).expectComplete().verify(TIMEOUT);

        INVENTORY.verify(3, putRequestedFor(urlEqualTo(DECREASE)).withHeader("Idempotency-Key", equalTo("order-7")));
    }

    @Test
    void slowInventoryTimesOutEachAttempt() {
        stubDecrease(InventoryStubs.decreased(CODE, 9).withFixedDelay(3_000));
        StepVerifier.create(gateway.reserveStock(1, "AC-1550", 1))
                .expectErrorSatisfies(error -> assertThat(error).isInstanceOf(InventoryUnavailableException.class)
                        .hasRootCauseInstanceOf(java.util.concurrent.TimeoutException.class))
                .verify(TIMEOUT);
        INVENTORY.verify(3, putRequestedFor(urlEqualTo(DECREASE)));
    }

    @Test
    void openCircuitFailsFastWithoutCallingInventory() {
        stubDecrease(problem(500));
        for (int call = 0; call < 2; call++) {
            StepVerifier.create(gateway.reserveStock(1, "AC-1550", 1))
                    .expectError(InventoryUnavailableException.class).verify(TIMEOUT);
        }
        assertThat(circuitBreaker.getState()).isEqualTo(CircuitBreaker.State.OPEN);
        INVENTORY.resetRequests();

        StepVerifier.create(gateway.reserveStock(1, "AC-1550", 1))
                .expectErrorSatisfies(error -> assertThat(error).isInstanceOf(InventoryUnavailableException.class)
                        .hasCauseInstanceOf(CallNotPermittedException.class))
                .verify(TIMEOUT);
        INVENTORY.verify(0, putRequestedFor(urlEqualTo(DECREASE)));
    }

    @Test
    void businessRejectionsNeverOpenTheCircuit() {
        stubDecrease(problem(404));
        // More calls than minimum-number-of-calls (5): with only four the circuit could not open anyway.
        for (int call = 0; call < 6; call++) {
            StepVerifier.create(gateway.reserveStock(call, "AC-1550", 1))
                    .expectError(InventoryRejectedException.class).verify(TIMEOUT);
        }
        assertThat(circuitBreaker.getState()).isEqualTo(CircuitBreaker.State.CLOSED);
        assertThat(circuitBreaker.getMetrics().getNumberOfFailedCalls()).isZero();
        INVENTORY.verify(6, putRequestedFor(urlEqualTo(DECREASE)));
    }

    private static void stubDecrease(com.github.tomakehurst.wiremock.client.ResponseDefinitionBuilder response) {
        INVENTORY.stubFor(put(urlEqualTo(DECREASE)).willReturn(response));
    }

    private static com.github.tomakehurst.wiremock.client.ResponseDefinitionBuilder problem(int status) {
        return InventoryStubs.problem(status, CODE);
    }
}

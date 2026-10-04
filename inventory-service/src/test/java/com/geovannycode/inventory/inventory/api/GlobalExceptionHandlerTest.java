package com.geovannycode.inventory.inventory.api;

import java.time.Duration;

import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.web.server.MockServerWebExchange;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;

final class GlobalExceptionHandlerTest {

    @Test
    void unexpectedErrorsDoNotExposeInternalDetails() {
        var exchange = MockServerWebExchange.from(MockServerHttpRequest.get("/services-inventory/inventories"));
        StepVerifier.create(new GlobalExceptionHandler().unexpected(new IllegalStateException("secret-password"), exchange))
                .assertNext(response -> {
                    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.INTERNAL_SERVER_ERROR);
                    var detail = (ProblemDetail) response.getBody();
                    assertThat(detail).isNotNull();
                    assertThat(detail.getDetail()).doesNotContain("secret-password").contains("error interno");
                    assertThat(detail.getType().toString()).isEqualTo("https://geovannycode.com/problems/internal-error");
                    assertThat(detail.getProperties()).containsKey("timestamp").doesNotContainKeys("trace", "exception");
                }).expectComplete().verify(Duration.ofSeconds(2));
    }

    @Test
    void committedResponsePropagatesOriginalFailure() {
        var exchange = MockServerWebExchange.from(MockServerHttpRequest.get("/inventories"));
        var failure = new IllegalStateException("stream interrupted");
        StepVerifier.create(exchange.getResponse().setComplete()).verifyComplete();
        var handler = new GlobalExceptionHandler();
        StepVerifier.create(handler.unexpected(failure, exchange)).expectErrorMatches(error -> error == failure).verify();
        StepVerifier.create(handler.handleExceptionInternal(failure, null, new org.springframework.http.HttpHeaders(),
                HttpStatus.INTERNAL_SERVER_ERROR, exchange)).expectErrorMatches(error -> error == failure).verify();
    }

    @Test
    void internalFrameworkFailureUsesSanitizedProblem() {
        var exchange = MockServerWebExchange.from(MockServerHttpRequest.get("/inventories"));
        StepVerifier.create(new GlobalExceptionHandler().handleExceptionInternal(new IllegalStateException("secret"), null,
                new org.springframework.http.HttpHeaders(), HttpStatus.INTERNAL_SERVER_ERROR, exchange))
                .assertNext(response -> {
                    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.INTERNAL_SERVER_ERROR);
                    assertThat(((ProblemDetail) response.getBody()).getDetail()).doesNotContain("secret");
                }).verifyComplete();
    }

    @Test
    void databaseOutageIsServiceUnavailableButRejectedStatementIsServerError() {
        var handler = new GlobalExceptionHandler();
        var connectionRefused = new org.springframework.dao.DataAccessResourceFailureException("Failed to obtain R2DBC Connection",
                new io.r2dbc.spi.R2dbcNonTransientResourceException("Connection refused"));
        var lockWaitTimeout = new org.springframework.dao.DataAccessResourceFailureException("executeMany",
                new io.r2dbc.spi.R2dbcNonTransientResourceException("Lock wait timeout exceeded", "HY000", 1205));
        var checkViolated = new org.springframework.dao.DataAccessResourceFailureException("executeMany",
                new io.r2dbc.spi.R2dbcNonTransientResourceException("Check constraint 'ck_products_code_format' is violated.",
                        "HY000", 3819));
        assertStatus(handler.databaseUnavailable(connectionRefused, exchange()), HttpStatus.SERVICE_UNAVAILABLE);
        assertStatus(handler.databaseUnavailable(lockWaitTimeout, exchange()), HttpStatus.SERVICE_UNAVAILABLE);
        assertStatus(handler.databaseUnavailable(checkViolated, exchange()), HttpStatus.INTERNAL_SERVER_ERROR);
    }

    private static MockServerWebExchange exchange() {
        return MockServerWebExchange.from(MockServerHttpRequest.put("/services-inventory/inventories/AC-1550"));
    }

    private static void assertStatus(reactor.core.publisher.Mono<org.springframework.http.ResponseEntity<Object>> response,
                                     HttpStatus expected) {
        StepVerifier.create(response).assertNext(entity -> assertThat(entity.getStatusCode()).isEqualTo(expected))
                .verifyComplete();
    }
}

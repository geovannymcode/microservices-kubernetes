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
                    assertThat(detail.getType().toString()).isEqualTo("https://codearti.com/problems/internal-error");
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
    void invalidReturnValueIsServerError() {
        var error = org.mockito.Mockito.mock(org.springframework.web.method.annotation.HandlerMethodValidationException.class);
        org.mockito.Mockito.when(error.isForReturnValue()).thenReturn(true);
        var exchange = MockServerWebExchange.from(MockServerHttpRequest.get("/inventories"));
        StepVerifier.create(new GlobalExceptionHandler().handleHandlerMethodValidationException(error,
                new org.springframework.http.HttpHeaders(), HttpStatus.INTERNAL_SERVER_ERROR, exchange))
                .assertNext(response -> assertThat(response.getStatusCode()).isEqualTo(HttpStatus.INTERNAL_SERVER_ERROR))
                .verifyComplete();
    }

    @Test
    void methodValidationPreservesFieldErrorsAndNamesUnnamedParameters() {
        var error = org.mockito.Mockito.mock(org.springframework.web.method.annotation.HandlerMethodValidationException.class);
        var result = org.mockito.Mockito.mock(org.springframework.validation.method.ParameterValidationResult.class);
        var parameter = org.mockito.Mockito.mock(org.springframework.core.MethodParameter.class);
        org.mockito.Mockito.when(result.getMethodParameter()).thenReturn(parameter);
        org.mockito.Mockito.when(result.getResolvableErrors()).thenReturn(java.util.List.of(
                new org.springframework.validation.FieldError("request", "stock", "Stock inválido."),
                new org.springframework.context.support.DefaultMessageSourceResolvable("invalid")));
        org.mockito.Mockito.when(error.getParameterValidationResults()).thenReturn(java.util.List.of(result));
        var exchange = MockServerWebExchange.from(MockServerHttpRequest.get("/inventories"));
        StepVerifier.create(new GlobalExceptionHandler().handleHandlerMethodValidationException(error,
                new org.springframework.http.HttpHeaders(), HttpStatus.BAD_REQUEST, exchange))
                .assertNext(response -> {
                    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
                    var detail = (ProblemDetail) response.getBody();
                    assertThat(detail.getProperties().get("errors")).isEqualTo(java.util.List.of(
                            new GlobalExceptionHandler.ValidationError("stock", "Stock inválido."),
                            new GlobalExceptionHandler.ValidationError("request", "Valor inválido.")));
                }).verifyComplete();
    }
}

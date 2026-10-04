package com.geovannycode.order.order.api;

import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

import com.geovannycode.order.order.domain.IllegalOrderStateException;
import com.geovannycode.order.order.domain.InventoryUnavailableException;
import com.geovannycode.order.order.domain.OrderNotFoundException;
import com.geovannycode.order.order.domain.OrderRejectedException;
import jakarta.validation.ConstraintViolationException;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.MediaType;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.bind.support.WebExchangeBindException;
import org.springframework.web.reactive.result.method.annotation.ResponseEntityExceptionHandler;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.ServerWebInputException;
import reactor.core.publisher.Mono;

/** RFC 9457 errors with the same type/title/instance/timestamp/errors shape as Inventory. */
@RestControllerAdvice
@Order(Ordered.HIGHEST_PRECEDENCE)
public class GlobalExceptionHandler extends ResponseEntityExceptionHandler {

    private static final Logger LOG = LoggerFactory.getLogger(GlobalExceptionHandler.class);

    private final Duration retryAfter;

    // Same property as the circuit breaker's open state: clients retry when the circuit may be half-open again.
    public GlobalExceptionHandler(
            @Value("${resilience4j.circuitbreaker.instances.inventory.wait-duration-in-open-state:30s}") Duration retryAfter) {
        this.retryAfter = retryAfter;
    }

    @ExceptionHandler(OrderNotFoundException.class)
    public Mono<ResponseEntity<Object>> orderNotFound(OrderNotFoundException error, ServerWebExchange exchange) {
        return problem(HttpStatus.NOT_FOUND, "order-not-found", "Orden no encontrada", message(error), exchange);
    }

    @ExceptionHandler(OrderRejectedException.class)
    public Mono<ResponseEntity<Object>> orderRejected(OrderRejectedException error, ServerWebExchange exchange) {
        var detail = details(HttpStatus.CONFLICT, "order-rejected", "Orden rechazada", message(error), exchange);
        detail.setProperty("reason", error.reason().name());
        return respond(HttpStatus.CONFLICT, detail);
    }

    @ExceptionHandler(IllegalOrderStateException.class)
    public Mono<ResponseEntity<Object>> illegalState(IllegalOrderStateException error, ServerWebExchange exchange) {
        var detail = details(HttpStatus.CONFLICT, "order-canceled", "Orden cancelada", message(error), exchange);
        detail.setProperty("reason", "ORDER_CANCELED");
        return respond(HttpStatus.CONFLICT, detail);
    }

    @ExceptionHandler(InventoryUnavailableException.class)
    public Mono<ResponseEntity<Object>> inventoryUnavailable(InventoryUnavailableException error, ServerWebExchange exchange) {
        var detail = details(HttpStatus.SERVICE_UNAVAILABLE, "inventory-unavailable", "Inventario no disponible",
                "El servicio de inventario no está disponible; la orden sigue pendiente. Inténtalo de nuevo más tarde.",
                exchange);
        return Mono.just(ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                .header(HttpHeaders.RETRY_AFTER, Long.toString(retryAfter.toSeconds()))
                .contentType(MediaType.APPLICATION_PROBLEM_JSON).body(detail));
    }

    @Override
    protected Mono<ResponseEntity<Object>> handleWebExchangeBindException(WebExchangeBindException error,
            HttpHeaders headers, HttpStatusCode status, ServerWebExchange exchange) {
        var errors = error.getFieldErrors().stream()
                .map(field -> new ValidationError(field.getField(),
                        Objects.requireNonNullElse(field.getDefaultMessage(), "Valor inválido.")))
                .sorted(Comparator.comparing(ValidationError::field).thenComparing(ValidationError::message))
                .toList();
        return validation(errors, exchange);
    }

    // Path and query constraints (@Min on orderId) arrive here: the generated API is @Validated (AOP validation).
    @ExceptionHandler(ConstraintViolationException.class)
    public Mono<ResponseEntity<Object>> constraintViolation(ConstraintViolationException error, ServerWebExchange exchange) {
        var errors = error.getConstraintViolations().stream().map(violation -> {
            String path = violation.getPropertyPath().toString();
            return new ValidationError(path.substring(path.lastIndexOf('.') + 1), violation.getMessage());
        }).sorted(Comparator.comparing(ValidationError::field)).toList();
        return validation(errors, exchange);
    }

    @Override
    protected Mono<ResponseEntity<Object>> handleServerWebInputException(ServerWebInputException error,
            HttpHeaders headers, HttpStatusCode status, ServerWebExchange exchange) {
        return problem(HttpStatus.BAD_REQUEST, "invalid-request", "Solicitud inválida",
                "El cuerpo JSON o los parámetros de la solicitud no son válidos.", exchange);
    }

    @Override
    protected Mono<ResponseEntity<Object>> handleExceptionInternal(Exception error, @Nullable Object body,
            HttpHeaders headers, HttpStatusCode status, ServerWebExchange exchange) {
        if (exchange.getResponse().isCommitted()) {
            return Mono.error(error);
        }
        if (status.is5xxServerError()) {
            return unexpected(error, exchange);
        }
        var detail = details(status, "http-" + status.value(), "Solicitud rechazada",
                "No se pudo procesar la solicitud HTTP.", exchange);
        return Mono.just(ResponseEntity.status(status).headers(headers)
                .contentType(MediaType.APPLICATION_PROBLEM_JSON).body(detail));
    }

    @ExceptionHandler(Exception.class)
    public Mono<ResponseEntity<Object>> unexpected(Exception error, ServerWebExchange exchange) {
        LOG.error("Error inesperado al procesar la ruta {}", exchange.getRequest().getPath().value(), error);
        if (exchange.getResponse().isCommitted()) {
            return Mono.error(error);
        }
        return problem(HttpStatus.INTERNAL_SERVER_ERROR, "internal-error", "Error interno",
                "Ocurrió un error interno. Inténtalo de nuevo más tarde.", exchange);
    }

    private Mono<ResponseEntity<Object>> validation(List<ValidationError> errors, ServerWebExchange exchange) {
        var detail = details(HttpStatus.BAD_REQUEST, "validation-error", "Solicitud inválida",
                "La solicitud tiene campos inválidos.", exchange);
        detail.setProperty("errors", errors);
        return respond(HttpStatus.BAD_REQUEST, detail);
    }

    private Mono<ResponseEntity<Object>> problem(HttpStatus status, String slug, String title, String message,
                                                  ServerWebExchange exchange) {
        return respond(status, details(status, slug, title, message, exchange));
    }

    private static Mono<ResponseEntity<Object>> respond(HttpStatus status, ProblemDetail detail) {
        return Mono.just(ResponseEntity.status(status).contentType(MediaType.APPLICATION_PROBLEM_JSON).body(detail));
    }

    private static ProblemDetail details(HttpStatusCode status, String slug, String title, String message,
                                         ServerWebExchange exchange) {
        var detail = ProblemDetail.forStatusAndDetail(status, message);
        detail.setTitle(title);
        detail.setType(URI.create("https://codearti.com/problems/" + slug));
        detail.setInstance(URI.create(exchange.getRequest().getPath().value()));
        detail.setProperty("timestamp", Instant.now().toString());
        return detail;
    }

    private static String message(RuntimeException error) {
        return Objects.requireNonNullElse(error.getMessage(), "Error de negocio.");
    }

    public record ValidationError(String field, String message) {
    }
}

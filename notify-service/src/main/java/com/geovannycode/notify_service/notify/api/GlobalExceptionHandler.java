package com.geovannycode.notify_service.notify.api;

import java.net.URI;
import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

import com.geovannycode.notify_service.notify.domain.NotifyNotFoundException;
import jakarta.validation.ConstraintViolationException;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
import org.springframework.web.reactive.result.method.annotation.ResponseEntityExceptionHandler;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.ServerWebInputException;
import reactor.core.publisher.Mono;

/** RFC 9457 errors with the same type/title/instance/timestamp/errors shape as Inventory and Order. */
@RestControllerAdvice
@Order(Ordered.HIGHEST_PRECEDENCE)
public class GlobalExceptionHandler extends ResponseEntityExceptionHandler {

    private static final Logger LOG = LoggerFactory.getLogger(GlobalExceptionHandler.class);

    @ExceptionHandler(NotifyNotFoundException.class)
    public Mono<ResponseEntity<Object>> notifyNotFound(NotifyNotFoundException error, ServerWebExchange exchange) {
        return problem(HttpStatus.NOT_FOUND, "notify-not-found", "Notificación no encontrada",
                Objects.requireNonNullElse(error.getMessage(), "La notificación no existe."), exchange);
    }

    // Query and path constraints (@Max on limit, @Pattern on notifyId) arrive here: the generated API is @Validated,
    // which validates through the AOP proxy, so Spring's HandlerMethodValidationException is never raised.
    @ExceptionHandler(ConstraintViolationException.class)
    public Mono<ResponseEntity<Object>> constraintViolation(ConstraintViolationException error, ServerWebExchange exchange) {
        var errors = error.getConstraintViolations().stream().map(violation -> {
            String path = violation.getPropertyPath().toString();
            String field = path.substring(path.lastIndexOf('.') + 1);
            return new ValidationError(field, validationMessage(field, violation.getMessage()));
        }).sorted(Comparator.comparing(ValidationError::field)).toList();
        return validation(errors, exchange);
    }

    // Unparseable parameters: ?status=otro (not in the enum), ?orderId=abc.
    @Override
    protected Mono<ResponseEntity<Object>> handleServerWebInputException(ServerWebInputException error,
            HttpHeaders headers, HttpStatusCode status, ServerWebExchange exchange) {
        return problem(HttpStatus.BAD_REQUEST, "invalid-request", "Solicitud inválida",
                "Los parámetros de la solicitud no son válidos.", exchange);
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
                "La solicitud tiene parámetros inválidos.", exchange);
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
        detail.setType(URI.create("https://geovannycode.com/problems/" + slug));
        detail.setInstance(URI.create(exchange.getRequest().getPath().value()));
        detail.setProperty("timestamp", Instant.now().toString());
        return detail;
    }

    // Single source for parameter messages: the generated interface carries @Min/@Max/@Pattern without messages,
    // and Hibernate Validator's defaults follow the JVM locale.
    private static String validationMessage(String field, @Nullable String fallback) {
        return switch (field) {
            case "orderId" -> "El ID de la orden debe ser 1 o mayor.";
            case "limit" -> "El límite debe estar entre 1 y 500.";
            case "notifyId" -> "El ID de la notificación debe tener 24 caracteres hexadecimales.";
            default -> Objects.requireNonNullElse(fallback, "Valor inválido.");
        };
    }

    public record ValidationError(String field, String message) {
    }
}

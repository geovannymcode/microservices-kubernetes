package com.geovannycode.inventory.inventory.api;

import java.net.URI;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import com.geovannycode.inventory.inventory.domain.DuplicateProductException;
import com.geovannycode.inventory.inventory.domain.IdempotencyKeyReusedException;
import com.geovannycode.inventory.inventory.domain.InsufficientStockException;
import com.geovannycode.inventory.inventory.domain.ProductNotFoundException;
import io.r2dbc.spi.R2dbcException;
import jakarta.validation.ConstraintViolationException;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.TransientDataAccessException;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.MediaType;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.transaction.CannotCreateTransactionException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.bind.support.WebExchangeBindException;
import org.springframework.web.reactive.result.method.annotation.ResponseEntityExceptionHandler;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.ServerWebInputException;
import reactor.core.publisher.Mono;

@RestControllerAdvice
@Order(Ordered.HIGHEST_PRECEDENCE)
public final class GlobalExceptionHandler extends ResponseEntityExceptionHandler {

    private static final Logger LOG = LoggerFactory.getLogger(GlobalExceptionHandler.class);
    // MySQL errors that mean "try again later": too many connections, server shutting down, lock wait timeout.
    private static final Set<Integer> TRANSIENT_SERVER_ERRORS = Set.of(1040, 1053, 1205);

    @ExceptionHandler(ProductNotFoundException.class)
    public Mono<ResponseEntity<Object>> productNotFound(ProductNotFoundException error, ServerWebExchange exchange) {
        return problem(HttpStatus.NOT_FOUND, "product-not-found", "Producto no encontrado",
                Objects.requireNonNullElse(error.getMessage(), "El producto no existe."), exchange);
    }

    @ExceptionHandler(DuplicateProductException.class)
    public Mono<ResponseEntity<Object>> duplicateProduct(DuplicateProductException error, ServerWebExchange exchange) {
        return problem(HttpStatus.CONFLICT, "duplicate-product", "Producto duplicado",
                Objects.requireNonNullElse(error.getMessage(), "El código ya existe."), exchange);
    }

    @ExceptionHandler(InsufficientStockException.class)
    public Mono<ResponseEntity<Object>> insufficientStock(InsufficientStockException error, ServerWebExchange exchange) {
        return problem(HttpStatus.CONFLICT, "insufficient-stock", "Stock insuficiente",
                Objects.requireNonNullElse(error.getMessage(), "No hay stock suficiente."), exchange);
    }

    @ExceptionHandler(IdempotencyKeyReusedException.class)
    public Mono<ResponseEntity<Object>> idempotencyKeyReused(IdempotencyKeyReusedException error,
                                                             ServerWebExchange exchange) {
        return problem(HttpStatus.UNPROCESSABLE_CONTENT, "idempotency-key-reused", "Clave de idempotencia reutilizada",
                Objects.requireNonNullElse(error.getMessage(), "La clave ya se usó con otra solicitud."), exchange);
    }

    /**
     * Database outage or pool exhaustion: 503 + Retry-After tells callers (Order's circuit breaker) the
     * failure is transient, and a one-line WARN avoids flooding the logs with a stack trace per request.
     */
    @ExceptionHandler({DataAccessResourceFailureException.class, TransientDataAccessException.class,
            CannotCreateTransactionException.class})
    public Mono<ResponseEntity<Object>> databaseUnavailable(Exception error, ServerWebExchange exchange) {
        if (isRejectedStatement(error)) {
            return unexpected(error, exchange);
        }
        LOG.warn("Base de datos no disponible al procesar la ruta {}: {}", exchange.getRequest().getPath().value(),
                error.toString());
        if (exchange.getResponse().isCommitted()) {
            return Mono.error(error);
        }
        return Mono.just(ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                .header(HttpHeaders.RETRY_AFTER, "5")
                .contentType(MediaType.APPLICATION_PROBLEM_JSON)
                .body(details(HttpStatus.SERVICE_UNAVAILABLE, "database-unavailable", "Servicio no disponible",
                        "El inventario no está disponible temporalmente. Inténtalo de nuevo en unos segundos.", exchange)));
    }

    @Override
    protected Mono<ResponseEntity<Object>> handleWebExchangeBindException(WebExchangeBindException error,
            HttpHeaders headers, HttpStatusCode status, ServerWebExchange exchange) {
        var errors = error.getFieldErrors().stream()
                .map(field -> new ValidationError(field.getField(),
                        Objects.requireNonNullElse(field.getDefaultMessage(), "Valor inválido.")))
                .sorted(java.util.Comparator.comparing(ValidationError::field).thenComparing(ValidationError::message))
                .toList();
        return validation(errors, exchange);
    }

    // Parameter validation (path, query, header) arrives here: @Validated on the controller validates through
    // the AOP proxy, so Spring's built-in HandlerMethodValidationException is never raised.
    @ExceptionHandler(ConstraintViolationException.class)
    public Mono<ResponseEntity<Object>> constraintViolation(ConstraintViolationException error, ServerWebExchange exchange) {
        var errors = error.getConstraintViolations().stream().map(violation -> {
            String path = violation.getPropertyPath().toString();
            String field = path.substring(path.lastIndexOf('.') + 1);
            return new ValidationError(field, validationMessage(field, violation.getMessage()));
        }).sorted(java.util.Comparator.comparing(ValidationError::field)).toList();
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

    /**
     * r2dbc-mysql reports server errors without a specific SQLSTATE (e.g. 3819, CHECK constraint violated) as
     * R2dbcNonTransientResourceException, which Spring translates into DataAccessResourceFailureException.
     * A statement the server rejected will fail again on retry, so it must not be answered as 503.
     */
    private static boolean isRejectedStatement(Throwable error) {
        for (Throwable cause = error; cause != null; cause = cause.getCause()) {
            if (cause instanceof R2dbcException r2dbc && r2dbc.getErrorCode() != 0) {
                return !TRANSIENT_SERVER_ERRORS.contains(r2dbc.getErrorCode());
            }
        }
        return false;
    }

    private Mono<ResponseEntity<Object>> validation(List<ValidationError> errors, ServerWebExchange exchange) {
        var detail = details(HttpStatus.BAD_REQUEST, "validation-error", "Solicitud inválida",
                "Los datos de entrada no son válidos.", exchange);
        detail.setProperty("errors", errors);
        return Mono.just(ResponseEntity.badRequest().contentType(MediaType.APPLICATION_PROBLEM_JSON).body(detail));
    }

    private Mono<ResponseEntity<Object>> problem(HttpStatus status, String slug, String title, String message,
                                                  ServerWebExchange exchange) {
        return Mono.just(ResponseEntity.status(status).contentType(MediaType.APPLICATION_PROBLEM_JSON)
                .body(details(status, slug, title, message, exchange)));
    }

    private ProblemDetail details(HttpStatusCode status, String slug, String title, String message,
                                  ServerWebExchange exchange) {
        var detail = ProblemDetail.forStatusAndDetail(status, message);
        detail.setTitle(title);
        detail.setType(URI.create("https://codearti.com/problems/" + slug));
        detail.setInstance(URI.create(exchange.getRequest().getPath().value()));
        detail.setProperty("timestamp", Instant.now().toString());
        return detail;
    }

    // Single source for parameter messages: the generated interface carries @Min/@Max/@Pattern without messages.
    private String validationMessage(String field, @Nullable String fallback) {
        return switch (field) {
            case "productId" -> "El código solo puede contener letras mayúsculas, números y guiones, entre 1 y 50 caracteres.";
            case "delayMs" -> "La demora debe estar entre 0 y 2000 ms.";
            case "page" -> "La página debe ser 0 o mayor.";
            case "size" -> "El tamaño de página debe estar entre 1 y 100.";
            case "idempotencyKey" -> "La Idempotency-Key solo puede contener letras, números, guiones y guiones bajos, entre 1 y 64 caracteres.";
            default -> Objects.requireNonNullElse(fallback, "Valor inválido.");
        };
    }

    public record ValidationError(String field, String message) {
    }
}

package com.geovannycode.notify_service.notify.infrastructure.sender;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.time.Duration;
import java.util.HexFormat;
import java.util.Objects;
import java.util.concurrent.TimeoutException;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import com.geovannycode.notify_service.notify.domain.NotificationDeliveryException;
import com.geovannycode.notify_service.notify.domain.NotificationMessage;
import com.geovannycode.notify_service.notify.domain.NotificationRejectedException;
import com.geovannycode.notify_service.notify.domain.NotificationSender;
import io.netty.channel.ChannelOption;
import io.netty.handler.timeout.ReadTimeoutException;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.core.NestedExceptionUtils;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.MediaType;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.ClientResponse;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;
import reactor.netty.http.client.HttpClient;
import tools.jackson.databind.json.JsonMapper;

/**
 * POSTs each notification as JSON to notification.sender.webhook.url.
 *
 * <ul>
 *   <li>Idempotency-Key = eventId: a Kafka redelivery resends the same notification, the receiver can drop it;</li>
 *   <li>X-Signature = hex HMAC-SHA256 of the exact body bytes, when a secret is configured;</li>
 *   <li>2xx: delivered. 4xx (except 408 and 429): {@link NotificationRejectedException}, not retried.
 *       5xx, 408, 429, timeout or connection error: {@link NotificationDeliveryException}, retried by the consumer.</li>
 * </ul>
 * No retries here: the Kafka consumer already retries the whole event, and both would multiply the attempts.
 */
@Component
@ConditionalOnProperty(name = "notification.sender.type", havingValue = "webhook")
public class WebhookNotificationSender implements NotificationSender {

    static final String IDEMPOTENCY_KEY = "Idempotency-Key";
    static final String SIGNATURE = "X-Signature";
    private static final String HMAC = "HmacSHA256";
    private static final Logger LOG = LoggerFactory.getLogger(WebhookNotificationSender.class);

    private final WebClient webClient;
    private final URI url;
    private final Duration responseTimeout;
    private final @Nullable SecretKeySpec signingKey;
    private final JsonMapper json;

    /** Boot's WebClient.Builder, so its customizers (observation: traces and metrics) apply to the webhook calls. */
    public WebhookNotificationSender(WebClient.Builder builder, NotificationSenderProperties properties,
                                     JsonMapper json) {
        var webhook = properties.webhook();
        // Already checked by NotificationSenderProperties when type=webhook.
        this.url = Objects.requireNonNull(webhook.uri());
        this.responseTimeout = webhook.responseTimeout();
        this.signingKey = webhook.signed()
                ? new SecretKeySpec(Objects.requireNonNull(webhook.secret()).getBytes(StandardCharsets.UTF_8), HMAC)
                : null;
        this.json = json;
        var httpClient = HttpClient.create()
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, Math.toIntExact(webhook.connectTimeout().toMillis()))
                .responseTimeout(responseTimeout);
        this.webClient = builder.clientConnector(new ReactorClientHttpConnector(httpClient)).build();
        // Scheme, host and port only: user info, path and query may carry credentials or a token.
        LOG.info("Canal webhook activo: destino={}://{}{} (ruta y query omitidas), firma={}", url.getScheme(),
                url.getHost(), url.getPort() == -1 ? "" : ":" + url.getPort(),
                signingKey != null ? "HMAC-SHA256" : "sin firma");
    }

    @Override
    public Mono<Void> send(NotificationMessage message) {
        String eventId = message.eventId();
        return Mono.fromCallable(() -> json.writeValueAsBytes(WebhookPayload.of(message)))
                .flatMap(body -> webClient.post()
                        .uri(url)
                        .contentType(MediaType.APPLICATION_JSON)
                        .header(IDEMPOTENCY_KEY, eventId)
                        .headers(headers -> {
                            if (signingKey != null) {
                                headers.set(SIGNATURE, sign(body, signingKey));
                            }
                        })
                        .bodyValue(body)
                        .exchangeToMono(response -> outcome(eventId, response)))
                .onErrorMap(error -> !(error instanceof NotificationDeliveryException),
                        error -> new NotificationDeliveryException(eventId, describe(error)));
    }

    @Override
    public String channel() {
        return "webhook";
    }

    private static Mono<Void> outcome(String eventId, ClientResponse response) {
        HttpStatusCode status = response.statusCode();
        if (status.is2xxSuccessful()) {
            return response.releaseBody();
        }
        String reason = "el receptor respondió HTTP " + status.value();
        return response.releaseBody().then(Mono.error(rejects(status)
                ? new NotificationRejectedException(eventId, reason)
                : new NotificationDeliveryException(eventId, reason)));
    }

    // 4xx means "this content is wrong", except 408 and 429, which ask to try again later.
    private static boolean rejects(HttpStatusCode status) {
        return status.is4xxClientError() && status.value() != 408 && status.value() != 429;
    }

    /** Root cause type and text only: WebClient's own messages include the full URL, and it may carry a token. */
    private String describe(Throwable error) {
        Throwable root = NestedExceptionUtils.getMostSpecificCause(error);
        if (root instanceof ReadTimeoutException || root instanceof TimeoutException) {
            return "sin respuesta del receptor en " + responseTimeout.toMillis() + " ms";
        }
        return root.getClass().getSimpleName() + (root.getMessage() == null ? "" : ": " + root.getMessage());
    }

    static String sign(byte[] body, SecretKeySpec key) {
        try {
            Mac mac = Mac.getInstance(HMAC);
            mac.init(key);
            return HexFormat.of().formatHex(mac.doFinal(body));
        } catch (GeneralSecurityException unavailable) {
            throw new IllegalStateException("HMAC-SHA256 no disponible en esta JVM", unavailable);
        }
    }
}

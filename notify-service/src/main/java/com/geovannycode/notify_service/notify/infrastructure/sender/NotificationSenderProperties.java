package com.geovannycode.notify_service.notify.infrastructure.sender;

import java.net.URI;
import java.time.Duration;
import java.util.Locale;

import jakarta.validation.Valid;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.NotNull;
import org.jspecify.annotations.Nullable;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;
import org.springframework.validation.annotation.Validated;

/**
 * Delivery channel. Validated at startup: an unknown type, or type=webhook without a usable URL, stops the
 * application with the reason instead of failing on the first event.
 *
 * @param type    log (default) or webhook; selects the single active NotificationSender
 * @param webhook settings of the webhook channel, ignored with type=log
 */
@Validated
@ConfigurationProperties("notification.sender")
public record NotificationSenderProperties(@DefaultValue("log") @NotNull Type type, @Valid @NotNull Webhook webhook) {

    public enum Type { LOG, WEBHOOK }

    @AssertTrue(message = "con notification.sender.type=webhook hay que definir notification.sender.webhook.url "
            + "(variable NOTIFY_WEBHOOK_URL) con una URL absoluta http o https")
    public boolean isWebhookUrlValidForType() {
        return type != Type.WEBHOOK || webhook.uri() != null;
    }

    /**
     * @param url             where notifications are POSTed; may carry a token, so it is never logged whole
     * @param connectTimeout  TCP connection
     * @param responseTimeout from request sent to response headers received
     * @param secret          HMAC-SHA256 key for X-Signature; empty means unsigned
     */
    public record Webhook(@Nullable String url, @NotNull Duration connectTimeout, @NotNull Duration responseTimeout,
                          @Nullable String secret) {

        /** The URL when it is an absolute http(s) URL with a host, null otherwise (missing or malformed). */
        public @Nullable URI uri() {
            if (url == null || url.isBlank()) {
                return null;
            }
            try {
                URI uri = URI.create(url.strip());
                String scheme = uri.getScheme() == null ? "" : uri.getScheme().toLowerCase(Locale.ROOT);
                return (scheme.equals("http") || scheme.equals("https")) && uri.getHost() != null ? uri : null;
            } catch (IllegalArgumentException malformed) {
                return null;
            }
        }

        public boolean signed() {
            return secret != null && !secret.isEmpty();
        }

        @Override
        public String toString() {
            // Records print every component: keep the token-bearing URL and the secret out of logs and errors.
            return "Webhook[url=" + (url == null || url.isBlank() ? "<no definida>" : "<oculta>")
                    + ", connectTimeout=" + connectTimeout + ", responseTimeout=" + responseTimeout
                    + ", secret=" + (signed() ? "<oculto>" : "<sin firma>") + "]";
        }
    }
}

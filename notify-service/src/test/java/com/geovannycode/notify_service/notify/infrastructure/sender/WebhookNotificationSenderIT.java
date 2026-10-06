package com.geovannycode.notify_service.notify.infrastructure.sender;

import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

import javax.crypto.spec.SecretKeySpec;

import com.geovannycode.notify_service.OrderEventsKafka;
import com.geovannycode.notify_service.TestcontainersConfiguration;
import com.geovannycode.notify_service.notify.domain.NotificationDeliveryException;
import com.geovannycode.notify_service.notify.domain.NotificationMessage;
import com.geovannycode.notify_service.notify.domain.NotificationRejectedException;
import com.geovannycode.notify_service.notify.domain.NotifyStatus;
import com.geovannycode.notify_service.notify.infrastructure.persistence.NotificationDocument;
import com.geovannycode.notify_service.notify.infrastructure.persistence.NotificationRepository;
import com.github.tomakehurst.wiremock.junit5.WireMockExtension;
import com.github.tomakehurst.wiremock.stubbing.Scenario;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.web.reactive.function.client.WebClient;
import org.testcontainers.kafka.KafkaContainer;
import reactor.test.StepVerifier;
import tools.jackson.databind.json.JsonMapper;

import static com.geovannycode.notify_service.OrderEventsKafka.event;
import static com.geovannycode.notify_service.OrderEventsKafka.header;
import static com.geovannycode.notify_service.OrderEventsKafka.newOrderId;
import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.equalToJson;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

// WireMock plays the external receiver. The URL carries a token in the query, as many real webhooks do, to check
// it never leaks into errors. The context keeps the real response timeout (3s): a short one for every test made the
// first, cold request flaky on a loaded machine; the slow-receiver case builds its own sender instead.
@SpringBootTest(properties = {"notification.sender.type=webhook", "notification.sender.webhook.secret=" + WebhookNotificationSenderIT.SECRET})
@Import(TestcontainersConfiguration.class)
@ActiveProfiles("test")
final class WebhookNotificationSenderIT {

    static final String SECRET = "webhook-test-secret";
    private static final String PATH = "/hooks/orders";
    private static final String TOKEN = "t0k3n-que-no-debe-salir";
    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    @RegisterExtension
    static final WireMockExtension RECEIVER = WireMockExtension.newInstance()
            .options(wireMockConfig().dynamicPort()).build();

    @DynamicPropertySource
    static void webhookUrl(DynamicPropertyRegistry registry) {
        registry.add("notification.sender.webhook.url", () -> RECEIVER.baseUrl() + PATH + "?token=" + TOKEN);
    }

    @Autowired private WebhookNotificationSender sender;
    @Autowired private JsonMapper json;
    @Autowired private KafkaContainer kafka;
    @Autowired private NotificationRepository repository;
    private OrderEventsKafka orders;

    @BeforeEach
    void connect() {
        RECEIVER.resetAll();
        orders = new OrderEventsKafka(kafka.getBootstrapServers());
    }

    @AfterEach
    void disconnect() {
        orders.close();
    }

    @Test
    void okIsDeliveredWithThePayloadIdempotencyKeyAndSignature() {
        RECEIVER.stubFor(post(urlPathEqualTo(PATH)).willReturn(aResponse().withStatus(200)));
        var message = message("OrderCanceled", "INSUFFICIENT_STOCK");

        StepVerifier.create(sender.send(message)).expectComplete().verify(TIMEOUT);

        var request = RECEIVER.getAllServeEvents().getFirst().getRequest();
        assertThat(request.getHeader("Idempotency-Key")).isEqualTo(message.eventId());
        assertThat(request.getHeader("Content-Type")).startsWith("application/json");
        assertThat(request.queryParameter("token").firstValue()).isEqualTo(TOKEN);
        // Recomputed over the exact bytes received: any re-serialization would break the signature.
        var key = new SecretKeySpec(SECRET.getBytes(StandardCharsets.UTF_8), "HmacSHA256");
        assertThat(request.getHeader("X-Signature")).isEqualTo(WebhookNotificationSender.sign(request.getBody(), key));
        RECEIVER.verify(1, postRequestedFor(urlPathEqualTo(PATH)).withRequestBody(equalToJson("""
                {"notificationId":"66ff1c2ab7e4d91a2c3d4e5f","eventId":"%s","eventType":"OrderCanceled",
                 "orderId":42,"codeProduct":"AC-1550","quantity":2,"cancelReason":"INSUFFICIENT_STOCK",
                 "message":"La orden 42 (AC-1550 x2) fue cancelada: no hay stock suficiente.",
                 "createdAt":"2026-10-04T21:00:00Z"}""".formatted(message.eventId()))));
    }

    @Test
    void serverErrorIsRetryable() {
        RECEIVER.stubFor(post(urlPathEqualTo(PATH)).willReturn(aResponse().withStatus(500)));

        StepVerifier.create(sender.send(message("OrderCompleted", null)))
                .expectErrorSatisfies(error -> assertThat(error)
                        .isExactlyInstanceOf(NotificationDeliveryException.class)
                        .hasMessageContaining("HTTP 500"))
                .verify(TIMEOUT);
    }

    @Test
    void clientErrorIsARejectionAndIsNotRetryable() {
        RECEIVER.stubFor(post(urlPathEqualTo(PATH)).willReturn(aResponse().withStatus(400)));

        StepVerifier.create(sender.send(message("OrderCompleted", null)))
                .expectErrorSatisfies(error -> assertThat(error)
                        .isInstanceOf(NotificationRejectedException.class)
                        .hasMessageContaining("HTTP 400"))
                .verify(TIMEOUT);
    }

    @Test
    void tooManyRequestsIsRetryableNotARejection() {
        RECEIVER.stubFor(post(urlPathEqualTo(PATH)).willReturn(aResponse().withStatus(429)));

        StepVerifier.create(sender.send(message("OrderCompleted", null)))
                .expectErrorSatisfies(error -> assertThat(error).isExactlyInstanceOf(NotificationDeliveryException.class))
                .verify(TIMEOUT);
    }

    @Test
    void slowerThanTheResponseTimeoutIsRetryable() {
        RECEIVER.stubFor(post(urlPathEqualTo(PATH)).willReturn(aResponse().withStatus(200).withFixedDelay(2_000)));
        var webhook = new NotificationSenderProperties.Webhook(RECEIVER.baseUrl() + PATH, Duration.ofSeconds(1),
                Duration.ofMillis(500), SECRET);
        var impatient = new WebhookNotificationSender(WebClient.builder(),
                new NotificationSenderProperties(NotificationSenderProperties.Type.WEBHOOK, webhook), json);

        StepVerifier.create(impatient.send(message("OrderCompleted", null)))
                .expectErrorSatisfies(error -> assertThat(error)
                        .isExactlyInstanceOf(NotificationDeliveryException.class)
                        .hasMessageContaining("sin respuesta del receptor"))
                .verify(TIMEOUT);
    }

    @Test
    void unreachableReceiverIsRetryableAndTheErrorHidesTheToken() throws Exception {
        int closedPort;
        try (var socket = new ServerSocket(0)) {
            closedPort = socket.getLocalPort();
        }
        var webhook = new NotificationSenderProperties.Webhook("http://localhost:" + closedPort + PATH + "?token=" + TOKEN,
                Duration.ofSeconds(1), Duration.ofSeconds(1), SECRET);
        var unreachable = new WebhookNotificationSender(WebClient.builder(),
                new NotificationSenderProperties(NotificationSenderProperties.Type.WEBHOOK, webhook), json);

        StepVerifier.create(unreachable.send(message("OrderCompleted", null)))
                .expectErrorSatisfies(error -> {
                    assertThat(error).isExactlyInstanceOf(NotificationDeliveryException.class).hasNoCause();
                    assertThat(error.getMessage()).doesNotContain(TOKEN).doesNotContain(PATH);
                })
                .verify(TIMEOUT);
    }

    @Test
    void endToEndTwoServerErrorsThenOkEndsSentAfterThreeAttemptsWithOneIdempotencyKey() {
        RECEIVER.stubFor(post(urlPathEqualTo(PATH)).inScenario("flaky").whenScenarioStateIs(Scenario.STARTED)
                .willReturn(aResponse().withStatus(500)).willSetStateTo("second"));
        RECEIVER.stubFor(post(urlPathEqualTo(PATH)).inScenario("flaky").whenScenarioStateIs("second")
                .willReturn(aResponse().withStatus(500)).willSetStateTo("third"));
        RECEIVER.stubFor(post(urlPathEqualTo(PATH)).inScenario("flaky").whenScenarioStateIs("third")
                .willReturn(aResponse().withStatus(200)));
        long orderId = newOrderId();
        String eventId = orders.publish(orderId, event(UUID.randomUUID().toString(), "OrderCompleted", orderId, "completed", null));

        var sent = awaitStatus(eventId, NotifyStatus.SENT);

        assertThat(sent.attempts()).isEqualTo(3);
        assertThat(sent.channel()).isEqualTo("webhook");
        RECEIVER.verify(3, postRequestedFor(urlPathEqualTo(PATH)).withHeader("Idempotency-Key", equalTo(eventId)));
        RECEIVER.verify(3, postRequestedFor(urlPathEqualTo(PATH)));
    }

    @Test
    void endToEndRejectionEndsFailedAfterOneAttemptAndInTheDlt() {
        RECEIVER.stubFor(post(urlPathEqualTo(PATH)).willReturn(aResponse().withStatus(422)));
        long orderId = newOrderId();
        String eventId = orders.publish(orderId, event(UUID.randomUUID().toString(), "OrderCompleted", orderId, "completed", null));

        var failed = awaitStatus(eventId, NotifyStatus.FAILED);

        assertThat(failed.attempts()).isEqualTo(1);
        var dead = orders.awaitDeadLetter(record -> record.value() != null && record.value().contains(eventId));
        assertThat(header(dead, "kafka_dlt-exception-cause-fqcn")).contains("NotificationRejectedException");
        assertThat(header(dead, "kafka_dlt-exception-message")).contains("HTTP 422").doesNotContain(TOKEN);
        RECEIVER.verify(1, postRequestedFor(urlPathEqualTo(PATH)));
    }

    private NotificationMessage message(String eventType, String cancelReason) {
        String text = cancelReason == null
                ? "La orden 42 (AC-1550 x2) fue completada."
                : "La orden 42 (AC-1550 x2) fue cancelada: no hay stock suficiente.";
        return new NotificationMessage("66ff1c2ab7e4d91a2c3d4e5f", UUID.randomUUID().toString(), eventType, 42,
                "AC-1550", 2, cancelReason, text, Instant.parse("2026-10-04T21:00:00Z"));
    }

    private NotificationDocument awaitStatus(String eventId, NotifyStatus status) {
        return await().atMost(TIMEOUT).until(() -> repository.findByEventId(eventId).block(TIMEOUT),
                notification -> notification != null && notification.status() == status);
    }
}

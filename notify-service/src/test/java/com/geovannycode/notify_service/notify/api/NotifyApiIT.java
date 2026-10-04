package com.geovannycode.notify_service.notify.api;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

import com.geovannycode.notify_service.TestcontainersConfiguration;
import com.geovannycode.notify_service.notify.domain.NotifyStatus;
import com.geovannycode.notify_service.notify.infrastructure.persistence.NotificationDocument;
import com.geovannycode.notify_service.notify.infrastructure.persistence.NotificationRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;

// Over HTTP with the real base path, against MongoDB (Testcontainers): filters, order and limit with real data.
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Import(TestcontainersConfiguration.class)
@ActiveProfiles("test")
final class NotifyApiIT {

    private static final String NOTIFY = "/services-notify/notify";
    private static final Duration TIMEOUT = Duration.ofSeconds(20);

    private final WebTestClient client;
    private final NotificationRepository repository;
    private List<NotificationDocument> saved = List.of();

    @Autowired
    NotifyApiIT(@Value("${local.server.port}") int port, NotificationRepository repository) {
        this.client = WebTestClient.bindToServer().baseUrl("http://localhost:" + port).responseTimeout(TIMEOUT).build();
        this.repository = repository;
    }

    private static NotificationDocument pending(long orderId) {
        return new NotificationDocument(null, UUID.randomUUID().toString(), "OrderCompleted", orderId, "AC-1550", 1, null,
                "La orden " + orderId + " (AC-1550 x1) fue completada.", "log", NotifyStatus.PENDING, 0, null, null, null);
    }

    /** Saved one by one, so createdAt grows: c (order 10) is the newest, a (order 10, sent) the oldest. */
    @BeforeEach
    void storeNotifications() {
        var documents = Flux.concat(
                repository.deleteAll().thenMany(Flux.empty()),
                repository.save(pending(10).withAttempt().markSent(Instant.now())),
                repository.save(pending(20)),
                repository.save(pending(10)));
        saved = Objects.requireNonNull(documents.collectList().block(TIMEOUT));
    }

    @Test
    void listsNewestFirst() {
        client.get().uri(NOTIFY).accept(MediaType.APPLICATION_JSON).exchange().expectStatus().isOk()
                .expectBody()
                .jsonPath("$.length()").isEqualTo(3)
                .jsonPath("$[0].id").isEqualTo(saved.get(2).id())
                .jsonPath("$[1].id").isEqualTo(saved.get(1).id())
                .jsonPath("$[2].id").isEqualTo(saved.get(0).id());
    }

    @Test
    void filtersByOrderAndStatusAndHonoursTheLimit() {
        client.get().uri(NOTIFY + "?orderId=10").exchange().expectBody()
                .jsonPath("$.length()").isEqualTo(2).jsonPath("$[0].id").isEqualTo(saved.get(2).id());
        client.get().uri(NOTIFY + "?orderId=10&status=sent").exchange().expectBody()
                .jsonPath("$.length()").isEqualTo(1).jsonPath("$[0].id").isEqualTo(saved.get(0).id())
                .jsonPath("$[0].sentAt").exists();
        client.get().uri(NOTIFY + "?status=pending").exchange().expectBody().jsonPath("$.length()").isEqualTo(2);
        client.get().uri(NOTIFY + "?limit=1").exchange().expectBody()
                .jsonPath("$.length()").isEqualTo(1).jsonPath("$[0].id").isEqualTo(saved.get(2).id());
        client.get().uri(NOTIFY + "?orderId=999").exchange().expectBody().jsonPath("$.length()").isEqualTo(0);
    }

    @Test
    void getsByIdAndAnswers404ForAMissingOne() {
        client.get().uri(NOTIFY + "/" + saved.get(1).id()).exchange().expectStatus().isOk()
                .expectBody().jsonPath("$.orderId").isEqualTo(20).jsonPath("$.status").isEqualTo("pending");
        client.get().uri(NOTIFY + "/000000000000000000000000").exchange().expectStatus().isNotFound()
                .expectHeader().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON)
                .expectBody().jsonPath("$.type").isEqualTo("https://geovannycode.com/problems/notify-not-found")
                .jsonPath("$.instance").isEqualTo(NOTIFY + "/000000000000000000000000");
        client.get().uri(NOTIFY + "/abc").exchange().expectStatus().isBadRequest();
    }

    @Test
    void serverSentEventsOnePerNotification() {
        var body = client.get().uri(NOTIFY).accept(MediaType.TEXT_EVENT_STREAM).exchange().expectStatus().isOk()
                .expectHeader().contentTypeCompatibleWith(MediaType.TEXT_EVENT_STREAM)
                .expectBody(String.class).returnResult().getResponseBody();
        assertThat(Objects.requireNonNull(body)).contains("event:notify", "id:" + saved.get(0).id(), "id:" + saved.get(2).id());
        assertThat(body.split("event:notify", -1)).hasSize(4);
    }

    @Test
    void swaggerUiServesTheStaticContract() {
        client.get().uri("/services-notify/v3/api-docs/swagger-config").exchange().expectStatus().isOk()
                .expectBody().jsonPath("$.url").isEqualTo("/services-notify/openapi/services-notify.yaml");
        var contract = client.get().uri("/services-notify/openapi/services-notify.yaml").exchange().expectStatus().isOk()
                .expectBody(String.class).returnResult().getResponseBody();
        assertThat(Objects.requireNonNull(contract)).contains("title: Servicio de Notificación", "version: 2.0.0");
        StepVerifier.create(repository.count()).expectNext(3L).expectComplete().verify(TIMEOUT);
    }
}

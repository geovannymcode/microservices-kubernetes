package com.geovannycode.notify_service.notify.api;

import java.lang.reflect.Method;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.UUID;

import com.geovannycode.notify_service.generated.api.NotifyApiDelegate;
import com.geovannycode.notify_service.generated.config.EnumConverterConfiguration;
import com.geovannycode.notify_service.generated.dto.NotifyResponse;
import com.geovannycode.notify_service.generated.dto.NotifyStatus;
import com.geovannycode.notify_service.notify.application.NotificationService;
import com.geovannycode.notify_service.notify.domain.NotifyNotFoundException;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webflux.test.autoconfigure.WebFluxTest;
import org.springframework.context.annotation.Import;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.MediaType;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

// The slice client does not go through the server's base path (/services-notify); NotifyApiIT does.
@WebFluxTest(properties = "spring.webflux.base-path=")
// The generated EnumConverterConfiguration is a @Configuration, which slices do not pick up on their own.
@Import({NotifyApiDelegateImpl.class, EnumConverterConfiguration.class})
final class NotifyApiTest {

    private static final OffsetDateTime CREATED = OffsetDateTime.of(2026, 10, 4, 21, 0, 0, 0, ZoneOffset.UTC);
    private static final String FIRST_ID = "66ff1c2ab7e4d91a2c3d4e5f";
    private static final String SECOND_ID = "66ff1c2ab7e4d91a2c3d4e60";

    @Autowired private WebTestClient client;
    @MockitoBean private NotificationService notifications;

    private static NotifyResponse notification(String id, long orderId, NotifyStatus status) {
        return new NotifyResponse(id, UUID.fromString("3f1c9a52-6b0e-4d7a-9a51-2f5c1e7b8d10"),
                NotifyResponse.EventTypeEnum.ORDER_COMPLETED, orderId, "AC-1550", 2,
                "La orden " + orderId + " (AC-1550 x2) fue completada.", status, CREATED)
                .channel(NotifyResponse.ChannelEnum.LOG).attempts(1).sentAt(CREATED.plusSeconds(1));
    }

    @Test
    void overridesEveryGeneratedOperation() throws NoSuchMethodException {
        // skipDefaultInterface must stay false with delegatePattern, so the compiler cannot enforce this.
        for (Method operation : NotifyApiDelegate.class.getMethods()) {
            if (operation.isDefault() && !operation.getName().equals("getRequest")) {
                var implementation = NotifyApiDelegateImpl.class.getMethod(operation.getName(), operation.getParameterTypes());
                assertThat(implementation.getDeclaringClass()).as(operation.getName()).isEqualTo(NotifyApiDelegateImpl.class);
            }
        }
    }

    @Test
    void listsJsonWithTheDefaultLimitAndEveryContractField() {
        when(notifications.findAll(isNull(), isNull(), anyInt()))
                .thenReturn(Flux.just(notification(FIRST_ID, 42, NotifyStatus.SENT), notification(SECOND_ID, 43, NotifyStatus.PENDING)));

        client.get().uri("/notify").accept(MediaType.APPLICATION_JSON).exchange()
                .expectStatus().isOk()
                .expectHeader().contentTypeCompatibleWith(MediaType.APPLICATION_JSON)
                .expectBody()
                .jsonPath("$.length()").isEqualTo(2)
                .jsonPath("$[0].id").isEqualTo(FIRST_ID)
                .jsonPath("$[0].eventId").isEqualTo("3f1c9a52-6b0e-4d7a-9a51-2f5c1e7b8d10")
                .jsonPath("$[0].eventType").isEqualTo("OrderCompleted")
                .jsonPath("$[0].orderId").isEqualTo(42)
                .jsonPath("$[0].status").isEqualTo("sent")
                .jsonPath("$[0].channel").isEqualTo("log")
                .jsonPath("$[0].createdAt").isEqualTo("2026-10-04T21:00:00Z")
                .jsonPath("$[1].status").isEqualTo("pending");
        verify(notifications).findAll(null, null, 100);
    }

    @Test
    void passesFiltersAndLimitToTheService() {
        when(notifications.findAll(any(), any(), anyInt())).thenReturn(Flux.empty());
        client.get().uri("/notify?orderId=42&status=failed&limit=5").exchange()
                .expectStatus().isOk().expectBody().jsonPath("$.length()").isEqualTo(0);
        verify(notifications).findAll(42L, com.geovannycode.notify_service.notify.domain.NotifyStatus.FAILED, 5);
    }

    @Test
    void streamsNdjson() {
        when(notifications.findAll(isNull(), isNull(), anyInt()))
                .thenReturn(Flux.just(notification(FIRST_ID, 42, NotifyStatus.SENT), notification(SECOND_ID, 43, NotifyStatus.SENT)));
        var result = client.get().uri("/notify").accept(MediaType.APPLICATION_NDJSON).exchange()
                .expectStatus().isOk()
                .expectHeader().contentTypeCompatibleWith(MediaType.APPLICATION_NDJSON)
                .returnResult(NotifyResponse.class);
        StepVerifier.create(result.getResponseBody().map(NotifyResponse::getId)).expectNext(FIRST_ID, SECOND_ID)
                .expectComplete().verify(Duration.ofSeconds(5));
    }

    @Test
    void streamsNamedServerSentEventsWithNotificationIds() {
        when(notifications.findAll(isNull(), isNull(), anyInt()))
                .thenReturn(Flux.just(notification(FIRST_ID, 42, NotifyStatus.SENT), notification(SECOND_ID, 43, NotifyStatus.SENT)));
        var type = new ParameterizedTypeReference<ServerSentEvent<NotifyResponse>>() { };
        var result = client.get().uri("/notify").accept(MediaType.TEXT_EVENT_STREAM).exchange()
                .expectStatus().isOk()
                .expectHeader().contentTypeCompatibleWith(MediaType.TEXT_EVENT_STREAM)
                .returnResult(type);
        StepVerifier.create(result.getResponseBody())
                .assertNext(event -> {
                    assertThat(event.id()).isEqualTo(FIRST_ID);
                    assertThat(event.event()).isEqualTo("notify");
                    assertThat(event.data().getOrderId()).isEqualTo(42L);
                })
                .assertNext(event -> assertThat(event.id()).isEqualTo(SECOND_ID))
                .expectComplete().verify(Duration.ofSeconds(5));
    }

    @Test
    void getsOneNotificationById() {
        when(notifications.findById(FIRST_ID)).thenReturn(Mono.just(notification(FIRST_ID, 42, NotifyStatus.SENT)));
        client.get().uri("/notify/" + FIRST_ID).exchange()
                .expectStatus().isOk()
                .expectHeader().contentTypeCompatibleWith(MediaType.APPLICATION_JSON)
                .expectBody().jsonPath("$.id").isEqualTo(FIRST_ID).jsonPath("$.message").isEqualTo("La orden 42 (AC-1550 x2) fue completada.");
    }

    @Test
    void missingNotificationIsProblem404() {
        when(notifications.findById("000000000000000000000000"))
                .thenReturn(Mono.error(new NotifyNotFoundException("000000000000000000000000")));
        problem(client.get().uri("/notify/000000000000000000000000").exchange(), 404, "notify-not-found")
                .jsonPath("$.detail").isEqualTo("No existe la notificación 000000000000000000000000")
                .jsonPath("$.instance").isEqualTo("/notify/000000000000000000000000");
    }

    @Test
    void invalidParametersAreProblem400WithFieldErrors() {
        problem(client.get().uri("/notify/abc").exchange(), 400, "validation-error")
                .jsonPath("$.errors[0].field").isEqualTo("notifyId");
        problem(client.get().uri("/notify?limit=9999").exchange(), 400, "validation-error")
                .jsonPath("$.errors[0].field").isEqualTo("limit")
                .jsonPath("$.errors[0].message").isEqualTo("El límite debe estar entre 1 y 500.");
        problem(client.get().uri("/notify?orderId=0").exchange(), 400, "validation-error")
                .jsonPath("$.errors[0].field").isEqualTo("orderId");
        problem(client.get().uri("/notify?status=otro").exchange(), 400, "invalid-request");
        verifyNoInteractions(notifications);
    }

    @Test
    void unexpectedErrorsAreSanitized500() {
        when(notifications.findById(FIRST_ID)).thenReturn(Mono.error(new IllegalStateException("secret-password")));
        problem(client.get().uri("/notify/" + FIRST_ID).exchange(), 500, "internal-error")
                .jsonPath("$.detail").value(detail -> assertThat((String) detail).doesNotContain("secret-password"));
    }

    private static WebTestClient.BodyContentSpec problem(WebTestClient.ResponseSpec response, int status, String slug) {
        return response.expectStatus().isEqualTo(status)
                .expectHeader().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON)
                .expectBody().jsonPath("$.type").isEqualTo("https://geovannycode.com/problems/" + slug)
                .jsonPath("$.status").isEqualTo(status).jsonPath("$.title").exists().jsonPath("$.timestamp").exists();
    }
}

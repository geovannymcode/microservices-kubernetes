package com.geovannycode.notify_service.notify.application;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;

import com.geovannycode.notify_service.notify.domain.NotificationDeliveryException;
import com.geovannycode.notify_service.notify.domain.NotificationMessage;
import com.geovannycode.notify_service.notify.domain.NotificationSender;
import com.geovannycode.notify_service.notify.domain.NotifyStatus;
import com.geovannycode.notify_service.notify.domain.OrderEvent;
import com.geovannycode.notify_service.notify.infrastructure.persistence.MongoIndexInitializer;
import com.geovannycode.notify_service.notify.infrastructure.persistence.NotificationDocument;
import com.geovannycode.notify_service.notify.infrastructure.persistence.NotificationRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.dao.DuplicateKeyException;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
final class NotificationServiceTest {

    private static final Instant NOW = Instant.parse("2026-10-04T21:00:00Z");
    private static final String EVENT_ID = "3f1c9a52-6b0e-4d7a-9a51-2f5c1e7b8d10";

    @Mock private NotificationRepository repository;
    @Mock private NotificationSender sender;
    @Mock private MongoIndexInitializer indexes;
    private NotificationService service;

    @BeforeEach
    void createService() {
        lenient().when(indexes.ready()).thenReturn(Mono.empty());
        lenient().when(sender.channel()).thenReturn("log");
        lenient().when(repository.save(any(NotificationDocument.class))).thenAnswer(call -> Mono.just(call.getArgument(0)));
        service = new NotificationService(repository, sender, indexes, Clock.fixed(NOW, ZoneOffset.UTC));
    }

    private static OrderEvent event(String eventType, String status, String cancelReason) {
        return new OrderEvent(EVENT_ID, eventType, NOW, 1, new OrderEvent.Data(42L, "AC-1550", 2, status, cancelReason));
    }

    private static NotificationDocument stored(NotifyStatus status, int attempts) {
        return new NotificationDocument("66ff1c2ab7e4d91a2c3d4e5f", EVENT_ID, OrderEvent.COMPLETED, 42, "AC-1550", 2, null,
                "La orden 42 (AC-1550 x2) fue completada.", "log", status, attempts, NOW, null, 0L);
    }

    private void insertReturnsArgument() {
        when(repository.insert(any(NotificationDocument.class))).thenAnswer(call -> Mono.just(call.getArgument(0)));
    }

    @Test
    void unknownEventTypeIsIgnoredWithoutTouchingStorageOrChannel() {
        StepVerifier.create(service.handle(event("OrderShipped", "shipped", null))).verifyComplete();
        verifyNoInteractions(repository);
        verify(sender, never()).send(any());
    }

    @Test
    void newEventIsInsertedPendingThenSentAndMarkedSent() {
        insertReturnsArgument();
        when(sender.send(any())).thenReturn(Mono.empty());

        StepVerifier.create(service.handle(event(OrderEvent.COMPLETED, "completed", null))).verifyComplete();

        var inserted = ArgumentCaptor.forClass(NotificationDocument.class);
        verify(repository).insert(inserted.capture());
        assertThat(inserted.getValue().status()).isEqualTo(NotifyStatus.PENDING);
        assertThat(inserted.getValue().attempts()).isZero();
        assertThat(inserted.getValue().channel()).isEqualTo("log");
        assertThat(inserted.getValue().message()).isEqualTo("La orden 42 (AC-1550 x2) fue completada.");
        verify(sender).send(new NotificationMessage(EVENT_ID, 42, "La orden 42 (AC-1550 x2) fue completada."));
        var saved = ArgumentCaptor.forClass(NotificationDocument.class);
        verify(repository).save(saved.capture());
        assertThat(saved.getValue().status()).isEqualTo(NotifyStatus.SENT);
        assertThat(saved.getValue().attempts()).isEqualTo(1);
        assertThat(saved.getValue().sentAt()).isEqualTo(NOW);
    }

    @Test
    void canceledEventMessageCarriesTheReadableReason() {
        insertReturnsArgument();
        when(sender.send(any())).thenReturn(Mono.empty());
        StepVerifier.create(service.handle(event(OrderEvent.CANCELED, "canceled", "INSUFFICIENT_STOCK"))).verifyComplete();
        var inserted = ArgumentCaptor.forClass(NotificationDocument.class);
        verify(repository).insert(inserted.capture());
        assertThat(inserted.getValue().message()).isEqualTo("La orden 42 (AC-1550 x2) fue cancelada: no hay stock suficiente.");
        assertThat(inserted.getValue().cancelReason()).isEqualTo("INSUFFICIENT_STOCK");
        assertThat(NotificationService.message(OrderEvent.CANCELED, 7, "NO-EXISTE", 1, "PRODUCT_NOT_FOUND"))
                .isEqualTo("La orden 7 (NO-EXISTE x1) fue cancelada: el producto no existe.");
    }

    @Test
    void alreadySentEventIsADuplicateAndIsIgnored() {
        when(repository.insert(any(NotificationDocument.class))).thenReturn(Mono.error(new DuplicateKeyException("eventId")));
        when(repository.findByEventId(EVENT_ID)).thenReturn(Mono.just(stored(NotifyStatus.SENT, 1)));

        StepVerifier.create(service.handle(event(OrderEvent.COMPLETED, "completed", null))).verifyComplete();

        verify(sender, never()).send(any());
        verify(repository, never()).save(any());
    }

    @Test
    void pendingNotificationFromAPreviousAttemptIsSentAgain() {
        when(repository.insert(any(NotificationDocument.class))).thenReturn(Mono.error(new DuplicateKeyException("eventId")));
        when(repository.findByEventId(EVENT_ID)).thenReturn(Mono.just(stored(NotifyStatus.PENDING, 1)));
        when(sender.send(any())).thenReturn(Mono.empty());

        StepVerifier.create(service.handle(event(OrderEvent.COMPLETED, "completed", null))).verifyComplete();

        var saved = ArgumentCaptor.forClass(NotificationDocument.class);
        verify(repository).save(saved.capture());
        assertThat(saved.getValue().status()).isEqualTo(NotifyStatus.SENT);
        assertThat(saved.getValue().attempts()).isEqualTo(2);
    }

    @Test
    void failedNotificationIsSentAgainWhenTheEventIsRedelivered() {
        when(repository.insert(any(NotificationDocument.class))).thenReturn(Mono.error(new DuplicateKeyException("eventId")));
        when(repository.findByEventId(EVENT_ID)).thenReturn(Mono.just(stored(NotifyStatus.FAILED, 3)));
        when(sender.send(any())).thenReturn(Mono.empty());

        StepVerifier.create(service.handle(event(OrderEvent.COMPLETED, "completed", null))).verifyComplete();

        var saved = ArgumentCaptor.forClass(NotificationDocument.class);
        verify(repository).save(saved.capture());
        assertThat(saved.getValue().status()).isEqualTo(NotifyStatus.SENT);
        assertThat(saved.getValue().attempts()).isEqualTo(4);
    }

    @Test
    void sendFailureCountsTheAttemptKeepsPendingAndPropagates() {
        insertReturnsArgument();
        when(sender.send(any())).thenReturn(Mono.error(new IllegalStateException("canal caído")));

        StepVerifier.create(service.handle(event(OrderEvent.COMPLETED, "completed", null)))
                .expectError(NotificationDeliveryException.class).verify();

        var saved = ArgumentCaptor.forClass(NotificationDocument.class);
        verify(repository).save(saved.capture());
        assertThat(saved.getValue().status()).isEqualTo(NotifyStatus.PENDING);
        assertThat(saved.getValue().attempts()).isEqualTo(1);
        assertThat(saved.getValue().sentAt()).isNull();
    }

    @Test
    void exhaustedRetriesMarkTheNotificationFailed() {
        when(repository.findByEventId(EVENT_ID)).thenReturn(Mono.just(stored(NotifyStatus.PENDING, 3)));
        StepVerifier.create(service.markFailed(EVENT_ID)).verifyComplete();
        var saved = ArgumentCaptor.forClass(NotificationDocument.class);
        verify(repository).save(saved.capture());
        assertThat(saved.getValue().status()).isEqualTo(NotifyStatus.FAILED);
        assertThat(saved.getValue().attempts()).isEqualTo(3);
    }

    @Test
    void markFailedNeverDowngradesASentNotification() {
        when(repository.findByEventId(EVENT_ID)).thenReturn(Mono.just(stored(NotifyStatus.SENT, 1)));
        StepVerifier.create(service.markFailed(EVENT_ID)).verifyComplete();
        verify(repository, never()).save(any());
    }

    @Test
    void processingWaitsForTheUniqueIndex() {
        when(indexes.ready()).thenReturn(Mono.error(new IllegalStateException("índices no disponibles")));
        StepVerifier.create(service.handle(event(OrderEvent.COMPLETED, "completed", null)))
                .expectErrorMessage("índices no disponibles").verify();
        verify(repository, never()).insert(any(NotificationDocument.class));
    }
}

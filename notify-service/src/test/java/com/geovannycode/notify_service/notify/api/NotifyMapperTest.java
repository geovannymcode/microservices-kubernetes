package com.geovannycode.notify_service.notify.api;

import java.time.Instant;
import java.time.ZoneOffset;
import java.util.UUID;

import com.geovannycode.notify_service.generated.dto.NotifyResponse;
import com.geovannycode.notify_service.notify.domain.NotifyStatus;
import com.geovannycode.notify_service.notify.infrastructure.persistence.NotificationDocument;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

final class NotifyMapperTest {

    private static final Instant CREATED = Instant.parse("2026-10-04T21:00:00.123Z");
    private static final Instant SENT = Instant.parse("2026-10-04T21:00:01Z");
    private static final UUID EVENT_ID = UUID.fromString("3f1c9a52-6b0e-4d7a-9a51-2f5c1e7b8d10");

    private final NotifyMapper mapper = new NotifyMapperImpl();

    @Test
    void sentNotificationMapsEveryFieldWithUtcDates() {
        var document = new NotificationDocument("66ff1c2ab7e4d91a2c3d4e5f", EVENT_ID.toString(), "OrderCompleted", 42,
                "AC-1550", 2, null, "La orden 42 (AC-1550 x2) fue completada.", "log", NotifyStatus.SENT, 1,
                CREATED, SENT, 3L);

        NotifyResponse response = mapper.toResponse(document);

        assertThat(response.getId()).isEqualTo("66ff1c2ab7e4d91a2c3d4e5f");
        assertThat(response.getEventId()).isEqualTo(EVENT_ID);
        assertThat(response.getEventType()).isEqualTo(NotifyResponse.EventTypeEnum.ORDER_COMPLETED);
        assertThat(response.getOrderId()).isEqualTo(42L);
        assertThat(response.getCodeProduct()).isEqualTo("AC-1550");
        assertThat(response.getQuantity()).isEqualTo(2);
        assertThat(response.getCancelReason()).isNull();
        assertThat(response.getMessage()).isEqualTo("La orden 42 (AC-1550 x2) fue completada.");
        assertThat(response.getChannel()).isEqualTo(NotifyResponse.ChannelEnum.LOG);
        assertThat(response.getStatus().getValue()).isEqualTo("sent");
        assertThat(response.getAttempts()).isEqualTo(1);
        assertThat(response.getCreatedAt()).isEqualTo(CREATED.atOffset(ZoneOffset.UTC));
        assertThat(response.getCreatedAt().getOffset()).isEqualTo(ZoneOffset.UTC);
        assertThat(response.getSentAt()).isEqualTo(SENT.atOffset(ZoneOffset.UTC));
    }

    @Test
    void pendingCanceledNotificationKeepsReasonAndHasNoSentAt() {
        var document = new NotificationDocument("66ff1c2ab7e4d91a2c3d4e60", EVENT_ID.toString(), "OrderCanceled", 43,
                "NO-EXISTE", 1, "PRODUCT_NOT_FOUND", "La orden 43 fue cancelada.", "webhook", NotifyStatus.PENDING, 0,
                CREATED, null, 0L);

        NotifyResponse response = mapper.toResponse(document);

        assertThat(response.getEventType()).isEqualTo(NotifyResponse.EventTypeEnum.ORDER_CANCELED);
        assertThat(response.getCancelReason()).isEqualTo("PRODUCT_NOT_FOUND");
        assertThat(response.getChannel()).isEqualTo(NotifyResponse.ChannelEnum.WEBHOOK);
        assertThat(response.getStatus().getValue()).isEqualTo("pending");
        assertThat(response.getSentAt()).isNull();
    }

    @Test
    void everyDomainStatusHasAContractValue() {
        for (NotifyStatus status : NotifyStatus.values()) {
            assertThat(mapper.toStatus(status).getValue()).isEqualTo(status.value());
        }
    }

    @Test
    void unknownEventTypeIsRejectedInsteadOfMappedToNull() {
        assertThatIllegalArgumentException().isThrownBy(() -> mapper.toEventType("OrderShipped"));
    }
}

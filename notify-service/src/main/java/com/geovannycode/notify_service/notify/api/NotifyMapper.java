package com.geovannycode.notify_service.notify.api;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

import com.geovannycode.notify_service.generated.dto.NotifyResponse;
import com.geovannycode.notify_service.generated.dto.NotifyStatus;
import com.geovannycode.notify_service.notify.infrastructure.persistence.NotificationDocument;
import org.jspecify.annotations.Nullable;
import org.mapstruct.InjectionStrategy;
import org.mapstruct.Mapper;

/**
 * Stored notification -> contract DTO. Enums go through their lowercase/contract value (fromValue), never through
 * the constant name, so a renamed constant cannot silently change the API. eventId String -> UUID is built in.
 */
@Mapper(componentModel = "spring", injectionStrategy = InjectionStrategy.CONSTRUCTOR)
public interface NotifyMapper {

    NotifyResponse toResponse(NotificationDocument document);

    // The contract exposes date-time in UTC; MongoDB and the domain keep Instant.
    default @Nullable OffsetDateTime toUtc(@Nullable Instant instant) {
        return instant == null ? null : instant.atOffset(ZoneOffset.UTC);
    }

    default NotifyStatus toStatus(com.geovannycode.notify_service.notify.domain.NotifyStatus status) {
        return NotifyStatus.fromValue(status.value());
    }

    default NotifyResponse.EventTypeEnum toEventType(String eventType) {
        return NotifyResponse.EventTypeEnum.fromValue(eventType);
    }

    default NotifyResponse.ChannelEnum toChannel(String channel) {
        return NotifyResponse.ChannelEnum.fromValue(channel);
    }
}

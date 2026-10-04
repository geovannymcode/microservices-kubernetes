package com.geovannycode.notify_service.notify.infrastructure.persistence;

import java.util.List;

import com.geovannycode.notify_service.notify.domain.NotifyStatus;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.convert.ReadingConverter;
import org.springframework.data.convert.WritingConverter;

/** Stores NotifyStatus as its lowercase value ("sent"), not the enum name ("SENT") Spring Data uses by default. */
public final class NotifyStatusConverters {

    private NotifyStatusConverters() {
    }

    public static List<Converter<?, ?>> all() {
        return List.of(new ToValue(), new FromValue());
    }

    @WritingConverter
    static final class ToValue implements Converter<NotifyStatus, String> {
        @Override
        public String convert(NotifyStatus status) {
            return status.value();
        }
    }

    @ReadingConverter
    static final class FromValue implements Converter<String, NotifyStatus> {
        @Override
        public NotifyStatus convert(String value) {
            return NotifyStatus.fromValue(value);
        }
    }
}

package com.geovannycode.notify_service.notify.domain;

import java.util.Locale;

/** Lifecycle of a notification. The value (lowercase) is what the API and MongoDB store. */
public enum NotifyStatus {

    PENDING("pending"),
    SENT("sent"),
    FAILED("failed");

    private final String value;

    NotifyStatus(String value) {
        this.value = value;
    }

    public String value() {
        return value;
    }

    public static NotifyStatus fromValue(String value) {
        for (NotifyStatus status : values()) {
            if (status.value.equals(value.toLowerCase(Locale.ROOT))) {
                return status;
            }
        }
        throw new IllegalArgumentException("Estado de notificación desconocido: " + value);
    }
}

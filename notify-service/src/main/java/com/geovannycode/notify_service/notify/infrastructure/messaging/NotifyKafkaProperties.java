package com.geovannycode.notify_service.notify.infrastructure.messaging;

import java.time.Duration;

import jakarta.validation.Valid;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

/**
 * @param topics            topics consumed (and the derived dead-letter topic)
 * @param processingTimeout how long the listener waits for one event to be stored and delivered
 * @param retry             redelivery of an event whose notification could not be sent
 */
@Validated
@ConfigurationProperties("notify")
public record NotifyKafkaProperties(@Valid @NotNull Topics topics, @NotNull Duration processingTimeout,
                                    @Valid @NotNull Retry retry) {

    public record Topics(@NotBlank String orderEvents) {

        /** Dead-letter topic: same name plus ".dlt", same partition as the failed record. */
        public String orderEventsDlt() {
            return orderEvents + ".dlt";
        }
    }

    /**
     * @param maxAttempts     total deliveries of a record, the first one included (3 = 1 + 2 retries)
     * @param initialInterval wait before the first retry
     * @param multiplier      growth of the wait between retries
     * @param maxInterval     ceiling for the wait
     */
    public record Retry(@Min(1) int maxAttempts, @NotNull Duration initialInterval, @DecimalMin("1.0") double multiplier,
                        @NotNull Duration maxInterval) {
    }
}

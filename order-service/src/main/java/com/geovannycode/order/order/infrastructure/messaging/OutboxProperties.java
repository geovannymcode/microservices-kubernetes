package com.geovannycode.order.order.infrastructure.messaging;

import java.time.Duration;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

/**
 * @param topic           topic the relay publishes to
 * @param pollInterval    pause between relay cycles
 * @param batchSize       events locked and sent per cycle (a full batch triggers another cycle right away)
 * @param retention       how long published events stay in the table before cleanup
 * @param cleanupInterval how often published events older than the retention are deleted
 */
@Validated
@ConfigurationProperties("outbox")
public record OutboxProperties(
        @NotBlank String topic,
        @NotNull Duration pollInterval,
        @Min(1) @Max(1000) int batchSize,
        @NotNull Duration retention,
        @NotNull Duration cleanupInterval) {
}

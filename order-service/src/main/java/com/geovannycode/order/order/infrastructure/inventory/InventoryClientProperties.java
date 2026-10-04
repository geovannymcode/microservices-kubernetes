package com.geovannycode.order.order.infrastructure.inventory;

import java.net.URI;
import java.time.Duration;

import jakarta.validation.constraints.NotNull;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

/** Connection to Inventory; validated at startup so a missing INVENTORY_BASE_URL fails fast. */
@Validated
@ConfigurationProperties("inventory.client")
public record InventoryClientProperties(
        @NotNull URI baseUrl,
        @NotNull Duration connectTimeout,
        @NotNull Duration responseTimeout) {
}

package com.geovannycode.inventory.inventory.api.dto;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;

public record OrderInvRequest(
        @NotNull(message = "La cantidad solicitada es obligatoria.")
        @Min(value = 1, message = "La cantidad solicitada debe ser como mínimo 1.")
        @Max(value = 5, message = "La cantidad solicitada no puede superar 5.")
        Integer orderCount) {
}

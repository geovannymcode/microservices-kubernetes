package com.geovannycode.inventory.inventory.api.dto;

import java.math.BigDecimal;

import jakarta.validation.constraints.Digits;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.Size;

public record InventoryRequest(
        @NotBlank(message = "El código es obligatorio.")
        @Size(min = 1, max = 50, message = "El código debe tener entre 1 y 50 caracteres.")
        @Pattern(regexp = "^[A-Za-z0-9-]{1,50}$", message = "El código solo puede contener letras, números y guiones.")
        String idProduct,
        @NotBlank(message = "El nombre es obligatorio.")
        @Size(min = 1, max = 200, message = "El nombre debe tener entre 1 y 200 caracteres.")
        String nameProduct,
        @NotNull(message = "El precio es obligatorio.")
        @Positive(message = "El precio debe ser mayor que cero.")
        @Digits(integer = 8, fraction = 2, message = "El precio admite hasta 8 dígitos enteros y 2 decimales.")
        BigDecimal price,
        @NotNull(message = "El stock es obligatorio.")
        @Min(value = 1, message = "El stock debe ser como mínimo 1.")
        @Max(value = 500, message = "El stock no puede superar 500.")
        Integer stock) {
}

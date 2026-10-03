package com.geovannycode.inventory.inventory.api.dto;

import java.math.BigDecimal;

import jakarta.validation.Validation;
import jakarta.validation.Validator;
import jakarta.validation.ValidatorFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;

final class InventoryDtoValidationTest {

    private static final ValidatorFactory FACTORY = Validation.buildDefaultValidatorFactory();
    private static final Validator VALIDATOR = FACTORY.getValidator();

    @AfterAll
    static void closeValidatorFactory() {
        FACTORY.close();
    }

    @ParameterizedTest
    @CsvSource({"0,false", "1,true", "500,true", "501,false", "-1,false"})
    void requestStockLimits(int stock, boolean valid) {
        assertValid(new InventoryRequest("AC-1550", "Lentes", new BigDecimal("123.50"), stock), valid);
    }

    @ParameterizedTest
    @CsvSource({"0,true", "1,true", "500,true", "501,false", "-1,false"})
    void responseStockLimits(int stock, boolean valid) {
        assertValid(new InventoryResponse("AC-1550", "Lentes", new BigDecimal("123.50"), stock), valid);
    }

    @ParameterizedTest
    @CsvSource({"0,false", "1,true", "5,true", "6,false", "-1,false"})
    void orderCountLimits(int count, boolean valid) {
        assertValid(new OrderInvRequest(count), valid);
    }

    @ParameterizedTest
    @CsvSource({"0,false", "0.01,true", "123.456,false", "-0.01,false", "99999999.99,true", "100000000,false"})
    void priceLimitsInBothInventoryDtos(String price, boolean valid) {
        assertValid(new InventoryRequest("AC-1550", "Lentes", new BigDecimal(price), 1), valid);
        assertValid(new InventoryResponse("AC-1550", "Lentes", new BigDecimal(price), 0), valid);
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = {" ", "AC_1550", "AC/1550", "á", "AC 1550", "AC-1550\n"})
    void rejectsInvalidCodes(String code) {
        assertValid(new InventoryRequest(code, "Lentes", BigDecimal.ONE, 1), false);
        assertValid(new InventoryResponse(code, "Lentes", BigDecimal.ONE, 0), false);
    }

    @ParameterizedTest
    @CsvSource({"1,true", "50,true", "51,false"})
    void codeLengthLimits(int length, boolean valid) {
        assertValid(new InventoryRequest("A".repeat(length), "Lentes", BigDecimal.ONE, 1), valid);
        assertValid(new InventoryResponse("A".repeat(length), "Lentes", BigDecimal.ONE, 0), valid);
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = {" ", "\t\n", "\u2003"})
    void rejectsBlankNames(String name) {
        assertValid(new InventoryRequest("AC-1550", name, BigDecimal.ONE, 1), false);
        assertValid(new InventoryResponse("AC-1550", name, BigDecimal.ONE, 0), false);
    }

    @ParameterizedTest
    @CsvSource({"1,true", "200,true", "201,false"})
    void nameLengthLimits(int length, boolean valid) {
        assertValid(new InventoryRequest("AC-1550", "ñ".repeat(length), BigDecimal.ONE, 1), valid);
        assertValid(new InventoryResponse("AC-1550", "ñ".repeat(length), BigDecimal.ONE, 0), valid);
    }

    @Test
    void rejectsMissingNumericFields() {
        assertThat(VALIDATOR.validate(new InventoryRequest("AC-1550", "Lentes", null, null)))
                .extracting(violation -> violation.getPropertyPath().toString())
                .containsExactlyInAnyOrder("price", "stock");
        assertThat(VALIDATOR.validate(new InventoryResponse("AC-1550", "Lentes", null, null)))
                .extracting(violation -> violation.getPropertyPath().toString())
                .containsExactlyInAnyOrder("price", "stock");
        assertThat(VALIDATOR.validate(new OrderInvRequest(null)))
                .extracting(violation -> violation.getPropertyPath().toString()).containsExactly("orderCount");
    }

    private static void assertValid(Object dto, boolean expected) {
        var violations = VALIDATOR.validate(dto);
        assertThat(violations.isEmpty()).as("Validación de %s: %s", dto, violations).isEqualTo(expected);
    }
}

package com.geovannycode.order.order.infrastructure.inventory;

import java.nio.file.Files;
import java.nio.file.Path;

import com.atlassian.oai.validator.OpenApiInteractionValidator;
import com.atlassian.oai.validator.model.Request;
import com.atlassian.oai.validator.model.SimpleRequest;
import com.atlassian.oai.validator.model.SimpleResponse;
import com.atlassian.oai.validator.report.ValidationReport;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Consumer side of Inventory's contract: what Order sends and what WireMock answers in Order's tests must be
 * valid for contracts/services-inventory.yaml (the same file Inventory serves and generates its API from).
 */
final class InventoryContractTest {

    private static final Path CONTRACT = Path.of("..", "contracts", "services-inventory.yaml");
    private static final String CODE = "AC-1550";

    private static final OpenApiInteractionValidator VALIDATOR = OpenApiInteractionValidator
            .createForSpecificationUrl(readable(CONTRACT).toUri().toString())
            .build();

    private static Path readable(Path contract) {
        assertThat(Files.isReadable(contract)).as("contrato de Inventory en %s", contract.toAbsolutePath()).isTrue();
        return contract;
    }

    @Test
    void decreaseRequestSentByTheGatewayMatchesTheContract() {
        var request = SimpleRequest.Builder.put(InventoryStubs.decreasePath(CODE))
                .withContentType("application/json")
                .withHeader("Idempotency-Key", "order-42")
                .withBody("{\"orderCount\":2}")
                .build();
        assertValid(VALIDATOR.validateRequest(request));
    }

    @Test
    void stubbedSuccessMatchesTheContract() {
        assertValid(validateDecreaseResponse(200, "application/json", InventoryStubs.decreasedBody(CODE, 48)));
    }

    @ParameterizedTest
    @ValueSource(ints = {404, 409, 422})
    void stubbedRejectionsMatchTheContract(int status) {
        assertValid(validateDecreaseResponse(status, "application/problem+json", InventoryStubs.problemBody(status, CODE)));
    }

    @Test
    void validatorRejectsAnswersOutsideTheContract() {
        // Guards the test itself: if the validator ignored the 3.1 schemas, every check above would pass vacuously.
        String negativeStock = "{\"idProduct\":\"AC-1550\",\"nameProduct\":\"Lentes\",\"price\":123.50,\"stock\":-1}";
        assertThat(validateDecreaseResponse(200, "application/json", negativeStock).hasErrors()).isTrue();

        String withoutDetail = "{\"type\":\"https://geovannycode.com/problems/insufficient-stock\",\"title\":\"x\",\"status\":409,"
                + "\"instance\":\"/services-inventory/inventories/AC-1550\",\"timestamp\":\"2026-10-04T15:00:00Z\"}";
        assertThat(validateDecreaseResponse(409, "application/problem+json", withoutDetail).hasErrors()).isTrue();

        assertThat(validateDecreaseResponse(418, "application/problem+json", InventoryStubs.problemBody(409, CODE))
                .hasErrors()).isTrue();
    }

    private static ValidationReport validateDecreaseResponse(int status, String contentType, String body) {
        var response = SimpleResponse.Builder.status(status).withContentType(contentType).withBody(body).build();
        return VALIDATOR.validateResponse(InventoryStubs.decreasePath(CODE), Request.Method.PUT, response);
    }

    private static void assertValid(ValidationReport report) {
        assertThat(report.getMessages()).as("violaciones del contrato de Inventory").isEmpty();
    }
}

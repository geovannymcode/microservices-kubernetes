package com.geovannycode.order.order.infrastructure.inventory;

import com.github.tomakehurst.wiremock.client.ResponseDefinitionBuilder;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;

/**
 * The only answers WireMock gives as Inventory. InventoryContractTest validates these exact bodies against
 * contracts/services-inventory.yaml, so a contract change in Inventory breaks the build instead of the stubs
 * silently drifting from reality.
 */
public final class InventoryStubs {

    public static final String BASE_PATH = "/services-inventory";

    private InventoryStubs() {
    }

    public static String decreasePath(String code) {
        return BASE_PATH + "/inventories/" + code;
    }

    public static String decreasedBody(String code, int stock) {
        return "{\"idProduct\":\"" + code + "\",\"nameProduct\":\"Lentes\",\"price\":123.50,\"stock\":" + stock + "}";
    }

    public static String problemBody(int status, String code) {
        String slug = switch (status) {
            case 404 -> "product-not-found";
            case 409 -> "insufficient-stock";
            case 422 -> "idempotency-key-reused";
            default -> "internal-error";
        };
        return "{\"type\":\"https://codearti.com/problems/" + slug + "\",\"title\":\"Problema\",\"status\":" + status
                + ",\"detail\":\"Respuesta simulada.\",\"instance\":\"" + decreasePath(code)
                + "\",\"timestamp\":\"2026-10-04T15:00:00Z\"}";
    }

    public static ResponseDefinitionBuilder decreased(String code, int stock) {
        return aResponse().withStatus(200).withHeader("Content-Type", "application/json")
                .withBody(decreasedBody(code, stock));
    }

    public static ResponseDefinitionBuilder problem(int status, String code) {
        return aResponse().withStatus(status).withHeader("Content-Type", "application/problem+json")
                .withBody(problemBody(status, code));
    }
}

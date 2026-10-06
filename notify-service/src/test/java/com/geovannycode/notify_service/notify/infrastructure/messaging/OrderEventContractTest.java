package com.geovannycode.notify_service.notify.infrastructure.messaging;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.geovannycode.notify_service.OrderEventsKafka;
import com.geovannycode.notify_service.notify.domain.OrderEvent;
import com.networknt.schema.Schema;
import com.networknt.schema.SchemaLocation;
import com.networknt.schema.SchemaRegistry;
import com.networknt.schema.SpecificationVersion;
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.dataformat.yaml.YAMLMapper;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The events this service's tests publish, and the examples of the contract, against the payload schemas of
 * contracts/events/order-events.yaml (the AsyncAPI document Order owns). If Order changes the event and its
 * contract, the test events stop matching and this fails; then the consumer and its tests must be updated.
 */
final class OrderEventContractTest {

    private static final Path CONTRACT = Path.of("..", "contracts", "events", "order-events.yaml").toAbsolutePath().normalize();
    private static final List<String> MESSAGES = List.of("OrderCompleted", "OrderCanceled");
    // Never fetched: the validator only loads classpath and remote IRIs, so the file's content is registered under
    // this name (the .yaml ending selects the YAML reader).
    private static final String CONTRACT_IRI = "https://contracts.test/events/order-events.yaml";
    // AsyncAPI 3 schemas are a superset of JSON Schema draft 7.
    private static final SchemaRegistry SCHEMAS = SchemaRegistry.withDefaultDialect(SpecificationVersion.DRAFT_7,
            registry -> registry.schemas(Map.of(CONTRACT_IRI, read(CONTRACT))));
    private static final JsonMapper JSON = JsonMapper.builder().build();
    private static final Validator VALIDATOR = Validation.buildDefaultValidatorFactory().getValidator();

    /** The payload schema of one message, resolved inside the AsyncAPI document ($refs to components/schemas). */
    private static Schema payloadSchema(String eventType) {
        return SCHEMAS.getSchema(SchemaLocation.of(CONTRACT_IRI + "#/components/messages/" + eventType + "/payload"));
    }

    private static String read(Path file) {
        try {
            return Files.readString(file);
        } catch (IOException missing) {
            throw new UncheckedIOException("No se puede leer el contrato " + file, missing);
        }
    }

    private static List<String> violations(String json) {
        JsonNode event = JSON.readTree(json);
        return payloadSchema(event.get("eventType").asString()).validate(event).stream()
                .map(error -> error.getMessage()).toList();
    }

    private static List<JsonNode> contractExamples() {
        JsonNode contract = YAMLMapper.builder().build().readTree(read(CONTRACT));
        var examples = new ArrayList<JsonNode>();
        for (String message : MESSAGES) {
            contract.at("/components/messages/" + message + "/examples").forEach(example -> examples.add(example.get("payload")));
        }
        assertThat(examples).as("ejemplos en %s", CONTRACT).hasSizeGreaterThanOrEqualTo(MESSAGES.size());
        return examples;
    }

    @Test
    void contractExamplesMatchTheirPayloadSchema() {
        for (JsonNode example : contractExamples()) {
            assertThat(violations(example.toString())).as("ejemplo %s", example).isEmpty();
        }
    }

    @Test
    void contractExamplesAreReadAndAcceptedByThisConsumer() {
        for (JsonNode example : contractExamples()) {
            OrderEvent event = JSON.treeToValue(example, OrderEvent.class);
            assertThat(VALIDATOR.validate(event)).as("ejemplo %s", example).isEmpty();
            assertThat(event.eventType()).isIn(OrderEvent.COMPLETED, OrderEvent.CANCELED);
        }
    }

    @Test
    void completedEventPublishedByTheTestsMatchesTheContract() {
        assertThat(violations(OrderEventsKafka.event(UUID.randomUUID().toString(), "OrderCompleted", 42, "completed", null)))
                .isEmpty();
    }

    @ParameterizedTest
    @ValueSource(strings = {"PRODUCT_NOT_FOUND", "INSUFFICIENT_STOCK"})
    void canceledEventPublishedByTheTestsMatchesTheContract(String reason) {
        assertThat(violations(OrderEventsKafka.event(UUID.randomUUID().toString(), "OrderCanceled", 42, "canceled", reason)))
                .isEmpty();
    }

    @Test
    void completedEventAsOrderSerializesItWithANullCancelReasonMatchesTheContract() {
        // Order's record has a @Nullable cancelReason and Jackson writes it as null, not omitted.
        String completed = OrderEventsKafka.event(UUID.randomUUID().toString(), "OrderCompleted", 42, "completed", null)
                .replace("\"status\":\"completed\"", "\"status\":\"completed\",\"cancelReason\":null");
        assertThat(violations(completed)).isEmpty();
    }

    @Test
    void eventWithoutARequiredFieldBreaksTheContract() {
        String withoutCode = OrderEventsKafka.event(UUID.randomUUID().toString(), "OrderCompleted", 42, "completed", null)
                .replace("\"codeProduct\":\"AC-1550\",", "");
        assertThat(violations(withoutCode)).anyMatch(message -> message.contains("codeProduct"));
    }

    @Test
    void canceledEventWithoutAReasonBreaksTheContract() {
        String withoutReason = OrderEventsKafka.event(UUID.randomUUID().toString(), "OrderCanceled", 42, "canceled", null);
        assertThat(violations(withoutReason)).anyMatch(message -> message.contains("cancelReason"));
        String nullReason = withoutReason.replace("\"status\":\"canceled\"", "\"status\":\"canceled\",\"cancelReason\":null");
        assertThat(violations(nullReason)).isNotEmpty();
    }

    @Test
    void statusThatDoesNotMatchTheEventTypeBreaksTheContract() {
        assertThat(violations(OrderEventsKafka.event(UUID.randomUUID().toString(), "OrderCompleted", 42, "canceled", null)))
                .isNotEmpty();
    }
}

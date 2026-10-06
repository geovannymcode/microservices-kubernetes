package com.geovannycode.notify_service;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Predicate;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/** Publishes order events the way Order does and reads the dead-letter topic back. Close it after each test. */
public final class OrderEventsKafka implements AutoCloseable {

    public static final String TOPIC = "orders.events.v1";
    public static final String DLT = "orders.events.v1.dlt";
    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    private final String bootstrapServers;
    private final KafkaProducer<String, String> producer;

    public OrderEventsKafka(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
        var settings = new Properties();
        settings.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        this.producer = new KafkaProducer<>(settings, new StringSerializer(), new StringSerializer());
    }

    public static long newOrderId() {
        return ThreadLocalRandom.current().nextLong(1_000_000, Long.MAX_VALUE / 2);
    }

    /** The JSON Order publishes, plus a field this consumer does not know (compatible additions are ignored). */
    public static String event(String eventId, String eventType, long orderId, String status, String cancelReason) {
        return "{\"eventId\":\"" + eventId + "\",\"eventType\":\"" + eventType + "\",\"occurredAt\":\""
                + Instant.now() + "\",\"version\":1,\"futureField\":true,\"data\":{\"orderId\":" + orderId
                + ",\"codeProduct\":\"AC-1550\",\"quantity\":2,\"status\":\"" + status + "\""
                + (cancelReason == null ? "" : ",\"cancelReason\":\"" + cancelReason + "\"") + "}}";
    }

    /** Sends the event keyed by order id with the eventId header, as Order does; returns the eventId. */
    public String publish(long orderId, String json) {
        String eventId = eventIdOf(json);
        send(orderId, json, Map.of("eventId", eventId));
        return eventId;
    }

    private static String eventIdOf(String json) {
        int start = json.indexOf("\"eventId\":\"") + 11;
        return json.substring(start, json.indexOf('"', start));
    }

    public void send(long orderId, String value, Map<String, String> headers) {
        send(null, orderId, value, headers);
    }

    /**
     * Forces the partition, ignoring the key. Order never does this; tests use it to make several consumer threads
     * process copies of the same event at the same time.
     */
    public String publishToPartition(int partition, long orderId, String json) {
        String eventId = eventIdOf(json);
        send(partition, orderId, json, Map.of("eventId", eventId));
        return eventId;
    }

    private void send(Integer partition, long orderId, String value, Map<String, String> headers) {
        var record = new ProducerRecord<>(TOPIC, partition, String.valueOf(orderId), value);
        headers.forEach((name, headerValue) -> record.headers().add(name, headerValue.getBytes(StandardCharsets.UTF_8)));
        try {
            producer.send(record).get();
        } catch (Exception error) {
            throw new IllegalStateException(error);
        }
    }

    public ConsumerRecord<String, String> awaitDeadLetter(Predicate<ConsumerRecord<String, String>> match) {
        return await().atMost(TIMEOUT).until(() -> deadLetters().stream().filter(match).findFirst(),
                Optional::isPresent).orElseThrow();
    }

    /** Every record currently in the DLT, read from the beginning up to its end offsets. */
    public List<ConsumerRecord<String, String>> deadLetters() {
        var settings = new Properties();
        settings.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        settings.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        try (var consumer = new KafkaConsumer<>(settings, new StringDeserializer(), new StringDeserializer())) {
            var partitions = consumer.partitionsFor(DLT).stream()
                    .map(info -> new TopicPartition(info.topic(), info.partition())).toList();
            consumer.assign(partitions);
            consumer.seekToBeginning(partitions);
            var end = consumer.endOffsets(partitions);
            var found = new ArrayList<ConsumerRecord<String, String>>();
            var deadline = Instant.now().plus(TIMEOUT);
            while (partitions.stream().anyMatch(partition -> consumer.position(partition) < end.get(partition))) {
                assertThat(Instant.now()).as("lectura de %s", DLT).isBefore(deadline);
                consumer.poll(Duration.ofMillis(200)).forEach(found::add);
            }
            return found;
        }
    }

    public static String header(ConsumerRecord<String, String> record, String name) {
        var header = record.headers().lastHeader(name);
        return header == null ? "" : new String(header.value(), StandardCharsets.UTF_8);
    }

    @Override
    public void close() {
        producer.close();
    }
}

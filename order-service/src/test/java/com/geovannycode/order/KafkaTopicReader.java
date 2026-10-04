package com.geovannycode.order;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import static org.assertj.core.api.Assertions.assertThat;

/** Reads a topic from the beginning up to its current end offsets (no consumer group, nothing committed). */
public final class KafkaTopicReader {

    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    private KafkaTopicReader() {
    }

    public static List<ConsumerRecord<String, String>> records(String bootstrapServers, String topic) {
        var settings = new Properties();
        settings.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        settings.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        try (var consumer = new KafkaConsumer<>(settings, new StringDeserializer(), new StringDeserializer())) {
            var partitions = consumer.partitionsFor(topic).stream()
                    .map(info -> new TopicPartition(info.topic(), info.partition())).toList();
            consumer.assign(partitions);
            consumer.seekToBeginning(partitions);
            var end = consumer.endOffsets(partitions);
            var found = new ArrayList<ConsumerRecord<String, String>>();
            var deadline = Instant.now().plus(TIMEOUT);
            while (partitions.stream().anyMatch(partition -> consumer.position(partition) < end.get(partition))) {
                assertThat(Instant.now()).as("lectura del topic %s", topic).isBefore(deadline);
                consumer.poll(Duration.ofMillis(200)).forEach(found::add);
            }
            return found;
        }
    }

    public static String header(ConsumerRecord<String, String> record, String name) {
        return new String(Objects.requireNonNull(record.headers().lastHeader(name), name).value(), StandardCharsets.UTF_8);
    }
}

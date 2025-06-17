package com.pinterest.kafka.tieredstorage.consumer;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * KafkaConsumerUtils is a utility class that provides helper methods for KafkaConsumer.
 */
@SuppressWarnings("unchecked")
public class KafkaConsumerUtils {
    private static final Logger LOG = LogManager.getLogger(KafkaConsumerUtils.class.getName());

    public static Map<TopicPartition, OffsetAndMetadata> getOffsetsAndMetadata(Map<TopicPartition, Long> offsets) {
        Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();
        offsets.forEach((key, value) -> offsetsToCommit.put(key, new OffsetAndMetadata(value)));
        return offsetsToCommit;
    }

    public static void commitSync(@SuppressWarnings("rawtypes") KafkaConsumer kafkaConsumer, Map<TopicPartition, OffsetAndMetadata> offsets, Duration timeout) {
        kafkaConsumer.commitSync(offsets, timeout);
        LOG.debug("Committed offsets: " + offsets);
    }

    public static void commitAsync(@SuppressWarnings("rawtypes") KafkaConsumer kafkaConsumer, Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback callback) {
        kafkaConsumer.commitAsync(offsets, callback);
        LOG.debug("Committed offsets: " + offsets);
    }

    public static void resetOffsetToLatest(@SuppressWarnings("rawtypes") KafkaConsumer kafkaConsumer, TopicPartition topicPartition) {
        kafkaConsumer.seekToEnd(Collections.singleton(topicPartition));
    }

    public static void resetOffsetToLatest(@SuppressWarnings("rawtypes") KafkaConsumer kafkaConsumer, Set<TopicPartition> topicPartitions) {
        kafkaConsumer.seekToEnd(topicPartitions);
    }

    public static void resetOffsets(KafkaConsumer kafkaConsumer, Map<TopicPartition, Long> offsets) {
        offsets.forEach((tp, o) -> kafkaConsumer.seek(tp, o));
    }
}

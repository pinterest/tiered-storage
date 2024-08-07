package com.pinterest.kafka.tieredstorage.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Wrapper class for {@link ConsumerRecords}
 */
public class TieredStorageRecords<K, V> {
    private static final Logger LOG = LogManager.getLogger(TieredStorageRecords.class.getName());
    private final Map<TopicPartition, List<ConsumerRecord<K, V>>> records = new HashMap<>();

    public TieredStorageRecords() {
    }

    public void addRecords(Map<TopicPartition, List<ConsumerRecord<K, V>>> newRecords) {
        newRecords.keySet().forEach(key -> addRecords(key, newRecords.get(key)));
    }

    public void addRecords(TopicPartition topicPartition, List<ConsumerRecord<K, V>> partitionRecords) {
        if (records.containsKey(topicPartition))
            records.get(topicPartition).addAll(partitionRecords);
        else
            records.put(topicPartition, partitionRecords);
    }

    public void addRecords(ConsumerRecords<K, V> consumerRecords) {
        consumerRecords.partitions().forEach(topicPartition ->
                addRecords(topicPartition, consumerRecords.records(topicPartition))
        );
    }

    public ConsumerRecords<K, V> records() {
        return new ConsumerRecords<>(records);
    }

    public void clear() {
        records.clear();
    }
}

package com.pinterest.kafka.tieredstorage.consumer;

import com.pinterest.kafka.tieredstorage.common.Utils;
import com.pinterest.kafka.tieredstorage.common.metrics.MetricsConfiguration;

import software.amazon.awssdk.services.s3.model.NoSuchKeyException;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.consumer.OffsetOutOfRangeException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

/**
 * S3PartitionsConsumer is a consumer that reads data from S3 for multiple partitions.
 */
public class S3PartitionsConsumer<K, V> {
    private static final Logger LOG = LogManager.getLogger(S3PartitionsConsumer.class.getName());
    private final Map<TopicPartition, S3PartitionConsumer<K, V>> s3PartitionConsumerMap = new HashMap<>();
    private final String consumerGroup;
    private int currPartitionIdx = -1;
    private Map<TopicPartition, Long> positions;
    private final Set<TopicPartition> pausedPartitions = new HashSet<>();
    private final Properties properties;
    private final MetricsConfiguration metricsConfiguration;
    private final Deserializer<K> keyDeserializer;
    private final Deserializer<V> valueDeserializer;

    public S3PartitionsConsumer(String consumerGroup, Properties properties, MetricsConfiguration metricsConfiguration, Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer) {
        this.consumerGroup = consumerGroup;
        this.properties = properties;
        this.metricsConfiguration = metricsConfiguration;
        this.keyDeserializer = keyDeserializer;
        this.valueDeserializer = valueDeserializer;
    }

    /**
     * Set the positions for the partitions. These positions should be the next offset to consume for each partition.
     * @param positions
     */
    public void setPositions(Map<TopicPartition, Long> positions) {
        this.positions = positions;
    }

    /**
     * Get the positions for the partitions. Each Long value represents the next offset to consume for each partition.
     * @return a map of positions for the partitions
     */
    public Map<TopicPartition, Long> getPositions() {
        return this.positions;
    }

    private int nextPartitionIndex(Collection<TopicPartition> partitions) {
        return (++currPartitionIdx) % partitions.size();
    }

    /**
     * Adds an S3 location to the consumer for consumption
     * @param location
     * @param topicPartition
     */
    public void add(String location, TopicPartition topicPartition) {
        if (!s3PartitionConsumerMap.containsKey(topicPartition)) {
            s3PartitionConsumerMap.put(
                    topicPartition,
                    new S3PartitionConsumer<>(location, topicPartition, consumerGroup, properties, metricsConfiguration, keyDeserializer, valueDeserializer)
            );
            LOG.debug(String.format("Added %s for topic-partition %s for S3 consumption.", location, topicPartition));
        } else {
            s3PartitionConsumerMap.get(topicPartition).update(location);
            LOG.debug(String.format("Updated %s for topic-partition %s for S3 consumption.", location, topicPartition));
        }
    }

    /**
     * Polls for records from the partitions
     * @param maxRecords
     * @return records
     */
    public ConsumerRecords<K, V> poll(int maxRecords, Collection<TopicPartition> partitions) {
        int consumedSoFar = 0;
        int round = 0;
        TieredStorageRecords<K, V> tieredStorageRecords = new TieredStorageRecords<>();
        List<TopicPartition> topicPartitions = new ArrayList<>(partitions);
        if (!s3PartitionConsumerMap.keySet().containsAll(partitions)) {
            LOG.error(String.format("Partitions %s are not assigned to this consumer. Assigned partitions: %s",
                    partitions, s3PartitionConsumerMap.keySet()));
            return tieredStorageRecords.records();
        }
        Map<TopicPartition, Long> outOfRangePartitions = new HashMap<>();
        while (round < partitions.size()) {
            LOG.debug(String.format("Current stored positions: %s", positions));
            TopicPartition topicPartition = topicPartitions.get(nextPartitionIndex(partitions));
            if (!partitions.contains(topicPartition)) {
                LOG.debug(String.format("Skipping topic partition %s as it is not in the list of partitions to consume.", topicPartition));
                continue;
            }
            if (pausedPartitions.contains(topicPartition)) {
                LOG.debug(String.format("Fetching from topic partition %s is paused.", topicPartition));
                continue;
            }
            LOG.debug(String.format("S3PartitionsConsumer Consumption round %s: partition: %s", round, topicPartition));
            int toPartiallyConsume = (int) Math.ceil((maxRecords - consumedSoFar) * 1.0 / (partitions.size() - round));
            LOG.debug(String.format("(int) Math.ceil(%s - %s) / (%s - %s) = %s",
                    maxRecords, consumedSoFar, partitions.size(), round, toPartiallyConsume));
            s3PartitionConsumerMap.get(topicPartition).setPosition(positions.get(topicPartition));
            List<ConsumerRecord<K, V>> records = Collections.emptyList();
            try {
                records = s3PartitionConsumerMap.get(topicPartition).poll(toPartiallyConsume, false);
            } catch (Exception e) {
                if (Utils.isAssignableFromRecursive(e, NoSuchKeyException.class) || Utils.isAssignableFromRecursive(e, OffsetOutOfRangeException.class)) {
                    outOfRangePartitions.put(topicPartition, s3PartitionConsumerMap.get(topicPartition).getPosition());
                } else {
                    throw e;
                }
            }
            consumedSoFar += records.size();
            ++round;
            tieredStorageRecords.addRecords(topicPartition, records);
        }
        if (!outOfRangePartitions.isEmpty()) {
            throw new OffsetOutOfRangeException(String.format("Offset out of range: " + outOfRangePartitions), outOfRangePartitions);
        }
        if (tieredStorageRecords.records().count() > 0) {
            s3PartitionConsumerMap.forEach((tp, c) -> {
                if (pausedPartitions.contains(tp) || !partitions.contains(tp)) {
                    return;
                }
                long pos = c.getPosition();
                positions.computeIfPresent(tp, (k, v) -> pos);
            });
        }
        LOG.debug("positions: " + positions);
        return tieredStorageRecords.records();
    }

    /**
     * Returns the beginning offsets of the given partitions
     * @param partitions
     * @return a map of beginning offsets for the partitions
     */
    public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions) {
        Map<TopicPartition, Long> offsets = new HashMap<>();
        partitions.forEach(tp -> {
            long offset = s3PartitionConsumerMap.get(tp).beginningOffset();
            if (offset >= 0) {
                offsets.put(tp, offset);
            }
        });
        return offsets;
    }

    /**
     * Returns the end offsets of the given partitions
     * @param partitions
     * @return a map of end offsets for the partitions
     */
    public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions) {
        Map<TopicPartition, Long> offsets = new HashMap<>();
        partitions.forEach(tp -> {
            long offset = s3PartitionConsumerMap.get(tp).endOffset();
            if (offset >= 0) {
                offsets.put(tp, offset);
            }
        });
        return offsets;
    }

    public void pause(Collection<TopicPartition> partitions) {
        pausedPartitions.addAll(partitions);
    }

    public void resume(Collection<TopicPartition> partitions) {
        pausedPartitions.removeAll(partitions);
    }

    @InterfaceStability.Evolving
    public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(
            Map<TopicPartition, Long> timestampsToSearch, Duration timeout) {
        Map<TopicPartition, OffsetAndTimestamp> results = new HashMap<>();
        if (timestampsToSearch == null || timestampsToSearch.isEmpty()) {
            return results;
        }

        timestampsToSearch.forEach((topicPartition, timestamp) -> {
            if (timestamp == null) {
                return;
            }

            S3PartitionConsumer<K, V> partitionConsumer = s3PartitionConsumerMap.get(topicPartition);
            if (partitionConsumer == null) {
                LOG.warn(String.format("No S3PartitionConsumer registered for %s when searching timestamp %s", topicPartition, timestamp));
                return;
            }

            Optional<OffsetAndTimestamp> maybeResult = partitionConsumer.offsetForTime(timestamp);
            if (!maybeResult.isPresent()) {
                LOG.warn(String.format("No offset found for timestamp %s in %s", timestamp, topicPartition));
                results.put(topicPartition, null);
                return;
            }

            OffsetAndTimestamp offsetAndTimestamp = maybeResult.get();
            results.put(topicPartition, offsetAndTimestamp);
        });

        return results;
    }

    /**
     * Unsubscribes from the partitions
     */
    public void unsubscribe() {
        this.s3PartitionConsumerMap.clear();
        // TODO: check if we need to close the partition consumer and clear positions
    }

    /**
     * Closes the consumer
     * @throws IOException
     */
    public void close() throws IOException {
        for (S3PartitionConsumer<K, V> partitionConsumer : s3PartitionConsumerMap.values()) {
            if (partitionConsumer != null) {
                partitionConsumer.close();
            }
        }
    }
}

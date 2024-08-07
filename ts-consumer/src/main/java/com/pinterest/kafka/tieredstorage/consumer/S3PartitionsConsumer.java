package com.pinterest.kafka.tieredstorage.consumer;

import com.pinterest.kafka.tieredstorage.common.metrics.MetricsConfiguration;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * S3PartitionsConsumer is a consumer that reads data from S3 for multiple partitions.
 */
public class S3PartitionsConsumer {
    private static final Logger LOG = LogManager.getLogger(S3PartitionsConsumer.class.getName());
    private final Map<TopicPartition, S3PartitionConsumer> s3PartitionConsumerMap = new HashMap<>();
    private final List<TopicPartition> topicPartitions = new ArrayList<>();
    private int currentPartitionIndex = -1;
    private final String consumerGroup;
    private Map<TopicPartition, Long> positions;
    private final Properties properties;
    private final MetricsConfiguration metricsConfiguration;

    public S3PartitionsConsumer(String consumerGroup, Properties properties, MetricsConfiguration metricsConfiguration) {
        this.consumerGroup = consumerGroup;
        this.properties = properties;
        this.metricsConfiguration = metricsConfiguration;
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

    private int nextPartitionIndex() {
        return (++currentPartitionIndex) % topicPartitions.size();
    }

    /**
     * Adds an S3 location to the consumer for consumption
     * @param location
     * @param topicPartition
     */
    public void add(String location, TopicPartition topicPartition) {
        if (!topicPartitions.contains(topicPartition)) {
            topicPartitions.add(topicPartition);
            s3PartitionConsumerMap.put(topicPartition, new S3PartitionConsumer(location, topicPartition, consumerGroup, properties, metricsConfiguration));
            LOG.info(String.format("Added %s for S3 consumption.", topicPartition));
        } else {
            s3PartitionConsumerMap.get(topicPartition).update(location);
            LOG.info(String.format("Updated %s for S3 consumption.", topicPartition));
        }
    }

    /**
     * Polls for records from the partitions
     * @param maxRecords
     * @return records
     */
    public ConsumerRecords<byte[], byte[]> poll(int maxRecords) {
        int consumedSoFar = 0;
        int round = 0;
        TieredStorageRecords<byte[], byte[]> tieredStorageRecords = new TieredStorageRecords<>();
        while (round < topicPartitions.size()) {
            LOG.debug(String.format("Current stored positions: %s", positions));
            TopicPartition topicPartition = topicPartitions.get(nextPartitionIndex());
            LOG.debug(String.format("S3PartitionsConsumer Consumption round %s: partition: %s", round, topicPartition));
            int toPartiallyConsume = (int) Math.ceil((maxRecords - consumedSoFar) * 1.0 / (topicPartitions.size() - round));
            LOG.debug(String.format("(int) Math.ceil(%s - %s) / (%s - %s) = %s",
                    maxRecords, consumedSoFar, topicPartitions.size(), round, toPartiallyConsume));
            s3PartitionConsumerMap.get(topicPartition).setPosition(positions.get(topicPartition));
            List<ConsumerRecord<byte[], byte[]>> records =
                    s3PartitionConsumerMap.get(topicPartition).poll(toPartiallyConsume, false);
            consumedSoFar += records.size();
            ++round;
            tieredStorageRecords.addRecords(topicPartition, records);
        }
        if (tieredStorageRecords.records().count() > 0) {
            s3PartitionConsumerMap.forEach((tp, c) -> {
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

    /**
     * Unsubscribes from the partitions
     */
    public void unsubscribe() {
        this.topicPartitions.clear();
        this.s3PartitionConsumerMap.clear();
        // TODO: check if we need to close the partition consumer and clear positions
    }

    /**
     * Closes the consumer
     * @throws IOException
     */
    public void close() throws IOException {
        for (S3PartitionConsumer partitionConsumer : s3PartitionConsumerMap.values()) {
            if (partitionConsumer != null) {
                partitionConsumer.close();
            }
        }
    }
}
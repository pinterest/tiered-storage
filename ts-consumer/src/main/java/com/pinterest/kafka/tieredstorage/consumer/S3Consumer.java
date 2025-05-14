package com.pinterest.kafka.tieredstorage.consumer;

import com.pinterest.kafka.tieredstorage.common.discovery.s3.S3StorageServiceEndpoint;
import com.pinterest.kafka.tieredstorage.common.metrics.MetricsConfiguration;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * S3Consumer is a consumer that reads data from S3.
 */
public class S3Consumer<K, V> {
    private static final Logger LOG = LogManager.getLogger(S3Consumer.class.getName());
    private final Map<String, S3StorageServiceEndpoint.Builder> s3Location = new HashMap<>();
    private final Set<TopicPartition> assignment = new HashSet<>();
    private final S3PartitionsConsumer<K, V> s3PartitionsConsumer;
    private Map<TopicPartition, Long> positions;

    public S3Consumer(String consumerGroup, Properties properties, MetricsConfiguration metricsConfiguration, Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer) {
        this.s3PartitionsConsumer = new S3PartitionsConsumer<>(consumerGroup, properties, metricsConfiguration, keyDeserializer, valueDeserializer);
    }

    /**
     * Adds an S3 location to the S3 consumer for consumption
     * @param topic the topic to add the location for
     * @param s3EndpointBuilder the S3 endpoint builder for the location
     */
    public void addLocation(String topic, S3StorageServiceEndpoint.Builder s3EndpointBuilder) {
        // s3Location at this point does not include the topic name, only up to the cluster name (without any hash prefix)
        this.s3Location.put(topic, s3EndpointBuilder);
    }

    /**
     * Assigns partitions to the S3 consumer
     * @param topicPartitions the partitions to assign
     */
    public void assign(Collection<TopicPartition> topicPartitions) {
        if (this.assignment.equals(topicPartitions))
            return;

        this.assignment.clear();
        this.assignment.addAll(topicPartitions);
        assignment.forEach(topicPartition -> {
            S3StorageServiceEndpoint s3FinalizedEndpoint = s3Location.get(topicPartition.topic())
                .setTopicPartition(topicPartition)
                .build();
            s3PartitionsConsumer.add(
                    s3FinalizedEndpoint.getPrefixExcludingTopicPartitionUri(),
                    topicPartition);
        });
        LOG.debug("Assigned partitions to S3 consumer: " + assignment);
    }

    /**
     * Sets the positions of the S3 consumer. Each Long value should be the next offset to consume for each partition.
     * @param positions the positions to set positions for
     */
    public void setPositions(Map<TopicPartition, Long> positions) {
        this.positions = positions;
        s3PartitionsConsumer.setPositions(positions);
        LOG.debug("S3 consumption positions: " + positions);
    }

    /**
     * Returns the beginning offsets of the given partitions
     * @param partitions the partitions to get the beginning offsets for
     * @return the beginning offsets of the given partitions
     */
    public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions) {
        return s3PartitionsConsumer.beginningOffsets(partitions);
    }

    /**
     * Returns the end offsets of the given partitions
     * @param partitions the partitions to get the end offsets for
     * @return the end offsets of the given partitions
     */
    public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions) {
        return s3PartitionsConsumer.endOffsets(partitions);
    }

    /**
     * resets offsets of given partitions on tiered storage based on offset reset policy
     * @param partitions that need their offset reset
     * @return set of partitions for which no tiered storage data was found
     */
    public Set<TopicPartition> resetOffsets(Set<TopicPartition> partitions) {
        return null;
    }

    /**
     * Returns the {@link ConsumerRecords} consumed from S3
     * @param maxRecordsToConsume the maximum number of records to consume
     * @return the {@link ConsumerRecords} consumed from S3
     */
    public ConsumerRecords<K, V> poll(int maxRecordsToConsume, Collection<TopicPartition> partitions) {
        ConsumerRecords<K, V> records = s3PartitionsConsumer.poll(maxRecordsToConsume, partitions);
        setPositions(s3PartitionsConsumer.getPositions());
        return records;
    }

    public Map<TopicPartition, Long> getPositions() {
        return this.positions;
    }

    public void pause(Collection<TopicPartition> partitions) {
        s3PartitionsConsumer.pause(partitions);
    }

    public void resume(Collection<TopicPartition> partitions) {
        s3PartitionsConsumer.resume(partitions);
    }

    /**
     * Unsubscribes the S3 consumer
     */
    public void unsubscribe() {
        this.assignment.clear();
        // TODO: Confirm if we need to clear positions.
        this.s3PartitionsConsumer.unsubscribe();
    }

    /**
     * Close the S3 consumer
     * @throws IOException
     */
    public void close() throws IOException {
        this.s3PartitionsConsumer.close();
    }
}

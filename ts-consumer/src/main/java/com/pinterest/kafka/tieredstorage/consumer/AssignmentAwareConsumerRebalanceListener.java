package com.pinterest.kafka.tieredstorage.consumer;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * AssignmentAwareConsumerRebalanceListener is a ConsumerRebalanceListener that will trigger a change in the
 * {@link TieredStorageConsumer}'s assignment and position when the underlying {@link KafkaConsumer}'s partitions are reassigned.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class AssignmentAwareConsumerRebalanceListener implements ConsumerRebalanceListener {
    private static final Logger LOG = LogManager.getLogger(AssignmentAwareConsumerRebalanceListener.class.getName());
    private final Set<TopicPartition> assignment;
    private final KafkaConsumer kafkaConsumer;
    private final String consumerGroup;
    private final Properties properties;
    private final Map<TopicPartition, Long> position;
    private final Map<TopicPartition, Long> committed;
    private final TieredStorageConsumer.OffsetReset offsetReset;
    private final AtomicBoolean isPartitionAssignmentComplete = new AtomicBoolean(false);
    private ConsumerRebalanceListener customListener = null;

    public AssignmentAwareConsumerRebalanceListener(
            KafkaConsumer kafkaConsumer, String consumerGroup, Properties properties,
            Set<TopicPartition> assignment, Map<TopicPartition, Long> position,
            Map<TopicPartition, Long> committed, TieredStorageConsumer.OffsetReset offsetReset) {
        this.kafkaConsumer = kafkaConsumer;
        this.consumerGroup = consumerGroup;
        this.properties = properties;
        this.assignment = assignment;
        this.position = position;
        this.committed = committed;
        this.offsetReset = offsetReset;
    }

    protected void setCustomRebalanceListener(ConsumerRebalanceListener customListener) {
        this.customListener = customListener;
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> collection) {
        isPartitionAssignmentComplete.set(false);
        LOG.info(String.format("Partitions revoked: " + collection));
        this.assignment.removeAll(collection);
        collection.forEach(position::remove);
        isPartitionAssignmentComplete.set(true);
        if (customListener != null)
            customListener.onPartitionsRevoked(collection);
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> collection) {
        isPartitionAssignmentComplete.set(false);
        LOG.info(String.format("Partitions assigned: " + collection));
        this.assignment.clear();
        this.assignment.addAll(collection);
        // consumer has received new partitions. the starting consumption offset would be
        // 1. stored group offset
        // 2. if there is no group offset, consumer config (earliest, latest, none)
        LOG.info("Getting position/commit ...");
        AdminClient adminClient = getAdminClient();
        try {
            committed.clear();
            position.clear();
            Map<TopicPartition, OffsetAndMetadata> partitionToGroupOffset;
            partitionToGroupOffset = adminClient.listConsumerGroupOffsets(consumerGroup).partitionsToOffsetAndMetadata().get();
            assignment.forEach(topicPartition -> {
                if (partitionToGroupOffset.containsKey(topicPartition)) {
                    // there is a starting position for this partition
                    long offset = partitionToGroupOffset.get(topicPartition).offset();
                    committed.put(topicPartition, offset);
                    position.put(topicPartition, offset);
                    LOG.info(String.format("\t%s: p(%s), c(%s)", topicPartition, position, committed));
                } else {
                    // there is no starting position for this partition
                    resetOffset(topicPartition);
                }
            });
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        } finally {
            adminClient.close();
        }
        isPartitionAssignmentComplete.set(true);
        if (customListener != null)
            customListener.onPartitionsAssigned(collection);
        LOG.info("Completed onPartitionsAssigned.");
    }

    private AdminClient getAdminClient() {
        return AdminClient.create(properties);
    }

    public boolean isPartitionAssignmentComplete() {
        return isPartitionAssignmentComplete.get();
    }

    /**
     * Reset the offset for the given topic partition based on the offset reset policy when no committed offset was found
     * for the given partition
     * @param topicPartition the topic partition to reset the offset for
     */
    private void resetOffset(TopicPartition topicPartition) {
        switch (offsetReset) {
            case LATEST:
                // consumption should start from the end of kafka log
                long kafkaOffset = (long) kafkaConsumer.endOffsets(Collections.singleton(topicPartition)).get(topicPartition);
                committed.put(topicPartition, kafkaOffset);
                position.put(topicPartition, kafkaOffset);
                LOG.info(String.format("\t%s(kafka): p(%s), c(%s)", topicPartition, kafkaOffset, kafkaOffset));
                break;
            case NONE:
                // there is no stored offset, so an exception should be thrown, no change is needed
                break;
            case EARLIEST:
                // consumption has to start from the earliest offset on S3
                committed.put(topicPartition, 0L);
                position.put(topicPartition, 0L);
                LOG.info(String.format("\t%s(s3): p(%s), c(%s)", topicPartition, 0, 0));
        }
    }

    /**
     * Reset the offset for the given topic partition
     * @param topicPartition the topic partition to reset the offset for
     * @param offset the offset to reset to
     */
    public void resetOffset(TopicPartition topicPartition, long offset) {
        switch (offsetReset) {
            case LATEST:
                // consumption should start from the end of kafka log
                long kafkaOffset = (long) kafkaConsumer.endOffsets(Collections.singleton(topicPartition)).get(topicPartition);
                committed.put(topicPartition, kafkaOffset);
                position.put(topicPartition, kafkaOffset);
                LOG.info(String.format("\t%s(kafka): p(%s), c(%s)", topicPartition, kafkaOffset, kafkaOffset));
                break;
            case NONE:
                // there is no stored offset, so an exception should be thrown, no change is needed
                break;
            case EARLIEST:
                committed.put(topicPartition, offset);
                position.put(topicPartition, offset);
                LOG.info(String.format("\t%s(s3): p(%s), c(%s)", topicPartition, offset, offset));
        }
    }

    public void resetOffsets(Map<TopicPartition, Long> offsetOutOfRangePartitions) {
        offsetOutOfRangePartitions.forEach(this::resetOffset);
    }
}

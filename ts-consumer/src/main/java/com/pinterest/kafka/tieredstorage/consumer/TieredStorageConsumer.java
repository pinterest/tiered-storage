package com.pinterest.kafka.tieredstorage.consumer;

import com.google.common.annotations.VisibleForTesting;
import com.pinterest.kafka.tieredstorage.common.discovery.StorageServiceEndpointProvider;
import com.pinterest.kafka.tieredstorage.common.discovery.s3.S3StorageServiceEndpoint;
import com.pinterest.kafka.tieredstorage.common.discovery.s3.S3StorageServiceEndpointProvider;
import com.pinterest.kafka.tieredstorage.common.metrics.MetricRegistryManager;
import com.pinterest.kafka.tieredstorage.common.metrics.MetricsConfiguration;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.InvalidOffsetException;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.NoOffsetForPartitionException;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.clients.consumer.OffsetOutOfRangeException;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.regex.Pattern;

/**
 * A consumer capable of reading log segment files from both Kafka and S3
 * <p>
 * Tiered storage consumption modes:
 * 1. KAFKA_PREFERRED: Consume from Kafka first, then from S3 if Kafka offsets are not available
 * 2. TIERED_STORAGE_PREFERRED: Consume from S3 first even if Kafka offsets are available (not supported at the moment)
 * 3. KAFKA_ONLY: Consume from Kafka only
 * 4. TIERED_STORAGE_ONLY: Consume from S3 only
 * <p>
 * Consumption modes KAFKA_PREFERRED, TIERED_STORAGE_PREFERRED, and TIERED_STORAGE_ONLY will be considered as "tiered storage consumption is possible".
 * KAFKA_ONLY will be considered as "tiered storage consumption is not possible".
 * TIERED_STORAGE_PREFERRED is not supported at the moment.
 */
public class TieredStorageConsumer<K, V> implements Consumer<K, V> {
    private static final Logger LOG = LogManager.getLogger(TieredStorageConsumer.class.getName());
    private StorageServiceEndpointProvider endpointProvider;
    private String kafkaClusterId;
    protected KafkaConsumer<K, V> kafkaConsumer;
    protected S3Consumer<K, V> s3Consumer;
    private OffsetResetStrategy offsetResetStrategy = OffsetResetStrategy.LATEST;
    private final Set<String> subscription = new HashSet<>();
    private int maxRecordsPerPoll = 50;
    private boolean autoCommitEnabled = true;
    private final TieredStorageMode tieredStorageMode;
    private final Map<TopicPartition, Long> positions = new HashMap<>();
    private final AssignmentAwareConsumerRebalanceListener rebalanceListener;
    private final TieredStorageRecords<K, V> records = new TieredStorageRecords<>();
    private final MetricsConfiguration metricsConfiguration;
    private int s3PrefixEntropyNumBits = -1;
    private String consumerGroup;

    public TieredStorageConsumer(Properties properties) throws IOException, ClassNotFoundException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
        this(properties, null, null);
    }

    public TieredStorageConsumer(Properties properties, Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer) throws IOException, ClassNotFoundException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
        this.tieredStorageMode = TieredStorageMode.valueOf(properties.getProperty(TieredStorageConsumerConfig.TIERED_STORAGE_MODE_CONFIG));
        if (!isModeSupported(tieredStorageMode)) {
            throw new IllegalArgumentException("Tiered storage mode " + tieredStorageMode + " is not supported at the moment");
        }

        // use thread-local metricRegistryManager for consumers for complete metrics in multi-consumer/multi-thread JVM's
        properties.setProperty(MetricsConfiguration.METRICS_REGISTRY_MANAGER_THREAD_LOCAL_CONFIG, "true");
        this.metricsConfiguration = MetricsConfiguration.getMetricsConfiguration(properties);

        String autoOffsetResetConfig = properties.getProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, OffsetResetStrategy.LATEST.toString()).toUpperCase().trim();
        this.offsetResetStrategy = OffsetResetStrategy.valueOf(autoOffsetResetConfig);
        LOG.info("Offset reset strategy: " + this.offsetResetStrategy);
        this.consumerGroup = properties.getProperty(ConsumerConfig.GROUP_ID_CONFIG);
        if (tieredStorageConsumptionPossible()) {
            LOG.info("Tiered storage consumption is possible. Consumption mode: " + tieredStorageMode);
            this.kafkaClusterId = properties.getProperty(TieredStorageConsumerConfig.KAFKA_CLUSTER_ID_CONFIG);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, OffsetResetStrategy.NONE.toString());    // if we are consuming from tiered storage, make kafkaConsumer throw an exception if no offset is available
            this.s3PrefixEntropyNumBits = Integer.parseInt(properties.getProperty(
                    TieredStorageConsumerConfig.STORAGE_SERVICE_ENDPOINT_S3_PREFIX_ENTROPY_NUM_BITS_CONFIG, "-1"));
            if (properties.containsKey(ConsumerConfig.MAX_POLL_RECORDS_CONFIG))
                this.maxRecordsPerPoll = Integer.parseInt(properties.get(ConsumerConfig.MAX_POLL_RECORDS_CONFIG).toString());
            if (properties.containsKey(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG))
                this.autoCommitEnabled = Boolean.parseBoolean(properties.get(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG).toString());
            this.s3Consumer = new S3Consumer<>(consumerGroup, properties, metricsConfiguration, keyDeserializer, valueDeserializer);
            if (properties.containsKey(TieredStorageConsumerConfig.STORAGE_SERVICE_ENDPOINT_PROVIDER_CLASS_CONFIG)) {
                this.endpointProvider = getEndpointProvider(properties.getProperty(TieredStorageConsumerConfig.STORAGE_SERVICE_ENDPOINT_PROVIDER_CLASS_CONFIG));
            } else {
                this.endpointProvider = getHardcodedS3EndpointProvider(properties);
            }
            this.endpointProvider.initialize(this.kafkaClusterId);
        }
        this.kafkaConsumer = new KafkaConsumer<>(properties, keyDeserializer, valueDeserializer);
        this.rebalanceListener = new AssignmentAwareConsumerRebalanceListener(
                kafkaConsumer, consumerGroup, properties, positions, new HashMap<>(), offsetResetStrategy
        );
        LOG.info("TieredStorageConsumer configs: " + properties);
    }

    private StorageServiceEndpointProvider getEndpointProvider(String fullClassName) throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
        Constructor<? extends StorageServiceEndpointProvider> endpointProviderConstructor = Class.forName(fullClassName)
                .asSubclass(StorageServiceEndpointProvider.class).getConstructor();
        return endpointProviderConstructor.newInstance();
    }

    private S3StorageServiceEndpointProvider getHardcodedS3EndpointProvider(Properties properties) {
        return new S3StorageServiceEndpointProvider() {
            String bucket;
            String basePrefix;
            @Override
            public void initialize(String clusterId) {
                bucket = properties.getProperty(TieredStorageConsumerConfig.STORAGE_SERVICE_ENDPOINT_S3_BUCKET_CONFIG);
                basePrefix = properties.getProperty(TieredStorageConsumerConfig.STORAGE_SERVICE_ENDPOINT_S3_PREFIX_CONFIG);
                if (bucket == null || basePrefix == null) {
                    throw new IllegalArgumentException("Bucket and prefix must both be present in properties if endpointProvider is not supplied explicitly");
                }
                LOG.info("Loaded bucket=" + bucket + " prefix=" + basePrefix + " from hardcoded properties");
            }

            @Override
            public S3StorageServiceEndpoint.Builder getStorageServiceEndpointBuilderForTopic(String topic) {
                if (kafkaClusterId == null) {
                    throw new IllegalArgumentException("Kafka cluster id must be set before calling getStorageServiceEndpointBuilderForTopic");
                }
                return new S3StorageServiceEndpoint.Builder()
                        .setBucket(bucket)
                        .setBasePrefix(basePrefix)
                        .setKafkaCluster(kafkaClusterId);
            }
        };
    }

    private boolean tieredStorageConsumptionPossible() {
        return tieredStorageMode != TieredStorageMode.KAFKA_ONLY;
    }

    @VisibleForTesting
    protected void setTieredStorageLocations(Collection<String> topics) {
        for (String topic: topics) {
            S3StorageServiceEndpoint.Builder endpointBuilder = ((S3StorageServiceEndpointProvider) endpointProvider).getStorageServiceEndpointBuilderForTopic(topic);
            if (s3PrefixEntropyNumBits > 0) {
                endpointBuilder.setPrefixEntropyNumBits(s3PrefixEntropyNumBits);
            }
            s3Consumer.addLocation(topic, endpointBuilder);
        }
    }

    @Override
    public Set<TopicPartition> assignment() {
        return kafkaConsumer.assignment();
    }

    @Override
    public Set<String> subscription() {
        return kafkaConsumer.subscription();
    }

    /**
     * Subscribes to the given topics
     * @param topics
     */
    @Override
    public void subscribe(Collection<String> topics) {
        subscribe(topics, null);
    }

    @Override
    public void subscribe(Collection<String> topics, ConsumerRebalanceListener callback) {
        subscription.clear();
        kafkaConsumer.unsubscribe();
        if (tieredStorageConsumptionPossible()) {
            rebalanceListener.setCustomRebalanceListener(callback);
            kafkaConsumer.subscribe(topics, rebalanceListener);
            setTieredStorageLocations(topics);
        }
        else {
            kafkaConsumer.subscribe(topics, new ConsumerRebalanceListener() {
                    @Override
                    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                        partitions.forEach(positions::remove);
                        if (callback != null) {
                            callback.onPartitionsRevoked(partitions);
                        }
                    }

                    @Override
                    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                        partitions.forEach(tp -> {
                            positions.put(tp, kafkaConsumer.position(tp));
                        });
                        if (callback != null) {
                            callback.onPartitionsAssigned(partitions);
                        }
                    }
                });
        }
        subscription.addAll(kafkaConsumer.subscription());
    }

    @Override
    public void subscribe(Pattern pattern, ConsumerRebalanceListener callback) {
        subscription.clear();
        kafkaConsumer.unsubscribe();
        if (tieredStorageConsumptionPossible()) {
            rebalanceListener.setCustomRebalanceListener(callback);
            kafkaConsumer.subscribe(pattern, rebalanceListener);
            setTieredStorageLocations(kafkaConsumer.subscription());
        } else {
            kafkaConsumer.subscribe(pattern, new ConsumerRebalanceListener() {
                @Override
                public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                    partitions.forEach(positions::remove);
                    if (callback != null) {
                        callback.onPartitionsRevoked(partitions);
                    }
                }

                @Override
                public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                    partitions.forEach(tp -> {
                        positions.put(tp, kafkaConsumer.position(tp));
                    });
                    if (callback != null) {
                        callback.onPartitionsAssigned(partitions);
                    }
                }
            });
        }
        subscription.addAll(kafkaConsumer.subscription());
    }

    @Override
    public void subscribe(Pattern pattern) {
        subscribe(pattern, null);
    }

    /**
     * Unsubscribes from the topics
     */
    @Override
    public void unsubscribe() {
        kafkaConsumer.unsubscribe();
        subscription.clear();
        if (s3Consumer != null) {
            s3Consumer.unsubscribe();
        }
    }

    /**
     * Assigns the given partitions
     * @param topicPartitions
     */
    @Override
    public void assign(Collection<TopicPartition> topicPartitions) {
        subscription.clear();
        kafkaConsumer.assign(topicPartitions);
        if (tieredStorageConsumptionPossible()) {
            rebalanceListener.onPartitionsRevoked(kafkaConsumer.assignment());
            rebalanceListener.onPartitionsAssigned(topicPartitions);
            topicPartitions.forEach(tp -> setTieredStorageLocations(Collections.singleton(tp.topic())));
            s3Consumer.assign(kafkaConsumer.assignment());
        }
    }

    /**
     * Poll for records. If tiered storage is enabled, it will consume from both Kafka and S3 depending on where the
     * offsets are available. If tiered storage is not enabled, it will consume from Kafka only.
     * <p>
     * Tiered storage consumption modes:
     * 1. KAFKA_PREFERRED: Consume from Kafka first, then from S3 if Kafka offsets are not available
     * 2. TIERED_STORAGE_PREFERRED: Consume from S3 first even if Kafka offsets are available (not supported at the moment)
     * 3. KAFKA_ONLY: Consume from Kafka only
     * 4. TIERED_STORAGE_ONLY: Consume from S3 only
     *
     * @param timeout
     * @return records
     */
    @Override
    public ConsumerRecords<K, V> poll(Duration timeout) {
        long beforePollMs = System.currentTimeMillis();
        ConsumerRecords<K, V> records;
        AtomicBoolean ts = new AtomicBoolean(false);
        // consumed count must be 0 at this point
        if (tieredStorageConsumptionPossible()) {
            ConsumerRecords<K, V> recordsAfterPartitionAssignment = maybeWaitForPartitionAssignment(timeout);
            if (!recordsAfterPartitionAssignment.isEmpty()) {
                // if we have records after partition assignment, we can return them
                records = recordsAfterPartitionAssignment;
            } else if (tieredStorageMode == TieredStorageMode.KAFKA_PREFERRED) {
                records = handleTieredStoragePoll(timeout, ts);
            } else if (tieredStorageMode == TieredStorageMode.TIERED_STORAGE_ONLY) {
                records = handleTieredStorageOnlyPoll(timeout, ts);
            } else {
                this.records.clear();
                LOG.warn("TieredStorageMode " + tieredStorageMode + " is not supported at the moment");
                records = this.records.records();   // return empty records for now
            }

        } else {
            // regular kafka poll
            records = kafkaConsumer.poll(timeout);
            ts.set(false);
        }

        // emit metrics
        long pollTime = System.currentTimeMillis() - beforePollMs;
        records.partitions().forEach(topicPartition -> {
            MetricRegistryManager.getInstance(metricsConfiguration).updateHistogram(topicPartition.topic(), topicPartition.partition(), ConsumerMetrics.CONSUMER_POLL_TIME_MS_METRIC,
                    pollTime, "ts=" + ts, "group=" + consumerGroup, "cluster=" + kafkaClusterId);
        });
        records.partitions().forEach(topicPartition -> {
            MetricRegistryManager.getInstance(metricsConfiguration).updateCounter(topicPartition.topic(), topicPartition.partition(), ConsumerMetrics.OFFSET_CONSUMED_TOTAL_METRIC,
                    records.records(topicPartition).size(),
                    "ts=" + ts, "group=" + consumerGroup, "cluster=" + kafkaClusterId);
            if (!records.records(topicPartition).isEmpty()) {
                MetricRegistryManager.getInstance(metricsConfiguration).updateCounter(topicPartition.topic(), topicPartition.partition(), ConsumerMetrics.OFFSET_CONSUMED_LATEST_METRIC,
                        records.records(topicPartition).get(records.records(topicPartition).size() - 1).offset(),
                        "ts=" + ts, "group=" + consumerGroup, "cluster=" + kafkaClusterId);
            }
        });

        // update positions
        records.partitions().forEach(tp -> {
            if (!records.records(tp).isEmpty()) {
                long lastOffset = records.records(tp).get(records.records(tp).size() - 1).offset();
                positions.put(tp, lastOffset + 1);
            }
        });

        if (ts.get()) {   // need to seek kafkaConsumer if previous consumption was from s3
            KafkaConsumerUtils.resetOffsets(kafkaConsumer, positions);
        }
        return records;
    }

    private ConsumerRecords<K, V> maybeWaitForPartitionAssignment(Duration timeout) {
        while (kafkaConsumer.assignment().isEmpty()) {
            LOG.info("Waiting for partition assignment to complete...");
            try {
                ConsumerRecords<K, V> records = kafkaConsumer.poll(timeout);
                if (!records.isEmpty()) {
                    return records;
                }
            } catch (NoOffsetForPartitionException | OffsetOutOfRangeException e) {
                // this is expected if tieredStorageConsumption is enabled and no offsets are available (auto.offset.reset=none)
                continue;
            }
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        return ConsumerRecords.empty();
    }

    @Override
    public ConsumerRecords<K, V> poll(long timeout) {
        return poll(Duration.ofMillis(timeout));
    }

    /**
     * Poll for records from tiered storage only, not Kafka
     * @param timeout
     * @param ts
     * @return records
     */
    private ConsumerRecords<K, V> handleTieredStorageOnlyPoll(Duration timeout, AtomicBoolean ts) {
        records.clear();
        s3Consumer.assign(kafkaConsumer.assignment());
        s3Consumer.setPositions(positions);
        records.addRecords(s3Consumer.poll(maxRecordsPerPoll, kafkaConsumer.assignment()));
        ts.set(true);
        return records.records();
    }

    /**
     * Poll for records from tiered storage and Kafka depending on where offsets are available.
     * <p>
     * If Kafka offsets are available, it will consume from Kafka first, then from S3 if Kafka offsets are not available.
     * TieredStorage-preferred consumption is not supported at the moment.
     * @param timeout
     * @param ts
     * @return
     */
    private ConsumerRecords<K, V> handleTieredStoragePoll(Duration timeout, AtomicBoolean ts) {
        records.clear();
        try {
            records.addRecords(kafkaConsumer.poll(timeout));
            ts.set(false);
        } catch (InvalidOffsetException e) {
            // If this is a NoOffsetForPartitionException, it means no Kafka offsets exist for some partitions
            // If this is an OffsetOutOfRangeException, it means that the offsets are out of range in Kafka.
            LOG.debug("InvalidOffsetException: " + e.getClass().getName() + " for partitions: " + e.partitions());
            handleInvalidOffsetException(ts, e);
        } finally {
            if (autoCommitEnabled) {
                commitSync();
            }
        }
        return records.records();
    }

    private void handleInvalidOffsetException(AtomicBoolean ts, InvalidOffsetException exception) {
        if (exception.getClass() != NoOffsetForPartitionException.class && exception.getClass() != OffsetOutOfRangeException.class) {
            throw exception;
        }
        s3Consumer.assign(exception.partitions());
        s3Consumer.setPositions(positions);
        ConsumerRecords<K, V> pollRecords = ConsumerRecords.empty();
        try {
            pollRecords = s3Consumer.poll(maxRecordsPerPoll, exception.partitions());
        } catch (OffsetOutOfRangeException e) {
            LOG.warn("Will reset offsets based on strategy: " + offsetResetStrategy + " for partitions: " + e.partitions());
            switch (offsetResetStrategy) {
                case LATEST:
                    KafkaConsumerUtils.resetOffsetToLatest(kafkaConsumer, e.partitions());
                    break;
                case NONE:
                    throw new OffsetOutOfRangeException("No offset found for partitions at positions", positions);
                case EARLIEST:
                    this.seekToBeginning(e.partitions());
                    break;
            }

        } catch (Exception e) {
            LOG.error("Error polling from S3", e);
            throw e;
        }
        records.addRecords(pollRecords);
        ts.set(true);
    }

    /**
     * Commits the offsets of the records returned by the last poll
     */
    @Override
    public void commitSync() {
        KafkaConsumerUtils.commitSync(kafkaConsumer, KafkaConsumerUtils.getOffsetsAndMetadata(positions), Duration.ofMillis(Long.MAX_VALUE));
    }

    @Override
    public void commitSync(Duration timeout) {
        KafkaConsumerUtils.commitSync(kafkaConsumer, KafkaConsumerUtils.getOffsetsAndMetadata(positions), timeout);
    }

    @Override
    public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
        KafkaConsumerUtils.commitSync(kafkaConsumer, offsets, Duration.ofMillis(Long.MAX_VALUE));
    }

    @Override
    public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets, Duration timeout) {
        KafkaConsumerUtils.commitSync(kafkaConsumer, offsets, timeout);
    }

    @Override
    public void commitAsync() {
        KafkaConsumerUtils.commitAsync(kafkaConsumer, KafkaConsumerUtils.getOffsetsAndMetadata(positions), null);
    }

    @Override
    public void commitAsync(OffsetCommitCallback callback) {
        KafkaConsumerUtils.commitAsync(kafkaConsumer, KafkaConsumerUtils.getOffsetsAndMetadata(positions), callback);
    }

    @Override
    public void commitAsync(Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback callback) {
        KafkaConsumerUtils.commitAsync(kafkaConsumer, offsets, callback);
    }

    @Override
    public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions) {
        return beginningOffsets(partitions, Duration.ofMillis(Long.MAX_VALUE));
    }

    /**
     * Returns the beginning offsets of the given partitions.
     * <p>
     * If tiered storage consumption is possible, it will return the earliest offsets from S3.
     * If tiered storage consumption is not possible, it will return the earliest offsets from Kafka.
     *
     * @param partitions
     * @return the earliest offsets stored on either tiered storage or kafka
     */
    @Override
    public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions, Duration timeout) {
        Map<TopicPartition, Long> s3Offsets;
        Map<TopicPartition, Long> result = new HashMap<>();
        if (tieredStorageConsumptionPossible()) {
            // if tiered storage consumption is possible, we want to see the TS offsets as well
            for (TopicPartition partition: partitions) {
                setTieredStorageLocations(Collections.singleton(partition.topic()));
            }
            s3Consumer.assign(partitions);
            s3Offsets = s3Consumer.beginningOffsets(partitions);
            LOG.info(String.format("S3 beginning offsets: %s", s3Offsets));
            if (tieredStorageMode == TieredStorageMode.TIERED_STORAGE_ONLY) {
                // only tiered storage consumption is enabled so TS beginning offsets is what we return
                return s3Offsets;
            }
        } else {
            s3Offsets = Collections.emptyMap();
        }
        // if we haven't yet returned, we need to get the beginning offsets from Kafka
        Map<TopicPartition, Long> kafkaOffsets = kafkaConsumer.beginningOffsets(partitions, timeout);
        LOG.info(String.format("Kafka beginning offsets: %s", kafkaOffsets));
        if (tieredStorageMode == TieredStorageMode.KAFKA_ONLY) {
            // only kafka consumption is enabled so kafka beginning offsets is what we return
            return kafkaOffsets;
        } else {
            // we are in KAFKA_PREFERRED mode, get earliest for either kafka or s3 depending on which is earlier
            partitions.forEach(partition -> {
                long kafkaOffset = kafkaOffsets.getOrDefault(partition, Long.MAX_VALUE);
                long s3Offset = s3Offsets.getOrDefault(partition, Long.MAX_VALUE);
                result.put(partition, Math.min(kafkaOffset, s3Offset));
            });
        }
        return result;
    }

    /**
     * Returns the committed offset for the given partition
     * @param partition
     * @return the committed offset
     */
    @Override
    public OffsetAndMetadata committed(TopicPartition partition) {
        return kafkaConsumer.committed(partition);
    }

    /**
     * Returns the committed offset for the given partition
     * @param partition
     * @param timeout
     * @return the committed offset
     */
    @Override
    public OffsetAndMetadata committed(TopicPartition partition, Duration timeout) {
        return kafkaConsumer.committed(partition, timeout);
    }

    /**
     * Returns the committed offsets for the given partitions.
     * @param set
     * @return the committed offsets
     */
    @Override
    public Map<TopicPartition, OffsetAndMetadata> committed(Set<TopicPartition> set) {
        return kafkaConsumer.committed(set);
    }

    /**
     * Returns the committed offsets for the given partitions within the given timeout period.
     * @param set
     * @param duration
     * @return the committed offsets
     */
    @Override
    public Map<TopicPartition, OffsetAndMetadata> committed(Set<TopicPartition> set,
                                                            Duration duration) {
        return kafkaConsumer.committed(set, duration);
    }


    @Override
    public Map<MetricName, ? extends Metric> metrics() {
        return kafkaConsumer.metrics();
    }

    /**
     * Returns the end offsets of the given partitions.
     * <p>
     * If tiered storage consumption is enabled, it will return the end offsets from S3 (defined as the first offset of the latest segment on S3)
     * If tiered storage consumption is not enabled, it will return the end offsets from Kafka.
     *
     * @param partitions
     * @return a map of end offsets for the partitions
     */
    @Override
    public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions) {
        return endOffsets(partitions, Duration.ofMillis(Long.MAX_VALUE));
    }

    @Override
    public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions, Duration timeout) {
        if (tieredStorageMode == TieredStorageMode.TIERED_STORAGE_ONLY) {
            // only tiered storage consumption is enabled so s3 end offsets is what we return
            for (TopicPartition partition: partitions) {
                setTieredStorageLocations(Collections.singleton(partition.topic()));
            }
            s3Consumer.assign(partitions);
            return s3Consumer.endOffsets(partitions);
        }
        // if not only s3, end offsets must be in Kafka
        return kafkaConsumer.endOffsets(partitions, timeout);
    }

    @Override
    @InterfaceStability.Evolving
    public OptionalLong currentLag(TopicPartition topicPartition) {
        throw new UnsupportedOperationException("currentLag is not supported for TieredStorageConsumer yet");
    }

    @Override
    @InterfaceStability.Evolving
    public ConsumerGroupMetadata groupMetadata() {
        throw new UnsupportedOperationException("groupMetadata is not supported for TieredStorageConsumer yet");
    }

    @Override
    @InterfaceStability.Evolving
    public void enforceRebalance() {
        throw new UnsupportedOperationException("enforceRebalance is not supported for TieredStorageConsumer yet");
    }

    @Override
    @InterfaceStability.Evolving
    public void enforceRebalance(String s) {
        throw new UnsupportedOperationException("enforceRebalance is not supported for TieredStorageConsumer yet");
    }

    /**
     * Seeks to the specified offset for the given partition
     * @param partition
     * @param offset
     */
    @Override
    public void seek(TopicPartition partition, long offset) {
        kafkaConsumer.seek(partition, offset);
        this.positions.put(partition, offset);
    }

    /**
     * Seeks to the specified offset for the given partition
     * @param partition
     * @param offsetAndMetadata
     */
    @Override
    public void	seek(TopicPartition partition, OffsetAndMetadata offsetAndMetadata) {
        kafkaConsumer.seek(partition, offsetAndMetadata);
        this.positions.put(partition, offsetAndMetadata.offset());
    }

    /**
     * Seeks to the beginning of the partitions
     * @param partitions
     */
    @Override
    public void	seekToBeginning(Collection<TopicPartition> partitions) {
        Map<TopicPartition, Long> beginningOffsets = beginningOffsets(partitions);
        for (Map.Entry<TopicPartition, Long> entry : beginningOffsets.entrySet()) {
            seek(entry.getKey(), entry.getValue());
        }
    }

    /**
     * Seeks to the end of the partitions.
     * <p>
     * If tiered storage consumption is enabled, it will seek to the first offset of the latest segment on S3.
     * @param partitions
     */
    @Override
    public void seekToEnd(Collection<TopicPartition> partitions) {
        Map<TopicPartition, Long> endOffsets = endOffsets(partitions);
        for (Map.Entry<TopicPartition, Long> entry : endOffsets.entrySet()) {
            seek(entry.getKey(), entry.getValue());
        }
    }

    @Override
    public long position(TopicPartition partition) {
        if (!kafkaConsumer.assignment().contains(partition)) {
            throw new IllegalStateException("Can only check position for partitions assigned to this consumer");
        }
        long kafkaConsumerPosition;
        try {
            kafkaConsumerPosition = kafkaConsumer.position(partition);
        } catch (NoOffsetForPartitionException e) {
            // we are likely in TIERED_STORAGE_ONLY or KAFKA_PREFERRED since this usually gets thrown when
            // we have no offset reset policy set for the underlying consumer
            if (tieredStorageMode == TieredStorageMode.KAFKA_ONLY) {
                // if we are in KAFKA_ONLY mode, throw it
                throw e;
            } else if (!this.positions.containsKey(partition)) {
                throw e;
            } else {
                return this.positions.get(partition);
            }
        }
        if (!this.positions.containsKey(partition)) {
            // if we don't have a position for this partition, it must be in KAFKA_ONLY mode because
            // other modes would have set the position via rebalanceListener
            // Therefore, we need to get the position from kafka
            return kafkaConsumerPosition;
        }
        // if TS consumer position is set, check if it is different from the kafka consumer position
        long tsPosition = this.positions.get(partition);
        if (tsPosition == kafkaConsumerPosition) {
            return tsPosition;
        }
        // if they are different, we need to return the position based on the tiered storage mode
        switch (tieredStorageMode) {
            case KAFKA_PREFERRED:
                // if we are in KAFKA_PREFERRED mode, return the kafka consumer position
                return kafkaConsumerPosition;
            case TIERED_STORAGE_ONLY:
                // if we are in TIERED_STORAGE_ONLY mode, return the TS position
                return tsPosition;
            case KAFKA_ONLY:
                // if we are in KAFKA_ONLY mode, return the kafka consumer position
                return kafkaConsumerPosition;
            default:
                throw new IllegalStateException("Unknown tiered storage mode: " + tieredStorageMode);
        }
    }

    @Override
    public long position(TopicPartition partition, Duration timeout) {
        return position(partition);
    }

    /**
     * Returns the current positions for the partitions
     * @return a map of current positions for the partitions
     */
    @VisibleForTesting
    protected Map<TopicPartition, Long> getPositions() {
        return this.positions;
    }

    /**
     * Close the consumer
     */
    @Override
    public void close() {
        LOG.info("Closing kafkaConsumer");
        this.kafkaConsumer.close();
        if (this.s3Consumer != null) {
            try {
                LOG.info("Closing s3Consumer");
                this.s3Consumer.close();
            } catch (IOException e) {
                LOG.warn("Exception while closing", e);
            }
        }
        LOG.info("Closing MetricRegistryManager");
        try {
            MetricRegistryManager.getInstance(metricsConfiguration).shutdown();
        } catch (InterruptedException e) {
            LOG.warn("Exception while shutting down MetricRegistryManager", e);
        }
    }

    @Override
    public void close(Duration timeout) {
        close();
    }

    @Override
    public void wakeup() {
        kafkaConsumer.wakeup();
    }

    @VisibleForTesting
    protected S3Consumer<K, V> getS3Consumer() {
        return s3Consumer;
    }

    /**
     * Returns the list of topics
     * @return a map of topics to partition info
     */
    @Override
    public Map<String, List<PartitionInfo>> listTopics() {
        return this.kafkaConsumer.listTopics();
    }

    /**
     * Returns the list of topics
     * @param timeout
     * @return
     */
    @Override
    public Map<String, List<PartitionInfo>> listTopics(Duration timeout) {
        return this.kafkaConsumer.listTopics(timeout);
    }

    @Override
    public Set<TopicPartition> paused() {
        return kafkaConsumer.paused();
    }

    @Override
    public void pause(Collection<TopicPartition> partitions) {
        kafkaConsumer.pause(partitions);
        if (tieredStorageConsumptionPossible() && s3Consumer != null) {
            s3Consumer.pause(partitions);
        }
    }

    @Override
    public void resume(Collection<TopicPartition> partitions) {
        kafkaConsumer.resume(partitions);
        if (tieredStorageConsumptionPossible() && s3Consumer != null) {
            s3Consumer.resume(partitions);
        }
    }

    @Override
    public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch) {
        return offsetsForTimes(timestampsToSearch, Duration.ofMillis(Long.MAX_VALUE));
    }

    @Override
    public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch, Duration timeout) {
        if (timestampsToSearch == null || timestampsToSearch.isEmpty()) {
            return Collections.emptyMap();
        }

        switch (tieredStorageMode) {
            case KAFKA_ONLY:
                return kafkaConsumer.offsetsForTimes(timestampsToSearch, timeout);
            case TIERED_STORAGE_ONLY:
                return offsetsForTimesFromS3(timestampsToSearch, timeout);
            case KAFKA_PREFERRED:
                Map<TopicPartition, OffsetAndTimestamp> kafkaResults = kafkaConsumer.offsetsForTimes(timestampsToSearch, timeout);
                Map<TopicPartition, OffsetAndTimestamp> s3Results = offsetsForTimesFromS3(timestampsToSearch, timeout);
                Map<TopicPartition, OffsetAndTimestamp> combined = new HashMap<>();
                // combine the kafka and s3 results, using the s3 results if their offsets are less than the kafka results
                timestampsToSearch.forEach((topicPartition, timestamp) -> {
                    OffsetAndTimestamp kafkaOffset = kafkaResults.get(topicPartition);
                    OffsetAndTimestamp s3Offset = s3Results.get(topicPartition);
                    if (kafkaOffset != null && s3Offset != null) {
                        if (s3Offset.offset() < kafkaOffset.offset()) {
                            combined.put(topicPartition, s3Offset);
                        } else {
                            combined.put(topicPartition, kafkaOffset);
                        }
                    } else if (kafkaOffset == null && s3Offset != null) {
                        combined.put(topicPartition, s3Offset);
                    } else if (s3Offset == null && kafkaOffset != null) {
                        combined.put(topicPartition, kafkaOffset);
                    } else {
                        combined.put(topicPartition, null);
                    }
                });
                return combined;
            default:
                return kafkaConsumer.offsetsForTimes(timestampsToSearch, timeout);
        }
    }

    private Map<TopicPartition, OffsetAndTimestamp> offsetsForTimesFromS3(Map<TopicPartition, Long> timestampsToSearch,
                                                                         Duration timeout) {
        return offsetsForTimesFromS3(timestampsToSearch, timeout, timestampsToSearch.keySet());
    }

    private Map<TopicPartition, OffsetAndTimestamp> offsetsForTimesFromS3(Map<TopicPartition, Long> timestampsToSearch,
                                                                         Duration timeout,
                                                                         Collection<TopicPartition> partitionsToQuery) {
        if (!tieredStorageConsumptionPossible() || s3Consumer == null || partitionsToQuery.isEmpty()) {
            return Collections.emptyMap();
        }

        setTieredStorageLocations(partitionsToQuery.stream().map(TopicPartition::topic).collect(Collectors.toSet()));
        s3Consumer.assign(partitionsToQuery);

        Map<TopicPartition, Long> filtered = timestampsToSearch.entrySet().stream()
                .filter(entry -> partitionsToQuery.contains(entry.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        return s3Consumer.offsetsForTimes(filtered, timeout);
    }

    @VisibleForTesting
    protected void setKafkaConsumer(KafkaConsumer<K, V> kc) {
        this.kafkaConsumer = kc;
    }

    @VisibleForTesting
    protected KafkaConsumer<K, V> getKafkaConsumer() {
        return this.kafkaConsumer;
    }

    /**
     * Returns the partitions for the given topic
     * @param topic
     * @return
     */
    @Override
    public List<PartitionInfo> partitionsFor(String topic) {
        return this.kafkaConsumer.partitionsFor(topic);
    }

    @Override
    public List<PartitionInfo> partitionsFor(String topic, Duration timeout) {
        return this.kafkaConsumer.partitionsFor(topic, timeout);
    }

    public enum TieredStorageMode {
        KAFKA_PREFERRED, KAFKA_ONLY, TIERED_STORAGE_ONLY
    }

    public static boolean isModeSupported(TieredStorageMode mode) {
        return mode == TieredStorageMode.KAFKA_PREFERRED || mode == TieredStorageMode.KAFKA_ONLY || mode == TieredStorageMode.TIERED_STORAGE_ONLY;
    }
}

package com.pinterest.kafka.tieredstorage.consumer;

import com.google.common.annotations.VisibleForTesting;
import com.pinterest.kafka.tieredstorage.common.discovery.StorageServiceEndpointProvider;
import com.pinterest.kafka.tieredstorage.common.discovery.s3.S3StorageServiceEndpoint;
import com.pinterest.kafka.tieredstorage.common.discovery.s3.S3StorageServiceEndpointProvider;
import com.pinterest.kafka.tieredstorage.common.metrics.MetricRegistryManager;
import com.pinterest.kafka.tieredstorage.common.metrics.MetricsConfiguration;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.NoOffsetForPartitionException;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.clients.consumer.OffsetOutOfRangeException;
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
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
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
    private OffsetReset offsetReset = OffsetReset.LATEST;
    private final Set<String> subscription = new HashSet<>();
    private int maxRecordsPerPoll = 50;
    private boolean autoCommitEnabled = true;
    private final Set<TopicPartition> assignments = new HashSet<>();
    private final TieredStorageMode tieredStorageMode;
    private final Map<TopicPartition, Long> positions = new HashMap<>();
    private AssignmentAwareConsumerRebalanceListener rebalanceListener;
    private final TieredStorageRecords<K, V> records = new TieredStorageRecords<>();
    private final Set<TopicPartition> tieredStoragePartitions = new HashSet<>();
    private final MetricsConfiguration metricsConfiguration;
    private int s3PrefixEntropyNumBits = -1;
    private String consumerGroup;

    public TieredStorageConsumer(Properties properties) throws IOException, ClassNotFoundException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
        this(properties, null, null);
    }

    public TieredStorageConsumer(Properties properties, Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer) throws IOException, ClassNotFoundException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
        this.tieredStorageMode = TieredStorageMode.valueOf(properties.getProperty(TieredStorageConsumerConfig.TIERED_STORAGE_MODE_CONFIG));

        this.metricsConfiguration = MetricsConfiguration.getMetricsConfiguration(properties);

        if (tieredStorageConsumptionPossible()) {
            LOG.info("Tiered storage consumption is possible");
            this.kafkaClusterId = properties.getProperty(TieredStorageConsumerConfig.KAFKA_CLUSTER_ID_CONFIG);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none");
            String offsetResetConfig = properties.getProperty(TieredStorageConsumerConfig.OFFSET_RESET_CONFIG, "latest").toLowerCase().trim();
            this.s3PrefixEntropyNumBits = Integer.parseInt(properties.getProperty(
                    TieredStorageConsumerConfig.STORAGE_SERVICE_ENDPOINT_S3_PREFIX_ENTROPY_NUM_BITS_CONFIG, "-1"));
            this.offsetReset = offsetResetConfig.equals("earliest") ? OffsetReset.EARLIEST :
                    offsetResetConfig.equals("none") ? OffsetReset.NONE :
                            OffsetReset.LATEST;
            LOG.info("Offset reset policy: " + this.offsetReset);
            if (properties.containsKey(ConsumerConfig.MAX_POLL_RECORDS_CONFIG))
                this.maxRecordsPerPoll = Integer.parseInt(properties.get(ConsumerConfig.MAX_POLL_RECORDS_CONFIG).toString());
            if (properties.containsKey(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG))
                this.autoCommitEnabled = Boolean.parseBoolean(properties.get(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG).toString());
            if (properties.containsKey(ConsumerConfig.GROUP_ID_CONFIG))
                this.consumerGroup = properties.getProperty(ConsumerConfig.GROUP_ID_CONFIG);

            this.consumerGroup = properties.getProperty(ConsumerConfig.GROUP_ID_CONFIG);
            this.kafkaConsumer = new KafkaConsumer<>(properties, keyDeserializer, valueDeserializer);
            this.s3Consumer = new S3Consumer<>(consumerGroup, properties, metricsConfiguration, keyDeserializer, valueDeserializer);
            Map<TopicPartition, Long> committed = new HashMap<>();
            this.rebalanceListener = new AssignmentAwareConsumerRebalanceListener(
                    kafkaConsumer, consumerGroup, properties, assignments, positions, committed, offsetReset
            );
            if (properties.containsKey(TieredStorageConsumerConfig.STORAGE_SERVICE_ENDPOINT_PROVIDER_CLASS_CONFIG)) {
                this.endpointProvider = getEndpointProvider(properties.getProperty(TieredStorageConsumerConfig.STORAGE_SERVICE_ENDPOINT_PROVIDER_CLASS_CONFIG));
            } else {
                this.endpointProvider = getHardcodedS3EndpointProvider(properties);
            }
            this.endpointProvider.initialize(this.kafkaClusterId);
        } else {
            this.kafkaConsumer = new KafkaConsumer<>(properties);
        }
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
        assignments.clear();
        subscription.clear();
        subscription.addAll(topics);
        kafkaConsumer.unsubscribe();
        if (tieredStorageConsumptionPossible()) {
            rebalanceListener.setCustomRebalanceListener(callback);
            kafkaConsumer.subscribe(topics, rebalanceListener);
            setTieredStorageLocations(topics);
        }
        else
            kafkaConsumer.subscribe(topics);
    }

    @Override
    public void subscribe(Pattern pattern, ConsumerRebalanceListener callback) {
        assignments.clear();
        subscription.clear();
        kafkaConsumer.unsubscribe();
        if (tieredStorageConsumptionPossible()) {
            rebalanceListener.setCustomRebalanceListener(callback);
            kafkaConsumer.subscribe(pattern, rebalanceListener);
            setTieredStorageLocations(kafkaConsumer.subscription());
        } else
            kafkaConsumer.subscribe(pattern, callback);
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
        assignments.clear();
        subscription.clear();
        if (tieredStorageConsumptionPossible()) {
            rebalanceListener.onPartitionsRevoked(kafkaConsumer.assignment());
            assignments.addAll(topicPartitions);
            kafkaConsumer.assign(topicPartitions);
            rebalanceListener.onPartitionsAssigned(topicPartitions);
            topicPartitions.forEach(tp -> setTieredStorageLocations(Collections.singleton(tp.topic())));
        } else {
            kafkaConsumer.assign(topicPartitions);
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
        // TODO: implement timeout check while waiting for partition assignment
        long beforePollMs = System.currentTimeMillis();
        ConsumerRecords<K, V> records;
        AtomicBoolean ts = new AtomicBoolean(false);
        if (tieredStorageConsumptionPossible()) {
            int count = 0;
            while (assignments.isEmpty() && subscription.isEmpty()) {
                count = kafkaConsumer.poll(Duration.ofMillis(1000)).count();
                while (!rebalanceListener.isPartitionAssignmentComplete()) {
                    LOG.info("Waiting for partition assignment!");
                    try {
                        Thread.sleep(500);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
            // consumed count must be 0 at this point
            assert count == 0;  // TODO: how to handle this case?

            if (tieredStorageMode == TieredStorageMode.KAFKA_PREFERRED) {
                records = handleTieredStoragePoll(timeout, ts);
            } else if (tieredStorageMode == TieredStorageMode.TIERED_STORAGE_ONLY) {
                records = handleTieredStorageOnlyPoll(timeout, ts);
            } else {
                tieredStoragePartitions.clear();
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
            MetricRegistryManager.getInstance(metricsConfiguration).updateHistogram(topicPartition.topic(), topicPartition.partition(), ConsumerMetrics.OFFSET_CONSUMED_TOTAL_METRIC,
                    records.records(topicPartition).size(),
                    "ts=" + ts, "group=" + consumerGroup, "cluster=" + kafkaClusterId);
            if (!records.records(topicPartition).isEmpty()) {
                MetricRegistryManager.getInstance(metricsConfiguration).updateHistogram(topicPartition.topic(), topicPartition.partition(), ConsumerMetrics.OFFSET_CONSUMED_LATEST_METRIC,
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

        if (ts.get())   // need to seek kafkaConsumer if previous consumption was from s3
            KafkaConsumerUtils.resetOffsets(kafkaConsumer, positions);
        return records;
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
        tieredStoragePartitions.clear();
        s3Consumer.setPositions(positions);
        records.addRecords(s3Consumer.poll(maxRecordsPerPoll));
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
        tieredStoragePartitions.clear();
        try {
            records.addRecords(kafkaConsumer.poll(timeout));
            ts.set(false);
        } catch (NoOffsetForPartitionException e1) {
            LOG.debug("Hit NoOffsetForPartitionException: " + e1.partitions());
            // tiered storage is enabled and no Kafka offset exists for some partitions.
            // offset reset should be handled based on offset reset policy, per below
            switch (offsetReset) {
                case NONE:
                    LOG.info(String.format("%s: Going to throw.", offsetReset));
                    throw e1;
                case LATEST:
                    LOG.info(String.format("%s: Going to reset offsets to latest.", offsetReset));
                    KafkaConsumerUtils.resetOffsetToLatest(kafkaConsumer, e1.partitions());
                    break;
                case EARLIEST:
                    //TODO: Add support for when there is no data on S3 yet and offsets have to be reset to Kafka's earliest offsets
                    s3Consumer.assign(e1.partitions());
                    LOG.debug(String.format("%s: NoOffsetForPartition on Kafka: Going to reset offsets to positions for S3 consumption: %s.",
                            offsetReset, positions));
                    s3Consumer.setPositions(positions);
                    records.addRecords(s3Consumer.poll(maxRecordsPerPoll));
                    tieredStoragePartitions.addAll(e1.partitions());
                    ts.set(true);
                    break;
            }
        } catch (OffsetOutOfRangeException e2) {
            LOG.debug("Hit OffsetOutOfRangeException: " + e2.offsetOutOfRangePartitions());
            // tiered storage is enabled and Kafka offset of some partitions are out of Kafka range.
            // offset reset should be handled based on offset reset policy, per below
            switch (offsetReset) {
                case NONE:
                    LOG.info(String.format("%s: Going to throw.", offsetReset));
                    throw e2;
                case LATEST:
                    LOG.info(String.format("%s: Going to reset offsets to latest.", offsetReset));
                    // ignore stored offsets and reset them to latest
                    KafkaConsumerUtils.resetOffsetToLatest(kafkaConsumer, e2.partitions());
                    break;
                case EARLIEST:
                    // set s3 position to stored offsets and consume from s3
                    s3Consumer.assign(e2.partitions());
                    s3Consumer.setPositions(positions);
                    LOG.debug(String.format("%s: OffsetOutOfRange on Kafka: Going to reset offsets to positions for S3 consumption: %s.",
                            offsetReset, positions));
                    records.addRecords(s3Consumer.poll(maxRecordsPerPoll));
                    tieredStoragePartitions.addAll(e2.partitions());
                    ts.set(true);
                    break;
            }
        } finally {
            if (autoCommitEnabled) {
                commitSync();
            }
        }
        return records.records();
    }

    private Map<TopicPartition, Long> getCurrentOffsetsToCommit() {
        Map<TopicPartition, Long> offsetsToCommit = new HashMap<>();
        tieredStoragePartitions.forEach(topicPartition -> {
            if (!(records.records().records(topicPartition).isEmpty())) {
                offsetsToCommit.put(
                        topicPartition,
                        records.records().records(topicPartition).get(records.records().records(topicPartition).size() - 1).offset()
                );
            }
        });
        return offsetsToCommit;
    }

    /**
     * Commits the offsets of the records returned by the last poll
     */
    @Override
    public void commitSync() {
        if (tieredStoragePartitions.isEmpty())
            kafkaConsumer.commitSync();
        else
            KafkaConsumerUtils.commitSync(kafkaConsumer, KafkaConsumerUtils.getOffsetsAndMetadata(getCurrentOffsetsToCommit()), Duration.ofMillis(Long.MAX_VALUE));
    }

    @Override
    public void commitSync(Duration timeout) {
        if (tieredStoragePartitions.isEmpty())
            kafkaConsumer.commitSync(timeout);
        else
            KafkaConsumerUtils.commitSync(kafkaConsumer, KafkaConsumerUtils.getOffsetsAndMetadata(getCurrentOffsetsToCommit()), timeout);
    }

    @Override
    public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
        if (tieredStoragePartitions.isEmpty())
            kafkaConsumer.commitSync(offsets);
        else
            KafkaConsumerUtils.commitSync(kafkaConsumer, offsets, Duration.ofMillis(Long.MAX_VALUE));
    }

    @Override
    public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets, Duration timeout) {
        if (tieredStoragePartitions.isEmpty())
            kafkaConsumer.commitSync(offsets, timeout);
        else
            KafkaConsumerUtils.commitSync(kafkaConsumer, offsets, timeout);
    }

    @Override
    public void commitAsync() {
        if (tieredStoragePartitions.isEmpty())
            kafkaConsumer.commitAsync();
        else
            KafkaConsumerUtils.commitAsync(kafkaConsumer, KafkaConsumerUtils.getOffsetsAndMetadata(getCurrentOffsetsToCommit()), null);
    }

    @Override
    public void commitAsync(OffsetCommitCallback callback) {
        if (tieredStoragePartitions.isEmpty())
            kafkaConsumer.commitAsync(callback);
        else
            KafkaConsumerUtils.commitAsync(kafkaConsumer, KafkaConsumerUtils.getOffsetsAndMetadata(getCurrentOffsetsToCommit()), callback);
    }

    @Override
    public void commitAsync(Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback callback) {
        if (tieredStoragePartitions.isEmpty())
            kafkaConsumer.commitAsync(offsets, callback);
        else
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
        Map<TopicPartition, Long> s3Offsets = null;
        Map<TopicPartition, Long> result = new HashMap<>();
        if (tieredStorageConsumptionPossible()) {
            // if tiered storage consumption is possible, we want to see the s3 offsets as well
            for (TopicPartition partition: partitions) {
                setTieredStorageLocations(Collections.singleton(partition.topic()));
            }
            s3Consumer.assign(partitions);
            s3Offsets = s3Consumer.beginningOffsets(partitions);
        }
        Map<TopicPartition, Long> kafkaOffsets = kafkaConsumer.beginningOffsets(partitions, timeout);
        LOG.info(String.format("Kafka beginning offsets: %s", kafkaOffsets));
        if (s3Offsets == null) {
            return kafkaOffsets;
        }
        // get earliest for either kafka or s3
        s3Offsets.forEach((topicPartition, s3Offset) -> {
            Long kafkaOffset = kafkaOffsets.get(topicPartition);
            result.put(topicPartition, Math.min(kafkaOffset, s3Offset));
        });
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

    @Override
    public Map<MetricName, ? extends Metric> metrics() {
        return null;
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
        return this.positions.get(partition);
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
            LOG.info("Closing s3Consumer");
            try {
                this.s3Consumer.close();
            } catch (IOException e) {
                LOG.warn("IOException while closing S3Consumer", e);
            }
        }
        LOG.info("Closing MetricRegistryManager");
        MetricRegistryManager.getInstance(metricsConfiguration).shutdown();
    }

    @Override
    public void close(long timeout, TimeUnit unit) {
        try {
            if (s3Consumer != null) {
                this.s3Consumer.close();
            }
            this.kafkaConsumer.close();
        } catch (IOException e) {
            LOG.error("Error closing consumer", e);
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
    @InterfaceStability.Evolving
    public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch) {
        throw new UnsupportedOperationException("offsetsForTimes is not supported for TieredStorageConsumer yet");
    }

    @Override
    @InterfaceStability.Evolving
    public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch, Duration timeout) {
        throw new UnsupportedOperationException("offsetsForTimes is not supported for TieredStorageConsumer yet");
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

    public enum OffsetReset {
        EARLIEST, LATEST, NONE
    }

    public enum TieredStorageMode {
        KAFKA_PREFERRED, TIERED_STORAGE_PREFERRED, KAFKA_ONLY, TIERED_STORAGE_ONLY
    }
}

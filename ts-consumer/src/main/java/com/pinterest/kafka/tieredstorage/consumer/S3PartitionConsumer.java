package com.pinterest.kafka.tieredstorage.consumer;

import com.pinterest.kafka.tieredstorage.common.metrics.MetricRegistryManager;
import com.pinterest.kafka.tieredstorage.common.metrics.MetricsConfiguration;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.S3ChannelRecordBatch;
import org.apache.kafka.common.record.S3Records;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.utils.Utils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.TreeMap;

/**
 * Consumes Kafka records in a given Kafka partition from S3
 */
public class S3PartitionConsumer<K, V> {
    private static final Logger LOG = LogManager.getLogger(S3PartitionConsumer.class.getName());
    private static final long DEFAULT_S3_METADATA_RELOAD_INTERVAL_MS = 3600000; // 1 hour
    private String location;
    private final TopicPartition topicPartition;
    private long position;
    private long activeS3Offset = -1;
    private Triple<String, String, Long> s3Path = null;
    // sorted map of offset -> <bucket, key>
    // offset is the smallest offset in the <bucket, key> object.
    private TreeMap<Long, Triple<String, String, Long>> offsetKeyMap;
    private long lastOffsetKeyMapReloadTimestamp = -1;
    private final String consumerGroup;
    private S3Records s3Records;
    private final int maxPartitionFetchSizeBytes;
    private final long s3MetadataReloadIntervalMs;
    private String latestS3Object = null;
    private final S3OffsetIndexHandler s3OffsetIndexHandler = new S3OffsetIndexHandler();
    private final MetricsConfiguration metricsConfiguration;
    private Deserializer<K> keyDeserializer;
    private Deserializer<V> valueDeserializer;

    public S3PartitionConsumer(String location, TopicPartition topicPartition, String consumerGroup, Properties properties, MetricsConfiguration metricsConfiguration) {
        this(location, topicPartition, consumerGroup, properties, metricsConfiguration, null, null);
    }

    public S3PartitionConsumer(String location, TopicPartition topicPartition, String consumerGroup, Properties properties, MetricsConfiguration metricsConfiguration, Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer) {
        this.location = location;
        this.topicPartition = topicPartition;
        this.consumerGroup = consumerGroup;
        ConsumerConfig consumerConfig = new ConsumerConfig(properties);
        this.maxPartitionFetchSizeBytes = consumerConfig.getInt(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG);
        this.s3MetadataReloadIntervalMs = Long.parseLong(properties.getProperty(TieredStorageConsumerConfig.STORAGE_SERVICE_ENDPOINT_S3_METADATA_RELOAD_INTERVAL_MS_CONFIG, Long.toString(DEFAULT_S3_METADATA_RELOAD_INTERVAL_MS)));
        this.metricsConfiguration = metricsConfiguration;
        this.keyDeserializer = keyDeserializer;
        this.valueDeserializer = valueDeserializer;
        initializeDeserializers(consumerConfig);
        LOG.info(String.format("Created S3PartitionConsumer for %s with maxPartitionFetchSizeBytes=%s and s3MetadataReloadIntervalMs=%s", topicPartition, maxPartitionFetchSizeBytes, s3MetadataReloadIntervalMs));
    }

    private void initializeDeserializers(ConsumerConfig consumerConfig) {
        // borrowed from KafkaConsumer
        if (keyDeserializer == null) {
            this.keyDeserializer = consumerConfig.getConfiguredInstance(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, Deserializer.class);
            this.keyDeserializer.configure(consumerConfig.originals(), true);
        } else {
            consumerConfig.ignore(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG);
        }
        if (valueDeserializer == null) {
            this.valueDeserializer = consumerConfig.getConfiguredInstance(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, Deserializer.class);
            this.valueDeserializer.configure(consumerConfig.originals(), false);
        } else {
            consumerConfig.ignore(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG);
        }
        // /borrowed from KafkaConsumer
    }

    /**
     * Returns the first offset of the first S3 object
     * @return the first offset of the first S3 object
     */
    public long beginningOffset() {
        maybeReloadOffsetKeyMap(0L);
        if (offsetKeyMap.isEmpty()) {
            LOG.error(String.format("No beginning offset found for %s", topicPartition));
            return -1L;
        }
        return offsetKeyMap.firstKey();
    }

    /**
     * Returns the first offset of the last S3 object (note that this is not the last offset of the last S3 object)
     * @return the first offset of the last S3 object
     */
    public long endOffset() {
        maybeReloadOffsetKeyMap(0L);
        if (offsetKeyMap.isEmpty()) {
            LOG.error(String.format("No end offset found for %s", topicPartition));
            return -1L;
        }
        return offsetKeyMap.lastKey();
    }

    /**
     * Sets the position to consume from. Next offset to consume should be this value.
     * @param position the position to consume from
     */
    public void setPosition(long position) {
        this.position = position;
    }

    /**
     * @return the last seen offset + 1. Next offset to consume should be this value.
     */
    public long getPosition() {
        return this.position;
    }

    /**
     * Reloads the offsetKeyMap only if it is not initialized yet or if it is expired, or if the given position is not found in the map
     */
    private void maybeReloadOffsetKeyMap(long position) {
        if (offsetKeyMap == null || offsetKeyMap.isEmpty()) {
            LOG.info(String.format("offsetKeyMap is empty / uninitialized. Loading offsetKeyMap for position=%s, topicPartition=%s", position, topicPartition));
            reloadOffsetKeyMap(position, null);
        } else if (offsetKeyMap.firstKey() > position || offsetKeyMap.lastKey() < position) {
            LOG.info(String.format("offsetKeyMap [%s,%s] does not contain position=%s. Reloading offsetKeyMap for topicPartition=%s", offsetKeyMap.firstKey(), offsetKeyMap.lastKey(), position, topicPartition));
            reloadOffsetKeyMap(position, null);
        } else if (System.currentTimeMillis() - lastOffsetKeyMapReloadTimestamp > s3MetadataReloadIntervalMs) {
            LOG.info(String.format("offsetKeyMap is expired after %sms. Reloading offsetKeyMap for position=%s, topicPartition=%s", s3MetadataReloadIntervalMs, position, topicPartition));
            reloadOffsetKeyMap(position, null);
        }
    }

    /**
     * Reloads the offsetKeyMap starting from the given position
     * @param position the position to start from
     * @param objectToStartFrom the object to start from
     */
    private void reloadOffsetKeyMap(long position, String objectToStartFrom) {
        LOG.info("Reloading offsetKeyMap for position " + position + " and objectToStartFrom " + objectToStartFrom + " for " + topicPartition);
        offsetKeyMap = S3Utils.getSortedOffsetKeyMap(location, topicPartition, S3Utils.getZeroPaddedOffset(position), objectToStartFrom, metricsConfiguration);
        lastOffsetKeyMapReloadTimestamp = System.currentTimeMillis();
    }

    /**
     * Sets the {@link S3Records} object which will allow the consumer to consume records from S3
     * @param position
     * @throws NoS3ObjectException
     * @throws IOException
     */
    private void maybeSetS3Records(long position) throws NoS3ObjectException, IOException {
        Triple<String, String, Long> s3PathForPosition = getS3PathForPosition(position);

        if (activeS3Offset > position) {
            LOG.warn(String.format("Lost offsets for %s: [%s, %s]", topicPartition, this.position, activeS3Offset - 1));
            MetricRegistryManager.getInstance(metricsConfiguration).updateCounter(
                    topicPartition.topic(), topicPartition.partition(),
                    ConsumerMetrics.OFFSET_CONSUMPTION_MISSED_METRIC,
                    activeS3Offset - this.position,
                    "ts=true", "group=" + consumerGroup, "from=" + this.position
            );
            this.position = activeS3Offset;
        }

        if (s3Path != null && s3Path.equals(s3PathForPosition)) {
            // continue using the same s3Records channel
            LOG.debug("Reusing same s3Records channel for s3Path " + s3Path);
            return;
        }

        s3Path = s3PathForPosition;
        LOG.debug(String.format("S3Path: %s", s3Path));
        if (s3Path == null)
            throw new NoS3ObjectException();

        int bytePositionInFile = s3OffsetIndexHandler.getMinimumBytePositionInFile(s3Path, this.position);

        s3Records = S3Records.open(
                s3Path.getLeft(),
                s3Path.getMiddle(),
                bytePositionInFile,
                false,
                true,
                s3Path.getRight().intValue(),
                true
        );
    }

    /**
     * Polls for records from S3 without necessarily reading the whole S3 log segment object
     * @param maxRecords
     * @return list of {@link org.apache.kafka.clients.consumer.ConsumerRecords}
     */
    public List<ConsumerRecord<K, V>> poll(int maxRecords) {
        return poll(maxRecords, false);
    }

    /**
     * Polls for records from S3
     * @param maxRecords
     * @param shouldReadWholeObject
     * @return list of {@link org.apache.kafka.clients.consumer.ConsumerRecords}
     */
    public List<ConsumerRecord<K, V>> poll(int maxRecords, boolean shouldReadWholeObject) {
        if (shouldReadWholeObject)
            LOG.debug(String.format("Trying to consume all records from each S3 object for %s", topicPartition));
        else
            LOG.debug(String.format("Trying to consume %s messages or %s bytes from %s", maxRecords, maxPartitionFetchSizeBytes, topicPartition));
        try {
            maybeSetS3Records(position);
        } catch (NoS3ObjectException e) {
            // no data on s3 yet
            return Collections.emptyList();
        } catch (IOException e) {
            LOG.error("IOException when trying to set s3Records", e);
            return Collections.emptyList();
        } catch (Exception e) {
            LOG.error("Unhandled exception while trying to set s3Records", e);
            return Collections.emptyList();
        }

        long recordCount = 0;
        long fetchSizeBytes = 0;
        boolean skipped = false;

        Iterator<S3ChannelRecordBatch> batches =
                s3Records.batchesFrom(s3OffsetIndexHandler.getMinimumBytePositionInFile(s3Path, position)).iterator();
        if (!batches.hasNext()) {
        }

        long lastSeenOffset = -1;
        List<ConsumerRecord<K, V>> records = new ArrayList<>();
        LOG.debug(String.format("For consuming from offset %s will be processing S3 object %s", position, s3Path));
        while (batches.hasNext() && (shouldReadWholeObject || (recordCount < maxRecords && fetchSizeBytes < maxPartitionFetchSizeBytes))) {
            S3ChannelRecordBatch batch = batches.next();
            for (Record record : batch) {
                lastSeenOffset = record.offset();

                // skip earlier records
                if (lastSeenOffset < position) {
                    if (!skipped) {
                        if (recordCount == 0)
                            LOG.debug(String.format("Skipping messages of %s starting at offset %s as the offset of interest is %s.", topicPartition, lastSeenOffset, position));
                        else
                            LOG.warn(String.format("Unexpected %s offset %s as messages have already been consumed (position: %s)", topicPartition, lastSeenOffset, position));
                        skipped = true;
                    }
                    continue;
                }

                if (recordCount == 0) {
                    LOG.debug(String.format("Starting to consume from %s offset %s (position: %s)", topicPartition, lastSeenOffset, position));
                    if (position == 0)
                        position = lastSeenOffset;
                }
                Headers headers = new RecordHeaders();
                // TODO: Confirm if the below line is needed.
                Arrays.stream(record.headers()).forEach(headers::add);

                records.add(new ConsumerRecord<>(
                    topicPartition.topic(),
                    topicPartition.partition(),
                    record.offset(),
                    record.timestamp(),
                    TimestampType.CREATE_TIME,
                    record.keySize(),
                    record.valueSize(),
                    record.key() == null ? null
                                         : keyDeserializer.deserialize(topicPartition.topic(),
                                             Utils.toArray(record.key())),
                    record.value() == null ? null
                                           : valueDeserializer.deserialize(topicPartition.topic(),
                                               Utils.toArray(record.value())),
                    headers, Optional.empty()
                ));
                ++recordCount;
                fetchSizeBytes += record.sizeInBytes();
                position = lastSeenOffset + 1;
                if (recordCount >= maxRecords || fetchSizeBytes >= maxPartitionFetchSizeBytes) {
                    break;
                }
            }
        }

        if (records.isEmpty()) {
            LOG.info(String.format("No record was consumed from %s (position %s)", s3Path, position));
            if (lastSeenOffset >= 0) {
                try {
                    // force reload the offsetKeyMap
                    reloadOffsetKeyMap(lastSeenOffset + 1, latestS3Object);
                    maybeSetS3Records(lastSeenOffset + 1);
                } catch (NoS3ObjectException | IOException e) {
                    e.printStackTrace();
                    return records;
                }
            }
        } else
            LOG.debug(String.format("Consumed %s messages from %s [%s, %s]",
                    records.size(), topicPartition, records.get(0).offset(), records.get(records.size() - 1).offset()));
        return records;
    }

    /**
     * Returns the first S3 object whose offset is equal or smaller than the given offset.
     * That object should contain the message with the given offset. If the found object is the last
     * object (the highest offset object) we try to refresh the list of object first in case the list
     * has become stale.
     * @return pair of bucket and prefix
     */
    private Triple<String, String, Long> getS3PathForPosition(long position) {
        maybeReloadOffsetKeyMap(position);

        LOG.debug(String.format("Looking for offset %s in offsetKeyMap: [%s, %s]", position,
                offsetKeyMap.firstEntry(), offsetKeyMap.lastEntry()));

        if (offsetKeyMap.isEmpty())
            return null;

        if (offsetKeyMap.size() == 1) {
            if (position == 0 || position >= offsetKeyMap.firstKey()) {
                activeS3Offset = offsetKeyMap.firstKey();
                return offsetKeyMap.firstEntry().getValue();
            } else
                return null;
        }

        Triple<String, String, Long> s3Object = null;
        Triple<String, String, Long> s3ObjectNext = null;

        if (position == 0) {
            // we want to start from earliest
            activeS3Offset = offsetKeyMap.firstKey();
            s3Object = offsetKeyMap.firstEntry().getValue();
            s3ObjectNext = offsetKeyMap.tailMap(offsetKeyMap.firstKey(), false).isEmpty() ? null :
                    offsetKeyMap.tailMap(offsetKeyMap.firstKey(), false).firstEntry().getValue();
        } else {
            int count = 0;
            Map.Entry<Long, Triple<String, String, Long>> lastEntry = null;
            for (Map.Entry<Long, Triple<String, String, Long>> entry : offsetKeyMap.entrySet()) {
                ++count;

                if (lastEntry == null && position < entry.getKey()) {
                    activeS3Offset = entry.getKey();
                    s3Object = entry.getValue();
                    s3ObjectNext = offsetKeyMap.tailMap(entry.getKey(), false).isEmpty() ? null :
                            offsetKeyMap.tailMap(entry.getKey(), false).firstEntry().getValue();
                    break;
                }

                if (lastEntry == null) {
                    // skip the first round so we get one more entry
                    lastEntry = entry;
                    continue;
                }

                if (position >= lastEntry.getKey() && position < entry.getKey()) {
                    activeS3Offset = lastEntry.getKey();
                    s3Object = lastEntry.getValue();
                    s3ObjectNext = entry.getValue();
                    break;
                }

                lastEntry = entry;
            }

            if (lastEntry != null && position >= lastEntry.getKey()) {
                activeS3Offset = lastEntry.getKey();
                s3Object = lastEntry.getValue();
            }

            if (s3Object == null) {
                // we should have found the object by now
                LOG.info(String.format("Could not find an S3 object for %s position %s in offsetKeyMap [%s, %s]",
                        topicPartition, position, offsetKeyMap.firstEntry(), offsetKeyMap.lastEntry()));
                assert false;
            }
        }

        latestS3Object = s3Object.getMiddle();
        LOG.debug(String.format("Found S3 object: %s, next: %s", latestS3Object, s3ObjectNext));
        latestS3Object = latestS3Object.substring(0, latestS3Object.lastIndexOf("."));
        return s3Object;
    }

    @InterfaceStability.Evolving
    public Map<TopicPartition, OffsetAndTimestamp> offsetForTime(Long timestamp, Long beginningOffset, Long endOffset) {
        //TODO: This needs a delicate implementation to avoid listing the whole prefix, which is an expensive operation,
        // if possible. A naive approach is to do a binary search since we have the first and last offset (log segment)
        // on S3
        throw new UnsupportedOperationException("offsetForTime is not implemented yet");
    }

    /**
     * Closes the S3Records object
     * @throws IOException
     */
    public void close() throws IOException {
        if (this.s3Records != null) {
            this.s3Records.close();
        }
    }

    /**
     * Updates the S3 location for consumption
     * @param location
     */
    public void update(String location) {
        this.location = location;
    }
}

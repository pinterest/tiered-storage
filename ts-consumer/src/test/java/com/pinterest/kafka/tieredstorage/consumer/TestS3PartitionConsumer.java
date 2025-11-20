package com.pinterest.kafka.tieredstorage.consumer;

import com.pinterest.kafka.tieredstorage.common.CommonTestUtils;
import com.pinterest.kafka.tieredstorage.common.SegmentUtils;
import com.pinterest.kafka.tieredstorage.common.Utils;
import com.pinterest.kafka.tieredstorage.common.metadata.TimeIndex;
import com.pinterest.kafka.tieredstorage.common.metrics.MetricsConfiguration;
import com.pinterest.kafka.tieredstorage.common.metrics.NoOpMetricsReporter;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.zookeeper.KeeperException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestS3PartitionConsumer extends TestS3Base {

    @BeforeEach
    @Override
    void setup() throws IOException, ExecutionException, InterruptedException, ClassNotFoundException, KeeperException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
        super.setup();
    }

    /**
     * Test that the correct beginning offset is retrieved. The beginning offset is the offset of the first log segment present in S3
     */
    @Test
    void testBeginningOffset() {
        putEmptyObjects(KAFKA_TOPIC, 4, 100L, 1000L, 100L);
        Properties properties = getConsumerProperties();
        S3Utils.overrideS3Client(s3Client);
        String metricsReporterClassName = NoOpMetricsReporter.class.getName();
        MetricsConfiguration metricsConfiguration = new MetricsConfiguration(true, metricsReporterClassName, null, null);
        S3PartitionConsumer<byte[], byte[]> s3PartitionConsumer = new S3PartitionConsumer<>(getS3BasePrefixWithCluster(), new TopicPartition(KAFKA_TOPIC, 4), CONSUMER_GROUP, properties, metricsConfiguration);
        assertEquals(100L, s3PartitionConsumer.beginningOffset());
        s3Client.deleteObject(DeleteObjectRequest.builder().bucket(S3_BUCKET).key(getS3ObjectKey(KAFKA_TOPIC, 4, 100L, SegmentUtils.SegmentFileType.LOG)).build());
        assertEquals(200L, s3PartitionConsumer.beginningOffset());

        // nonexistent offset for partition
        S3PartitionConsumer<byte[], byte[]> s3PartitionConsumer2 = new S3PartitionConsumer<>(getS3BasePrefixWithCluster(), new TopicPartition(KAFKA_TOPIC, 6), CONSUMER_GROUP, properties, metricsConfiguration);
        assertEquals(-1L, s3PartitionConsumer2.beginningOffset());
        putEmptyObjects(KAFKA_TOPIC, 6, 50L, 200L, 20L);
        assertEquals(50L, s3PartitionConsumer2.beginningOffset());
    }

    @Test
    void testBeginningOffsetWithMetadata() {
        putEmptyObjects(KAFKA_TOPIC, 4, 100L, 1000L, 100L);
        putMetadataFile(KAFKA_CLUSTER_ID, KAFKA_TOPIC, 4, 100L, 1000L, 100L);
        Properties properties = getConsumerProperties();
        S3Utils.overrideS3Client(s3Client);
        String metricsReporterClassName = NoOpMetricsReporter.class.getName();
        MetricsConfiguration metricsConfiguration = new MetricsConfiguration(true, metricsReporterClassName, null, null);
        S3PartitionConsumer<byte[], byte[]> s3PartitionConsumer = new S3PartitionConsumer<>(getS3BasePrefixWithCluster(), new TopicPartition(KAFKA_TOPIC, 4), CONSUMER_GROUP, properties, metricsConfiguration);
        assertEquals(100L, s3PartitionConsumer.beginningOffset());
    }

    @Test
    void testEndOffsetWithMetadata() {
        putEmptyObjects(KAFKA_TOPIC, 4, 100L, 1000L, 100L);
        putMetadataFile(KAFKA_CLUSTER_ID, KAFKA_TOPIC, 4, 100L, 1000L, 100L);
        Properties properties = getConsumerProperties();
        S3Utils.overrideS3Client(s3Client);
        String metricsReporterClassName = NoOpMetricsReporter.class.getName();
        MetricsConfiguration metricsConfiguration = new MetricsConfiguration(true, metricsReporterClassName, null, null);
        S3PartitionConsumer<byte[], byte[]> s3PartitionConsumer = new S3PartitionConsumer<>(getS3BasePrefixWithCluster(), new TopicPartition(KAFKA_TOPIC, 4), CONSUMER_GROUP, properties, metricsConfiguration);
        assertEquals(1000L, s3PartitionConsumer.endOffset());
    }

    @Test
    void testBeginningOffsetDangling() {
        putEmptyObjectsDanglingEarliest(KAFKA_CLUSTER_ID, KAFKA_TOPIC, 4, 100L, 1000L, 100L);
        Properties properties = getConsumerProperties();
        S3Utils.overrideS3Client(s3Client);
        String metricsReporterClassName = NoOpMetricsReporter.class.getName();
        MetricsConfiguration metricsConfiguration = new MetricsConfiguration(true, metricsReporterClassName, null, null);
        S3PartitionConsumer<byte[], byte[]> s3PartitionConsumer = new S3PartitionConsumer<>(getS3BasePrefixWithCluster(), new TopicPartition(KAFKA_TOPIC, 4), CONSUMER_GROUP, properties, metricsConfiguration);
        assertEquals(200L, s3PartitionConsumer.beginningOffset());  // should skip 100
    }

    /**
     * Test that the correct end offset is retrieved. The end offset is the offset of the last log segment present in S3
     */
    @Test
    void testEndOffset() {
        putEmptyObjects(KAFKA_TOPIC, 9, 100L, 1000L, 100L);
        Properties properties = getConsumerProperties();
        S3Utils.overrideS3Client(s3Client);
        String metricsReporterClassName = NoOpMetricsReporter.class.getName();
        MetricsConfiguration metricsConfiguration = new MetricsConfiguration(true, metricsReporterClassName, null, null);
        S3PartitionConsumer<byte[], byte[]> s3PartitionConsumer = new S3PartitionConsumer<>(getS3BasePrefixWithCluster(), new TopicPartition(KAFKA_TOPIC, 9), CONSUMER_GROUP, properties, metricsConfiguration);
        assertEquals(1000L, s3PartitionConsumer.endOffset());

        putEmptyObjects(KAFKA_TOPIC, 9, 1100L, 1150L, 100L);
        assertEquals(1100L, s3PartitionConsumer.endOffset());
    }

    /**
     * Ensure that the read logic for S3 consumption works as expected
     * @throws IOException
     */
    @Test
    void testConsumption() throws IOException {
        prepareS3Mocks();

        putObjects(KAFKA_CLUSTER_ID, "test_topic_a", 0, "src/test/resources/log-files/test_topic_a-0");
        Properties properties = getConsumerProperties();
        S3Utils.overrideS3Client(s3Client);
        S3OffsetIndexHandler.overrideS3Client(s3Client);
        String metricsReporterClassName = NoOpMetricsReporter.class.getName();
        MetricsConfiguration metricsConfiguration = new MetricsConfiguration(true, metricsReporterClassName, null, null);
        S3PartitionConsumer<String, String> s3PartitionConsumer = new S3PartitionConsumer<>(getS3BasePrefixWithCluster(), new TopicPartition("test_topic_a", 0), CONSUMER_GROUP, properties, metricsConfiguration,
                new StringDeserializer(), new StringDeserializer());
        List<ConsumerRecord<String, String>> records;
        int numRecords = 0;
        while (!(records = s3PartitionConsumer.poll(100)).isEmpty()) {
            for (ConsumerRecord<String, String> record : records) {
                CommonTestUtils.validateRecordContent(CommonTestUtils.RecordContentType.TIERED_STORAGE, record);
                numRecords++;
            }
        }
        assertEquals(TEST_TOPIC_A_P0_NUM_RECORDS, numRecords); // based on log segment files
        closeS3Mocks();
    }

    @Test
    void testOffsetForTime() throws IOException {
        prepareS3Mocks();

        putObjects(KAFKA_CLUSTER_ID, "test_topic_a", 0, "src/test/resources/log-files/test_topic_a-0");
        Properties properties = getConsumerProperties();
        S3Utils.overrideS3Client(s3Client);
        S3OffsetIndexHandler.overrideS3Client(s3Client);
        String metricsReporterClassName = NoOpMetricsReporter.class.getName();
        MetricsConfiguration metricsConfiguration = new MetricsConfiguration(true, metricsReporterClassName, null, null);
        TopicPartition topicPartition = new TopicPartition("test_topic_a", 0);
        S3PartitionConsumer<String, String> s3PartitionConsumer = new S3PartitionConsumer<>(getS3BasePrefixWithCluster(), topicPartition, CONSUMER_GROUP, properties, metricsConfiguration,
                new StringDeserializer(), new StringDeserializer());

        TreeMap<Long, Triple<String, String, Long>> offsetMap = S3Utils.getSortedOffsetKeyMap(getS3BasePrefixWithCluster(), topicPartition, Utils.getZeroPaddedOffset(0L), null, metricsConfiguration);
        assertFalse(offsetMap.isEmpty());

        long targetSegmentBaseOffset = offsetMap.keySet().stream().skip(3).findFirst().orElse(offsetMap.firstKey());
        TimeIndex segmentTimeIndex = S3Utils.loadSegmentTimeIndex(getS3BasePrefixWithCluster(), topicPartition, targetSegmentBaseOffset).orElseThrow(IllegalStateException::new);
        assertFalse(segmentTimeIndex.isEmpty());

        TimeIndex.TimeIndexEntry chosenEntry = segmentTimeIndex.getEntriesCopy().stream().skip(3).findFirst().orElse(segmentTimeIndex.getFirstEntry());
        assertNotNull(chosenEntry);

        long targetTimestamp = chosenEntry.getTimestamp();
        long expectedOffset = chosenEntry.getBaseOffset() + chosenEntry.getRelativeOffset();

        Optional<OffsetAndTimestamp> result = s3PartitionConsumer.offsetForTime(targetTimestamp);
        assertTrue(result.isPresent());
        OffsetAndTimestamp offsetAndTimestamp = result.get();
        assertEquals(expectedOffset, offsetAndTimestamp.offset());
        assertEquals(chosenEntry.getTimestamp(), offsetAndTimestamp.timestamp());
        closeS3Mocks();
    }

    @Test
    void testOffsetForTimeBeforeEarliestTimestampReturnsEarliest() throws IOException {
        prepareS3Mocks();

        putObjects(KAFKA_CLUSTER_ID, "test_topic_a", 0, "src/test/resources/log-files/test_topic_a-0");
        Properties properties = getConsumerProperties();
        S3Utils.overrideS3Client(s3Client);
        S3OffsetIndexHandler.overrideS3Client(s3Client);
        String metricsReporterClassName = NoOpMetricsReporter.class.getName();
        MetricsConfiguration metricsConfiguration = new MetricsConfiguration(true, metricsReporterClassName, null, null);
        TopicPartition topicPartition = new TopicPartition("test_topic_a", 0);
        S3PartitionConsumer<String, String> s3PartitionConsumer = new S3PartitionConsumer<>(getS3BasePrefixWithCluster(), topicPartition, CONSUMER_GROUP, properties, metricsConfiguration,
                new StringDeserializer(), new StringDeserializer());

        TreeMap<Long, Triple<String, String, Long>> offsetMap = S3Utils.getSortedOffsetKeyMap(getS3BasePrefixWithCluster(), topicPartition, Utils.getZeroPaddedOffset(0L), null, metricsConfiguration);
        assertFalse(offsetMap.isEmpty());

        long earliestSegmentBaseOffset = offsetMap.firstKey();
        long targetTimestamp = 1L;

        Optional<OffsetAndTimestamp> result = s3PartitionConsumer.offsetForTime(targetTimestamp);
        assertTrue(result.isPresent());
        OffsetAndTimestamp offsetAndTimestamp = result.get();
        assertEquals(earliestSegmentBaseOffset, offsetAndTimestamp.offset());
        closeS3Mocks();
    }

    @Test
    void testOffsetForTimeAfterLatestTimestampReturnsEmpty() throws IOException {
        prepareS3Mocks();

        putObjects(KAFKA_CLUSTER_ID, "test_topic_a", 0, "src/test/resources/log-files/test_topic_a-0");
        Properties properties = getConsumerProperties();
        S3Utils.overrideS3Client(s3Client);
        S3OffsetIndexHandler.overrideS3Client(s3Client);
        String metricsReporterClassName = NoOpMetricsReporter.class.getName();
        MetricsConfiguration metricsConfiguration = new MetricsConfiguration(true, metricsReporterClassName, null, null);
        TopicPartition topicPartition = new TopicPartition("test_topic_a", 0);
        S3PartitionConsumer<String, String> s3PartitionConsumer = new S3PartitionConsumer<>(getS3BasePrefixWithCluster(), topicPartition, CONSUMER_GROUP, properties, metricsConfiguration,
                new StringDeserializer(), new StringDeserializer());

        TreeMap<Long, Triple<String, String, Long>> offsetMap = S3Utils.getSortedOffsetKeyMap(getS3BasePrefixWithCluster(), topicPartition, Utils.getZeroPaddedOffset(0L), null, metricsConfiguration);
        assertFalse(offsetMap.isEmpty());

        long latestSegmentBaseOffset = offsetMap.lastKey();
        TimeIndex latestSegmentTimeIndex = S3Utils.loadSegmentTimeIndex(getS3BasePrefixWithCluster(), topicPartition, latestSegmentBaseOffset).orElseThrow(IllegalStateException::new);
        TimeIndex.TimeIndexEntry latestEntry = latestSegmentTimeIndex.getLastEntry();
        assertNotNull(latestEntry);

        long targetTimestamp = latestEntry.getTimestamp() + 1000;

        Optional<OffsetAndTimestamp> result = s3PartitionConsumer.offsetForTime(targetTimestamp);
        assertFalse(result.isPresent());
        closeS3Mocks();
    }

    @Test
    void testOffsetForTimeWithTimeIndexGap() throws IOException {
        prepareS3Mocks();

        String directory = "src/test/resources/log-files/test_topic_a-0";
        long missingBaseOffset = 362L;
        putObjectsWithGapInTimeIndex(KAFKA_CLUSTER_ID, "test_topic_a", 0, directory, missingBaseOffset);

        Properties properties = getConsumerProperties();
        S3Utils.overrideS3Client(s3Client);
        S3OffsetIndexHandler.overrideS3Client(s3Client);
        String metricsReporterClassName = NoOpMetricsReporter.class.getName();
        MetricsConfiguration metricsConfiguration = new MetricsConfiguration(true, metricsReporterClassName, null, null);
        TopicPartition topicPartition = new TopicPartition("test_topic_a", 0);
        S3PartitionConsumer<String, String> s3PartitionConsumer = new S3PartitionConsumer<>(getS3BasePrefixWithCluster(), topicPartition, CONSUMER_GROUP, properties, metricsConfiguration,
                new StringDeserializer(), new StringDeserializer());

        TimeIndex segmentTimeIndex = S3Utils.loadSegmentTimeIndex(getS3BasePrefixWithCluster(), topicPartition, missingBaseOffset).orElseThrow(IllegalStateException::new);
        assertFalse(segmentTimeIndex.isEmpty());
        TimeIndex.TimeIndexEntry targetEntry = segmentTimeIndex.getLastEntry();
        assertNotNull(targetEntry);

        long targetTimestamp = targetEntry.getTimestamp();
        long expectedOffset = targetEntry.getBaseOffset() + targetEntry.getRelativeOffset();

        Optional<OffsetAndTimestamp> result = s3PartitionConsumer.offsetForTime(targetTimestamp);
        assertTrue(result.isPresent());
        OffsetAndTimestamp offsetAndTimestamp = result.get();
        assertEquals(expectedOffset, offsetAndTimestamp.offset());
        assertEquals(targetEntry.getTimestamp(), offsetAndTimestamp.timestamp());
        closeS3Mocks();
    }

    protected static Properties getConsumerProperties() {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP);
        properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, CLIENT_ID);
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, Integer.toString(MAX_POLL_RECORDS));
        properties.setProperty(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, Long.toString(MAX_PARTITION_FETCH_BYTES));
        properties.setProperty(TieredStorageConsumerConfig.TIERED_STORAGE_MODE_CONFIG, TieredStorageConsumer.TieredStorageMode.KAFKA_PREFERRED.toString());
        properties.setProperty(TieredStorageConsumerConfig.KAFKA_CLUSTER_ID_CONFIG, KAFKA_CLUSTER_ID);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, OffsetResetStrategy.EARLIEST.toString());
        return properties;
    }

}

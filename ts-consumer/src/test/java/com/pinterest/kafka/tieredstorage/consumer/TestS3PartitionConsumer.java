package com.pinterest.kafka.tieredstorage.consumer;

import com.pinterest.kafka.tieredstorage.common.metrics.MetricsConfiguration;
import com.pinterest.kafka.tieredstorage.common.metrics.NoOpMetricsReporter;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.zookeeper.KeeperException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;

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
        MetricsConfiguration metricsConfiguration = new MetricsConfiguration(metricsReporterClassName, null, null);
        S3PartitionConsumer s3PartitionConsumer = new S3PartitionConsumer(getS3BasePrefixWithCluster(), new TopicPartition(KAFKA_TOPIC, 4), CONSUMER_GROUP, properties, metricsConfiguration);
        assertEquals(100L, s3PartitionConsumer.beginningOffset());
        s3Client.deleteObject(DeleteObjectRequest.builder().bucket(S3_BUCKET).key(getS3ObjectKey(KAFKA_TOPIC, 4, 100L, FileType.LOG)).build());
        assertEquals(200L, s3PartitionConsumer.beginningOffset());

        // nonexistent offset for partition
        S3PartitionConsumer s3PartitionConsumer2 = new S3PartitionConsumer(getS3BasePrefixWithCluster(), new TopicPartition(KAFKA_TOPIC, 6), CONSUMER_GROUP, properties, metricsConfiguration);
        assertEquals(-1L, s3PartitionConsumer2.beginningOffset());
        putEmptyObjects(KAFKA_TOPIC, 6, 50L, 200L, 20L);
        assertEquals(50L, s3PartitionConsumer2.beginningOffset());
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
        MetricsConfiguration metricsConfiguration = new MetricsConfiguration(metricsReporterClassName, null, null);
        S3PartitionConsumer s3PartitionConsumer = new S3PartitionConsumer(getS3BasePrefixWithCluster(), new TopicPartition(KAFKA_TOPIC, 9), CONSUMER_GROUP, properties, metricsConfiguration);
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
        MetricsConfiguration metricsConfiguration = new MetricsConfiguration(metricsReporterClassName, null, null);
        S3PartitionConsumer s3PartitionConsumer = new S3PartitionConsumer(getS3BasePrefixWithCluster(), new TopicPartition("test_topic_a", 0), CONSUMER_GROUP, properties, metricsConfiguration);
        List<ConsumerRecord<byte[], byte[]>> records;
        int numRecords = 0;
        while (!(records = s3PartitionConsumer.poll(100)).isEmpty()) {
            for (ConsumerRecord<byte[], byte[]> record : records) {
                String currRecordNumString = String.valueOf(numRecords);

                // known record values
                assertEquals(currRecordNumString, new String(record.key()));
                assertEquals("val-" + currRecordNumString, new String(record.value()));
                assertEquals("header1", record.headers().headers("header1").iterator().next().key());
                assertEquals("header1-val", new String(record.headers().headers("header1").iterator().next().value()));

                numRecords++;
            }
        }
        assertEquals(TEST_DATA_NUM_RECORDS, numRecords); // based on log segment files
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
        properties.setProperty(TieredStorageConsumerConfig.OFFSET_RESET_CONFIG, TieredStorageConsumer.OffsetReset.EARLIEST.toString());
        return properties;
    }

}

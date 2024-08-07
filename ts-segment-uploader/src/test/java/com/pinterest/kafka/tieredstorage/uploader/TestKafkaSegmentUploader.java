package com.pinterest.kafka.tieredstorage.uploader;

import com.pinterest.kafka.tieredstorage.common.discovery.s3.S3StorageServiceEndpoint;
import com.pinterest.kafka.tieredstorage.common.discovery.s3.S3StorageServiceEndpointProvider;
import com.salesforce.kafka.test.junit5.SharedKafkaTestResource;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.zookeeper.KeeperException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestKafkaSegmentUploader extends TestBase {
    @RegisterExtension
    private static final SharedKafkaTestResource sharedKafkaTestResource = new SharedKafkaTestResource()
            .withBrokerProperty("log.segment.bytes", "30000")
            .withBrokerProperty("log.index.interval.bytes", "100")
            .withBrokerProperty("log.segment.delete.delay.ms", "5000")
            .withBrokerProperty("log.dir", "/tmp/kafka-unit");
    private static final String TEST_TOPIC_A = "test_topic_a";
    private static final String TEST_TOPIC_B = "test_topic_b";
    private static final String TEST_CLUSTER = "test-cluster-2";
    private static AdminClient adminClient;
    private KafkaSegmentUploader uploader;
    private KafkaEnvironmentProvider environmentProvider;

    @Override
    @BeforeEach
    void setup() throws InterruptedException, IOException, KeeperException, ConfigurationException, ExecutionException, ClassNotFoundException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
        super.setup();
        environmentProvider = new KafkaEnvironmentProvider() {

            private String zookeeperConnect;
            private String logDir;
            @Override
            public void load() {
                this.zookeeperConnect = sharedKafkaTestResource.getZookeeperConnectString();
                try {
                    this.logDir = getBrokerConfig(sharedKafkaTestResource, 1, "log.dir");
                } catch (ExecutionException | InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public String clusterId() {
                return TEST_CLUSTER;
            }

            @Override
            public Integer brokerId() {
                return 1;
            }

            @Override
            public String zookeeperConnect() {
                return zookeeperConnect;
            }

            @Override
            public String logDir() {
                return logDir;
            }
        };
        createTopicAndVerify(sharedKafkaTestResource, TEST_TOPIC_A, 3);
        createTopicAndVerify(sharedKafkaTestResource, TEST_TOPIC_B, 6);
        startSegmentUploaderThread();
    }

    @Override
    @AfterEach
    void tearDown() throws IOException, InterruptedException, ExecutionException {
        uploader.stop();
        DirectoryTreeWatcher.unsetKafkaLeadershipWatcher();
        deleteTopicAndVerify(sharedKafkaTestResource, TEST_TOPIC_A);
        sharedKafkaTestResource.getKafkaTestUtils().getAdminClient().close();
        super.tearDown();
    }

    @BeforeAll
    static void prepare() {
        adminClient = sharedKafkaTestResource.getKafkaTestUtils().getAdminClient();
    }

    @AfterAll
    static void tearDownAll() {
        adminClient.close();
    }

    private void startSegmentUploaderThread() throws InterruptedException, IOException, KeeperException, ClassNotFoundException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
        String configDirectory = "src/test/resources";
        MultiThreadedS3FileUploader.overrideS3Client(s3Client);
        S3FileDownloader.overrideS3Client(s3Client);
        uploader = new KafkaSegmentUploader(configDirectory, environmentProvider);
        uploader.start();
    }

    /**
     * Test that the segment uploader can handle filesystem monitoring and segment uploads for a single topic-partition
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    void testSegmentUploaderSinglePartition() throws IOException, InterruptedException {
        long now = System.currentTimeMillis();
        long numRecordsSent = 0;
        while (System.currentTimeMillis() - now < 45000) {
            sendTestData(sharedKafkaTestResource, TEST_TOPIC_A, 0, 100);
            numRecordsSent += 100;
        }

        S3StorageServiceEndpoint storageEndpoint = ((S3StorageServiceEndpointProvider) uploader.getEndpointProvider())
                .getStorageServiceEndpointBuilderForTopic(TEST_TOPIC_A)
                .setTopicPartition(new TopicPartition(TEST_TOPIC_A, 0))
                .setPrefixEntropyNumBits(uploader.getSegmentUploaderConfiguration().getS3PrefixEntropyBits())
                .build();

        Thread.sleep(10000);

        ResponseInputStream<GetObjectResponse> getObjectResponse = getObjectResponse(
                S3_BUCKET,
                storageEndpoint.getFullPrefix() + "/offset.wm",
                s3Client
        );
        BufferedReader reader = new BufferedReader(
                new InputStreamReader(getObjectResponse, StandardCharsets.UTF_8));
        String line = reader.readLine();
        reader.close();

        // assert that last watermark upload corresponds to an offset that is less than the number of records sent but not too far behind
        assertTrue(Long.parseLong(line) < numRecordsSent, "numRecordsSent was " + numRecordsSent + " but offset.wm was " + Long.parseLong(line));
        assertTrue(Long.parseLong(line) > numRecordsSent * 0.8, "numRecordsSent was " + numRecordsSent + " but offset.wm was "  + Long.parseLong(line));

        deleteTopicAndVerify(sharedKafkaTestResource, TEST_TOPIC_A);
        deleteTopicAndVerify(sharedKafkaTestResource, TEST_TOPIC_B);
    }

    /**
     * Test that the segment uploader can handle multiple topic-partitions
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    void testSegmentUploaderMultiplePartitions() throws IOException, InterruptedException {
        long now = System.currentTimeMillis();
        long numRecordsSent = 0;
        while (System.currentTimeMillis() - now < 45000) {
            sendTestData(sharedKafkaTestResource, TEST_TOPIC_A, 0, 100);
            sendTestData(sharedKafkaTestResource, TEST_TOPIC_B, 0, 100);
            numRecordsSent += 100;
        }

        S3StorageServiceEndpoint storageEndpointA = ((S3StorageServiceEndpointProvider) uploader.getEndpointProvider())
                .getStorageServiceEndpointBuilderForTopic(TEST_TOPIC_A)
                .setTopicPartition(new TopicPartition(TEST_TOPIC_A, 0))
                .setPrefixEntropyNumBits(uploader.getSegmentUploaderConfiguration().getS3PrefixEntropyBits())
                .build();

        S3StorageServiceEndpoint storageEndpointB = ((S3StorageServiceEndpointProvider) uploader.getEndpointProvider())
                .getStorageServiceEndpointBuilderForTopic(TEST_TOPIC_B)
                .setTopicPartition(new TopicPartition(TEST_TOPIC_B, 0))
                .setPrefixEntropyNumBits(uploader.getSegmentUploaderConfiguration().getS3PrefixEntropyBits())
                .build();

        Thread.sleep(10000);

        ResponseInputStream<GetObjectResponse> getObjectResponse = getObjectResponse(
                S3_BUCKET,
                storageEndpointA.getFullPrefix() + "/offset.wm",
                s3Client
        );
        BufferedReader reader = new BufferedReader(
                new InputStreamReader(getObjectResponse, StandardCharsets.UTF_8));
        String topicAOffsetWm = reader.readLine();
        reader.close();

        getObjectResponse = getObjectResponse(
                S3_BUCKET,
                storageEndpointB.getFullPrefix() + "/offset.wm",
                s3Client
        );
        reader = new BufferedReader(
                new InputStreamReader(getObjectResponse, StandardCharsets.UTF_8));
        String topicBOffsetWm = reader.readLine();
        reader.close();

        // assert that last watermark upload corresponds to an offset that is less than the number of records sent but not too far behind
        assertTrue(Long.parseLong(topicAOffsetWm) < numRecordsSent, "numRecordsSent was " + numRecordsSent + " but offset.wm was " + Long.parseLong(topicAOffsetWm));
        assertTrue(Long.parseLong(topicAOffsetWm) > numRecordsSent * 0.8, "numRecordsSent was " + numRecordsSent + " but offset.wm was "  + Long.parseLong(topicAOffsetWm));

        assertTrue(Long.parseLong(topicBOffsetWm) < numRecordsSent, "numRecordsSent was " + numRecordsSent + " but offset.wm was " + Long.parseLong(topicBOffsetWm));
        assertTrue(Long.parseLong(topicBOffsetWm) > numRecordsSent * 0.8, "numRecordsSent was " + numRecordsSent + " but offset.wm was "  + Long.parseLong(topicBOffsetWm));

        deleteTopicAndVerify(sharedKafkaTestResource, TEST_TOPIC_A);
        deleteTopicAndVerify(sharedKafkaTestResource, TEST_TOPIC_B);
    }

    /**
     * This method is disabled by default. Enable it to write test log segments to disk with specified record format.
     * @throws InterruptedException
     */
    @Test
    @Disabled
    void writeTestLogSegments() throws InterruptedException {
        long now = System.currentTimeMillis();
        long numRecordsSent = 0;
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, sharedKafkaTestResource.getKafkaConnectString());
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        KafkaProducer<byte[], byte[]> kafkaProducer = new KafkaProducer<>(props);
        while (System.currentTimeMillis() - now < 10000) {
            RecordHeaders recordHeaders = new RecordHeaders();
            recordHeaders.add(new RecordHeader("header1", "header1-val".getBytes()));
            recordHeaders.add(new RecordHeader("header2", "header2-val".getBytes()));
            /*
             * Record looks like:
             * Headers: {header1=header1-val, header2=header2-val}
             * Key: (int)
             * Value: val-(int)
             */
            ProducerRecord<byte[], byte[]> producerRecord = new ProducerRecord<>(TEST_TOPIC_A, 0, String.valueOf(numRecordsSent).getBytes(), ("val-" + numRecordsSent).getBytes(), recordHeaders);
            kafkaProducer.send(producerRecord);
            numRecordsSent++;
        }
        Thread.sleep(30000);
    }
}


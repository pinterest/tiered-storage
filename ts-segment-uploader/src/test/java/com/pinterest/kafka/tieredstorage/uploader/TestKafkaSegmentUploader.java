package com.pinterest.kafka.tieredstorage.uploader;

import com.pinterest.kafka.tieredstorage.common.CommonTestUtils;
import com.pinterest.kafka.tieredstorage.common.discovery.s3.S3StorageServiceEndpoint;
import com.pinterest.kafka.tieredstorage.common.discovery.s3.S3StorageServiceEndpointProvider;
import com.pinterest.kafka.tieredstorage.uploader.management.S3SegmentManager;
import com.salesforce.kafka.test.junit5.SharedKafkaTestResource;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.common.TopicPartition;
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
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutionException;

import static com.pinterest.kafka.tieredstorage.uploader.TestBase.TEST_TOPIC_B;
import static com.pinterest.kafka.tieredstorage.uploader.TestBase.createTopicAndVerify;
import static com.pinterest.kafka.tieredstorage.uploader.TestBase.deleteTopicAndVerify;
import static com.pinterest.kafka.tieredstorage.uploader.TestBase.overrideS3AsyncClientForFileUploader;
import static com.pinterest.kafka.tieredstorage.uploader.TestBase.overrideS3ClientForFileDownloader;
import static com.pinterest.kafka.tieredstorage.uploader.TestBase.sendTestData;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestKafkaSegmentUploader extends TestS3ContainerBase {
    @RegisterExtension
    private static final SharedKafkaTestResource sharedKafkaTestResource = new SharedKafkaTestResource()
            .withBrokerProperty("log.segment.bytes", "30000")
            .withBrokerProperty("log.index.interval.bytes", "100")
            .withBrokerProperty("log.segment.delete.delay.ms", "5000");
    private static AdminClient adminClient;
    private KafkaSegmentUploader uploader;
    private KafkaEnvironmentProvider environmentProvider;

    @Override
    @BeforeEach
    public void setup() throws Exception {
        super.setup();
        environmentProvider = TestBase.createTestEnvironmentProvider(sharedKafkaTestResource);
        createTopicAndVerify(sharedKafkaTestResource, TEST_TOPIC_A, 3);
        createTopicAndVerify(sharedKafkaTestResource, TEST_TOPIC_B, 6);
        startSegmentUploaderThread();
    }

    @Override
    @AfterEach
    public void tearDown() throws IOException, InterruptedException, ExecutionException {
        uploader.stop();
        DirectoryTreeWatcher.unsetLeadershipWatcher();
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

    private void startSegmentUploaderThread() throws Exception {
        String configDirectory = "src/test/resources";
        overrideS3ClientForFileDownloader(s3Client);
        overrideS3AsyncClientForFileUploader(s3AsyncClient);
        S3SegmentManager.setS3Client(s3Client);
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
        CommonTestUtils.writeExpectedRecordFormatTestData(CommonTestUtils.RecordContentType.TIERED_STORAGE, sharedKafkaTestResource, TEST_TOPIC_A, 0, 15000);
        Thread.sleep(30000);    // sleep for 30 seconds to allow the uploader to upload the segments
    }
}


package com.pinterest.kafka.tieredstorage.uploader;

import com.pinterest.kafka.tieredstorage.common.discovery.s3.MockS3StorageServiceEndpointProvider;
import com.pinterest.kafka.tieredstorage.common.discovery.s3.S3StorageServiceEndpoint;
import com.pinterest.kafka.tieredstorage.common.discovery.s3.S3StorageServiceEndpointProvider;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.exception.ApiCallTimeoutException;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;

import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Paths;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.pinterest.kafka.tieredstorage.uploader.TestBase.TEST_DATA_LOG_DIRECTORY_PATH;
import static com.pinterest.kafka.tieredstorage.uploader.TestBase.TEST_TOPIC_B;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestMultiThreadedS3FileUploader extends TestS3ContainerBase {
    protected MultiThreadedS3FileUploader s3FileUploader;
    private S3StorageServiceEndpointProvider endpointProvider;
    private KafkaEnvironmentProvider environmentProvider;
    private SegmentUploaderConfiguration config;

    @BeforeEach
    public void setup() throws Exception {
        super.setup();
        // NO-OP
        environmentProvider = new KafkaEnvironmentProvider() {
            @Override
            public void load() {
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
                // NO-OP
                return null;
            }

            @Override
            public String logDir() {
                // NO-OP
                return null;
            }
        };
        endpointProvider = new MockS3StorageServiceEndpointProvider();
        endpointProvider.initialize(TEST_CLUSTER);
        MultiThreadedS3FileUploader.overrideS3Client(s3AsyncClient);
        config = getSegmentUploaderConfiguration(TEST_CLUSTER);
        s3FileUploader = new MultiThreadedS3FileUploader(endpointProvider, config, environmentProvider);
    }

    /**
     * Test a simple upload to desired S3 endpoint
     * @throws InterruptedException
     */
    @Test
    void testSimpleUpload() throws InterruptedException, ExecutionException {
        TopicPartition tp = new TopicPartition(TEST_TOPIC_A, 0);
        String offset = "00000000000000000000";
        DirectoryTreeWatcher.UploadTask uploadTask = new DirectoryTreeWatcher.UploadTask(
                tp,
                offset,
                String.format("%s.index", offset),
                TEST_DATA_LOG_DIRECTORY_PATH.resolve(Paths.get(String.format("%s.index", offset)))
        );
        s3FileUploader.uploadFile(uploadTask, null);
        Thread.sleep(1000); // wait for upload to complete

        S3StorageServiceEndpoint endpoint = endpointProvider.getStorageServiceEndpointBuilderForTopic(TEST_TOPIC_A)
                .setPrefixEntropyNumBits(config.getS3PrefixEntropyBits())
                .setTopicPartition(tp)
                .build();

        ListObjectsV2Response response = getListObjectsV2Response(
                S3_BUCKET,
                endpoint.getFullPrefix(),
                s3AsyncClient
        );

        assertEquals(1, response.contents().size());
        assertEquals(endpoint.getFullPrefix() + "/" + String.format("%s.index", offset), response.contents().iterator().next().key());

        uploadTask = new DirectoryTreeWatcher.UploadTask(
                tp,
                offset,
                String.format("%s.wm", offset),
                TEST_DATA_LOG_DIRECTORY_PATH.resolve(Paths.get(String.format("%s.wm", offset)))
        );
        s3FileUploader.uploadFile(uploadTask, null);
        Thread.sleep(1000);

        response = getListObjectsV2Response(
                S3_BUCKET,
                endpoint.getFullPrefix(),
                s3AsyncClient
        );

        assertEquals(2, response.contents().size());
        AtomicBoolean offsetWatermarkPresent = new AtomicBoolean(false);
        response.contents().forEach(c -> {
            if (c.key().equals(endpoint.getFullPrefix() + "/offset.wm"))
                offsetWatermarkPresent.set(true);
        });
        assertTrue(offsetWatermarkPresent.get());
    }

    /**
     * Ensure that a NoSuchFileException exception is thrown when trying to upload a non-existent file
     */
    @Test
    void testNonExistentUpload() {
        TopicPartition tp = new TopicPartition(TEST_TOPIC_A, 0);
        String offset = "00000000000000000003";

        DirectoryTreeWatcher.UploadTask uploadTask = new DirectoryTreeWatcher.UploadTask(
                tp,
                offset,
                String.format("%s.index", offset),
                TEST_DATA_LOG_DIRECTORY_PATH.resolve(Paths.get(String.format("%s.index", offset)))
        );

        s3FileUploader.uploadFile(uploadTask, new S3UploadCallback() {
            @Override
            public void onCompletion(DirectoryTreeWatcher.UploadTask uploadTask, long totalTimeMs, Throwable throwable, int statusCode) {
                assertNotNull(throwable);
                assertTrue(Utils.isAssignableFromRecursive(throwable, NoSuchFileException.class));
            }
        });
    }

    /**
     * Ensure that an upload that times out returns the correct error code and exception
     */
    @Test
    void testTimeoutUpload() throws IOException {
        // override s3AsyncClient to have a very short timeout
        MultiThreadedS3FileUploader.overrideS3Client(getS3AsyncClientWithCustomApiCallTimeout(1L));

        // upload a log file
        TopicPartition tp = new TopicPartition(TEST_TOPIC_B, 0);
        String offset = "00000000000000000000";

        DirectoryTreeWatcher.UploadTask uploadTask = new DirectoryTreeWatcher.UploadTask(
                tp,
                offset,
                String.format("%s.log", offset),
                TEST_DATA_LOG_DIRECTORY_PATH.resolve(Paths.get(String.format("%s.log", offset)))
        );

        s3FileUploader.uploadFile(uploadTask, new S3UploadCallback() {
            @Override
            public void onCompletion(DirectoryTreeWatcher.UploadTask uploadTask, long totalTimeMs, Throwable throwable, int statusCode) {
                assertTrue(Utils.isAssignableFromRecursive(throwable, ApiCallTimeoutException.class));
                assertEquals(statusCode, MultiThreadedS3FileUploader.UPLOAD_TIMEOUT_ERROR_CODE);
            }
        });

        // override s3AsyncClient back to original
        MultiThreadedS3FileUploader.overrideS3Client(s3AsyncClient);
    }
}

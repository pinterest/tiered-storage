package com.pinterest.kafka.tieredstorage.uploader;

import com.pinterest.kafka.tieredstorage.common.discovery.StorageServiceEndpointProvider;
import com.pinterest.kafka.tieredstorage.common.discovery.s3.MockS3StorageServiceEndpointProvider;
import com.pinterest.kafka.tieredstorage.uploader.dlq.DeadLetterQueueHandler;
import com.pinterest.kafka.tieredstorage.uploader.leadership.ZookeeperLeadershipWatcher;
import com.salesforce.kafka.test.junit5.SharedKafkaTestResource;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.WatchKey;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestDirectoryTreeWatcher extends TestBase {
    @RegisterExtension
    private static final SharedKafkaTestResource sharedKafkaTestResource = new SharedKafkaTestResource()
            .withBrokerProperty("log.segment.bytes", "30000")
            .withBrokerProperty("log.segment.delete.delay.ms", "5000");
    private DirectoryTreeWatcher directoryTreeWatcher;
    private KafkaEnvironmentProvider environmentProvider;
    private static AdminClient adminClient;
    private SegmentUploaderConfiguration config;

    @BeforeEach
    @Override
    public void setup() throws Exception {
        super.setup();

        // environment provider setup
        environmentProvider = createTestEnvironmentProvider(sharedKafkaTestResource);
        environmentProvider.load();

        // override s3 client
        overrideS3ClientForFileDownloader(s3Client);
        overrideS3AsyncClientForFileUploader(s3AsyncClient);

        // endpoint provider setup
        MockS3StorageServiceEndpointProvider endpointProvider = new MockS3StorageServiceEndpointProvider();
        endpointProvider.initialize(TEST_CLUSTER);

        // s3 uploader setup
        config = getSegmentUploaderConfiguration(TEST_CLUSTER);
        S3FileUploader s3FileUploader = new MultiThreadedS3FileUploader(endpointProvider, config, environmentProvider);

        // create topic
        createTopicAndVerify(sharedKafkaTestResource, TEST_TOPIC_A,  3);

        // start directory tree watcher
        directoryTreeWatcher = new DirectoryTreeWatcher(s3FileUploader, config, environmentProvider);
        DirectoryTreeWatcher.setLeadershipWatcher(new ZookeeperLeadershipWatcher(directoryTreeWatcher, config, environmentProvider));
        directoryTreeWatcher.initialize();
        directoryTreeWatcher.start();
    }

    @AfterEach
    @Override
    public void tearDown() throws IOException, ExecutionException, InterruptedException {
        directoryTreeWatcher.stop();
        DirectoryTreeWatcher.unsetLeadershipWatcher();
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

    /**
     * Test the watch key map for a single broker when increasing partition count for watched topics
     * @throws InterruptedException
     */
    @Test
    void testWatchKeyMapSingleBroker() throws InterruptedException {
        Map<Path, WatchKey> map = directoryTreeWatcher.getWatchKeyMap();
        // 3 paths so far for 3 partitions of TEST_TOPIC_A
        assertEquals(3, map.size());
        assertTrue(map.containsKey(Paths.get(String.format("%s/%s", environmentProvider.logDir(), new TopicPartition(TEST_TOPIC_A, 0)))));
        assertTrue(map.containsKey(Paths.get(String.format("%s/%s", environmentProvider.logDir(), new TopicPartition(TEST_TOPIC_A, 1)))));
        assertTrue(map.containsKey(Paths.get(String.format("%s/%s", environmentProvider.logDir(), new TopicPartition(TEST_TOPIC_A, 2)))));

        // test partition increase for existing topic
        increasePartitionsAndVerify(sharedKafkaTestResource, TEST_TOPIC_A, 12);
        Thread.sleep(10000);
        map = directoryTreeWatcher.getWatchKeyMap();
        assertEquals(12, map.size());
        for (int i = 0; i < 11; i++) {
            // assert every path is now watched
            assertTrue(map.containsKey(Paths.get(String.format("%s/%s", environmentProvider.logDir(), new TopicPartition(TEST_TOPIC_A, i)))));
        }

        // test topic creation
        createTopicAndVerify(sharedKafkaTestResource, TEST_TOPIC_B, 6);
        Thread.sleep(10000); // wait a bit for watcher to register new topic
        map = directoryTreeWatcher.getWatchKeyMap();
        assertEquals(18, map.size());
        for (int i = 0; i < 11; i++) {
            // assert every path is now watched
            assertTrue(map.containsKey(Paths.get(String.format("%s/%s", environmentProvider.logDir(), new TopicPartition(TEST_TOPIC_A, i)))));
        }
        for (int i = 0; i < 5; i++) {
            assertTrue(map.containsKey(Paths.get(String.format("%s/%s", environmentProvider.logDir(), new TopicPartition(TEST_TOPIC_B, i)))));
        }

        // test topic deletion
        deleteTopicAndVerify(sharedKafkaTestResource, TEST_TOPIC_A);
        Thread.sleep(15000);
        map = directoryTreeWatcher.getWatchKeyMap();
        assertEquals(6, map.size());
        for (int i = 0; i < 11; i++) {
            // assert TEST_TOPIC_A paths are unwatched
            assertFalse(map.containsKey(Paths.get(String.format("%s/%s", environmentProvider.logDir(), new TopicPartition(TEST_TOPIC_A, i)))));
        }
        for (int i = 0; i < 5; i++) {
            // assert that TEST_TOPIC_B paths are unaffected
            assertTrue(map.containsKey(Paths.get(String.format("%s/%s", environmentProvider.logDir(), new TopicPartition(TEST_TOPIC_B, i)))));
        }

        deleteTopicAndVerify(sharedKafkaTestResource, TEST_TOPIC_B);
    }

    @Test
    void testExponentialBackoffRetries() throws InterruptedException {
        File[] testLogDirectoryFiles = TEST_DATA_LOG_DIRECTORY_PATH.toFile().listFiles();
        if (testLogDirectoryFiles != null && testLogDirectoryFiles.length > 0) {
            // this should always be the case
            File firstFile = testLogDirectoryFiles[0];
            String offset = firstFile.getName().split("\\.")[0];
            DirectoryTreeWatcher.UploadTask uploadTask = new DirectoryTreeWatcher.UploadTask(new TopicPartition(TEST_TOPIC_A, 0), offset, firstFile.getName(), TEST_DATA_LOG_DIRECTORY_PATH.resolve(firstFile.getName()));
            assertEquals(-1, uploadTask.getNextRetryNotBeforeTimestamp());

            List<Long> waitingTimes = new ArrayList<>();

            for (int retryNum = 1; retryNum <= 6; retryNum++) {
                uploadTask.retry();
                long startWaitTimestamp = System.currentTimeMillis();
                while (System.currentTimeMillis() < uploadTask.getNextRetryNotBeforeTimestamp()) {
                    Thread.sleep(100);
                }
                long waitingTime = System.currentTimeMillis() - startWaitTimestamp;
                Thread.sleep(500);
                assertTrue(uploadTask.isReadyForUpload());
                waitingTimes.add(waitingTime);
            }

            long prevDifference = -1;
            for (int i = 1; i < waitingTimes.size(); i++) {
                long diff = waitingTimes.get(i) - waitingTimes.get(i - 1);
                if (prevDifference != -1) {
                    assertTrue(diff > prevDifference);
                }
                prevDifference = diff;
            }
        }
    }

    /**
     * Test that offset.wm files don't retry on upload failure and are not sent to DLQ
     */
    @Test
    void testOffsetWmFilesNoRetryAndNoDlq() throws Exception {
        // override s3AsyncClient to have a very short timeout to force failures
        MultiThreadedS3FileUploader.overrideS3Client(getS3AsyncClientWithCustomApiCallTimeout(1L));

        Map<DirectoryTreeWatcher.UploadTask, Integer> taskToSendMap = new ConcurrentHashMap<>();

        // set DLQ to track failed uploads
        directoryTreeWatcher.setDeadLetterQueueHandler(new DeadLetterQueueHandler(config) {
            @Override
            protected void validateConfig() {
                // no-op
            }

            @Override
            public Future<Boolean> send(DirectoryTreeWatcher.UploadTask uploadTask, Throwable throwable) {
                taskToSendMap.computeIfAbsent(uploadTask, k -> 0);
                taskToSendMap.put(uploadTask, taskToSendMap.get(uploadTask) + 1);
                return CompletableFuture.completedFuture(true);
            }

            @Override
            public Collection<DirectoryTreeWatcher.UploadTask> poll() {
                // no-op
                return null;
            }
        });

        StorageServiceEndpointProvider endpointProvider = new MockS3StorageServiceEndpointProvider();
        endpointProvider.initialize(TEST_CLUSTER);

        MultiThreadedS3FileUploader uploader = new MultiThreadedS3FileUploader(endpointProvider, config, environmentProvider);

        // create an actual offset.wm file for testing
        TopicPartition tp = new TopicPartition(TEST_TOPIC_A, 0);
        String offset = "00000000000000000000";
        
        // Create a temporary offset.wm file
        Path tempWatermarkFile = TEST_DATA_LOG_DIRECTORY_PATH.resolve("test_offset.wm");
        try {
            Files.write(tempWatermarkFile, offset.getBytes());
        } catch (IOException e) {
            throw new RuntimeException("Failed to create test watermark file", e);
        }
        
        DirectoryTreeWatcher.UploadTask watermarkUploadTask = new DirectoryTreeWatcher.UploadTask(
                tp,
                offset,
                "test_offset.wm",
                tempWatermarkFile
        );

        S3UploadCallback callback = new S3UploadCallback() {
            @Override
            public void onCompletion(DirectoryTreeWatcher.UploadTask uploadTask, long totalTimeMs, Throwable throwable, int statusCode) {
                assertNotNull(throwable);
                assertEquals(MultiThreadedS3FileUploader.UPLOAD_TIMEOUT_ERROR_CODE, statusCode);
                assertTrue(Utils.isAssignableFromRecursive(throwable, CompletionException.class));
                directoryTreeWatcher.handleUploadCallback(uploadTask, totalTimeMs, throwable, statusCode);
            }
        };

        uploader.uploadFile(watermarkUploadTask, callback);

        Thread.sleep(5000);    // wait for the upload to fail and be handled

        // verify that the watermark file has 0 retries and was NOT sent to DLQ
        assertEquals(0, watermarkUploadTask.getTries());
        assertEquals(0, taskToSendMap.size()); // DLQ should not receive any tasks
        assertFalse(taskToSendMap.containsKey(watermarkUploadTask)); // specifically verify watermark task not in DLQ

        // clean up the temporary test file
        try {
            Files.deleteIfExists(tempWatermarkFile);
        } catch (IOException e) {
            // Log warning but don't fail the test
            System.err.println("Failed to clean up test watermark file: " + e.getMessage());
        }

        // override the s3AsyncClient back to original
        MultiThreadedS3FileUploader.overrideS3Client(s3AsyncClient);
    }

    /**
     * Test that regular files (non-watermark) still go to DLQ after retry exhaustion
     */
    @Test
    void testRegularFilesStillUseDlq() throws Exception {
        // override s3AsyncClient to have a very short timeout to force failures
        MultiThreadedS3FileUploader.overrideS3Client(getS3AsyncClientWithCustomApiCallTimeout(1L));

        Map<DirectoryTreeWatcher.UploadTask, Integer> taskToSendMap = new ConcurrentHashMap<>();

        // set DLQ to track failed uploads
        directoryTreeWatcher.setDeadLetterQueueHandler(new DeadLetterQueueHandler(config) {
            @Override
            protected void validateConfig() {
                // no-op
            }

            @Override
            public Future<Boolean> send(DirectoryTreeWatcher.UploadTask uploadTask, Throwable throwable) {
                taskToSendMap.computeIfAbsent(uploadTask, k -> 0);
                taskToSendMap.put(uploadTask, taskToSendMap.get(uploadTask) + 1);
                return CompletableFuture.completedFuture(true);
            }

            @Override
            public Collection<DirectoryTreeWatcher.UploadTask> poll() {
                // no-op
                return null;
            }
        });

        StorageServiceEndpointProvider endpointProvider = new MockS3StorageServiceEndpointProvider();
        endpointProvider.initialize(TEST_CLUSTER);

        MultiThreadedS3FileUploader uploader = new MultiThreadedS3FileUploader(endpointProvider, config, environmentProvider);

        // create a regular .log file upload task
        TopicPartition tp = new TopicPartition(TEST_TOPIC_A, 0);
        String offset = "00000000000000000000";
        
        DirectoryTreeWatcher.UploadTask logUploadTask = new DirectoryTreeWatcher.UploadTask(
                tp,
                offset,
                String.format("%s.log", offset),
                TEST_DATA_LOG_DIRECTORY_PATH.resolve(Paths.get(String.format("%s.log", offset)))
        );

        S3UploadCallback callback = new S3UploadCallback() {
            @Override
            public void onCompletion(DirectoryTreeWatcher.UploadTask uploadTask, long totalTimeMs, Throwable throwable, int statusCode) {
                assertNotNull(throwable);
                assertEquals(MultiThreadedS3FileUploader.UPLOAD_TIMEOUT_ERROR_CODE, statusCode);
                assertTrue(Utils.isAssignableFromRecursive(throwable, CompletionException.class));
                directoryTreeWatcher.handleUploadCallback(uploadTask, totalTimeMs, throwable, statusCode);
            }
        };

        uploader.uploadFile(logUploadTask, callback);

        Thread.sleep(5000);    // wait for retries to exhaust and DLQ handling

        // verify that the log file went through retries and was sent to DLQ
        assertEquals(config.getUploadMaxRetries(), logUploadTask.getTries());
        assertEquals(1, taskToSendMap.size()); // DLQ should receive the log file
        assertTrue(taskToSendMap.containsKey(logUploadTask)); // verify log task was sent to DLQ
        assertEquals(1, taskToSendMap.get(logUploadTask));

        // override the s3AsyncClient back to original
        MultiThreadedS3FileUploader.overrideS3Client(s3AsyncClient);
    }

    /**
     * Test that retry exhaustion calls the DLQ handler
     */
    @Test
    void testRetryExhaustion() throws Exception {
        // override s3AsyncClient to have a very short timeout
        MultiThreadedS3FileUploader.overrideS3Client(getS3AsyncClientWithCustomApiCallTimeout(1L));

        Map<DirectoryTreeWatcher.UploadTask, Integer> taskToSendMap = new ConcurrentHashMap<>();

        // set DLQ
        directoryTreeWatcher.setDeadLetterQueueHandler(new DeadLetterQueueHandler(config) {
            @Override
            protected void validateConfig() {
                // no-op
            }

            @Override
            public Future<Boolean> send(DirectoryTreeWatcher.UploadTask uploadTask, Throwable throwable) {
                taskToSendMap.computeIfAbsent(uploadTask, k -> 0);
                taskToSendMap.put(uploadTask, taskToSendMap.get(uploadTask) + 1);
                return CompletableFuture.completedFuture(true);
            }

            @Override
            public Collection<DirectoryTreeWatcher.UploadTask> poll() {
                // no-op
                return null;
            }
        });

        StorageServiceEndpointProvider endpointProvider = new MockS3StorageServiceEndpointProvider();
        endpointProvider.initialize(TEST_CLUSTER);

        MultiThreadedS3FileUploader uploader = new MultiThreadedS3FileUploader(endpointProvider, config, environmentProvider);

        // upload two log files
        TopicPartition tp = new TopicPartition(TEST_TOPIC_A, 0);
        String offset = "00000000000000000000";
        String offset2 = "00000000000000000294";
        String nonExistentOffset = "00000000000000000295";

        DirectoryTreeWatcher.UploadTask uploadTask = new DirectoryTreeWatcher.UploadTask(
                tp,
                offset,
                String.format("%s.log", offset),
                TEST_DATA_LOG_DIRECTORY_PATH.resolve(Paths.get(String.format("%s.log", offset)))
        );
        DirectoryTreeWatcher.UploadTask uploadTask2 = new DirectoryTreeWatcher.UploadTask(
                tp,
                offset2,
                String.format("%s.log", offset2),
                TEST_DATA_LOG_DIRECTORY_PATH.resolve(Paths.get(String.format("%s.log", offset2)))
        );
        DirectoryTreeWatcher.UploadTask nonExistentUploadTask = new DirectoryTreeWatcher.UploadTask(
                tp,
                nonExistentOffset,
                String.format("%s.log", nonExistentOffset),
                TEST_DATA_LOG_DIRECTORY_PATH.resolve(Paths.get(String.format("%s.log", nonExistentOffset)))
        );

        S3UploadCallback callback = new S3UploadCallback() {
            @Override
            public void onCompletion(DirectoryTreeWatcher.UploadTask uploadTask, long totalTimeMs, Throwable throwable, int statusCode) {
                assertNotNull(throwable);
                if (uploadTask == nonExistentUploadTask) {
                    assertEquals(MultiThreadedS3FileUploader.UPLOAD_FILE_NOT_FOUND_ERROR_CODE, statusCode);
                    assertTrue(Utils.isAssignableFromRecursive(throwable, NoSuchFileException.class));
                } else {
                    assertEquals(MultiThreadedS3FileUploader.UPLOAD_TIMEOUT_ERROR_CODE, statusCode);
                    assertTrue(Utils.isAssignableFromRecursive(throwable, CompletionException.class));
                }
                directoryTreeWatcher.handleUploadCallback(uploadTask, totalTimeMs, throwable, statusCode);
            }
        };

        uploader.uploadFile(uploadTask, callback);
        uploader.uploadFile(uploadTask2, callback);
        uploader.uploadFile(nonExistentUploadTask, callback);

        Thread.sleep(15000);    // wait for retries to exhaust

        assertEquals(config.getUploadMaxRetries(), uploadTask.getTries());
        assertEquals(3, taskToSendMap.size());
        assertEquals(1, taskToSendMap.get(uploadTask));
        assertEquals(1, taskToSendMap.get(uploadTask2));
        assertEquals(1, taskToSendMap.get(nonExistentUploadTask));

        // override the s3AsyncClient back to original
        MultiThreadedS3FileUploader.overrideS3Client(s3AsyncClient);
    }

    private static void increasePartitionsAndVerify(SharedKafkaTestResource sharedKafkaTestResource, String topic, int newPartitionCount) throws InterruptedException {
        sharedKafkaTestResource.getKafkaTestUtils().getAdminClient().createPartitions(Collections.singletonMap(topic, NewPartitions.increaseTo(newPartitionCount)));
        while (sharedKafkaTestResource.getKafkaTestUtils().describeTopic(topic).partitions().size() != newPartitionCount)
            Thread.sleep(200);
    }

}

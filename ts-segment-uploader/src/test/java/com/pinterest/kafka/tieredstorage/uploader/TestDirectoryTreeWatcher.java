package com.pinterest.kafka.tieredstorage.uploader;

import com.pinterest.kafka.tieredstorage.common.discovery.s3.MockS3StorageServiceEndpointProvider;
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
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.WatchKey;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestDirectoryTreeWatcher extends TestBase {
    @RegisterExtension
    private static final SharedKafkaTestResource sharedKafkaTestResource = new SharedKafkaTestResource()
            .withBrokerProperty("log.segment.bytes", "30000")
            .withBrokerProperty("log.segment.delete.delay.ms", "5000");
    private static final String TEST_TOPIC_B = "test_topic_b";
    private DirectoryTreeWatcher directoryTreeWatcher;
    private KafkaEnvironmentProvider environmentProvider;
    private static AdminClient adminClient;

    @BeforeEach
    @Override
    public void setup() throws Exception {
        super.setup();

        // environment provider setup
        environmentProvider = createTestEnvironmentProvider(sharedKafkaTestResource);
        environmentProvider.load();

        // override s3 client
        overrideS3ClientForFileUploaderAndDownloader(s3Client);

        // endpoint provider setup
        MockS3StorageServiceEndpointProvider endpointProvider = new MockS3StorageServiceEndpointProvider();
        endpointProvider.initialize(TEST_CLUSTER);

        // s3 uploader setup
        SegmentUploaderConfiguration config = new SegmentUploaderConfiguration("src/test/resources", TEST_CLUSTER);
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

    private static void increasePartitionsAndVerify(SharedKafkaTestResource sharedKafkaTestResource, String topic, int newPartitionCount) throws InterruptedException {
        sharedKafkaTestResource.getKafkaTestUtils().getAdminClient().createPartitions(Collections.singletonMap(topic, NewPartitions.increaseTo(newPartitionCount)));
        while (sharedKafkaTestResource.getKafkaTestUtils().describeTopic(topic).partitions().size() != newPartitionCount)
            Thread.sleep(200);
    }

}

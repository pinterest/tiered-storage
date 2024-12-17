package com.pinterest.kafka.tieredstorage.uploader;

import com.pinterest.kafka.tieredstorage.common.discovery.s3.MockS3StorageServiceEndpointProvider;
import com.pinterest.kafka.tieredstorage.uploader.leadership.ZookeeperLeadershipWatcher;
import com.salesforce.kafka.test.junit5.SharedKafkaTestResource;
import org.apache.kafka.clients.admin.AdminClient;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.WatchKey;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestDirectoryTreeWatcherMultiBroker extends TestBase {

    @RegisterExtension
    private static final SharedKafkaTestResource sharedKafkaTestResource = new SharedKafkaTestResource()
            .withBrokerProperty("log.segment.bytes", "30000")
            .withBrokerProperty("log.segment.delete.delay.ms", "5000")
            .withBrokers(2);
    private DirectoryTreeWatcher directoryTreeWatcher;
    private static AdminClient adminClient;

    @BeforeEach
    @Override
    public void setup() throws Exception {
        super.setup();

        // environment provider setup
        KafkaEnvironmentProvider environmentProvider = createTestEnvironmentProvider(sharedKafkaTestResource);
        environmentProvider.load();

        // override s3 client
        overrideS3ClientForFileDownloader(s3Client);
        overrideS3AsyncClientForFileUploader(s3AsyncClient);

        // endpoint provider setup
        MockS3StorageServiceEndpointProvider endpointProvider = new MockS3StorageServiceEndpointProvider();
        endpointProvider.initialize(TEST_CLUSTER);

        // s3 uploader setup
        SegmentUploaderConfiguration config = getSegmentUploaderConfiguration(TEST_CLUSTER);
        S3FileUploader s3FileUploader = new MultiThreadedS3FileUploader(endpointProvider, config, environmentProvider);

        // create topic with replicationFactor = 2
        createTopicAndVerify(sharedKafkaTestResource, TEST_TOPIC_A,  6, (short) 2);

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
     * Test the watch key map when there are 2 brokers, from the perspective of broker 1.
     * Initially broker 1 should host partitions 0-2 and broker 2 should host partitions 3-5.
     * After broker 1 is shut down, broker 2 should host all 6 partitions, and broker 1 should host none.
     * After broker 1 is started again, broker 1 should host partitions 0-2.
     * After broker 2 is shut down, broker 1 should host all 6 partitions.
     * @throws Exception
     */
    @Test
    void testWatchKeyMapMultiBroker() throws Exception {
        Map<Path, WatchKey> map = directoryTreeWatcher.getWatchKeyMap();
        // 3 partitions watched per broker
        assertEquals(3, map.size());

        // shut down broker 1
        sharedKafkaTestResource.getKafkaBrokers().getBrokerById(1).stop();
        Thread.sleep(5000);

        // broker 1 should lead 0 partitions
        map = directoryTreeWatcher.getWatchKeyMap();
        assertEquals(0, map.size());

        // start broker 1
        sharedKafkaTestResource.getKafkaBrokers().getBrokerById(1).start();

        // reassign partitions 0-2 back to broker 1
        Map<String, Map<Integer, List<Integer>>> assignmentMap = new HashMap<>();
        assignmentMap.put(TEST_TOPIC_A, new HashMap<>());
        assignmentMap.get(TEST_TOPIC_A).put(0, List.of(1));
        assignmentMap.get(TEST_TOPIC_A).put(1, List.of(1));
        assignmentMap.get(TEST_TOPIC_A).put(2, List.of(1));
        reassignPartitions(sharedKafkaTestResource, assignmentMap);

        Thread.sleep(10000);

        // broker 1 should lead 3 partitions now
        map = directoryTreeWatcher.getWatchKeyMap();
        assertEquals(3, map.size());

        // shut down broker 2
        sharedKafkaTestResource.getKafkaBrokers().getBrokerById(2).stop();
        Thread.sleep(5000);

        // broker 1 should lead all 6 partitions now
        map = directoryTreeWatcher.getWatchKeyMap();
        assertEquals(6, map.size());

        deleteTopicAndVerify(sharedKafkaTestResource, TEST_TOPIC_B);
    }

}

package com.pinterest.kafka.tieredstorage.uploader.leadership;

import com.pinterest.kafka.tieredstorage.common.discovery.s3.MockS3StorageServiceEndpointProvider;
import com.pinterest.kafka.tieredstorage.uploader.DirectoryTreeWatcher;
import com.pinterest.kafka.tieredstorage.uploader.KafkaEnvironmentProvider;
import com.pinterest.kafka.tieredstorage.uploader.SegmentUploaderConfiguration;
import com.pinterest.kafka.tieredstorage.uploader.TestBase;
import com.salesforce.kafka.test.junit5.SharedKafkaTestResource;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Test for {@link ZookeeperLeadershipWatcher}
 */
public class TestZookeeperLeadershipWatcher extends TestBase {

    @RegisterExtension
    private static final SharedKafkaTestResource sharedKafkaTestResource = new SharedKafkaTestResource()
            .withBrokerProperty("log.segment.bytes", "30000")
            .withBrokerProperty("log.segment.delete.delay.ms", "5000")
            .withBrokers(12);
    private static Set<TopicPartition> watchedTopicPartitions = new HashSet<>();
    private DirectoryTreeWatcher mockDirectoryTreeWatcher;
    private LeadershipWatcher zkLeadershipWatcher;

    @BeforeEach
    public void setup() throws Exception {

        // environment provider setup
        KafkaEnvironmentProvider environmentProvider = createTestEnvironmentProvider(sharedKafkaTestResource);
        environmentProvider.load();

        // endpoint provider setup
        MockS3StorageServiceEndpointProvider endpointProvider = new MockS3StorageServiceEndpointProvider();
        endpointProvider.initialize(TEST_CLUSTER);

        // s3 uploader setup
        SegmentUploaderConfiguration config = getSegmentUploaderConfiguration(TEST_CLUSTER);

        // start directory tree watcher
        mockDirectoryTreeWatcher = new TestLeadershipWatcher.MockDirectoryTreeWatcher(watchedTopicPartitions, config, environmentProvider);
        zkLeadershipWatcher = new ZookeeperLeadershipWatcher(mockDirectoryTreeWatcher, config, environmentProvider);
        DirectoryTreeWatcher.setLeadershipWatcher(zkLeadershipWatcher);
        zkLeadershipWatcher.initialize();
    }

    @AfterEach
    public void tearDown() throws InterruptedException {
        deleteTopicAndVerify(sharedKafkaTestResource, TEST_TOPIC_A);
        mockDirectoryTreeWatcher.stop();
    }

    /**
     * Test {@link ZookeeperLeadershipWatcher#queryCurrentLeadingPartitions()} method
     */
    @Test
    void testQueryCurrentLeadingPartitions() throws Exception {
        Set<TopicPartition> currentLeadingPartitions = zkLeadershipWatcher.queryCurrentLeadingPartitions();

        // currently no leading partitions
        assertEquals(0, currentLeadingPartitions.size());
        assertEquals(0, watchedTopicPartitions.size());

        // create topic with 1 partition led by each of the 12 brokers
        createTopicAndVerify(sharedKafkaTestResource, TEST_TOPIC_A,  12, (short) 2);

        currentLeadingPartitions = zkLeadershipWatcher.queryCurrentLeadingPartitions();
        assertEquals(1, currentLeadingPartitions.size());
        TopicPartition leadingPartition = currentLeadingPartitions.iterator().next();

        Map<String, Map<Integer, List<Integer>>> assignmentMap = new HashMap<>();
        assignmentMap.put(TEST_TOPIC_A, new HashMap<>());

        // reassign original leading partition to broker 2
        assignmentMap.get(TEST_TOPIC_A).put(leadingPartition.partition(), List.of(2));
        reassignPartitions(sharedKafkaTestResource, assignmentMap);
        Thread.sleep(2000); // wait for reassignment to complete

        currentLeadingPartitions = zkLeadershipWatcher.queryCurrentLeadingPartitions();
        assertEquals(0, currentLeadingPartitions.size());

        // broker 1 leads partitions 0-2
        assignmentMap.get(TEST_TOPIC_A).clear();
        assignmentMap.get(TEST_TOPIC_A).put(0, List.of(1));
        assignmentMap.get(TEST_TOPIC_A).put(1, List.of(1));
        assignmentMap.get(TEST_TOPIC_A).put(2, List.of(1));
        reassignPartitions(sharedKafkaTestResource, assignmentMap);
        Thread.sleep(2000); // wait for reassignment to complete

        currentLeadingPartitions = zkLeadershipWatcher.queryCurrentLeadingPartitions();
        assertEquals(3, currentLeadingPartitions.size());
        assertLeadingPartitions(Set.of(0, 1, 2), currentLeadingPartitions);

        // broker 1 leads partitions 0-2, 4-6, 8-11
        assignmentMap.get(TEST_TOPIC_A).clear();
        assignmentMap.get(TEST_TOPIC_A).put(4, List.of(1));
        assignmentMap.get(TEST_TOPIC_A).put(5, List.of(1));
        assignmentMap.get(TEST_TOPIC_A).put(6, List.of(1));
        assignmentMap.get(TEST_TOPIC_A).put(8, List.of(1));
        assignmentMap.get(TEST_TOPIC_A).put(9, List.of(1));
        assignmentMap.get(TEST_TOPIC_A).put(10, List.of(1));
        assignmentMap.get(TEST_TOPIC_A).put(11, List.of(1));
        reassignPartitions(sharedKafkaTestResource, assignmentMap);
        Thread.sleep(2000); // wait for reassignment to complete

        currentLeadingPartitions = zkLeadershipWatcher.queryCurrentLeadingPartitions();
        assertEquals(10, currentLeadingPartitions.size());
        assertLeadingPartitions(Set.of(0, 1, 2, 4, 5, 6, 8, 9, 10, 11), currentLeadingPartitions);

        // broker 1 leads partitions 4-11
        assignmentMap.get(TEST_TOPIC_A).clear();
        assignmentMap.get(TEST_TOPIC_A).put(0, List.of(2));
        assignmentMap.get(TEST_TOPIC_A).put(1, List.of(2));
        assignmentMap.get(TEST_TOPIC_A).put(2, List.of(2));
        assignmentMap.get(TEST_TOPIC_A).put(3, List.of(2));
        assignmentMap.get(TEST_TOPIC_A).put(4, List.of(1));
        assignmentMap.get(TEST_TOPIC_A).put(5, List.of(1));
        assignmentMap.get(TEST_TOPIC_A).put(6, List.of(1));
        assignmentMap.get(TEST_TOPIC_A).put(7, List.of(1));
        assignmentMap.get(TEST_TOPIC_A).put(8, List.of(1));
        assignmentMap.get(TEST_TOPIC_A).put(9, List.of(1));
        assignmentMap.get(TEST_TOPIC_A).put(10, List.of(1));
        assignmentMap.get(TEST_TOPIC_A).put(11, List.of(1));
        reassignPartitions(sharedKafkaTestResource, assignmentMap);
        Thread.sleep(2000); // wait for reassignment to complete

        currentLeadingPartitions = zkLeadershipWatcher.queryCurrentLeadingPartitions();
        assertEquals(8, currentLeadingPartitions.size());
        assertLeadingPartitions(Set.of(4, 5, 6, 7, 8, 9, 10, 11), currentLeadingPartitions);
    }

    private static void assertLeadingPartitions(Set<Integer> expectedPartitions, Set<TopicPartition> currentLeadingPartitions) {
        Set<TopicPartition> expectedLeadingPartitions = new HashSet<>();
        for (int partition : expectedPartitions) {
            expectedLeadingPartitions.add(new TopicPartition(TEST_TOPIC_A, partition));
        }
        assertEquals(expectedLeadingPartitions, currentLeadingPartitions);
    }
}

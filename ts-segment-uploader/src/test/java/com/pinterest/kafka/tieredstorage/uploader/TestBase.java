package com.pinterest.kafka.tieredstorage.uploader;

import com.adobe.testing.s3mock.junit5.S3MockExtension;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.salesforce.kafka.test.junit5.SharedKafkaTestResource;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.RegisterExtension;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class TestBase {
    @RegisterExtension
    protected static final S3MockExtension S3_MOCK =
            S3MockExtension.builder().silent().withSecureConnection(false).build();
    protected static final String S3_BUCKET = "test-bucket";
    protected static final String TEST_CLUSTER = "test-cluster-2";
    protected static final String TEST_TOPIC_A = "test_topic_a";
    protected static final Path TEST_DATA_LOG_DIRECTORY_PATH = Paths.get("src/test/resources/log_segments/test_topic_a-0");
    protected S3Client s3Client;

    @BeforeEach
    void setup() throws InterruptedException, IOException, KeeperException, ConfigurationException, ExecutionException, ClassNotFoundException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
        s3Client = S3Client.builder()
                .endpointOverride(URI.create(S3_MOCK.getServiceEndpoint()))
                .region(Region.US_EAST_1)
                .credentialsProvider(AnonymousCredentialsProvider.create())
                .build();
        s3Client.createBucket(CreateBucketRequest.builder().bucket(S3_BUCKET).build());
    }

    @AfterEach
    void tearDown() throws IOException, InterruptedException, ExecutionException {
        Thread.sleep(5000);
        s3Client.close();
    }

    protected static void sendTestData(SharedKafkaTestResource sharedKafkaTestResource, String topic, int partition, int numRecords) {
        sharedKafkaTestResource.getKafkaTestUtils().produceRecords(numRecords, topic, partition);
    }

    protected static void sendTestData(SharedKafkaTestResource sharedKafkaTestResource, String topic, int partition, Map<byte[], byte[]> keysAndValues) {
        sharedKafkaTestResource.getKafkaTestUtils().produceRecords(keysAndValues, topic, partition);
    }

    protected static void createTopicAndVerify(SharedKafkaTestResource testResource, String topic, int partitions)
            throws InterruptedException {
        testResource.getKafkaTestUtils().createTopic(topic, partitions, (short) 1);
        while (!testResource.getKafkaTestUtils().getTopicNames().contains(topic)) {
            Thread.sleep(200);
        }
    }

    protected static void createTopicAndVerify(SharedKafkaTestResource testResource, String topic, int partitions, short replicationFactor)
            throws InterruptedException {
        testResource.getKafkaTestUtils().createTopic(topic, partitions, replicationFactor);
        while (!testResource.getKafkaTestUtils().getTopicNames().contains(topic)) {
            Thread.sleep(200);
        }
    }

    public static void deleteTopicAndVerify(SharedKafkaTestResource testResource, String topic) throws InterruptedException {
        try {
            testResource.getKafkaTestUtils().getAdminClient().deleteTopics(Collections.singletonList(topic)).all().get();
        } catch (InterruptedException | ExecutionException e) {
        }
        while (testResource.getKafkaTestUtils().getTopicNames().contains(topic)) {
            Thread.sleep(200);
        }
    }

    private static void deleteDirectory(Path directoryPath) throws IOException {
        if (Files.exists(directoryPath)) {
            Files.walk(directoryPath)
                    .sorted((path1, path2) -> -path1.compareTo(path2)) // Reverse order for proper deletion
                    .forEach(path -> {
                        try {
                            Files.delete(path);
                        } catch (IOException e) {
                        }
                    });
        }
    }

    public static String getBrokerConfig(SharedKafkaTestResource sharedKafkaTestResource, int brokerId, String configName) throws ExecutionException, InterruptedException {
        ConfigResource configResource = new ConfigResource(ConfigResource.Type.BROKER, Integer.toString(brokerId));
        DescribeConfigsResult result = sharedKafkaTestResource.getKafkaTestUtils().getAdminClient().describeConfigs(Collections.singleton(configResource));
        return result.all().get().get(configResource).get(configName).value();
    }

    protected static ResponseInputStream<GetObjectResponse> getObjectResponse(String bucket, String key, S3Client s3Client) {
        return s3Client.getObject(GetObjectRequest.builder()
                .bucket(bucket)
                .key(key)
                .build()
        );
    }

    protected static ListObjectsV2Response getListObjectsV2Response(String bucket, String prefix, S3Client s3Client) {
        return s3Client.listObjectsV2(ListObjectsV2Request.builder().bucket(bucket).prefix(prefix).build());
    }

    protected static void reassignPartitions(SharedKafkaTestResource sharedKafkaTestResource, Map<String, Map<Integer, List<Integer>>> assignmentMap) throws IOException, InterruptedException, KeeperException {
        ZooKeeper zk = new ZooKeeper(sharedKafkaTestResource.getZookeeperConnectString(), 10000, null);
        String assignmentJson = assignmentMapToJson(assignmentMap);
        zk.create("/admin/reassign_partitions", assignmentJson.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zk.close();

        // wait for reassignment to complete
        boolean reassignmentComplete = false;
        while (!reassignmentComplete) {
            reassignmentComplete = assignmentMap.entrySet().stream().allMatch(e -> {
                String topic = e.getKey();
                return e.getValue().entrySet().stream().allMatch(e1 -> {
                    int partition = e1.getKey();
                    List<Integer> assignmentMapReplicas = e1.getValue();
                    List<Integer> actualReplicas = sharedKafkaTestResource.getKafkaTestUtils().describeTopic(topic).partitions().get(partition).replicas().stream().map(r -> r.id()).collect(Collectors.toList());
                    return assignmentMapReplicas.equals(actualReplicas);
                });
            });
        }
    }

    private static String assignmentMapToJson(Map<String, Map<Integer, List<Integer>>> assignmentMap) {
        Gson gson = new Gson();
        JsonObject obj = new JsonObject();
        JsonArray assignments = new JsonArray();
        obj.add("partitions", assignments);
        for (Map.Entry<String, Map<Integer, List<Integer>>> entry : assignmentMap.entrySet()) {
            String topic = entry.getKey();
            Map<Integer, List<Integer>> td = entry.getValue();
            for (Map.Entry<Integer, List<Integer>> topicPartitionInfo : td.entrySet()) {
                JsonObject assignmentEntry = new JsonObject();
                assignmentEntry.addProperty("topic", topic);
                assignmentEntry.addProperty("partition", topicPartitionInfo.getKey());
                JsonArray assignmentArray = new JsonArray();
                for (Integer brokerId : topicPartitionInfo.getValue()) {
                    assignmentArray.add(brokerId);
                }
                assignmentEntry.add("replicas", assignmentArray);
                assignments.add(assignmentEntry);
            }
        }
        return gson.toJson(obj);
    }
}

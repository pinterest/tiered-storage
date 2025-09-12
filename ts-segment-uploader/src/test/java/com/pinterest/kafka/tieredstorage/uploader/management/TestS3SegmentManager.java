package com.pinterest.kafka.tieredstorage.uploader.management;

import com.pinterest.kafka.tieredstorage.common.SegmentUtils;
import com.pinterest.kafka.tieredstorage.common.Utils;
import com.pinterest.kafka.tieredstorage.common.discovery.s3.MockS3StorageServiceEndpointProvider;
import com.pinterest.kafka.tieredstorage.common.discovery.s3.S3StorageServiceEndpoint;
import com.pinterest.kafka.tieredstorage.common.metadata.TimeIndex;
import com.pinterest.kafka.tieredstorage.common.metadata.TopicPartitionMetadata;
import com.pinterest.kafka.tieredstorage.uploader.KafkaEnvironmentProvider;
import com.pinterest.kafka.tieredstorage.uploader.SegmentUploaderConfiguration;
import com.pinterest.kafka.tieredstorage.uploader.TestBase;
import com.pinterest.kafka.tieredstorage.uploader.leadership.LeadershipWatcher;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.when;

public class TestS3SegmentManager extends TestBase {

    private S3SegmentManager s3SegmentManager;
    private MockS3StorageServiceEndpointProvider endpointProvider;
    private KafkaEnvironmentProvider environmentProvider;
    private SegmentUploaderConfiguration config;
    private TopicPartition tp;

    @BeforeEach
    @Override
    public void setup() throws Exception {
        super.setup();

        // environment provider setup
        environmentProvider = createTestEnvironmentProvider("sampleZkConnect", "sampleLogDir");
        environmentProvider.load();

        // endpoint provider setup
        endpointProvider = new MockS3StorageServiceEndpointProvider();
        endpointProvider.initialize(TEST_CLUSTER);

        config = getSegmentUploaderConfiguration(TEST_CLUSTER);

        LeadershipWatcher leadershipWatcher = Mockito.mock(LeadershipWatcher.class);
        tp = new TopicPartition(TEST_TOPIC_A, 0);
        when(leadershipWatcher.getLeadingPartitions()).thenReturn(Collections.singleton(tp));
        S3SegmentManager.setS3Client(s3Client);
        s3SegmentManager = new S3SegmentManager(config, environmentProvider, endpointProvider, leadershipWatcher);
    }

    @AfterEach
    @Override
    public void tearDown() throws IOException, ExecutionException, InterruptedException {
        super.tearDown();
    }

    @Test
    void testGetTopicMetadataFromStorage() throws IOException {
        // put empty metadata
        TopicPartitionMetadata emptyMetadata = new TopicPartitionMetadata(tp);
        S3StorageServiceEndpoint endpoint = endpointProvider.getStorageServiceEndpointBuilderForTopic(tp.topic())
                .setTopicPartition(tp)
                .setPrefixEntropyNumBits(config.getS3PrefixEntropyBits())
                .build();
        PutObjectResponse response = putObjectResponse(endpoint.getBucket(), endpoint.getFullPrefix() + "/" + TopicPartitionMetadata.FILENAME, s3Client, emptyMetadata.getAsJsonString());
        assertEquals(200, response.sdkHttpResponse().statusCode(), "Failed to put metadata");
        TopicPartitionMetadata retrievedMetadata = s3SegmentManager.getTopicPartitionMetadataFromStorage(tp);
        assertEquals(retrievedMetadata, emptyMetadata);

        TimeIndex timeIndex = new TimeIndex(TimeIndex.TimeIndexType.TOPIC_PARTITION);
        timeIndex.insertEntry(new TimeIndex.TimeIndexEntry(0, 0, 0L));
        timeIndex.insertEntry(new TimeIndex.TimeIndexEntry(10, 0, 100));
        timeIndex.insertEntry(new TimeIndex.TimeIndexEntry(20, 0, 200));
        emptyMetadata.updateMetadata(TopicPartitionMetadata.TIMEINDEX_KEY, timeIndex);

        response = putObjectResponse(endpoint.getBucket(), endpoint.getFullPrefix() + "/" + TopicPartitionMetadata.FILENAME, s3Client, emptyMetadata.getAsJsonString());
        assertEquals(200, response.sdkHttpResponse().statusCode(), "Failed to put metadata");

        retrievedMetadata = s3SegmentManager.getTopicPartitionMetadataFromStorage(tp);
        assertEquals(3, retrievedMetadata.getTimeIndex().size());
        assertEquals(0L, retrievedMetadata.getTimeIndex().getFirstEntry().getBaseOffset());
        assertEquals(100L, retrievedMetadata.getTimeIndex().getEntry(1).getBaseOffset());
        assertEquals(200L, retrievedMetadata.getTimeIndex().getLastEntry().getBaseOffset());

        clearAllObjects(endpoint.getBucket());
    }

    @Test
    void testWriteMetadataToStorage() throws IOException {
        TopicPartitionMetadata metadata = new TopicPartitionMetadata(tp);
        TimeIndex timeIndex = new TimeIndex(TimeIndex.TimeIndexType.TOPIC_PARTITION);
        timeIndex.insertEntry(new TimeIndex.TimeIndexEntry(0, 0, 0L));
        timeIndex.insertEntry(new TimeIndex.TimeIndexEntry(10, 0, 100));
        timeIndex.insertEntry(new TimeIndex.TimeIndexEntry(20, 0, 200));
        metadata.updateMetadata(TopicPartitionMetadata.TIMEINDEX_KEY, timeIndex);

        boolean success = s3SegmentManager.writeMetadataToStorage(metadata);
        assertTrue(success);

        S3StorageServiceEndpoint endpoint = endpointProvider.getStorageServiceEndpointBuilderForTopic(tp.topic())
                .setTopicPartition(tp)
                .setPrefixEntropyNumBits(config.getS3PrefixEntropyBits())
                .build();
        ResponseInputStream<GetObjectResponse> response = getObjectResponse(endpoint.getBucket(), endpoint.getFullPrefix() + "/" + TopicPartitionMetadata.FILENAME, s3Client);
        TopicPartitionMetadata deserialized = TopicPartitionMetadata.loadFromJson(new String(response.readAllBytes()));
        assertEquals(deserialized, metadata);

        clearAllObjects(endpoint.getBucket());
    }

    @Test
    void testDeleteSegmentsBeforeBaseOffsetInclusive() throws ExecutionException, InterruptedException {
        S3StorageServiceEndpoint endpoint = endpointProvider.getStorageServiceEndpointBuilderForTopic(tp.topic())
                .setTopicPartition(tp)
                .setPrefixEntropyNumBits(config.getS3PrefixEntropyBits())
                .build();
        putEmptyObjects(0L, 1000L, 100L, endpoint);
        putObjectResponse(endpoint.getBucket(), endpoint.getFullPrefix() + "/offset.wm", s3Client, "");
        putObjectResponse(endpoint.getBucket(), endpoint.getFullPrefix() + "/_metadata", s3Client, "");
        putObjectResponse(endpoint.getBucket(), endpoint.getFullPrefix() + "/asdf", s3Client, "");

        Set<Long> deleted = s3SegmentManager.deleteSegmentsBeforeBaseOffsetInclusive(tp, 300L);
        assertEquals(4, deleted.size());
        assertTrue(deleted.contains(0L));
        assertTrue(deleted.contains(100L));
        assertTrue(deleted.contains(200L));
        assertTrue(deleted.contains(300L));

        ListObjectsV2Response response = getListObjectsV2Response(endpoint.getBucket(), endpoint.getFullPrefix(), s3AsyncClient);
        assertEquals(7 * 3 + 3, response.contents().size());
        List<String> responseObjects = new ArrayList<>();
        for (S3Object object : response.contents()) {
            responseObjects.add(object.key().substring(object.key().lastIndexOf("/") + 1));
        }

        // ensure correct segments are retained
        for (long i = 400; i <= 1000; i += 100) {
            assertTrue(responseObjects.contains(Utils.getZeroPaddedOffset(i) + "." + SegmentUtils.SegmentFileType.INDEX.toString().toLowerCase()));
            assertTrue(responseObjects.contains(Utils.getZeroPaddedOffset(i) + "." + SegmentUtils.SegmentFileType.LOG.toString().toLowerCase()));
            assertTrue(responseObjects.contains(Utils.getZeroPaddedOffset(i) + "." + SegmentUtils.SegmentFileType.TIMEINDEX.toString().toLowerCase()));
        }

        // ensure non-segments are not deleted
        assertTrue(responseObjects.contains("offset.wm"));
        assertTrue(responseObjects.contains("_metadata"));
        assertTrue(responseObjects.contains("asdf"));

        clearAllObjects(endpoint.getBucket());
    }

    @Test
    void testDeleteSegmentsBeforeBaseOffsetInclusiveWithSimilarPartitions() throws ExecutionException, InterruptedException {
        // Find two partitions that have the same prefix entropy hash
        int[] partitions = findPartitionsWithSamePrefixHash(TEST_CLUSTER, TEST_TOPIC_A, config.getS3PrefixEntropyBits());
        if (partitions[0] == -1 || partitions[1] == -1) {
            fail("No two partitions with the same prefix entropy hash found");
        }
        TopicPartition tp1 = new TopicPartition(TEST_TOPIC_A, partitions[0]);
        TopicPartition tp2 = new TopicPartition(TEST_TOPIC_A, partitions[1]);

        // Get endpoints for both partitions
        S3StorageServiceEndpoint endpoint1 = endpointProvider.getStorageServiceEndpointBuilderForTopic(tp1.topic())
                .setTopicPartition(tp1)
                .setPrefixEntropyNumBits(config.getS3PrefixEntropyBits())
                .build();
        S3StorageServiceEndpoint endpoint2 = endpointProvider.getStorageServiceEndpointBuilderForTopic(tp2.topic())
                .setTopicPartition(tp2)
                .setPrefixEntropyNumBits(config.getS3PrefixEntropyBits())
                .build();

        // Verify both partitions have the same prefix (due to same entropy hash)
        assertEquals(endpoint1.getPrefixExcludingTopicPartition(), endpoint2.getPrefixExcludingTopicPartition(),
                "Partitions should have same S3 prefix due to same entropy hash");

        // Put test objects for first partition (offsets 0, 100, 200, 300)
        putEmptyObjects(0L, 300L, 100L, endpoint1);
        
        // Put test objects for second partition (offsets 15, 30, 45...)
        putEmptyObjects(15, 300L, 15L, endpoint2);

        // Delete segments before offset 200 (inclusive) for first partition only
        Set<Long> deleted = s3SegmentManager.deleteSegmentsBeforeBaseOffsetInclusive(tp1, 200L);
        
        // Verify first partition deletions
        assertEquals(3, deleted.size());
        assertTrue(deleted.contains(0L));
        assertTrue(deleted.contains(100L));
        assertTrue(deleted.contains(200L));

        // Verify first partition has only offset 300 remaining
        ListObjectsV2Response response1 = getListObjectsV2Response(endpoint1.getBucket(), endpoint1.getFullPrefix() + "/", s3AsyncClient);
        assertEquals(3, response1.contents().size()); // 300.log, 300.index, 300.timeindex

        // Verify second partition is completely untouched (should have all 4 offsets * 3 file types = 12 files)
        ListObjectsV2Response response2 = getListObjectsV2Response(endpoint2.getBucket(), endpoint2.getFullPrefix() + "/", s3AsyncClient);
        assertEquals(60, response2.contents().size()); // All segments for second partition should remain

        clearAllObjects(endpoint1.getBucket());
        clearAllObjects(endpoint2.getBucket());
    }

    /**
     * Find two partitions that have the same prefix entropy hash for testing prefix filtering
     */
    private int[] findPartitionsWithSamePrefixHash(String cluster, String topic, int prefixEntropyBits) {
        String firstHash = null;
        int firstPartition = -1;
        
        // Search through partition numbers to find two with the same hash
        for (int partition = 1; partition < 1000; partition++) {
            String hash = com.pinterest.kafka.tieredstorage.common.Utils.getBinaryHashForClusterTopicPartition(
                    cluster, topic, partition, prefixEntropyBits);
            
            if (firstHash == null) {
                firstHash = hash;
                firstPartition = partition;
            } else if (hash.equals(firstHash) && partition != firstPartition && Integer.toString(partition).startsWith(Integer.toString(firstPartition))) {
                return new int[]{firstPartition, partition};
            }
        }
        
        return new int[]{-1, -1};
    }
    
}

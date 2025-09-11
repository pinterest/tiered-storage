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
    
}

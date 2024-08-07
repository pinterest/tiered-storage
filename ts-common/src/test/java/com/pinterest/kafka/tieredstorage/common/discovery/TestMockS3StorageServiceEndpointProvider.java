package com.pinterest.kafka.tieredstorage.common.discovery;

import com.pinterest.kafka.tieredstorage.common.Utils;
import com.pinterest.kafka.tieredstorage.common.discovery.s3.MockS3StorageServiceEndpointProvider;
import com.pinterest.kafka.tieredstorage.common.discovery.s3.S3StorageServiceEndpoint;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestMockS3StorageServiceEndpointProvider {

    /**
     * Test the retrieval of endpoints from the mock endpoint provider
     */
    @Test
    void testRetrieveEndpoints() {
        MockS3StorageServiceEndpointProvider provider = new MockS3StorageServiceEndpointProvider();
        provider.initialize("test-cluster");

        S3StorageServiceEndpoint endpoint = provider.getStorageServiceEndpointBuilderForTopic("test_topic")
                .setTopicPartition(new TopicPartition("test_topic", 0))
                .build();
        assertEquals("test-bucket", endpoint.getBucket());
        assertEquals("retention-3days/tiered_storage_test/test-cluster/test_topic-0", endpoint.getFullPrefix());
        assertEquals("retention-3days/tiered_storage_test/test-cluster", endpoint.getPrefixExcludingTopicPartition());

        endpoint = provider.getStorageServiceEndpointBuilderForTopic("foo123")
                .setTopicPartition(new TopicPartition("foo123", 133))
                .setPrefixEntropyNumBits(5)
                .build();
        assertEquals("test-bucket", endpoint.getBucket());
        assertEquals(
                "retention-3days/tiered_storage_test/" +
                Utils.getBinaryHashForClusterTopicPartition("test-cluster", "foo123", 133, 5) +
                "/test-cluster/foo123-133",
                endpoint.getFullPrefix()
        );
        assertEquals(
                "retention-3days/tiered_storage_test/" +
                        Utils.getBinaryHashForClusterTopicPartition("test-cluster", "foo123", 133, 5) +
                        "/test-cluster",
                endpoint.getPrefixExcludingTopicPartition()
        );

        endpoint = provider.getStorageServiceEndpointBuilderForTopic("test_topic_2")
                .setTopicPartition(new TopicPartition("test_topic_2", 333))
                .build();
        assertEquals("test-bucket-2", endpoint.getBucket());
        assertEquals("retention-365days/tiered_storage_test/test-cluster/test_topic_2-333", endpoint.getFullPrefix());
        assertEquals("retention-365days/tiered_storage_test/test-cluster", endpoint.getPrefixExcludingTopicPartition());

        endpoint = provider.getStorageServiceEndpointBuilderForTopic("test_topic_2")
                .setTopicPartition(new TopicPartition("test_topic_2", 8))
                .setPrefixEntropyNumBits(5)
                .build();
        assertEquals("test-bucket-2", endpoint.getBucket());
        assertEquals(
                "retention-365days/tiered_storage_test/" +
                Utils.getBinaryHashForClusterTopicPartition("test-cluster", "test_topic_2", 8, 5) +
                "/test-cluster/test_topic_2-8", endpoint.getFullPrefix()
        );
        assertEquals(
                "retention-365days/tiered_storage_test/" +
                        Utils.getBinaryHashForClusterTopicPartition("test-cluster", "test_topic_2", 8, 5) +
                        "/test-cluster", endpoint.getPrefixExcludingTopicPartition()
        );

        endpoint = provider.getStorageServiceEndpointBuilderForTopic("test_topic_2")
                .setTopicPartition(new TopicPartition("test_topic_2", 40))
                .setPrefixEntropyNumBits(5)
                .build();
        assertEquals("test-bucket-2", endpoint.getBucket());
        assertEquals(
                "retention-365days/tiered_storage_test/" +
                        Utils.getBinaryHashForClusterTopicPartition("test-cluster", "test_topic_2", 40, 5) +
                        "/test-cluster/test_topic_2-40", endpoint.getFullPrefix()
        );
        assertEquals(
                "retention-365days/tiered_storage_test/" +
                        Utils.getBinaryHashForClusterTopicPartition("test-cluster", "test_topic_2", 40, 5) +
                        "/test-cluster", endpoint.getPrefixExcludingTopicPartition()
        );

    }
}

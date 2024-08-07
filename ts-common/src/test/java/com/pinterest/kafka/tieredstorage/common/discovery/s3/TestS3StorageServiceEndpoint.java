package com.pinterest.kafka.tieredstorage.common.discovery.s3;

import com.pinterest.kafka.tieredstorage.common.Utils;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class TestS3StorageServiceEndpoint {

    @Test
    void testEndpointConstruction() {
        S3StorageServiceEndpoint.Builder builder = new S3StorageServiceEndpoint.Builder();
        builder.setKafkaCluster("test-cluster")
                .setBucket("my-bucket")
                .setBasePrefix("base-prefix");
        assertThrows(IllegalArgumentException.class, builder::build);
        TopicPartition tp0 = new TopicPartition("test-topic", 0);
        builder.setTopicPartition(tp0);
        S3StorageServiceEndpoint endpoint = builder.build();
        assertEquals("my-bucket", endpoint.getBucket());
        assertEquals("base-prefix/test-cluster", endpoint.getPrefixExcludingTopicPartition());
        assertEquals("base-prefix/test-cluster/" + tp0, endpoint.getFullPrefix());
        assertEquals("s3://my-bucket/base-prefix/test-cluster/" + tp0, endpoint.getFullPrefixUri());
        assertEquals("s3://my-bucket/base-prefix/test-cluster", endpoint.getPrefixExcludingTopicPartitionUri());

        endpoint = builder.setPrefixEntropyNumBits(5).build();

        assertEquals("my-bucket", endpoint.getBucket());
        String hash = Utils.getBinaryHashForClusterTopicPartition("test-cluster", tp0.topic(), tp0.partition(), 5);
        assertEquals("base-prefix/" + hash + "/test-cluster", endpoint.getPrefixExcludingTopicPartition());
        assertEquals("base-prefix/" + hash + "/test-cluster/" + tp0, endpoint.getFullPrefix());
        assertEquals("s3://my-bucket/base-prefix/" + hash + "/test-cluster/" + tp0, endpoint.getFullPrefixUri());
        assertEquals("s3://my-bucket/base-prefix/" + hash + "/test-cluster", endpoint.getPrefixExcludingTopicPartitionUri());

        endpoint = builder.setPrefixEntropyNumBits(0).build();
        assertEquals("base-prefix/test-cluster", endpoint.getPrefixExcludingTopicPartition());
        assertEquals("base-prefix/test-cluster/" + tp0, endpoint.getFullPrefix());
        assertEquals("s3://my-bucket/base-prefix/test-cluster/" + tp0, endpoint.getFullPrefixUri());
        assertEquals("s3://my-bucket/base-prefix/test-cluster", endpoint.getPrefixExcludingTopicPartitionUri());

    }
}

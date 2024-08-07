package com.pinterest.kafka.tieredstorage.common.discovery.s3;

import com.pinterest.kafka.tieredstorage.common.Utils;
import com.pinterest.kafka.tieredstorage.common.discovery.StorageServiceEndpoint;
import com.pinterest.kafka.tieredstorage.common.discovery.StorageServiceEndpointBuilder;
import org.apache.kafka.common.TopicPartition;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Represents an {@link StorageServiceEndpoint} for S3.
 */
public class S3StorageServiceEndpoint implements StorageServiceEndpoint {

    private static final Pattern PATH_REGEX = Pattern.compile("(?<bucket>[a-zA-Z0-9-_.]+)/(?<prefix>[a-zA-Z0-9-_./]+)/(?<topicPartition>[a-zA-z0-9-_.]+-[0-9]+$)");
    private final String bucket;
    private final String prefix;
    private final String topicPartition;

    private S3StorageServiceEndpoint(String path) {
        Matcher matcher = PATH_REGEX.matcher(path);
        if (!matcher.matches()) {
            throw new IllegalArgumentException(String.format("Path %s does not match expected regex", path));
        }
        this.bucket = matcher.group("bucket");
        this.prefix = matcher.group("prefix");
        this.topicPartition = matcher.group("topicPartition");
    }

    /**
     * Get the bucket name
     * @return bucket
     */
    public String getBucket() {
        return bucket;
    }

    /**
     * Get the full prefix, including topic partition. For example, if the path is s3://bucket/prefix/prefix2/topic-0,
     * this method will return prefix/prefix2/topic-0
     * @return prefix
     */
    public String getFullPrefix() {
        return prefix + "/" + topicPartition;
    }

    /**
     * Get the prefix excluding topic partition. For example, if the path is s3://bucket/prefix/prefix2/topic-0,
     * this method will return prefix/prefix2
     * @return prefix
     */
    public String getPrefixExcludingTopicPartition() {
        return prefix;
    }

    /**
     * Get the full path, including the protocol, bucket, prefix and topic partition.
     * For example, if the path is s3://bucket/prefix/prefix2/topic-0,
     * this method will return s3://bucket/prefix/prefix2/topic-0
     * @return full path
     */
    public String getFullPrefixUri() {
        return "s3://" + bucket + "/" + getFullPrefix();
    }

    /**
     * Get the prefix excluding the topic partition. For example, if the path is s3://bucket/prefix/prefix2/topic-0,
     * this method will return s3://bucket/prefix/prefix2
     * @return prefix
     */
    public String getPrefixExcludingTopicPartitionUri() {
        return "s3://" + bucket + "/" + getPrefixExcludingTopicPartition();
    }

    public static class Builder implements StorageServiceEndpointBuilder {
        private String bucket;
        private String basePrefix;
        private String kafkaCluster;
        private int prefixEntropyNumBits = -1;
        private TopicPartition topicPartition;

        public Builder setBucket(String bucket) {
            this.bucket = bucket;
            return this;
        }

        public Builder setBasePrefix(String basePrefix) {
            this.basePrefix = basePrefix;
            return this;
        }

        public Builder setKafkaCluster(String kafkaCluster) {
            this.kafkaCluster = kafkaCluster;
            return this;
        }

        public Builder setTopicPartition(TopicPartition topicPartition) {
            this.topicPartition = topicPartition;
            return this;
        }

        public Builder setPrefixEntropyNumBits(int numBits) {
            this.prefixEntropyNumBits = numBits;
            return this;
        }

        /**
         * Build the {@link S3StorageServiceEndpoint}. This is where prefix entropy bits will be injected into the
         * prefix, if applicable.
         * @return A fully-constructed {@link S3StorageServiceEndpoint}
         */
        public S3StorageServiceEndpoint build() {
            if (bucket == null || basePrefix == null || kafkaCluster == null || topicPartition == null) {
                throw new IllegalArgumentException("Bucket, basePrefix, kafkaCluster and topicPartition must be set");
            }

            if (prefixEntropyNumBits > 0) {
                return new S3StorageServiceEndpoint(String.format("%s/%s/%s/%s/%s-%d",
                        bucket,
                        basePrefix,
                        Utils.getBinaryHashForClusterTopicPartition(kafkaCluster, topicPartition.topic(), topicPartition.partition(), prefixEntropyNumBits),
                        kafkaCluster,
                        topicPartition.topic(),
                        topicPartition.partition()
                ));
            } else {
                return new S3StorageServiceEndpoint(String.format("%s/%s/%s/%s-%d",
                        bucket,
                        basePrefix,
                        kafkaCluster,
                        topicPartition.topic(),
                        topicPartition.partition())
                );
            }
        }

    }
}

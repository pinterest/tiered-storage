package com.pinterest.kafka.tieredstorage.consumer;

import com.pinterest.kafka.tieredstorage.common.metrics.MetricsConfiguration;
import com.pinterest.kafka.tieredstorage.common.metrics.NoOpMetricsReporter;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestS3Utils extends TestS3Base {

    /**
     * Ensure that the offsetKeyMap is correctly retrieved
     */
    @Test
    void testGetSortedOffsetKeyMap() {
        putEmptyObjects(KAFKA_TOPIC, 0, 0L, 20L, 10L);
        putEmptyObjects(KAFKA_TOPIC, 1, 0L, 40L, 10L);
        S3Utils.overrideS3Client(s3Client);
        String metricsReporterClassName = NoOpMetricsReporter.class.getName();
        MetricsConfiguration metricsConfiguration = new MetricsConfiguration(metricsReporterClassName, null, null);

        TreeMap<Long, Triple<String, String, Long>> map0 = S3Utils.getSortedOffsetKeyMap(getS3BasePrefixWithCluster(), new TopicPartition(KAFKA_TOPIC, 0), S3Utils.getZeroPaddedOffset(0L), null, metricsConfiguration);
        TreeMap<Long, Triple<String, String, Long>> map1 = S3Utils.getSortedOffsetKeyMap(getS3BasePrefixWithCluster(), new TopicPartition(KAFKA_TOPIC, 1), S3Utils.getZeroPaddedOffset(0L), null, metricsConfiguration);
        assertEquals(new HashSet<>(Arrays.asList(0L, 10L, 20L)), map0.keySet());
        assertEquals(new HashSet<>(Arrays.asList(0L, 10L, 20L, 30L, 40L)), map1.keySet());

        map0 = S3Utils.getSortedOffsetKeyMap(getS3BasePrefixWithCluster(), new TopicPartition(KAFKA_TOPIC, 0), S3Utils.getZeroPaddedOffset(5L), null, metricsConfiguration);
        map1 = S3Utils.getSortedOffsetKeyMap(getS3BasePrefixWithCluster(), new TopicPartition(KAFKA_TOPIC, 1), S3Utils.getZeroPaddedOffset(15L), null, metricsConfiguration);
        assertEquals(new HashSet<>(Arrays.asList(0L, 10L, 20L)), map0.keySet());
        assertEquals(new HashSet<>(Arrays.asList(10L, 20L, 30L, 40L)), map1.keySet());

        map0 = S3Utils.getSortedOffsetKeyMap(getS3BasePrefixWithCluster(), new TopicPartition(KAFKA_TOPIC, 0), S3Utils.getZeroPaddedOffset(20L), null, metricsConfiguration);
        map1 = S3Utils.getSortedOffsetKeyMap(getS3BasePrefixWithCluster(), new TopicPartition(KAFKA_TOPIC, 1), S3Utils.getZeroPaddedOffset(30L), null, metricsConfiguration);
        assertEquals(new HashSet<>(Arrays.asList(10L, 20L)), map0.keySet());
        assertEquals(new HashSet<>(Arrays.asList(20L, 30L, 40L)), map1.keySet());

        map0 = S3Utils.getSortedOffsetKeyMap(getS3BasePrefixWithCluster(), new TopicPartition(KAFKA_TOPIC, 0), S3Utils.getZeroPaddedOffset(33L), null, metricsConfiguration);
        map1 = S3Utils.getSortedOffsetKeyMap(getS3BasePrefixWithCluster(), new TopicPartition(KAFKA_TOPIC, 1), S3Utils.getZeroPaddedOffset(45L), null, metricsConfiguration);
        assertEquals(new HashSet<>(Collections.singletonList(20L)), map0.keySet());
        assertEquals(new HashSet<>(Collections.singletonList(40L)), map1.keySet());

        map0 = S3Utils.getSortedOffsetKeyMap(getS3BasePrefixWithCluster(), new TopicPartition(KAFKA_TOPIC, 0), S3Utils.getZeroPaddedOffset(0L), getS3ObjectKey(KAFKA_TOPIC, 0, 10L, FileType.LOG), metricsConfiguration);
        map1 = S3Utils.getSortedOffsetKeyMap(getS3BasePrefixWithCluster(), new TopicPartition(KAFKA_TOPIC, 1), S3Utils.getZeroPaddedOffset(0L), getS3ObjectKey(KAFKA_TOPIC, 1, 20L, FileType.LOG), metricsConfiguration);
        assertEquals(new HashSet<>(Collections.singletonList(20L)), map0.keySet());
        assertEquals(new HashSet<>(Arrays.asList(30L, 40L)), map1.keySet());
    }
}

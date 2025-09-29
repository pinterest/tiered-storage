package com.pinterest.kafka.tieredstorage.common;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.pinterest.kafka.tieredstorage.common.Utils;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestUtils {

    @Test
    void testGetBinaryHashForClusterTopicPartition() {
        String baseClusterString = "cluster";
        String baseTopicString = "topic";

        int numTestCombinations = 100000;
        int numSoFar = 0;
        int numDigits = 5;

        Map<String, Integer> hashToCount = new HashMap<>();

        while (numSoFar < numTestCombinations) {
            numSoFar++;

            String cluster = baseClusterString + new Random().nextInt(numTestCombinations);
            String topic = baseTopicString + new Random().nextInt(numTestCombinations);
            int partition = new Random().nextInt(1000);

            String hash = Utils.getBinaryHashForClusterTopicPartition(cluster, topic, partition, numDigits);
            assertEquals(numDigits, hash.length());
            hashToCount.putIfAbsent(hash, 0);
            hashToCount.put(hash, hashToCount.get(hash) + 1);
        }

        assertEquals(Math.pow(2, numDigits), hashToCount.size());   // Ensure that all possible hashes are generated

        // Ensure that the distribution of hashes is relatively uniform
        MetricRegistry metricRegistry = new MetricRegistry();
        Histogram histogram = metricRegistry.histogram("hashDistribution");
        hashToCount.values().forEach(histogram::update);
        double stddev = histogram.getSnapshot().getStdDev();

        // Ensure that the standard deviation is less than 0.1% of the total number of tests
        assertTrue(stddev < (double) numTestCombinations / 1000);

    }

    @Test
    void testGetBaseOffsetFromFilename() {
        String index = "00000000000000000362.index";
        String timeIndex = "00000000000000000362.timeindex";
        String log = "00000000000000000362.log";

        assertEquals(362L, Utils.getBaseOffsetFromFilename(index).get());
        assertEquals(362L, Utils.getBaseOffsetFromFilename(timeIndex).get());
        assertEquals(362L, Utils.getBaseOffsetFromFilename(log).get());

        assertEquals(362L, Utils.getBaseOffsetFromFilename("/directory/directory2/" + index).get());
        assertEquals(362L, Utils.getBaseOffsetFromFilename("directory/directory2/" + timeIndex).get());
        assertEquals(362L, Utils.getBaseOffsetFromFilename("/directory/directory2/directory3/" + log).get());

        assertFalse(Utils.getBaseOffsetFromFilename("offset.wm").isPresent());
        assertFalse(Utils.getBaseOffsetFromFilename("/directory/asdf/_metadata").isPresent());
    }
}

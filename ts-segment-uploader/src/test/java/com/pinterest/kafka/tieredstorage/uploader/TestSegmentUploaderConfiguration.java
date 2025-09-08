package com.pinterest.kafka.tieredstorage.uploader;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static com.pinterest.kafka.tieredstorage.uploader.TestBase.getSegmentUploaderConfiguration;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestSegmentUploaderConfiguration {

    /**
     * Ensure that the configuration is correctly initialized
     * @throws IOException
     */
    @Test
    public void testInitialization() throws IOException {
        SegmentUploaderConfiguration allIncludeConfiguration = getSegmentUploaderConfiguration("test-cluster-all-include");
        SegmentUploaderConfiguration allExcludeConfiguration = getSegmentUploaderConfiguration("test-cluster-all-exclude");
        SegmentUploaderConfiguration testConfiguration = getSegmentUploaderConfiguration("test-cluster-specific-include-exclude");
        SegmentUploaderConfiguration testSpecificIncludeConfiguration = getSegmentUploaderConfiguration("test-cluster-specific-include");
        SegmentUploaderConfiguration testSpecificExcludeConfiguration = getSegmentUploaderConfiguration( "test-cluster-specific-exclude");

        for (int i = 0; i < 20; i++) {
            String generatedString = RandomStringUtils.randomAlphanumeric(10);
            assertTrue(allIncludeConfiguration.shouldWatchTopic(generatedString));
            assertFalse(allExcludeConfiguration.shouldWatchTopic(generatedString));
            assertTrue(allIncludeConfiguration.isInInclusionCache(generatedString));
            assertFalse(allIncludeConfiguration.isInExclusionCache(generatedString));
            assertFalse(allExcludeConfiguration.isInInclusionCache(generatedString));
            assertTrue(allExcludeConfiguration.isInExclusionCache(generatedString));
        }

        // more complex matching
        assertFalse(testConfiguration.shouldWatchTopic("test_topic_0"));
        assertFalse(testConfiguration.shouldWatchTopic("test_topic_1"));
        assertFalse(testConfiguration.shouldWatchTopic("test_topic_2"));
        assertFalse(testConfiguration.shouldWatchTopic("my_topic"));
        assertFalse(testConfiguration.shouldWatchTopic("foo_topic"));
        assertFalse(testConfiguration.shouldWatchTopic("bar_topic"));
        assertTrue(testConfiguration.shouldWatchTopic("dev_topic_0"));
        assertTrue(testConfiguration.isInInclusionCache("dev_topic_0"));
        assertTrue(testConfiguration.isInExclusionCache("test_topic_0"));

        assertTrue(testSpecificIncludeConfiguration.shouldWatchTopic("test_topic_0"));
        assertTrue(testSpecificIncludeConfiguration.shouldWatchTopic("test_topic_1"));
        assertTrue(testSpecificIncludeConfiguration.shouldWatchTopic("test_topic_2"));
        assertTrue(testSpecificIncludeConfiguration.shouldWatchTopic("my_topic"));
        assertTrue(testSpecificIncludeConfiguration.shouldWatchTopic("foo_topic"));
        assertTrue(testSpecificIncludeConfiguration.shouldWatchTopic("bar_topic"));
        assertTrue(testSpecificIncludeConfiguration.shouldWatchTopic("dev_topic_0"));
        assertTrue(testSpecificIncludeConfiguration.isInInclusionCache("bar_topic"));
        assertFalse(testSpecificIncludeConfiguration.isInExclusionCache("foo_topic"));

        assertFalse(testSpecificExcludeConfiguration.shouldWatchTopic("test_topic_0"));
        assertFalse(testSpecificExcludeConfiguration.shouldWatchTopic("test_topic_1"));
        assertFalse(testSpecificExcludeConfiguration.shouldWatchTopic("test_topic_2"));
        assertFalse(testSpecificExcludeConfiguration.shouldWatchTopic("my_topic"));
        assertFalse(testSpecificExcludeConfiguration.shouldWatchTopic("foo_topic"));
        assertFalse(testSpecificExcludeConfiguration.shouldWatchTopic("bar_topic"));
        assertFalse(testSpecificExcludeConfiguration.shouldWatchTopic("dev_topic_0"));
        assertFalse(testSpecificExcludeConfiguration.isInInclusionCache("bar_topic"));
        assertTrue(testSpecificExcludeConfiguration.isInExclusionCache("foo_topic"));
        assertTrue(testSpecificExcludeConfiguration.shouldWatchTopic("test_topic_3"));
    }

    @Test
    void testSegmentManagerConfigurations() throws IOException {
        SegmentUploaderConfiguration config = getSegmentUploaderConfiguration("test-cluster-base");
        assertEquals(1800, config.getSegmentManagerGcIntervalSeconds());
        assertEquals(7200, config.getSegmentManagerGcRetentionSeconds("test_topic_c"));
        assertEquals(3600, config.getSegmentManagerGcRetentionSeconds("test_topic_a"));
        assertEquals(43200, config.getSegmentManagerGcRetentionSeconds("test_topic_b"));
        assertEquals(7200, config.getSegmentManagerGcRetentionSeconds(null));
        assertEquals(7200, config.getSegmentManagerGcRetentionSeconds(""));
    }
}

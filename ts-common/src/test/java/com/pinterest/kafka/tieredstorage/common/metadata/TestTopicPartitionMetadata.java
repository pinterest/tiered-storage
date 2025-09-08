package com.pinterest.kafka.tieredstorage.common.metadata;

import com.google.gson.JsonObject;
import com.pinterest.kafka.tieredstorage.common.Utils;

import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URL;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class TestTopicPartitionMetadata {

    @Test
    void testGsonSerialization() throws IOException {
        TopicPartitionMetadata topicPartitionMetadata = new TopicPartitionMetadata(new TopicPartition("test-topic", 0));

        String resourcePath = "log-files/timeindex_test/00000000000000022964.timeindex";
        URL resourceUrl = getClass().getClassLoader().getResource(resourcePath);

        if (resourceUrl == null) {
            fail("Test resource not found: " + resourcePath);
        }

        File timeIndexFile = new File(resourceUrl.getFile());
        FileInputStream fis = new FileInputStream(timeIndexFile);
        TimeIndex timeIndex = TimeIndex.loadFromSegmentTimeIndex(fis, Utils.getBaseOffsetFromFilename(resourcePath).get());
        topicPartitionMetadata.updateMetadata(TopicPartitionMetadata.TIMEINDEX_KEY, timeIndex);
        JsonObject tpMetadataJsonObject = topicPartitionMetadata.getAsJsonObject();
        assertEquals(0, tpMetadataJsonObject.getAsJsonObject(TopicPartitionMetadata.TOPIC_PARTITION_KEY).getAsJsonPrimitive(TopicPartitionMetadata.PARTITION_KEY).getAsInt());
        assertEquals("test-topic", tpMetadataJsonObject.getAsJsonObject(TopicPartitionMetadata.TOPIC_PARTITION_KEY).getAsJsonPrimitive(TopicPartitionMetadata.TOPIC_KEY).getAsString());
        JsonObject metadata = tpMetadataJsonObject.getAsJsonObject(TopicPartitionMetadata.METADATA_KEY);
        JsonObject timeIndexJsonObject = metadata.getAsJsonObject(TopicPartitionMetadata.TIMEINDEX_KEY);
        assertTrue(timeIndexJsonObject.keySet().contains(TimeIndex.ENTRIES_KEY));
        assertTrue(timeIndexJsonObject.keySet().contains(TimeIndex.SIZE_KEY));
        assertEquals(timeIndexJsonObject.getAsJsonArray(TimeIndex.ENTRIES_KEY).size(), timeIndexJsonObject.getAsJsonPrimitive(TimeIndex.SIZE_KEY).getAsInt());

        // test convert back to pojo
        TopicPartitionMetadata tpMetadataFromJson = TopicPartitionMetadata.loadFromJson(tpMetadataJsonObject.toString());
        assertEquals(topicPartitionMetadata.getTopicPartition(), tpMetadataFromJson.getTopicPartition());
        assertEquals(topicPartitionMetadata.getMetadataCopy().size(), tpMetadataFromJson.getMetadataCopy().size());
        assertEquals(topicPartitionMetadata.getMetadataCopy(), tpMetadataFromJson.getMetadataCopy());
        assertEquals(topicPartitionMetadata.getTimeIndex(), tpMetadataFromJson.getTimeIndex());

        // test insert entry and convert back to pojo
        timeIndex.insertEntry(new TimeIndex.TimeIndexEntry(System.currentTimeMillis(), 300, 35120L));
        JsonObject tpMetadataJsonObjectAfterInsert = topicPartitionMetadata.getAsJsonObject();
        JsonObject metadataJsonAfterInsert = tpMetadataJsonObjectAfterInsert.getAsJsonObject(TopicPartitionMetadata.METADATA_KEY);
        JsonObject timeIndexJsonAfterInsert = metadataJsonAfterInsert.getAsJsonObject(TopicPartitionMetadata.TIMEINDEX_KEY);
        assertEquals(timeIndexJsonObject.getAsJsonArray(TimeIndex.ENTRIES_KEY).size() + 1, timeIndexJsonAfterInsert.getAsJsonArray(TimeIndex.ENTRIES_KEY).size());

        TopicPartitionMetadata tpMetadataFromJsonAfterInsert = TopicPartitionMetadata.loadFromJson(tpMetadataJsonObjectAfterInsert.toString());
        TimeIndex timeIndexAfterInsert = tpMetadataFromJsonAfterInsert.getTimeIndex();
        assertEquals(timeIndexJsonObject.getAsJsonArray(TimeIndex.ENTRIES_KEY).size() + 1, timeIndexAfterInsert.size());
        assertEquals(timeIndex.getFirstEntry(), timeIndexAfterInsert.getFirstEntry());
        assertEquals(35120L, timeIndex.getLastEntry().getBaseOffset());
        
    }
}

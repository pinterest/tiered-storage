package com.pinterest.kafka.tieredstorage.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestTieredStorageRecords {

    @Test
    void testIsEmpty() {
        TieredStorageRecords<String, String> records = new TieredStorageRecords<>();
        assertTrue(records.records().isEmpty());
    }
}

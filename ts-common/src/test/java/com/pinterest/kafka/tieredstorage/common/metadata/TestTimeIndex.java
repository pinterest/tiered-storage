package com.pinterest.kafka.tieredstorage.common.metadata;

import com.google.gson.JsonObject;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

/**
 * Tests for the TimeIndex class
 */
public class TestTimeIndex {
    
    @Test
    void testLoadEmptyTimeIndex() throws IOException {
        ByteArrayInputStream emptyStream = new ByteArrayInputStream(new byte[0]);
        TimeIndex timeIndex = TimeIndex.loadFromSegmentTimeIndex(emptyStream, 0L);
        
        assertTrue(timeIndex.isEmpty());
        assertEquals(0, timeIndex.size());
        assertNull(timeIndex.getFirstEntry());
        assertNull(timeIndex.getLastEntry());
    }
    
    @Test
    void testLoadSingleEntry() throws IOException {
        // Create a single entry: timestamp=1000, relativeOffset=100
        ByteBuffer buffer = ByteBuffer.allocate(12);
        buffer.putLong(1000L);
        buffer.putInt(100);
        
        ByteArrayInputStream stream = new ByteArrayInputStream(buffer.array());
        TimeIndex timeIndex = TimeIndex.loadFromSegmentTimeIndex(stream, 0L);
        
        assertFalse(timeIndex.isEmpty());
        assertEquals(1, timeIndex.size());
        
        TimeIndex.TimeIndexEntry entry = timeIndex.getFirstEntry();
        assertNotNull(entry);
        assertEquals(1000L, entry.getTimestamp());
        assertEquals(100, entry.getRelativeOffset());
        
        // First and last should be the same for single entry
        assertEquals(entry, timeIndex.getLastEntry());
    }
    
    @Test
    void testLoadMultipleEntries() throws IOException {
        // Create multiple entries
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        
        // Entry 1: timestamp=1000, offset=0
        ByteBuffer buffer1 = ByteBuffer.allocate(12);
        buffer1.putLong(1000L);
        buffer1.putInt(0);
        baos.write(buffer1.array());
        
        // Entry 2: timestamp=2000, offset=500
        ByteBuffer buffer2 = ByteBuffer.allocate(12);
        buffer2.putLong(2000L);
        buffer2.putInt(500);
        baos.write(buffer2.array());
        
        // Entry 3: timestamp=3000, offset=1000
        ByteBuffer buffer3 = ByteBuffer.allocate(12);
        buffer3.putLong(3000L);
        buffer3.putInt(1000);
        baos.write(buffer3.array());
        
        ByteArrayInputStream stream = new ByteArrayInputStream(baos.toByteArray());
        TimeIndex timeIndex = TimeIndex.loadFromSegmentTimeIndex(stream, 0L);
        
        assertEquals(3, timeIndex.size());
        
        // Test first entry
        TimeIndex.TimeIndexEntry firstEntry = timeIndex.getFirstEntry();
        assertEquals(1000L, firstEntry.getTimestamp());
        assertEquals(0, firstEntry.getRelativeOffset());
        
        // Test last entry
        TimeIndex.TimeIndexEntry lastEntry = timeIndex.getLastEntry();
        assertEquals(3000L, lastEntry.getTimestamp());
        assertEquals(1000, lastEntry.getRelativeOffset());
        
        // Test middle entry
        TimeIndex.TimeIndexEntry middleEntry = timeIndex.getEntry(1);
        assertEquals(2000L, middleEntry.getTimestamp());
        assertEquals(500, middleEntry.getRelativeOffset());
        
        // Test all entries
        ConcurrentSkipListSet<TimeIndex.TimeIndexEntry> allEntries = timeIndex.getEntriesCopy();
        assertEquals(3, allEntries.size());
        assertEquals(firstEntry, allEntries.first());
        assertEquals(lastEntry, allEntries.last());
    }
    
    @Test
    void testFindEntryForTimestamp() throws IOException {
        // Create test data with sorted timestamps
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        
        // Entry 1: timestamp=1000, offset=0
        ByteBuffer buffer1 = ByteBuffer.allocate(12);
        buffer1.putLong(1000L);
        buffer1.putInt(0);
        baos.write(buffer1.array());
        
        // Entry 2: timestamp=2000, offset=500
        ByteBuffer buffer2 = ByteBuffer.allocate(12);
        buffer2.putLong(2000L);
        buffer2.putInt(500);
        baos.write(buffer2.array());
        
        // Entry 3: timestamp=3000, offset=1000
        ByteBuffer buffer3 = ByteBuffer.allocate(12);
        buffer3.putLong(3000L);
        buffer3.putInt(1000);
        baos.write(buffer3.array());
        
        ByteArrayInputStream stream = new ByteArrayInputStream(baos.toByteArray());
        TimeIndex timeIndex = TimeIndex.loadFromSegmentTimeIndex(stream, 0L);
        
        // Test exact matches
        TimeIndex.TimeIndexEntry entry = timeIndex.findEntryForTimestamp(2000L);
        assertNotNull(entry);
        assertEquals(2000L, entry.getTimestamp());
        assertEquals(500, entry.getRelativeOffset());
        
        // Test timestamp between entries (should return previous entry)
        entry = timeIndex.findEntryForTimestamp(1500L);
        assertNotNull(entry);
        assertEquals(1000L, entry.getTimestamp());
        assertEquals(0, entry.getRelativeOffset());
        
        // Test timestamp before all entries
        entry = timeIndex.findEntryForTimestamp(500L);
        assertNull(entry);
        
        // Test timestamp after all entries
        entry = timeIndex.findEntryForTimestamp(4000L);
        assertNotNull(entry);
        assertEquals(3000L, entry.getTimestamp());
        assertEquals(1000, entry.getRelativeOffset());
    }
    
    @Test
    void testGetRelativeOffsetForTimestamp() throws IOException {
        // Create test data
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        
        // Entry 1: timestamp=1000, offset=0
        ByteBuffer buffer1 = ByteBuffer.allocate(12);
        buffer1.putLong(1000L);
        buffer1.putInt(0);
        baos.write(buffer1.array());
        
        // Entry 2: timestamp=2000, offset=500
        ByteBuffer buffer2 = ByteBuffer.allocate(12);
        buffer2.putLong(2000L);
        buffer2.putInt(500);
        baos.write(buffer2.array());
        
        // Entry 3: timestamp=3000, offset=1000
        ByteBuffer buffer3 = ByteBuffer.allocate(12);
        buffer3.putLong(3000L);
        buffer3.putInt(1000);
        baos.write(buffer3.array());
        
        ByteArrayInputStream stream = new ByteArrayInputStream(baos.toByteArray());
        TimeIndex timeIndex = TimeIndex.loadFromSegmentTimeIndex(stream, 0L);
        
        // Test various timestamps
        assertEquals(0, timeIndex.getRelativeOffsetForTimestamp(1000L));
        assertEquals(0, timeIndex.getRelativeOffsetForTimestamp(1500L));
        assertEquals(500, timeIndex.getRelativeOffsetForTimestamp(2000L));
        assertEquals(500, timeIndex.getRelativeOffsetForTimestamp(2500L));
        assertEquals(1000, timeIndex.getRelativeOffsetForTimestamp(3000L));
        assertEquals(1000, timeIndex.getRelativeOffsetForTimestamp(4000L));
        assertEquals(-1, timeIndex.getRelativeOffsetForTimestamp(500L));
    }
    
    @Test
    void testTimeIndexEntry() {
        TimeIndex.TimeIndexEntry entry = new TimeIndex.TimeIndexEntry(1000L, 500, 0L);
        
        assertEquals(1000L, entry.getTimestamp());
        assertEquals(500, entry.getRelativeOffset());
        
        // Test toString
        String str = entry.toString();
        assertTrue(str.contains("1000"));
        assertTrue(str.contains("500"));
        
        // Test equals and hashCode
        TimeIndex.TimeIndexEntry entry2 = new TimeIndex.TimeIndexEntry(1000L, 500, 0L);
        TimeIndex.TimeIndexEntry entry3 = new TimeIndex.TimeIndexEntry(2000L, 500, 0L);
        
        assertEquals(entry, entry2);
        assertNotEquals(entry, entry3);
        assertEquals(entry.hashCode(), entry2.hashCode());
    }
    
    @Test
    void testInvalidEntrySize() throws IOException {
        // Create data that's not a multiple of 12 bytes (should stop at last complete entry)
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        
        // Entry 1: complete entry (12 bytes)
        ByteBuffer buffer1 = ByteBuffer.allocate(12);
        buffer1.putLong(1000L);
        buffer1.putInt(100);
        baos.write(buffer1.array());
        
        // Partial entry (only 8 bytes)
        ByteBuffer buffer2 = ByteBuffer.allocate(8);
        buffer2.putLong(2000L);
        baos.write(buffer2.array());
        
        ByteArrayInputStream stream = new ByteArrayInputStream(baos.toByteArray());
        TimeIndex timeIndex = TimeIndex.loadFromSegmentTimeIndex(stream, 0L);
        
        // Should only have one complete entry
        assertEquals(1, timeIndex.size());
        assertEquals(1000L, timeIndex.getFirstEntry().getTimestamp());
        assertEquals(100, timeIndex.getFirstEntry().getRelativeOffset());
    }

    @Test
    void testLoadRealTimeIndexFile() {
        String resourcePath = "log-files/timeindex_test/00000000000000022964.timeindex";
        URL resourceUrl = getClass().getClassLoader().getResource(resourcePath);
        
        if (resourceUrl == null) {
            fail("Test resource not found: " + resourcePath);
        }
        
        File timeIndexFile = new File(resourceUrl.getFile());
        
        try (FileInputStream fis = new FileInputStream(timeIndexFile)) {
            TimeIndex timeIndex = TimeIndex.loadFromSegmentTimeIndex(fis, 0L);
            
            // Verify the index is properly loaded
            assertFalse(timeIndex.isEmpty());
            assertTrue(timeIndex.size() > 0);
            assertNotNull(timeIndex.getFirstEntry());
            assertNotNull(timeIndex.getLastEntry());
            
            // Verify entries are properly ordered by timestamp
            if (timeIndex.size() > 1) {
                for (int i = 1; i < timeIndex.size(); i++) {
                    long prevTimestamp = timeIndex.getEntry(i - 1).getTimestamp();
                    long currentTimestamp = timeIndex.getEntry(i).getTimestamp();
                    assertTrue(currentTimestamp >= prevTimestamp,
                            "Entries should be ordered by timestamp");
                }
            }

            // Verify correctness of first and last entries
            assertEquals(1753819244270L, timeIndex.getFirstEntry().getTimestamp());
            assertEquals(30, timeIndex.getFirstEntry().getRelativeOffset());
            assertEquals(1753819281929L, timeIndex.getLastEntry().getTimestamp());
            assertEquals(7600, timeIndex.getLastEntry().getRelativeOffset());

            // Verify correctness of findEntryForTimestamp

            // exact match for first entry
            assertEquals(1753819244270L, timeIndex.findEntryForTimestamp(1753819244270L).getTimestamp());
            assertEquals(30, timeIndex.findEntryForTimestamp(1753819244270L).getRelativeOffset());
            // exact match for last entry
            assertEquals(1753819281929L, timeIndex.findEntryForTimestamp(1753819281929L).getTimestamp());
            assertEquals(7600, timeIndex.findEntryForTimestamp(1753819281929L).getRelativeOffset());
            // generic cases
            assertEquals(1753819245863L, timeIndex.findEntryForTimestamp(1753819246011L).getTimestamp());
            assertEquals(300, timeIndex.findEntryForTimestamp(1753819246011L).getRelativeOffset());
            assertEquals(1753819281864L, timeIndex.findEntryForTimestamp(1753819281927L).getTimestamp());
            assertEquals(7590, timeIndex.findEntryForTimestamp(1753819281927L).getRelativeOffset());
            
        } catch (IOException e) {
            fail("Failed to load timeindex file: " + e.getMessage());
        }
    }

    @Test
    void testGsonSerialization() {
        String resourcePath = "log-files/timeindex_test/00000000000000022964.timeindex";
        URL resourceUrl = getClass().getClassLoader().getResource(resourcePath);

        if (resourceUrl == null) {
            fail("Test resource not found: " + resourcePath);
        }

        File timeIndexFile = new File(resourceUrl.getFile());

        try (FileInputStream fis = new FileInputStream(timeIndexFile)) {
            TimeIndex timeIndex = TimeIndex.loadFromSegmentTimeIndex(fis, 0L);
            JsonObject jsonObject = timeIndex.getAsJsonObject();
            assertTrue(jsonObject.keySet().contains(TimeIndex.ENTRIES_KEY));
            assertTrue(jsonObject.keySet().contains(TimeIndex.SIZE_KEY));
            int size = jsonObject.getAsJsonPrimitive(TimeIndex.SIZE_KEY).getAsInt();
            int entriesSize = jsonObject.getAsJsonArray(TimeIndex.ENTRIES_KEY).size();
            assertEquals(size, entriesSize);

            timeIndex.insertEntry(new TimeIndex.TimeIndexEntry(0, 0, 0L));
            jsonObject = timeIndex.getAsJsonObject();
            int newSize = jsonObject.getAsJsonPrimitive(TimeIndex.SIZE_KEY).getAsInt();
            int newEntriesSize = jsonObject.getAsJsonArray(TimeIndex.ENTRIES_KEY).size();;
            assertEquals(size + 1, newSize);
            assertEquals(entriesSize + 1, newEntriesSize);
            assertEquals(newSize, newEntriesSize);

            // test removal using specific offset boundary
            Set<TimeIndex.TimeIndexEntry> removed = timeIndex.removeAndGetEntriesOlderThanTimestamp(1753819245508L);
            assertEquals(8, removed.size());
            for (TimeIndex.TimeIndexEntry entry : removed) {
                assertTrue(entry.getTimestamp() < 1753819245508L);
            }
            assertEquals(newSize - 8, timeIndex.size());
            assertEquals(newEntriesSize - 8, timeIndex.getEntriesCopy().size());
            assertEquals(1753819245510L, timeIndex.getFirstEntry().getTimestamp());
            assertEquals(1753819281929L, timeIndex.getLastEntry().getTimestamp());
        } catch (IOException e) {
            fail("Unexpected IOException: " + e);
        }
    }

    @Test
    void testRemoveEntriesBeforeBaseOffsetInclusive() {
        TimeIndex timeIndex = new TimeIndex(TimeIndex.TimeIndexType.SEGMENT);
        timeIndex.insertEntry(new TimeIndex.TimeIndexEntry(1000L, 100, 0L));
        timeIndex.insertEntry(new TimeIndex.TimeIndexEntry(2000L, 200, 0L));
        timeIndex.insertEntry(new TimeIndex.TimeIndexEntry(3000L, 300, 0L));

        assertEquals(3, timeIndex.size());
        timeIndex.removeEntriesBeforeBaseOffsetInclusive(1000L);
        assertEquals(0, timeIndex.size());
        assertEquals(0, timeIndex.getEntriesCopy().size());
        assertNull(timeIndex.getFirstEntry());
        assertNull(timeIndex.getLastEntry());
        
        timeIndex = new TimeIndex(TimeIndex.TimeIndexType.TOPIC_PARTITION);
        timeIndex.insertEntry(new TimeIndex.TimeIndexEntry(1000L, 100, 0L));
        timeIndex.insertEntry(new TimeIndex.TimeIndexEntry(2000L, 100, 1000L));
        timeIndex.insertEntry(new TimeIndex.TimeIndexEntry(3000L, 250, 2000L));

        assertEquals(3, timeIndex.size());
        timeIndex.removeEntriesBeforeBaseOffsetInclusive(1000L);
        assertEquals(1, timeIndex.size());
        assertEquals(1, timeIndex.getEntriesCopy().size());
        assertEquals(2000L, timeIndex.getFirstEntry().getBaseOffset());
        assertEquals(3000L, timeIndex.getFirstEntry().getTimestamp());
        assertEquals(250, timeIndex.getFirstEntry().getRelativeOffset());
        assertEquals(2000L, timeIndex.getLastEntry().getBaseOffset());
        assertEquals(3000L, timeIndex.getLastEntry().getTimestamp());
        assertEquals(250, timeIndex.getLastEntry().getRelativeOffset());
    }

    @Test
    void testInsertEntryMaintainsOrder() {
        TimeIndex timeIndex = new TimeIndex(TimeIndex.TimeIndexType.SEGMENT);

        // Insert entries out of order
        TimeIndex.TimeIndexEntry entry3 = new TimeIndex.TimeIndexEntry(3000L, 300, 0L);
        TimeIndex.TimeIndexEntry entry1 = new TimeIndex.TimeIndexEntry(1000L, 100, 0L);
        TimeIndex.TimeIndexEntry entry2 = new TimeIndex.TimeIndexEntry(2000L, 200, 0L);

        assertTrue(timeIndex.insertEntry(entry3));
        assertTrue(timeIndex.insertEntry(entry1));
        assertTrue(timeIndex.insertEntry(entry2));

        assertEquals(3, timeIndex.size());

        // Verify they are in ascending order
        assertEquals(1000L, timeIndex.getFirstEntry().getTimestamp());
        assertEquals(3000L, timeIndex.getLastEntry().getTimestamp());

        // Check all entries are in order
        assertEquals(1000L, timeIndex.getEntry(0).getTimestamp());
        assertEquals(2000L, timeIndex.getEntry(1).getTimestamp());
        assertEquals(3000L, timeIndex.getEntry(2).getTimestamp());
    }

    @Test
    void testInsertDuplicateEntry() {
        TimeIndex timeIndex = new TimeIndex(TimeIndex.TimeIndexType.SEGMENT);

        TimeIndex.TimeIndexEntry entry1 = new TimeIndex.TimeIndexEntry(1000L, 100, 0L);
        TimeIndex.TimeIndexEntry duplicate = new TimeIndex.TimeIndexEntry(1000L, 100, 0L);

        assertTrue(timeIndex.insertEntry(entry1));
        assertFalse(timeIndex.insertEntry(duplicate)); // Should return false for duplicate

        assertEquals(1, timeIndex.size());
    }

    @Test
    void testInsertSameTimestampDifferentOffset() {
        TimeIndex timeIndex = new TimeIndex(TimeIndex.TimeIndexType.SEGMENT);

        TimeIndex.TimeIndexEntry entry1 = new TimeIndex.TimeIndexEntry(1000L, 100, 0L);
        TimeIndex.TimeIndexEntry entry2 = new TimeIndex.TimeIndexEntry(1000L, 200, 0L);

        assertTrue(timeIndex.insertEntry(entry1));
        assertTrue(timeIndex.insertEntry(entry2)); // Should be added as it has different offset

        assertEquals(2, timeIndex.size());

        // Verify they are sorted by offset when timestamp is same
        assertEquals(100, timeIndex.getEntry(0).getRelativeOffset());
        assertEquals(200, timeIndex.getEntry(1).getRelativeOffset());
    }

    @Test
    void testFindEntryAfterInsert() {
        TimeIndex timeIndex = new TimeIndex(TimeIndex.TimeIndexType.SEGMENT);

        timeIndex.insertEntry(new TimeIndex.TimeIndexEntry(1000L, 100, 0L));
        timeIndex.insertEntry(new TimeIndex.TimeIndexEntry(3000L, 300, 0L));
        timeIndex.insertEntry(new TimeIndex.TimeIndexEntry(2000L, 200, 0L));

        // Test finding entry for exact timestamp
        TimeIndex.TimeIndexEntry found = timeIndex.findEntryForTimestamp(2000L);
        assertNotNull(found);
        assertEquals(2000L, found.getTimestamp());
        assertEquals(200, found.getRelativeOffset());

        // Test finding entry for timestamp between entries
        found = timeIndex.findEntryForTimestamp(1500L);
        assertNotNull(found);
        assertEquals(1000L, found.getTimestamp()); // Should return previous entry

        // Test finding entry for timestamp after all entries
        found = timeIndex.findEntryForTimestamp(4000L);
        assertNotNull(found);
        assertEquals(3000L, found.getTimestamp()); // Should return last entry

        // Test finding entry for timestamp before all entries
        found = timeIndex.findEntryForTimestamp(500L);
        assertNull(found); // Should return null
    }

    @Test
    void testGetRelativeOffsetAfterInsert() {
        TimeIndex timeIndex = new TimeIndex(TimeIndex.TimeIndexType.SEGMENT);

        timeIndex.insertEntry(new TimeIndex.TimeIndexEntry(1000L, 100, 0L));
        timeIndex.insertEntry(new TimeIndex.TimeIndexEntry(3000L, 300, 0L));
        timeIndex.insertEntry(new TimeIndex.TimeIndexEntry(2000L, 200, 0L));

        assertEquals(200, timeIndex.getRelativeOffsetForTimestamp(2000L));
        assertEquals(100, timeIndex.getRelativeOffsetForTimestamp(1500L));
        assertEquals(300, timeIndex.getRelativeOffsetForTimestamp(4000L));
        assertEquals(-1, timeIndex.getRelativeOffsetForTimestamp(500L));
    }

    @Test
    void testInsertIntoEmptyIndex() {
        TimeIndex timeIndex = new TimeIndex(TimeIndex.TimeIndexType.SEGMENT);
        assertTrue(timeIndex.isEmpty());

        TimeIndex.TimeIndexEntry entry = new TimeIndex.TimeIndexEntry(1000L, 100, 0L);
        assertTrue(timeIndex.insertEntry(entry));

        assertFalse(timeIndex.isEmpty());
        assertEquals(1, timeIndex.size());
        assertEquals(entry, timeIndex.getFirstEntry());
        assertEquals(entry, timeIndex.getLastEntry());
    }
}

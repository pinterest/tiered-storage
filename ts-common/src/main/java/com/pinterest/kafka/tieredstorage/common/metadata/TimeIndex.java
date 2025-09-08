package com.pinterest.kafka.tieredstorage.common.metadata;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.reflect.TypeToken;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.concurrent.ConcurrentSkipListSet;

/**
 * Represents a time index file that maps timestamps to relative offsets.
 * Each entry in the file is 12 bytes: 8 bytes for timestamp (long) + 4 bytes for relative offset (int).
 */

public class TimeIndex implements MetadataJsonSerializable {
    private static final int ENTRY_SIZE = 12; // 8 bytes timestamp + 4 bytes offset
    private static final Gson GSON = new GsonBuilder().registerTypeAdapter(TimeIndex.class, new TimeIndexDeserializer()).create();
    public static final String ENTRIES_KEY = "entries";
    public static final String SIZE_KEY = "size";
    private final ConcurrentSkipListSet<TimeIndexEntry> entries;
    private final TimeIndexType timeIndexType;
    private int size = 0;

    public enum TimeIndexType {
        SEGMENT, TOPIC_PARTITION;
    }
    
    private TimeIndex(ConcurrentSkipListSet<TimeIndexEntry> entries, TimeIndexType timeIndexType) {
        this.entries = entries;
        this.size += entries.size();
        this.timeIndexType = timeIndexType;
    }
    
    /**
     * Create a new empty TimeIndex
     */
    public TimeIndex(TimeIndexType timeIndexType) {
        this.entries = new ConcurrentSkipListSet<>();
        this.size = 0;
        this.timeIndexType = timeIndexType;
    }

    public TimeIndexType getTimeIndexType() {
        return timeIndexType;
    }
    
    /**
     * Load a TimeIndex from an InputStream
     * @param inputStream the input stream containing the time index data
     * @return a TimeIndex instance
     * @throws IOException if there's an error reading from the stream
     */
    public static TimeIndex loadFromSegmentTimeIndex(InputStream inputStream, long baseOffset) throws IOException {
        ConcurrentSkipListSet<TimeIndexEntry> entries = new ConcurrentSkipListSet<>();
        
        try (ReadableByteChannel channel = Channels.newChannel(inputStream)) {
            ByteBuffer buffer = ByteBuffer.allocate(ENTRY_SIZE);
            
            while (channel.read(buffer) == ENTRY_SIZE) {
                buffer.flip();
                
                long timestamp = buffer.getLong();
                int relativeOffset = buffer.getInt();
                
                entries.add(new TimeIndex.TimeIndexEntry(timestamp, relativeOffset, baseOffset));
                buffer.clear();
            }
        }
        
        return new TimeIndex(entries, TimeIndexType.SEGMENT);
    }
    
    /**
     * Get the last entry in the time index. This corresponds to the last record in the segment.
     * @return the last TimeIndexEntry, or null if the index is empty
     */
    public TimeIndexEntry getLastEntry() {
        if (entries.isEmpty()) {
            return null;
        }
        return entries.last();
    }
    
    /**
     * Get the first entry in the time index. This does not necessarily correspond to the first record in the segment.
     * @return the first TimeIndexEntry, or null if the index is empty
     */
    public TimeIndexEntry getFirstEntry() {
        if (entries.isEmpty()) {
            return null;
        }
        return entries.first();
    }
    
    /**
     * Get the number of entries in this time index
     * @return the number of entries
     */
    public int size() {
        return size;
    }
    
    /**
     * Check if the time index is empty
     * @return true if the index contains no entries
     */
    public boolean isEmpty() {
        return size == 0;
    }
    
    /**
     * Get an entry by index
     * @param index the index of the entry (0-based)
     * @return the TimeIndexEntry at the specified index
     * @throws IndexOutOfBoundsException if the index is out of range
     */
    public TimeIndexEntry getEntry(int index) {
        if (index < 0 || index >= entries.size()) {
            throw new IndexOutOfBoundsException("Index: " + index + ", Size: " + entries.size());
        }
        
        int currentIndex = 0;
        for (TimeIndexEntry entry : entries) {
            if (currentIndex == index) {
                return entry;
            }
            currentIndex++;
        }
        throw new IndexOutOfBoundsException("Index: " + index + ", Size: " + entries.size());
    }
    
    /**
     * Get all entries in the time index
     * @return a list of all TimeIndexEntry objects
     */
    public ConcurrentSkipListSet<TimeIndexEntry> getEntriesCopy() {
        return new ConcurrentSkipListSet<>(entries);
    }
    
    /**
     * Insert a new entry into the time index while preserving ascending order.
     * The ConcurrentSkipListSet automatically maintains sorted order by timestamp (and relativeOffset as secondary sort).
     * 
     * @param entry the TimeIndexEntry to insert
     * @return true if the entry was added, false if it was already present
     */
    public boolean insertEntry(TimeIndexEntry entry) {
        if (entries.add(entry)) {
            size++;
            return true;
        }
        return false;
    }

    /**
     * Remove the given entry from the timeindex while preserving ascending order.
     * @param entry
     * @return true if the entry was removed, false otherwise
     */
    public boolean removeEntry(TimeIndexEntry entry) {
        if (entries.remove(entry)) {
            size--;
            return true;
        }
        return false;
    }
    
    public ConcurrentSkipListSet<TimeIndexEntry> removeAndGetEntriesOlderThanTimestamp(long timestamp) {
        ConcurrentSkipListSet<TimeIndexEntry> older = new ConcurrentSkipListSet<>(entries.headSet(new TimeIndexEntry(timestamp, -1, -1)));
        entries.removeAll(older);
        size -= older.size();
        return older;
    }

    public int removeEntriesBeforeBaseOffsetInclusive(long baseOffset) {
        entries.removeIf(entry -> entry.getBaseOffset() <= baseOffset);
        int removed = size - entries.size();
        size = entries.size();
        return removed;
    }

    public TimeIndexEntry getHighestEntrySmallerThanTimestamp(long timestamp) {
        return entries.floor(new TimeIndexEntry(timestamp, Integer.MAX_VALUE, Integer.MAX_VALUE));
    }
    
    /**
     * Find the entry with the largest timestamp that is less than or equal to the target timestamp.
     * This is useful for finding the segment position for a given timestamp.
     * 
     * @param targetTimestamp the timestamp to search for
     * @return the TimeIndexEntry with the largest timestamp <= targetTimestamp, or null if no such entry exists
     */
    public TimeIndexEntry findEntryForTimestamp(long targetTimestamp) {
        // Use ConcurrentSkipListSet's floor method for efficient lookup
        // Create a temporary entry with max relativeOffset to get the highest entry with the target timestamp
        TimeIndexEntry targetEntry = new TimeIndexEntry(targetTimestamp, Integer.MAX_VALUE, Integer.MAX_VALUE);
        TimeIndexEntry result = entries.floor(targetEntry);
        
        return result;
    }
    
    /**
     * Find the relative offset for a given timestamp using ConcurrentSkipListSet's efficient lookup.
     * This leverages the sorted nature of ConcurrentSkipListSet for O(log n) performance.
     * 
     * @param targetTimestamp the timestamp to search for
     * @return the relative offset for the largest timestamp <= targetTimestamp, or -1 if no such entry exists
     */
    public int getRelativeOffsetForTimestamp(long targetTimestamp) {
        TimeIndexEntry entry = findEntryForTimestamp(targetTimestamp);
        return entry != null ? entry.getRelativeOffset() : -1;
    }

    @Override
    public JsonObject getAsJsonObject() {
        return GSON.fromJson(getAsJsonString(), JsonObject.class);
    }

    @Override
    public String getAsJsonString() {
        return GSON.toJson(this);
    }

    public static TimeIndex loadFromJson(String jsonString) {
        return GSON.fromJson(jsonString, TimeIndex.class);
    }

    @Override
    public String toString() {
        return String.format("TimeIndex{entries=%d, firstTimestamp=%s, lastTimestamp=%s}", 
                entries.size(), 
                isEmpty() ? "null" : getFirstEntry().getTimestamp(),
                isEmpty() ? "null" : getLastEntry().getTimestamp());
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (this.getClass() != other.getClass()) return false;
        return this.entries.equals(((TimeIndex) other).getEntriesCopy());
    }

    /**
     * Represents a single entry in a Kafka time index file.
     * Each entry contains a timestamp and relative offset.
     */
    public static class TimeIndexEntry implements Comparable<TimeIndexEntry> {
        private final long timestamp;
        private final int relativeOffset;
        private final long baseOffset;
        
        public TimeIndexEntry(long timestamp, int relativeOffset, long baseOffset) {
            this.timestamp = timestamp;
            this.relativeOffset = relativeOffset;
            this.baseOffset = baseOffset;
        }
        
        /**
         * Get the timestamp for this entry
         * @return timestamp in milliseconds since epoch
         */
        public long getTimestamp() {
            return timestamp;
        }
        
        /**
         * Get the relative offset for this entry
         * @return relative offset within the segment
         */
        public int getRelativeOffset() {
            return relativeOffset;
        }

        public long getBaseOffset() {
            return baseOffset;
        }
        
        @Override
        public String toString() {
            return String.format("TimeIndexEntry{timestamp=%d, relativeOffset=%d}", timestamp, relativeOffset);
        }
        
        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null || getClass() != obj.getClass()) return false;
            TimeIndexEntry that = (TimeIndexEntry) obj;
            return timestamp == that.timestamp && relativeOffset == that.relativeOffset;
        }
        
        @Override
        public int hashCode() {
            return Long.hashCode(timestamp) ^ Long.hashCode(baseOffset) ^ Integer.hashCode(relativeOffset);
        }
        
        @Override
        public int compareTo(TimeIndexEntry other) {
            // Primary sort by timestamp
            int timestampComparison = Long.compare(this.timestamp, other.timestamp);
            if (timestampComparison != 0) {
                return timestampComparison;
            }
            // sort by baseOffset to handle duplicate timestamps and offsets
            int baseOffsetComparison = Long.compare(this.baseOffset, other.baseOffset);
            if (baseOffsetComparison != 0) {
                return baseOffsetComparison;
            }
            
            return Integer.compare(this.relativeOffset, other.relativeOffset);

        }
    }

    private static class TimeIndexDeserializer implements JsonDeserializer<TimeIndex> {

        @Override
        public TimeIndex deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) throws JsonParseException {
            JsonObject obj = json.getAsJsonObject();
            Type entriesSetType = new TypeToken<ConcurrentSkipListSet<TimeIndexEntry>>(){}.getType();
            ConcurrentSkipListSet<TimeIndexEntry> entries = context.deserialize(obj.get(ENTRIES_KEY), entriesSetType);
            return new TimeIndex(entries, TimeIndexType.TOPIC_PARTITION);
        }
    }
}

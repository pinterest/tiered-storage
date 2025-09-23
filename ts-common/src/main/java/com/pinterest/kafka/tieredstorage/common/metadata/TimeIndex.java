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
 * Time-index structure for Kafka segments and topic-partitions.
 *
 * <p>Stores ordered pairs of timestamp, relative offset, and base offset
 * to enable efficient lookup of the segment position nearest to a target timestamp.
 * 
 * Instances can be constructed for either a single segment
 * or for an entire topic-partition via {@link TimeIndexType}.</p>
 * 
 * When TimeIndexType is TOPIC_PARTITION, the entries are ordered by timestamp and base offset.
 * When TimeIndexType is SEGMENT, the entries are ordered by relative offset since the base offset is the same for all entries in a segment.
 * 
 * Example TimeIndex JSON structure (TOPIC_PARTITION type):
 * {
 *   "entries": [
 *     {
 *       "timestamp": 1727164800000,
 *       "relativeOffset": 0,
 *       "baseOffset": 100
 *     },
 *     {
 *       "timestamp": 1727164805000,
 *       "relativeOffset": 0,
 *       "baseOffset": 200
 *     },
 *     {
 *       "timestamp": 1727164810000,
 *       "relativeOffset": 0,
 *       "baseOffset": 300
 *     }
 *   ],
 *   "timeIndexType": "TOPIC_PARTITION",
 *   "size": 3
 * }
 * 
 * Example TimeIndex JSON structure (SEGMENT type):
 * {
 *   "entries": [
 *     {
 *       "timestamp": 1727164800000,
 *       "relativeOffset": 30,
 *       "baseOffset": 100
 *     },
 *     {
 *       "timestamp": 1727164815000,
 *       "relativeOffset": 60,
 *       "baseOffset": 100
 *     },
 *     {
 *       "timestamp": 1727164820000,
 *       "relativeOffset": 90,
 *       "baseOffset": 100
 *     }
 *   ],
 *   "timeIndexType": "SEGMENT",
 *   "size": 3
 * }
 */

public class TimeIndex implements MetadataJsonSerializable {
    private static final int ENTRY_SIZE = 12; // 8 bytes timestamp + 4 bytes offset
    private static final Gson GSON = new GsonBuilder().registerTypeAdapter(TimeIndex.class, new TimeIndexDeserializer()).create();
    public static final String ENTRIES_KEY = "entries";
    public static final String SIZE_KEY = "size";
    private final ConcurrentSkipListSet<TimeIndexEntry> entries;
    private final TimeIndexType timeIndexType;
    private int size = 0;

    /**
     * Type of the time index: segment or topic-partition scope.
     */
    public enum TimeIndexType {
        SEGMENT, TOPIC_PARTITION;
    }
    
    private TimeIndex(ConcurrentSkipListSet<TimeIndexEntry> entries, TimeIndexType timeIndexType) {
        this.entries = entries;
        this.size += entries.size();
        this.timeIndexType = timeIndexType;
    }
    
    /**
     * Create a new empty TimeIndex.
     *
     * @param timeIndexType scope of this time index
     */
    public TimeIndex(TimeIndexType timeIndexType) {
        this.entries = new ConcurrentSkipListSet<>();
        this.size = 0;
        this.timeIndexType = timeIndexType;
    }

    /**
     * Return the scope of this time index.
     *
     * @return the {@link TimeIndexType} for this index
     */
    public TimeIndexType getTimeIndexType() {
        return timeIndexType;
    }
    
    /**
     * Load a segment-scoped TimeIndex from the native .timeindex format for a segment.
     *
     * @param inputStream the input stream containing the timeindex bytes
     * @param baseOffset the base offset of the segment that produced the timeindex
     * @return a TimeIndex instance populated from the stream
     * @throws IOException if an error occurs while reading from the stream
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
     * Return the last entry in the time index.
     *
     * @return the last {@link TimeIndexEntry}, or null if the index is empty
     */
    public TimeIndexEntry getLastEntry() {
        if (entries.isEmpty()) {
            return null;
        }
        return entries.last();
    }
    
    /**
     * Return the first entry in the time index.
     *
     * @return the first {@link TimeIndexEntry}, or null if the index is empty
     */
    public TimeIndexEntry getFirstEntry() {
        if (entries.isEmpty()) {
            return null;
        }
        return entries.first();
    }
    
    /**
     * Return the number of entries.
     *
     * @return entry count
     */
    public int size() {
        return size;
    }
    
    /**
     * Whether the time index has no entries.
     *
     * @return true if the index contains no entries
     */
    public boolean isEmpty() {
        return size == 0;
    }
    
    /**
     * Return an entry by ordinal position.
     *
     * @param index zero-based position
     * @return the {@link TimeIndexEntry} at the specified position
     * @throws IndexOutOfBoundsException if index is out of range
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
     * Return a defensive copy of all entries.
     *
     * @return copy of entries in ascending order
     */
    public ConcurrentSkipListSet<TimeIndexEntry> getEntriesCopy() {
        return new ConcurrentSkipListSet<>(entries);
    }
    
    /**
     * Insert a new entry while preserving ascending order.
     *
     * @param entry the {@link TimeIndexEntry} to insert
     * @return true if added, false if already present
     */
    public boolean insertEntry(TimeIndexEntry entry) {
        if (entries.add(entry)) {
            size++;
            return true;
        }
        return false;
    }

    /**
     * Remove the given entry.
     *
     * @param entry entry to remove
     * @return true if removed, false otherwise
     */
    public boolean removeEntry(TimeIndexEntry entry) {
        if (entries.remove(entry)) {
            size--;
            return true;
        }
        return false;
    }
    
    /**
     * Remove and return entries strictly older than the given timestamp.
     *
     * @param timestamp exclusive upper bound for timestamps to remove
     * @return removed entries in ascending order
     */
    public ConcurrentSkipListSet<TimeIndexEntry> removeAndGetEntriesOlderThanTimestamp(long timestamp) {
        ConcurrentSkipListSet<TimeIndexEntry> older = new ConcurrentSkipListSet<>(entries.headSet(new TimeIndexEntry(timestamp, -1, -1)));
        entries.removeAll(older);
        size -= older.size();
        return older;
    }

    /**
     * Remove entries whose baseOffset is less than or equal to the provided value.
     *
     * @param baseOffset inclusive upper bound
     * @return number of entries removed
     */
    public int removeEntriesBeforeBaseOffsetInclusive(long baseOffset) {
        entries.removeIf(entry -> entry.getBaseOffset() <= baseOffset);
        int removed = size - entries.size();
        size = entries.size();
        return removed;
    }

    /**
     * Return the entry with largest timestamp strictly less than the target.
     *
     * @param timestamp target timestamp
     * @return floor entry strictly smaller than timestamp, or null
     */
    public TimeIndexEntry getHighestEntrySmallerThanTimestamp(long timestamp) {
        return entries.floor(new TimeIndexEntry(timestamp, Integer.MAX_VALUE, Integer.MAX_VALUE));
    }
    
    /**
     * Find the entry with the largest timestamp less than or equal to the target.
     *
     * @param targetTimestamp timestamp to search for
     * @return floor entry, or null if no such entry exists
     */
    public TimeIndexEntry findEntryForTimestamp(long targetTimestamp) {
        // Use ConcurrentSkipListSet's floor method for efficient lookup
        // Create a temporary entry with max relativeOffset to get the highest entry with the target timestamp
        TimeIndexEntry targetEntry = new TimeIndexEntry(targetTimestamp, Integer.MAX_VALUE, Integer.MAX_VALUE);
        TimeIndexEntry result = entries.floor(targetEntry);
        
        return result;
    }
    
    /**
     * Return the relative offset for the floor of a target timestamp.
     *
     * @param targetTimestamp timestamp to search for
     * @return relative offset, or -1 if no floor entry exists
     */
    public int getRelativeOffsetForTimestamp(long targetTimestamp) {
        TimeIndexEntry entry = findEntryForTimestamp(targetTimestamp);
        return entry != null ? entry.getRelativeOffset() : -1;
    }

    /** {@inheritDoc} */
    @Override
    public JsonObject getAsJsonObject() {
        return GSON.fromJson(getAsJsonString(), JsonObject.class);
    }

    /** {@inheritDoc} */
    @Override
    public String getAsJsonString() {
        return GSON.toJson(this);
    }

    /**
     * Deserialize a topic-partition-scoped TimeIndex from JSON. This is typically the metadata file for a topic-partition residing in remote storage.
     *
     * @param jsonString json string for a TimeIndex
     * @return deserialized TimeIndex
     */
    public static TimeIndex loadFromJson(String jsonString) {
        return GSON.fromJson(jsonString, TimeIndex.class);
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return String.format("TimeIndex{entries=%d, firstEntry=%s, lastEntry=%s}",
                entries.size(), 
                isEmpty() ? "null" : getFirstEntry(),
                isEmpty() ? "null" : getLastEntry());
    }

    /** {@inheritDoc} */
    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (this.getClass() != other.getClass()) return false;
        return this.entries.equals(((TimeIndex) other).getEntriesCopy());
    }

    /**
     * An ordered entry describing a timestamp and relative offset for a segment.
     */
    public static class TimeIndexEntry implements Comparable<TimeIndexEntry> {
        private final long timestamp;
        private final int relativeOffset;
        private final long baseOffset;
        
        /**
         * Construct a new entry.
         *
         * @param timestamp timestamp in milliseconds since epoch
         * @param relativeOffset relative offset within the segment
         * @param baseOffset base offset of the segment
         */
        public TimeIndexEntry(long timestamp, int relativeOffset, long baseOffset) {
            this.timestamp = timestamp;
            this.relativeOffset = relativeOffset;
            this.baseOffset = baseOffset;
        }
        
        /**
         * Return the timestamp for this entry.
         *
         * @return timestamp in milliseconds since epoch
         */
        public long getTimestamp() {
            return timestamp;
        }
        
        /**
         * Return the relative offset for this entry.
         *
         * @return relative offset within the segment
         */
        public int getRelativeOffset() {
            return relativeOffset;
        }

        /**
         * Return the base offset for this entry.
         *
         * @return base offset of the segment this entry belongs to
         */
        public long getBaseOffset() {
            return baseOffset;
        }
        
        /** {@inheritDoc} */
        @Override
        public String toString() {
            return String.format("TimeIndexEntry{timestamp=%d, relativeOffset=%d, baseOffset=%d}", timestamp, relativeOffset, baseOffset);
        }
        
        /** {@inheritDoc} */
        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null || getClass() != obj.getClass()) return false;
            TimeIndexEntry that = (TimeIndexEntry) obj;
            return timestamp == that.timestamp && relativeOffset == that.relativeOffset;
        }
        
        /** {@inheritDoc} */
        @Override
        public int hashCode() {
            return Long.hashCode(timestamp) ^ Long.hashCode(baseOffset) ^ Integer.hashCode(relativeOffset);
        }
        
        /** {@inheritDoc} */
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

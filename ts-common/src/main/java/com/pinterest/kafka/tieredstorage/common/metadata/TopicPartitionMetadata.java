package com.pinterest.kafka.tieredstorage.common.metadata;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.reflect.TypeToken;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Container for per-topic-partition metadata stored in remote storage.
 *
 * <p>Holds a map of metadata entries keyed by logical names (e.g. {@link #TIMEINDEX_KEY})
 * where values implement {@link MetadataJsonSerializable}. This object is serialized to and
 * from JSON for persistence. Callers may also use {@link #getLoadHash()} to store a version
 * or entity tag loaded from the storage system to support optimistic concurrency.</p>
 */
public class TopicPartitionMetadata implements MetadataJsonSerializable {
    private final static Logger LOG = LogManager.getLogger(TopicPartitionMetadata.class.getName());
    public static final String TIMEINDEX_KEY = "timeindex";
    public static final String TOPIC_PARTITION_KEY = "topicPartition";
    public static final String TOPIC_KEY = "topic";
    public static final String PARTITION_KEY = "partition";
    public static final String METADATA_KEY = "metadata";
    public static final String FILENAME = "_metadata";
    private static final Gson GSON = new GsonBuilder().registerTypeAdapter(TopicPartitionMetadata.class, new TopicPartitionMetadataDeserializer()).create();
    private final TopicPartition topicPartition;
    private final ConcurrentHashMap<String, Object> metadata;
    private String loadHash;

    /**
     * Construct a metadata container with an empty backing map.
     *
     * @param partition the topic-partition identity
     */
    public TopicPartitionMetadata(TopicPartition partition) {
        this(partition, new ConcurrentHashMap<>());
    }

    /**
     * Construct a metadata container with a supplied backing map.
     *
     * @param partition the topic-partition identity
     * @param metadata backing map for metadata entries
     */
    public TopicPartitionMetadata(TopicPartition partition, ConcurrentHashMap<String, Object> metadata) {
        this.topicPartition = partition;
        this.metadata = metadata;
    }

    /**
     * Return the version/hash captured when this object was loaded from storage.
     *
     * @return version or entity tag, or null if not set
     */
    public String getLoadHash() {
        return loadHash;
    }

    /**
     * Set the version/hash captured when loading from storage.
     *
     * @param hash version or entity tag from storage
     */
    public void setLoadHash(String hash) {
        this.loadHash = hash;
    }

    /**
     * Insert or replace a metadata entry.
     *
     * @param key metadata key (e.g., {@link #TIMEINDEX_KEY})
     * @param value metadata value implementing {@link MetadataJsonSerializable}
     */
    public void updateMetadata(String key, MetadataJsonSerializable value) {
        metadata.put(key, value);
    }

    /**
     * Return the topic-partition identity.
     *
     * @return {@link TopicPartition}
     */
    public TopicPartition getTopicPartition() {
        return this.topicPartition;
    }

    /**
     * Return a defensive copy of the metadata map.
     *
     * @return copy of metadata entries
     */
    public Map<String, Object> getMetadataCopy() {
        return new HashMap<>(this.metadata);
    }

    /**
     * Convenience accessor for the time index associated with this topic-partition.
     *
     * @return {@link TimeIndex} or null if not present
     */
    public TimeIndex getTimeIndex() {
        return (TimeIndex) this.metadata.get(TIMEINDEX_KEY);
    }

    /** {@inheritDoc} */
    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (this.getClass() != other.getClass()) return false;
        TopicPartitionMetadata otherMetadata = (TopicPartitionMetadata) other;
        return this.topicPartition.equals(otherMetadata.topicPartition) && this.metadata.equals(otherMetadata.metadata);
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
     * Deserialize a {@link TopicPartitionMetadata} from JSON.
     *
     * @param jsonString JSON string
     * @return deserialized {@link TopicPartitionMetadata}
     */
    public static TopicPartitionMetadata loadFromJson(String jsonString) {
        return GSON.fromJson(jsonString, TopicPartitionMetadata.class);
    }

    private static class TopicPartitionMetadataDeserializer implements JsonDeserializer<TopicPartitionMetadata> {

        /** {@inheritDoc} */
        @Override
        public TopicPartitionMetadata deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) throws JsonParseException {
            JsonObject obj = json.getAsJsonObject();
            Type metadataMapType = new TypeToken<ConcurrentHashMap<String, Object>>(){}.getType();
            ConcurrentHashMap<String, Object> deserializedMetadata = new ConcurrentHashMap<>();
            ConcurrentHashMap<String, Object> metadata = context.deserialize(obj.get(METADATA_KEY), metadataMapType);
            JsonObject metadataObj = obj.getAsJsonObject(METADATA_KEY);
            for (Map.Entry<String, Object> metadataEntry: metadata.entrySet()) {
                Object metadataConcrete = null;
                switch (metadataEntry.getKey()) {
                    case TIMEINDEX_KEY:
                        metadataConcrete = TimeIndex.loadFromJson(metadataObj.getAsJsonObject(TIMEINDEX_KEY).toString());
                        break;
                }
                if (metadataConcrete == null) {
                    LOG.warn("Skipping deserialization of unknown metadata type: " + metadataEntry.getKey());
                    continue;
                }
                deserializedMetadata.put(metadataEntry.getKey(), metadataConcrete);
            }
            String topic = obj.getAsJsonObject(TOPIC_PARTITION_KEY).getAsJsonPrimitive(TOPIC_KEY).getAsString();
            int partition = obj.getAsJsonObject(TOPIC_PARTITION_KEY).getAsJsonPrimitive(PARTITION_KEY).getAsInt();
            return new TopicPartitionMetadata(new TopicPartition(topic, partition), deserializedMetadata);
        }
    }
}

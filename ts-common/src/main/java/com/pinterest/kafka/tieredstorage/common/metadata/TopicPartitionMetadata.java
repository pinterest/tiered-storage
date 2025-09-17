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

    public TopicPartitionMetadata(TopicPartition partition) {
        this(partition, new ConcurrentHashMap<>());
    }

    public TopicPartitionMetadata(TopicPartition partition, ConcurrentHashMap<String, Object> metadata) {
        this.topicPartition = partition;
        this.metadata = metadata;
    }

    public String getLoadHash() {
        return loadHash;
    }

    public void setLoadHash(String hash) {
        this.loadHash = hash;
    }

    public void updateMetadata(String key, MetadataJsonSerializable value) {
        metadata.put(key, value);
    }

    public TopicPartition getTopicPartition() {
        return this.topicPartition;
    }

    public Map<String, Object> getMetadataCopy() {
        return new HashMap<>(this.metadata);
    }

    public TimeIndex getTimeIndex() {
        return (TimeIndex) this.metadata.get(TIMEINDEX_KEY);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (this.getClass() != other.getClass()) return false;
        TopicPartitionMetadata otherMetadata = (TopicPartitionMetadata) other;
        return this.topicPartition.equals(otherMetadata.topicPartition) && this.metadata.equals(otherMetadata.metadata);
    }

    @Override
    public JsonObject getAsJsonObject() {
        return GSON.fromJson(getAsJsonString(), JsonObject.class);
    }

    @Override
    public String getAsJsonString() {
        return GSON.toJson(this);
    }

    public static TopicPartitionMetadata loadFromJson(String jsonString) {
        return GSON.fromJson(jsonString, TopicPartitionMetadata.class);
    }

    private static class TopicPartitionMetadataDeserializer implements JsonDeserializer<TopicPartitionMetadata> {

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

package com.pinterest.kafka.tieredstorage.common.metadata;

import com.google.gson.JsonObject;

/**
 * Contract for metadata objects that support JSON serialization.
 *
 * <p>Implementations are expected to provide both a structured {@link JsonObject}
 * representation and a compact JSON string representation. These APIs are used by
 * higher-level containers (for example {@link TopicPartitionMetadata}) to persist and
 * exchange metadata across components.</p>
 */
public interface MetadataJsonSerializable {

    /**
     * Return a structured JSON representation of this metadata.
     *
     * @return a {@link JsonObject} describing this metadata
     */
    JsonObject getAsJsonObject();

    /**
     * Return a compact JSON string representation of this metadata.
     *
     * @return a JSON string describing this metadata
     */
    String getAsJsonString();

}

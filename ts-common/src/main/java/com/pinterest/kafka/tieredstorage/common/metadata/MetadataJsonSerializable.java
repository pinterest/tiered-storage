package com.pinterest.kafka.tieredstorage.common.metadata;

import com.google.gson.JsonObject;

public interface MetadataJsonSerializable {

    JsonObject getAsJsonObject();

    String getAsJsonString();

}

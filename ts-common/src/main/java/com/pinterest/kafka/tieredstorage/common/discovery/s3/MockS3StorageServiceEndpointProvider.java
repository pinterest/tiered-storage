package com.pinterest.kafka.tieredstorage.common.discovery.s3;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;

/**
 * An example implementation of StorageServiceEndpointProvider that reads from a local file.
 */
public class MockS3StorageServiceEndpointProvider extends S3StorageServiceEndpointProvider {

    private static final String CONFIG_DIRECTORY = "src/test/resources/";
    private static final String BUCKET_KEY = "ts_bucket";
    private static final String BASE_PREFIX_KEY = "ts_base_prefix";
    private static final String OVERRIDES_KEY = "overrides";
    private String clusterId;
    private String globalBucket;
    private String globalBasePrefix;
    private JsonObject overrides;

    @Override
    public void initialize(String clusterId) {
        this.clusterId = clusterId;
        String configFilename = String.format("%s.json", clusterId);
        try (Reader reader = new FileReader(CONFIG_DIRECTORY + configFilename)) {
            Gson gson = new Gson();
            JsonObject jsonObject = gson.fromJson(reader, JsonObject.class);

            globalBucket = jsonObject.get(BUCKET_KEY).getAsString();
            globalBasePrefix = jsonObject.get(BASE_PREFIX_KEY).getAsString();
            overrides = jsonObject.getAsJsonObject(OVERRIDES_KEY);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public S3StorageServiceEndpoint.Builder getStorageServiceEndpointBuilderForTopic(String topic) {
        S3StorageServiceEndpoint.Builder builder = new S3StorageServiceEndpoint.Builder();
        if (overrides != null && overrides.has(topic)) {
            builder.setBucket(overrides.getAsJsonObject(topic).get(BUCKET_KEY).getAsString())
                    .setBasePrefix(overrides.getAsJsonObject(topic).get(BASE_PREFIX_KEY).getAsString());
        } else {
            builder.setBucket(globalBucket).setBasePrefix(globalBasePrefix);
        }
        return builder.setKafkaCluster(clusterId);
    }
}

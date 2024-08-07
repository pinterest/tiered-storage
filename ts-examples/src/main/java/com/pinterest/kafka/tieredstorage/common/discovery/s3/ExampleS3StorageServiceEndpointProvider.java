package com.pinterest.kafka.tieredstorage.common.discovery.s3;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.stream.JsonReader;
import com.pinterest.kafka.tieredstorage.common.discovery.s3.S3StorageServiceEndpoint.Builder;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class ExampleS3StorageServiceEndpointProvider extends S3StorageServiceEndpointProvider {
    private static final Logger LOG = LogManager.getLogger(ExampleS3StorageServiceEndpointProvider.class.getName());
    private static final String CONFIG_DIRECTORY_SYS_PROPERTY_KEY = "storage.service.endpoint.config.directory";
    private static final String BUCKET_PUBLIC_CONFIG = "bucket_public";
    private static final String BUCKET_PII_CONFIG = "bucket_pii";
    private static final String PREFIX_CONFIG = "prefix";
    private static final String TOPICS_CONFIG = "topics";
    private static final String PII_BOOL_CONFIG = "pii";
    private String clusterId;
    private String bucketPublic;
    private String bucketPii;
    private String prefix;

    private final Set<String> piiTopics = new HashSet<>();

    @Override
    public void initialize(String clusterId) throws IOException {
        String configDirectory = System.getProperty(CONFIG_DIRECTORY_SYS_PROPERTY_KEY);
        this.clusterId = clusterId;
        String filename = clusterId + ".json";

        Gson gson = new Gson();
        JsonReader jsonReader = new JsonReader(new FileReader(new File(configDirectory, filename)));
        JsonObject json = gson.fromJson(jsonReader, JsonObject.class);
        LOG.info(String.format("Loading S3 endpoints from file: %s", filename));

        bucketPublic = json.get(BUCKET_PUBLIC_CONFIG).getAsString();
        bucketPii = json.get(BUCKET_PII_CONFIG).getAsString();
        prefix = json.get(PREFIX_CONFIG).getAsString();

        JsonObject topics = json.getAsJsonObject(TOPICS_CONFIG);
        for (String topic: topics.keySet()) {
            JsonObject topicOverrides = topics.getAsJsonObject(topic);
            if (!topicOverrides.has(PII_BOOL_CONFIG)) {
                throw new IllegalArgumentException("Topic " + topic + " must set pii to either true or false");
            }
            if (topicOverrides.get(PII_BOOL_CONFIG).getAsBoolean()) {
                piiTopics.add(topic);
            }
        }
        LOG.info(String.format("Loaded bucketPublic=%s bucketPrivate=%s prefix=%s from %s", bucketPublic, bucketPii, prefix, filename));
        LOG.info("Loaded pii topics: " + piiTopics);
    }

    @Override
    public Builder getStorageServiceEndpointBuilderForTopic(String topic) {
        S3StorageServiceEndpoint.Builder builder = new S3StorageServiceEndpoint.Builder();
        if (piiTopics.contains(topic)) {
            builder.setBucket(bucketPii);
        } else {
            builder.setBucket(bucketPublic);
        }
        return builder.setBasePrefix(prefix).setKafkaCluster(clusterId);
    }
    
}

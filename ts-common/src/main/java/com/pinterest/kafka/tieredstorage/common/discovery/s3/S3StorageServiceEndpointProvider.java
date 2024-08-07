package com.pinterest.kafka.tieredstorage.common.discovery.s3;

import com.pinterest.kafka.tieredstorage.common.discovery.StorageServiceEndpointProvider;

import java.io.IOException;

public abstract class S3StorageServiceEndpointProvider implements StorageServiceEndpointProvider {

    @Override
    public abstract void initialize(String clusterId) throws IOException;

    @Override
    public abstract S3StorageServiceEndpoint.Builder getStorageServiceEndpointBuilderForTopic(String topic);
}

package com.pinterest.kafka.tieredstorage.common.discovery;

import java.io.IOException;

/**
 * StorageServiceEndpointProvider is an interface that provides the endpoint for tiered storage uploader and consumer
 */
public interface StorageServiceEndpointProvider {

    /**
     * Initialize the endpoint provider
     * @param clusterId
     * @throws IOException
     */
    void initialize(String clusterId) throws IOException;

    /**
     * Get the StorageServiceEndpointBuilder for the given topic which incorporates information
     * about cluster and topic (but not topic-partition)
     * @param topic
     * @return A partially constructed StorageServiceEndpointBuilder (missing topic-partition information)
     */
    StorageServiceEndpointBuilder getStorageServiceEndpointBuilderForTopic(String topic);
}

package com.pinterest.kafka.tieredstorage.consumer;

public class TieredStorageConsumerConfig {

    public static final String TIERED_STORAGE_MODE_CONFIG = "tiered.storage.mode";
    public static final String KAFKA_CLUSTER_ID_CONFIG = "kafka.cluster.id";
    public static final String OFFSET_RESET_CONFIG = "offset.reset.policy";
    public static final String STORAGE_SERVICE_ENDPOINT_PROVIDER_CLASS_CONFIG = "storage.service.endpoint.provider.class";
    public static final String STORAGE_SERVICE_ENDPOINT_S3_BUCKET_CONFIG = "storage.service.endpoint.s3.bucket";
    public static final String STORAGE_SERVICE_ENDPOINT_S3_PREFIX_CONFIG = "storage.service.endpoint.s3.prefix";
    public static final String STORAGE_SERVICE_ENDPOINT_S3_PREFIX_ENTROPY_NUM_BITS_CONFIG = "storage.service.endpoint.s3.prefix.entropy.num.bits";
    public static final String STORAGE_SERVICE_ENDPOINT_S3_METADATA_RELOAD_INTERVAL_MS_CONFIG = "storage.service.endpoint.s3.metadata.reload.interval.ms";
}

package com.pinterest.kafka.tieredstorage.uploader;

/**
 * Interface to provide Kafka environment information
 */
public interface KafkaEnvironmentProvider {
    void load();
    String clusterId();
    Integer brokerId();
    String zookeeperConnect();
    String logDir();
}

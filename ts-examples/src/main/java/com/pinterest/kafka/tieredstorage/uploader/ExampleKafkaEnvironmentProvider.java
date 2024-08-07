package com.pinterest.kafka.tieredstorage.uploader;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.FileReader;
import java.util.Properties;

public class ExampleKafkaEnvironmentProvider implements KafkaEnvironmentProvider {

    private static final Logger LOG = LogManager.getLogger(ExampleKafkaEnvironmentProvider.class.getName());
    private static final String KAFKA_SERVER_CONFIG_FILEPATH_SYS_PROPERTY_KEY = "kafka.server.config";
    private int brokerId;
    private String clusterId;
    private String zkConnect;
    private String logDir;

    @Override
    public void load() {
        Properties configProps = new Properties();
        String configFilepath = System.getProperty(KAFKA_SERVER_CONFIG_FILEPATH_SYS_PROPERTY_KEY);
        try {
            configProps.load(new FileReader(configFilepath));
            this.brokerId = Integer.parseInt(configProps.getProperty("broker.id"));
            this.zkConnect = configProps.getProperty("zookeeper.connect");
            this.logDir = configProps.getProperty("log.dirs");
            this.clusterId = "my_test_kafka_cluster"; // arbitrary name for cluster
            LOG.info(String.format("Loaded brokerId=%s, zookeeperConnect=%s, logDir=%s, clusterId=%s from %s", this.brokerId, this.zkConnect, this.logDir, clusterId(), configFilepath));
        } catch (Exception e) {
            LOG.info("Unable to load server.properties from " + configFilepath, e);
        }
    }

    @Override
    public String clusterId() {
        return this.clusterId;
    }

    @Override
    public Integer brokerId() {
        return this.brokerId;
    }

    @Override
    public String zookeeperConnect() {
        return this.zkConnect;
    }

    @Override
    public String logDir() {
        return this.logDir;
    }
}
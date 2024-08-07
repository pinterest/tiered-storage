package com.pinterest.kafka.tieredstorage.uploader;

import com.google.common.annotations.VisibleForTesting;
import com.pinterest.kafka.tieredstorage.common.discovery.StorageServiceEndpointProvider;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

/**
 * Entry point for KafkaSegmentUploader
 */
public class KafkaSegmentUploader {
    private static final Logger LOG = LogManager.getLogger(KafkaSegmentUploader.class);
    private final MultiThreadedS3FileUploader multiThreadedS3FileUploader;
    private final DirectoryTreeWatcher directoryTreeWatcher;
    private final StorageServiceEndpointProvider endpointProvider;
    private final SegmentUploaderConfiguration config;

    public KafkaSegmentUploader(String configDirectory) throws IOException, InterruptedException, KeeperException, ClassNotFoundException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
        Utils.acquireLock();
        KafkaEnvironmentProvider environmentProvider = getEnvironmentProvider();
        environmentProvider.load();

        config = new SegmentUploaderConfiguration(configDirectory, environmentProvider.clusterId());

        endpointProvider = getEndpointProviderFromConfigs(config);
        endpointProvider.initialize(environmentProvider.clusterId());

        multiThreadedS3FileUploader = new MultiThreadedS3FileUploader(endpointProvider, config, environmentProvider);
        directoryTreeWatcher = new DirectoryTreeWatcher(multiThreadedS3FileUploader, config, environmentProvider);
    }

    @VisibleForTesting
    protected KafkaSegmentUploader(String configDirectory, KafkaEnvironmentProvider environmentProvider) throws IOException, ClassNotFoundException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException, InterruptedException, KeeperException {
        Utils.acquireLock();
        environmentProvider.load();
        config = new SegmentUploaderConfiguration(configDirectory, environmentProvider.clusterId());

        endpointProvider = getEndpointProviderFromConfigs(config);
        endpointProvider.initialize(environmentProvider.clusterId());

        multiThreadedS3FileUploader = new MultiThreadedS3FileUploader(endpointProvider, config, environmentProvider);
        directoryTreeWatcher = new DirectoryTreeWatcher(multiThreadedS3FileUploader, config, environmentProvider);
    }

    public void start() {
        directoryTreeWatcher.start();
    }

    public void stop() throws InterruptedException, IOException {
        directoryTreeWatcher.stop();
        multiThreadedS3FileUploader.stop();
        Utils.releaseLock();
    }

    private KafkaEnvironmentProvider getEnvironmentProvider() throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
        String kafkaEnvironmentProviderClass = System.getProperty("kafkaEnvironmentProviderClass");
        if (kafkaEnvironmentProviderClass == null) {
            throw new RuntimeException("kafkaEnvironmentProviderClass must be set as a JVM argument");
        }
        LOG.info(String.format("KafkaEnvironmentProvider: %s", kafkaEnvironmentProviderClass));
        Constructor<? extends KafkaEnvironmentProvider> environmentProviderConstructor = Class.forName(kafkaEnvironmentProviderClass)
                .asSubclass(KafkaEnvironmentProvider.class).getConstructor();
        return environmentProviderConstructor.newInstance();
    }

    @VisibleForTesting
    protected StorageServiceEndpointProvider getEndpointProvider() {
        return endpointProvider;
    }

    @VisibleForTesting
    protected SegmentUploaderConfiguration getSegmentUploaderConfiguration() {
        return config;
    }

    private StorageServiceEndpointProvider getEndpointProviderFromConfigs(SegmentUploaderConfiguration config) throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
        Constructor<? extends StorageServiceEndpointProvider> endpointProviderConstructor = Class.forName(config.getStorageServiceEndpointProviderClassName())
                .asSubclass(StorageServiceEndpointProvider.class).getConstructor();
        return endpointProviderConstructor.newInstance();
    }

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException, ConfigurationException, ClassNotFoundException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
        if (args.length != 1) {
            LOG.error("configDirectory is required as an argument");
            System.exit(1);
        }
        String configDirectory = args[0];
        KafkaSegmentUploader kafkaSegmentUploader = new KafkaSegmentUploader(configDirectory);
        try {
            kafkaSegmentUploader.start();
            LOG.info("KafkaSegmentUploader started.");
        } catch (Exception e) {
            kafkaSegmentUploader.stop();
            LOG.error("KafkaSegmentUploader stopped.", e);
        }
    }
}

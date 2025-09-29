package com.pinterest.kafka.tieredstorage.uploader;

import com.google.common.annotations.VisibleForTesting;
import com.pinterest.kafka.tieredstorage.common.discovery.StorageServiceEndpointProvider;
import com.pinterest.kafka.tieredstorage.uploader.leadership.LeadershipWatcher;
import com.pinterest.kafka.tieredstorage.uploader.management.SegmentManager;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
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

    public KafkaSegmentUploader(String configDirectory) throws Exception {
        this(configDirectory, getEnvironmentProvider());
    }

    @VisibleForTesting
    protected KafkaSegmentUploader(String configDirectory, KafkaEnvironmentProvider environmentProvider) throws Exception {
        Utils.acquireLock();
        environmentProvider.load();
        config = new SegmentUploaderConfiguration(configDirectory, environmentProvider.clusterId());

        endpointProvider = getEndpointProviderFromConfigs(config);
        endpointProvider.initialize(environmentProvider.clusterId());

        multiThreadedS3FileUploader = new MultiThreadedS3FileUploader(endpointProvider, config, environmentProvider);
        directoryTreeWatcher = new DirectoryTreeWatcher(multiThreadedS3FileUploader, config, environmentProvider);

        LeadershipWatcher leadershipWatcher = LeadershipWatcher.createLeadershipWatcher(directoryTreeWatcher, config, environmentProvider);
        SegmentManager segmentManager = SegmentManager.createSegmentManager(config, environmentProvider, endpointProvider, leadershipWatcher);
        DirectoryTreeWatcher.setLeadershipWatcher(leadershipWatcher);
        DirectoryTreeWatcher.setSegmentManager(segmentManager);

        directoryTreeWatcher.initialize();
    }

    public void start() {
        directoryTreeWatcher.start();
    }

    public void stop() throws InterruptedException, IOException {
        directoryTreeWatcher.stop();
        multiThreadedS3FileUploader.stop();
        Utils.releaseLock();
    }

    private static KafkaEnvironmentProvider getEnvironmentProvider() throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
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

    private static StorageServiceEndpointProvider getEndpointProviderFromConfigs(SegmentUploaderConfiguration config) throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
        String endpointProviderClassName = config.getStorageServiceEndpointProviderClassName();
        LOG.info(String.format("StorageServiceEndpointProvider: %s", endpointProviderClassName));
        Constructor<? extends StorageServiceEndpointProvider> endpointProviderConstructor = Class.forName(endpointProviderClassName)
                .asSubclass(StorageServiceEndpointProvider.class).getConstructor();
        return endpointProviderConstructor.newInstance();
    }

    public static void main(String[] args) throws Exception {
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

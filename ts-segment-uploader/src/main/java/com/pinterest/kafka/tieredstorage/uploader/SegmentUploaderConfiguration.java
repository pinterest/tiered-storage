package com.pinterest.kafka.tieredstorage.uploader;

import com.google.common.annotations.VisibleForTesting;
import com.pinterest.kafka.tieredstorage.common.metrics.MetricsConfiguration;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

/**
 * SegmentUploaderConfiguration is a class that reads and holds the configuration for the SegmentUploader
 */
public class SegmentUploaderConfiguration {

    private static final Logger LOG = LogManager.getLogger(SegmentUploaderConfiguration.class);
    private static final String TS_SEGMENT_UPLOADER_PREFIX = "ts.segment.uploader";
    private static final String KAFKA_PREFIX = TS_SEGMENT_UPLOADER_PREFIX + "." + "kafka";
    private static final String TOPICS_INCLUDE_PREFIX = KAFKA_PREFIX + "." + "topics.include";
    private static final String TOPICS_EXCLUDE_PREFIX = KAFKA_PREFIX + "." + "topics.exclude";
    private static final String STORAGE_SERVICE_ENDPOINT_PROVIDER_PREFIX = "storage.service.endpoint.provider";
    private static final String STORAGE_SERVICE_ENDPOINT_PROVIDER_CLASS_KEY = STORAGE_SERVICE_ENDPOINT_PROVIDER_PREFIX + "." + "class";
    private static final String OFFSET_RESET_STRATEGY_KEY = TS_SEGMENT_UPLOADER_PREFIX + "." + "offset.reset.strategy";
    private static final String UPLOADER_THREAD_COUNT_KEY = TS_SEGMENT_UPLOADER_PREFIX + "." + "upload.thread.count";
    private static final String ZK_WATCHER_POLL_INTERVAL_SECONDS = TS_SEGMENT_UPLOADER_PREFIX + "." + "zk.watcher.poll.interval.seconds";
    private static final String S3_PREFIX_ENTROPY_BITS = TS_SEGMENT_UPLOADER_PREFIX + "." + "s3.prefix.entropy.bits";
    private static final String UPLOAD_TIMEOUT_MS = TS_SEGMENT_UPLOADER_PREFIX + "." + "upload.timeout.ms";
    private static final String UPLOAD_MAX_RETRIES = TS_SEGMENT_UPLOADER_PREFIX + "." + "upload.max.retries";
    private final Properties properties = new Properties();
    private final Set<Pattern> includeRegexes = ConcurrentHashMap.newKeySet();
    private final Set<Pattern> excludeRegexes = ConcurrentHashMap.newKeySet();
    private final Set<String> includeTopicsCache = ConcurrentHashMap.newKeySet();
    private final Set<String> excludeTopicsCache = ConcurrentHashMap.newKeySet();
    private final String storageServiceEndpointProviderClassName;
    private final MetricsConfiguration metricsConfiguration;

    public SegmentUploaderConfiguration(String configDirectory, String clusterId) throws IOException {
        String filename = clusterId + ".properties";
        try {
            InputStream inputStream = Files.newInputStream(new File(configDirectory, filename).toPath());
            LOG.info(String.format("Loading SegmentUploaderConfiguration file: %s", filename));
            properties.load(inputStream);

            loadPatterns(includeRegexes, TOPICS_INCLUDE_PREFIX);
            loadPatterns(excludeRegexes, TOPICS_EXCLUDE_PREFIX);

            if (!properties.containsKey(STORAGE_SERVICE_ENDPOINT_PROVIDER_CLASS_KEY)) {
                throw new RuntimeException(String.format("Configuration %s must be provided", STORAGE_SERVICE_ENDPOINT_PROVIDER_CLASS_KEY));
            }
            storageServiceEndpointProviderClassName = properties.getProperty(STORAGE_SERVICE_ENDPOINT_PROVIDER_CLASS_KEY);

            metricsConfiguration = MetricsConfiguration.getMetricsConfiguration(properties);

            LOG.info(String.format("Loaded SegmentUploaderConfiguration from file: %s", filename));
            LOG.info(String.format("Exclude regexes: %s", excludeRegexes));
            LOG.info(String.format("Include regexes: %s", includeRegexes));
            LOG.info(String.format("Segment uploader properties: %s", properties));
        } catch (IOException e) {
            throw new IOException("Error in initializing SegmentUploaderConfiguration", e);
        }
    }

    public boolean deleteTopic(String topicName) {
        return excludeTopicsCache.remove(topicName) || includeTopicsCache.remove(topicName);
    }

    /**
     * Check if we should watch the topic. If the topic is in the exclude list, we should not watch it. If the topic is in the include list, we should watch it as long as it does
     * not match a regex provided in the exclude list. In other words, the exclude regexes take precedence over the include regexes.
     * @param topicName
     * @return
     */
    public boolean shouldWatchTopic(String topicName) {
        // prioritize exclusions
        if (excludeTopicsCache.contains(topicName)) {
            LOG.debug(String.format("Cache hit for excludeTopicsCache: %s", topicName));
            return false;
        }
        if (includeTopicsCache.contains(topicName)) {
            LOG.debug(String.format("Cache hit for includeTopicsCache: %s", topicName));
            return true;
        }
        for (Pattern excludeRegex : excludeRegexes) {
            if (excludeRegex.matcher(topicName).matches()) {
                excludeTopicsCache.add(topicName);
                LOG.debug(String.format("topicName %s matches %s exclusion pattern", topicName, excludeRegex));
                return false;
            }
        }
        for (Pattern includeRegex : includeRegexes) {
            if (includeRegex.matcher(topicName).matches()) {
                includeTopicsCache.add(topicName);
                LOG.debug(String.format("topicName %s matches %s inclusion pattern", topicName, includeRegex));
                return true;
            }
        }
        LOG.debug(String.format("topicName %s not found in any include/exclude cache and regex; by default we will not watch this topic", topicName));
        return false;   // default to false
    }

    private void loadPatterns(Set<Pattern> toAdd, String prefix) {
        String[] regexes = properties.getProperty(prefix).replace(" ", "").split(",");
        for (String regex : regexes) {
            Pattern pattern = Pattern.compile(regex);
            toAdd.add(pattern);
        }
    }

    public String getStorageServiceEndpointProviderClassName() {
        return this.storageServiceEndpointProviderClassName;
    }

    public MetricsConfiguration getMetricsConfiguration() {
        return this.metricsConfiguration;
    }

    public int getUploadThreadCount() {
        if (properties.containsKey(UPLOADER_THREAD_COUNT_KEY)) {
            return Integer.parseInt(properties.getProperty(UPLOADER_THREAD_COUNT_KEY));
        }
        return Defaults.DEFAULT_UPLOADER_THREAD_POOL_SIZE;
    }

    public int getZkWatcherPollIntervalSeconds() {
        if (properties.containsKey(ZK_WATCHER_POLL_INTERVAL_SECONDS)) {
            return Integer.parseInt(properties.getProperty(ZK_WATCHER_POLL_INTERVAL_SECONDS));
        }
        return Defaults.DEFAULT_ZK_WATCHER_POLL_INTERVAL_SECONDS;
    }

    public OffsetResetStrategy getOffsetResetStrategy() {
        return OffsetResetStrategy.valueOf(properties.getProperty(OFFSET_RESET_STRATEGY_KEY, Defaults.DEFAULT_OFFSET_RESET_STRATEGY).toUpperCase());
    }

    public int getS3PrefixEntropyBits() {
        if (properties.containsKey(S3_PREFIX_ENTROPY_BITS)) {
            return Integer.parseInt(properties.getProperty(S3_PREFIX_ENTROPY_BITS));
        }
        return Defaults.DEFAULT_S3_PREFIX_ENTROPY_BITS;
    }

    public long getUploadTimeoutMs() {
        return Long.parseLong(properties.getProperty(UPLOAD_TIMEOUT_MS, String.valueOf(Defaults.DEFAULT_UPLOAD_TIMEOUT_MS)));
    }

    public int getUploadMaxRetries() {
        return Integer.parseInt(properties.getProperty(UPLOAD_MAX_RETRIES, String.valueOf(Defaults.DEFAULT_UPLOAD_MAX_RETRIES)));
    }

    @VisibleForTesting
    protected boolean isInInclusionCache(String topicName) {
        return includeTopicsCache.contains(topicName);
    }

    @VisibleForTesting
    protected boolean isInExclusionCache(String topicName) {
        return excludeTopicsCache.contains(topicName);
    }

    public enum OffsetResetStrategy {
        EARLIEST, LATEST
    }

    public static class Defaults {
        private static final String DEFAULT_OFFSET_RESET_STRATEGY = "EARLIEST";
        private static final int DEFAULT_UPLOADER_THREAD_POOL_SIZE = 3;
        private static final int DEFAULT_ZK_WATCHER_POLL_INTERVAL_SECONDS = 60;
        private static final int DEFAULT_S3_PREFIX_ENTROPY_BITS = -1;
        private static final int DEFAULT_UPLOAD_TIMEOUT_MS = 60000;
        private static final int DEFAULT_UPLOAD_MAX_RETRIES = 3;
    }
}

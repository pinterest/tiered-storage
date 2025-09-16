package com.pinterest.kafka.tieredstorage.uploader.management;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.pinterest.kafka.tieredstorage.common.discovery.StorageServiceEndpointProvider;
import com.pinterest.kafka.tieredstorage.common.metadata.TimeIndex;
import com.pinterest.kafka.tieredstorage.common.metadata.TopicPartitionMetadata;
import com.pinterest.kafka.tieredstorage.common.metadata.TopicPartitionMetadataUtil;
import com.pinterest.kafka.tieredstorage.common.metrics.MetricRegistryManager;
import com.pinterest.kafka.tieredstorage.uploader.KafkaEnvironmentProvider;
import com.pinterest.kafka.tieredstorage.uploader.SegmentUploaderConfiguration;
import com.pinterest.kafka.tieredstorage.uploader.UploaderMetrics;
import com.pinterest.kafka.tieredstorage.uploader.leadership.LeadershipWatcher;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import static com.pinterest.kafka.tieredstorage.uploader.SegmentUploaderConfiguration.TS_SEGMENT_UPLOADER_PREFIX;

import java.io.IOException;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

public abstract class SegmentManager {
    private static final Logger LOG = LogManager.getLogger(SegmentManager.class.getName());
    private static final String SEGMENT_MANAGER_CLASS_CONFIG_KEY = TS_SEGMENT_UPLOADER_PREFIX + "." + "segment.manager.class";
    private ScheduledExecutorService garbageCollectionExecutor;
    protected final SegmentUploaderConfiguration config;
    protected final KafkaEnvironmentProvider environmentProvider;
    protected final StorageServiceEndpointProvider endpointProvider;
    protected final LeadershipWatcher leadershipWatcher;

    public SegmentManager(SegmentUploaderConfiguration config, KafkaEnvironmentProvider environmentProvider, StorageServiceEndpointProvider endpointProvider, LeadershipWatcher leadershipWatcher) {
        this.config = config;
        this.environmentProvider = environmentProvider;
        this.endpointProvider = endpointProvider;
        this.leadershipWatcher = leadershipWatcher;
        if (isGcEnabled()) {
            ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("segment-manager-gc-thread-%d").build();
            this.garbageCollectionExecutor = Executors.newSingleThreadScheduledExecutor(threadFactory);
        }
        initialize();
    }

    public abstract void initialize();

    public void runGarbageCollection() {
        long startTs = System.currentTimeMillis();
        Set<TopicPartition> leadingPartitions = leadershipWatcher.getLeadingPartitions();
        LOG.info(String.format("Starting garbage collection for %s partitions: %s", leadingPartitions.size(), leadingPartitions));
        for (TopicPartition leadingPartition: leadingPartitions) {
            long startTsForTopicPartition = System.currentTimeMillis();
            int retentionSeconds = config.getSegmentManagerGcRetentionSeconds(leadingPartition.topic());
            long cutoffTimestamp = System.currentTimeMillis() - Duration.ofSeconds(retentionSeconds).toMillis();

            LOG.info(String.format("Executing garbage collection for topicPartition=%s with retention=%ss and cutoffTimestamp=%s", leadingPartition, retentionSeconds, cutoffTimestamp));

            // get cutoff offset from metadata based on timestamp
            boolean lockAcquired = TopicPartitionMetadataUtil.tryAcquireLock(leadingPartition, 5000L);
            if (!lockAcquired) {
                LOG.warn("Failed to acquire lock for TopicPartitionMetadata for " + leadingPartition + ", skipping since it is best-effort");
                continue;
            }
            long cutoffOffset = -1L;
            try {
                TopicPartitionMetadata tpMetadata;
                try {
                    tpMetadata = getTopicPartitionMetadataFromStorage(leadingPartition);
                    if (tpMetadata == null) {
                        // create new metadata
                        tpMetadata = new TopicPartitionMetadata(leadingPartition);
                        TimeIndex timeIndex = new TimeIndex(TimeIndex.TimeIndexType.TOPIC_PARTITION);
                        tpMetadata.updateMetadata(TopicPartitionMetadata.TIMEINDEX_KEY, timeIndex);
                    }
                } catch (IOException e) {
                    LOG.warn("Failed to get TopicPartitionMetadata for " + leadingPartition + ", skipping since it is best-effort", e);
                    continue;
                }
                TimeIndex timeIndex = tpMetadata.getTimeIndex();
                TimeIndex.TimeIndexEntry cutoffEntry = timeIndex.getHighestEntrySmallerThanTimestamp(cutoffTimestamp);
                if (cutoffEntry == null) {
                    // nothing is expired according to metadata timeindex
                    LOG.info(String.format("No segments are expired for topicPartition=%s", leadingPartition));
                    continue;
                }
                cutoffOffset = cutoffEntry.getBaseOffset();    // everything including and before this offset should be removed
                LOG.info(String.format("Cutoff offset for topicPartition=%s is %s", leadingPartition, cutoffOffset));

                // first update metadata
                int removed = timeIndex.removeEntriesBeforeBaseOffsetInclusive(cutoffOffset);
                LOG.info(String.format("Removed %s entries from timeindex for topicPartition=%s", removed, leadingPartition));
                long metadataWriteStartTs = System.currentTimeMillis();
                boolean metadataUpdateSuccess = writeMetadataToStorage(tpMetadata);
                if (metadataUpdateSuccess) {
                    MetricRegistryManager.getInstance(config.getMetricsConfiguration()).updateCounter(
                            leadingPartition.topic(),
                            leadingPartition.partition(),
                            UploaderMetrics.METADATA_UPDATE_LATENCY_MS_METRIC,
                            System.currentTimeMillis() - metadataWriteStartTs,
                            "cluster=" + environmentProvider.clusterId(),
                            "broker=" + environmentProvider.brokerId(),
                            "update_reason=gc"
                    );
                    MetricRegistryManager.getInstance(config.getMetricsConfiguration()).updateCounter(
                            leadingPartition.topic(),
                            leadingPartition.partition(),
                            UploaderMetrics.SEGMENT_MANAGER_LOG_START_OFFSET_METRIC,
                            timeIndex.getFirstEntry().getBaseOffset(),
                            "cluster=" + environmentProvider.clusterId(),
                            "broker=" + environmentProvider.brokerId()
                    );
                    LOG.info("Successfully updated TopicPartitionMetadata for " + leadingPartition);
                } else {
                    LOG.warn("Failed to update TopicPartitionMetadata for " + leadingPartition + ", skipping since it is best-effort");
                    MetricRegistryManager.getInstance(config.getMetricsConfiguration()).incrementCounter(
                            leadingPartition.topic(),
                            leadingPartition.partition(),
                            UploaderMetrics.METADATA_UPDATE_FAILURE_COUNT_METRIC,
                            "cluster=" + environmentProvider.clusterId(),
                            "broker=" + environmentProvider.brokerId(),
                            "update_reason=gc"
                    );
                    // continue to next partition since we don't want to remove any segments if metadata update fails
                    continue;
                }
            } finally {
                TopicPartitionMetadataUtil.releaseLock(leadingPartition);
            }

            if (cutoffOffset < 0) {
                LOG.warn("Failed to find cutoff offset for topicPartition=" + leadingPartition);
                continue;
            }

            // clean up actual data
            Set<Long> deletedSegments = deleteSegmentsBeforeBaseOffsetInclusive(leadingPartition, cutoffOffset);
            LOG.info(String.format("Deleted segments before and including baseOffset=%s for topicPartition=%s: %s", cutoffOffset, leadingPartition, deletedSegments));
            long duration = System.currentTimeMillis() - startTsForTopicPartition;
            LOG.info(String.format("Completed garbage collection for %s in %sms", leadingPartition, duration));
        }
        long cycleDuration = System.currentTimeMillis() - startTs;
        MetricRegistryManager.getInstance(config.getMetricsConfiguration()).updateCounter(
            null,
            null,
            UploaderMetrics.SEGMENT_MANAGER_GC_DURATION_MS_METRIC,
            cycleDuration,
            "cluster=" + environmentProvider.clusterId(),
            "broker=" + environmentProvider.brokerId()
        );
        LOG.info(String.format("Completed garbage collection for all %s topicPartitions in %sms", leadingPartitions.size(), cycleDuration));
    }

    public abstract TopicPartitionMetadata getTopicPartitionMetadataFromStorage(TopicPartition topicPartition) throws IOException;

    public abstract boolean writeMetadataToStorage(TopicPartitionMetadata tpMetadata);

    public abstract Set<Long> deleteSegmentsBeforeBaseOffsetInclusive(TopicPartition topicPartition, long baseOffset);

    /**
     * Check if garbage collection is enabled.
     * @return true if garbage collection is enabled, false otherwise
     */
    protected boolean isGcEnabled() {
        return config.getSegmentManagerGcIntervalSeconds() > 0;
    }

    public void start() {
        if (isGcEnabled()) {
            garbageCollectionExecutor.scheduleAtFixedRate(
                    this::runGarbageCollection,
                    config.getSegmentManagerGcIntervalSeconds() / 2,    // initial delay to prevent metadata contention with potentially large volumes of uploads upon startup
                    config.getSegmentManagerGcIntervalSeconds(),
                    TimeUnit.SECONDS
            );
        }
        LOG.info(String.format("Started SegmentManager with GC interval: %d seconds", config.getSegmentManagerGcIntervalSeconds()));
    }

    public void stop() {
        garbageCollectionExecutor.shutdown();
        LOG.info("Stopped SegmentManager");
    }

    public static SegmentManager createSegmentManager(SegmentUploaderConfiguration config, KafkaEnvironmentProvider environmentProvider, StorageServiceEndpointProvider endpointProvider, LeadershipWatcher leadershipWatcher) {
        String className = config.getProperty(SEGMENT_MANAGER_CLASS_CONFIG_KEY);
        if (className == null) {
            throw new IllegalArgumentException("Missing required property: " + SEGMENT_MANAGER_CLASS_CONFIG_KEY);
        }
        try {
            return (SegmentManager) Class.forName(className).getConstructor(SegmentUploaderConfiguration.class, KafkaEnvironmentProvider.class, StorageServiceEndpointProvider.class, LeadershipWatcher.class).newInstance(config, environmentProvider, endpointProvider, leadershipWatcher);
        } catch (Exception e) {
            throw new RuntimeException("Failed to create SegmentManager", e);
        }
    }
}

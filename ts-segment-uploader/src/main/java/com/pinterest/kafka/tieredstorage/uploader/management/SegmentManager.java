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

/**
 * Abstract class to manage the remote segments and metadata. The concrete implementation is determined by the
 * {@link #SEGMENT_MANAGER_CLASS_CONFIG_KEY} configuration.
 */
public abstract class SegmentManager {
    private static final Logger LOG = LogManager.getLogger(SegmentManager.class.getName());
    private static final String SEGMENT_MANAGER_CLASS_CONFIG_KEY = TS_SEGMENT_UPLOADER_PREFIX + "." + "segment.manager.class";
    private ScheduledExecutorService garbageCollectionExecutor;
    protected final SegmentUploaderConfiguration config;
    protected final KafkaEnvironmentProvider environmentProvider;
    protected final StorageServiceEndpointProvider endpointProvider;
    protected final LeadershipWatcher leadershipWatcher;

    /**
     * Create a new SegmentManager instance.
     *
     * <p>If garbage collection is enabled by configuration, this constructor also
     * initializes a single-threaded scheduled executor to run GC in the background
     * after {@link #start()} is invoked. Finally, it calls {@link #initialize()} so
     * that implementations can perform any setup.</p>
     *
     * @param config uploader configuration
     * @param environmentProvider provider for cluster and broker identity
     * @param endpointProvider provider for storage service endpoints
     * @param leadershipWatcher watcher that reports the set of leading partitions
     */
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

    /**
     * Perform implementation-specific initialization. Called from the constructor.
     */
    public abstract void initialize();

    /**
     * Run one garbage collection cycle across all leading partitions.
     *
     * <p>The GC process determines a cutoff timestamp from the configured retention,
     * converts it to a cutoff offset using metadata, updates the metadata, and then
     * deletes expired segment files up to and including the cutoff offset. The process
     * is best-effort and records metrics and logs for success and failure cases.</p>
     */
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
                MetricRegistryManager.getInstance(config.getMetricsConfiguration()).incrementCounter(
                    leadingPartition.topic(),
                    leadingPartition.partition(),
                    UploaderMetrics.METADATA_UPDATE_LOCK_ACQUISITION_FAILURE_METRIC,
                    "cluster=" + environmentProvider.clusterId(),
                    "broker=" + environmentProvider.brokerId(),
                    "update_reason=gc"
                );
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
                            timeIndex.getFirstEntry() == null ? -1L : timeIndex.getFirstEntry().getBaseOffset(),
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
            } catch (Exception e) {
                LOG.error("Encountered unexpected exception while executing garbage collection for topicPartition=" + leadingPartition, e);
                MetricRegistryManager.getInstance(config.getMetricsConfiguration()).incrementCounter(
                    leadingPartition.topic(),
                    leadingPartition.partition(),
                    UploaderMetrics.SEGMENT_MANAGER_GC_EXCEPTION_METRIC,
                    "cluster=" + environmentProvider.clusterId(),
                    "broker=" + environmentProvider.brokerId()
                );
                continue;
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

    /**
     * Load the current {@link TopicPartitionMetadata} for the given topic-partition from the
     * underlying storage system. The implementation of this method should set the {@link TopicPartitionMetadata#getLoadHash()} 
     * to a unique hash of the metadata at the time of read / load to enable optimistic concurrency control when writing.
     *
     * @param topicPartition topic-partition for which to load metadata
     * @return the loaded metadata, or null if no metadata exists yet
     * @throws IOException if metadata cannot be read due to I/O issues
     */
    public abstract TopicPartitionMetadata getTopicPartitionMetadataFromStorage(TopicPartition topicPartition) throws IOException;

    /**
     * Persist the provided {@link TopicPartitionMetadata} to the underlying storage system. This method should not throw an exception if the write fails.
     * Instead, it should return false to indicate failure since metadata updates are best-effort.
     * 
     * The implementation of this method should also implement optimistic concurrency control by checking the {@link TopicPartitionMetadata#getLoadHash()} which was set during
     * read / load in the {@link #getTopicPartitionMetadataFromStorage(TopicPartition)} method. It should only overwrite the metadata if the existing metadata has the same load hash prior
     * to this write.
     *
     * @param tpMetadata metadata to write
     * @return true if the write succeeds, false otherwise
     */
    public abstract boolean writeMetadataToStorage(TopicPartitionMetadata tpMetadata);

    /**
     * Delete all segment files for the given topic-partition whose base offsets are less than or equal to
     * the supplied baseOffset.
     *
     * @param topicPartition topic-partition to delete from
     * @param baseOffset inclusive upper bound for base offsets to delete
     * @return the set of base offsets that were actually deleted
     */
    public abstract Set<Long> deleteSegmentsBeforeBaseOffsetInclusive(TopicPartition topicPartition, long baseOffset);

    /**
     * Check if garbage collection is enabled.
     * @return true if garbage collection is enabled, false otherwise
     */
    protected boolean isGcEnabled() {
        return config.getSegmentManagerGcIntervalSeconds() > 0;
    }

    /**
     * Start background services such as periodic garbage collection if enabled by configuration.
     */
    public void start() {
        if (isGcEnabled()) {
            // jitter to prevent metadata contention upon startup and concurrent request rate spikes. This delay will be between 5 and 10 minutes.
            int initialDelay = 60 * 5 + (int) (Math.random() * 60 * 5);
            garbageCollectionExecutor.scheduleAtFixedRate(
                    this::runGarbageCollection,
                    initialDelay,
                    config.getSegmentManagerGcIntervalSeconds(),
                    TimeUnit.SECONDS
            );
            LOG.info(String.format("Started SegmentManager with GC interval: %d seconds, delay: %d seconds", config.getSegmentManagerGcIntervalSeconds(), initialDelay));
        } else {
            LOG.info("Garbage collection is disabled. Will not start garbage collection executor.");
        }
    }

    /**
     * Stop background services and release resources held by this manager.
     */
    public void stop() {
        garbageCollectionExecutor.shutdown();
        LOG.info("Stopped SegmentManager");
    }

    /**
     * Create a {@link SegmentManager} implementation via reflection using the class name specified in
     * configuration.
     *
     * @param config uploader configuration
     * @param environmentProvider provider for cluster and broker identity
     * @param endpointProvider provider for storage service endpoints
     * @param leadershipWatcher watcher that reports the set of leading partitions
     * @return a concrete {@link SegmentManager} implementation
     * @throws IllegalArgumentException if the configuration property is missing
     * @throws RuntimeException if instantiation fails
     */
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

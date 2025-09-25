package com.pinterest.kafka.tieredstorage.uploader.management;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.pinterest.kafka.tieredstorage.common.discovery.StorageServiceEndpointProvider;
import com.pinterest.kafka.tieredstorage.common.metadata.TimeIndex;
import com.pinterest.kafka.tieredstorage.common.metadata.TopicPartitionMetadata;
import com.pinterest.kafka.tieredstorage.common.metadata.TopicPartitionMetadataUtil;
import com.pinterest.kafka.tieredstorage.common.metrics.MetricRegistryManager;
import com.pinterest.kafka.tieredstorage.uploader.DirectoryTreeWatcher;
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
 * <p>
 *     Abstract class to manage the remote segments and metadata. The concrete implementation is determined by the
 *     {@link #SEGMENT_MANAGER_CLASS_CONFIG_KEY} configuration.
 * </p>
 * 
 * <p>
 *     The core safety principle behind the SegmentManager and metadata management is that sparse metadata (i.e. missing entries) is ok,
 *     but dangling references (entries pointing to non-existent segments) is not. We will avoid creating dangling references by ensuring that the metadata is updated
 *     before performing actual deletions. If metadata update fails, we will skip the garbage collection for that topic partition and let the next GC cycle take care of it.
 * </p>
 *
 * <p>
 * Concurrency control (avoiding dangling references):<br>
 * ----------------------------<br>
 * There are two levels of concurrency control implemented to ensure that the metadata does not contain dangling references. This is due to the fact that metadata updates require
 * a read-modify-write operation in both the GC thread and the main uploader thread (contention within a signle uploader instance), and between
 * multiple uploader instances during simultaneous garbage collection in one broker/uploader and segment upload in another uploader instance. This could happen if there is a leadership
 * change and the new leader starts segment upload while the old leader is still performing garbage collection.<br>
 * <br>
 * The first level of concurrency control is a topic-partition level lock acquired via
 * {@link TopicPartitionMetadataUtil#tryAcquireLock(TopicPartition, long)}. This prevents contention within a single uploader instance between the GC thread and the main uploader thread.<br>
 * <br>
 * The second level of concurrency control is enforced via optimistic concurrency control by checking the {@link TopicPartitionMetadata#getLoadHash()} which was set during
 * read / load in the {@link #getTopicPartitionMetadataFromStorage(TopicPartition)} method. It should only overwrite the metadata if the existing metadata has the same load hash prior
 * to this write. The specific implementation of this is left to the concrete implementation of {@link SegmentManager}.
 * </p>
 *
 * <p>
 * Metadata Sparsity:<br>
 * ----------------------------<br>
 * There are 3 main scenarios which could result in sparse metadata (i.e. missing entries / gaps / holes), which is non-problematic by design. These scenarios are:<br>
 * <br>
 * 1. Metadata write failure during upload:<br>
 * During upload in {@link DirectoryTreeWatcher}, metadata is updated after the upload of the segment files are complete. If the metadata update fails,
 * the segment files will remain in remote storage but the metadata will miss the entry for that particular segment. This missing entry will never be filled in.<br>
 * 
 * Example:
 * <pre>
 * {@code
 * Segment files: [0, 100, 200, 300, 400, 500]
 * Metadata:      [0, 100, 200, 300, 400, 500]
 * 
 * 1. Upload segment=600 (success)
 * 2. Append segment=600 to metadata and upload (failure)
 * 
 * Segment files: [0, 100, 200, 300, 400, 500, 600]
 * Metadata:      [0, 100, 200, 300, 400, 500]
 * 
 * 3. Upload segment=700 (success)
 * 4. Append segment=700 to metadata and upload (success)
 * 
 * Segment files: [0, 100, 200, 300, 400, 500, 600, 700]
 * Metadata:      [0, 100, 200, 300, 400, 500, 700]
 * 
 * }
 * </pre>
 * In the example above, the metadata will be sparse with the entry for segment=600 missing.<br>
 * <br>
 * 2. During garbage collection in {@link SegmentManager}, metadata is updated before the actual deletion of segment files. If the metadata update succeeds,
 * but the subsequent deletion of segment files fails, the metadata's earliest offset will be greater than the actual earliest offset of segment files remaining in remote storage.<br>
 * <br>
 * Example:
 * <pre>
 *     {@code
 *          GC Cycle 1:
 *          ----------------------------
 *          Segment files: [0, 100, 200, 300, 400, 500]
 *          Metadata:      [0, 100, 200, 300, 400, 500]
 * 
 *          1. Remove from metadata segments=[0, 100] (success)
 *          2. Delete segments=[0, 100] (failure)
 * 
 *          Segment files: [0, 100, 200, 300, 400, 500]
 *          Metadata:      [200, 300, 400, 500]
 * 
 *          Metadata missing entries for segments=[0, 100]
 * 
 *          GC Cycle 2:
 *          ----------------------------
 *          Segment files: [0, 100, 200, 300, 400, 500, 600, 700]
 *          Metadata:      [200, 300, 400, 500, 600, 700]
 * 
 *          1. Remove from metadata segments=[200, 300] (success)
 *          2. Delete segments=[0, 100, 200, 300] (success)
 * 
 *          Segment files: [400, 500, 600, 700]
 *          Metadata:      [400, 500, 600, 700]
 * 
 *          Metadata and segments are in sync again
 *     }
 * </pre>
 * 
 * In the example above, the metadata will be missing entries for segments=[0, 100] between the two GC cycles.<br>
 * <br>
 * 3. Leadership change during garbage collection:<br>
 * If the leadership changes during garbage collection, the new leader might start uploading new segments while the old leader is still performing garbage collection.
 * This is due to the fact that {@link LeadershipWatcher} is polling for leadership changes every X seconds, and the old leader might have already started garbage collection
 * while the new leader detects the leadership change and starts uploading new segments.<br>
 * <br>
 * Example:
 * <pre>
 *     {@code
 *          T0:
 *          ----------------------------
 *          Broker A is the leader for topic-0
 *          Broker B is the follower for topic-0
 * 
 *          Segment files: [0, 100, 200, 300, 400, 500]
 *          Metadata:      [0, 100, 200, 300, 400, 500]
 * 
 *          Broker A starts garbage collection for topic-0, reads metadata as [0, 100, 200, 300, 400, 500] and detects [0, 100] as expired 
 *          Broker B detects leadership change and starts uploading new segment=600 for topic-0
 * 
 *          No changes yet to metadata or segment files:
 *          Segment files: [0, 100, 200, 300, 400, 500]
 *          Metadata:      [0, 100, 200, 300, 400, 500]
 * 
 *          T1:
 *          ----------------------------
 *          Broker A is the follower for topic-0
 *          Broker B is the leader for topic-0
 * 
 *          Segment files: [0, 100, 200, 300, 400, 500, 600]
 *          Metadata:      [0, 100, 200, 300, 400, 500]
 * 
 *          Broker B finishes uploading new segment=600 for topic-0, prepares metadata update. It reads current metadata as [0, 100, 200, 300, 400, 500]
 *          Broker A finishes garbage collection for topic-0, removes [0, 100] from metadata and deletes [0, 100] from segment files
 * 
 *          Segment files: [200, 300, 400, 500, 600]
 *          Metadata:      [200, 300, 400, 500]
 * 
 *          T2:
 *          ----------------------------
 *          Broker A is the follower for topic-0
 *          Broker B is the leader for topic-0
 * 
 *          Segment files: [200, 300, 400, 500, 600]
 *          Metadata:      [200, 300, 400, 500]
 * 
 *          Broker B appends segment=600 to metadata. Because it initially read the metadata as [0, 100, 200, 300, 400, 500], 
 *          it will try to overwrite the metadata with [0, 100, 200, 300, 400, 500, 600],
 *          but it will fail since the current metadata has a different load hash after Broker A updated the metadata in its GC cycle
 * 
 *          Segment files: [200, 300, 400, 500, 600]
 *          Metadata:      [200, 300, 400, 500]
 * 
 *          Metadata missing entry for segment=600
 *     }
 * </pre>
 * 
 * In the example above, the metadata will be missing entry for segment=600 and it will never be filled in.<br>
 * <br>
 * Dealing with metadata sparsity:<br>
 * ----------------------------<br>
 * Scenarios 1 and 3 result in permanent sparsity in the metadata, while scenario 2 results in a temporary sparsity in the metadata.
 * In all cases, the metadata will never point to dangling / non-existent segments. Metadata sparsity is non-problematic by design
 * because users of the metadata should handle the sparsity gracefully. For example, a consumer using the metadata for offset or 
 * timestamp seeking should use the metadata to reduce the search space, and then perform a linear search to find the desired record 
 * for the given offset or timestamp. This design is similar to how Kafka's index and timeindex files are sparse by design.
 * 
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

            // Acquire lock (best-effort) to avoid race conditions when updating metadata. If lock acquisition fails, we will skip the metadata update and garbage collection altogether
            // to prevent blocking the garbage collection. It is ok since the next GC cycle will take care of the missed GC cycle for this topic partition.
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
            // get cutoff offset from metadata based on timestamp
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

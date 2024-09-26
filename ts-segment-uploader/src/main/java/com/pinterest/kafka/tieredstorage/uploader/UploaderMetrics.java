package com.pinterest.kafka.tieredstorage.uploader;

/**
 * Metrics for the tiered storage uploader
 */
public class UploaderMetrics {
    private static final String UPLOADER_METRIC_PREFIX = "tieredstorage.uploader";
    public static final String UPLOAD_STATUS_CODE_METRIC = UPLOADER_METRIC_PREFIX + "." + "status.code";
    public static final String FILE_NOT_FOUND_METRIC = UPLOADER_METRIC_PREFIX + "." + "file.not.found";
    public static final String UPLOAD_RETRIES_METRIC = UPLOADER_METRIC_PREFIX + "." + "retries";
    public static final String UPLOAD_ERROR_METRIC = UPLOADER_METRIC_PREFIX + "." + "error";
    public static final String UPLOAD_SIZE_BYTES_METRIC = UPLOADER_METRIC_PREFIX + "." + "upload.size.bytes";
    public static final String UPLOAD_TIME_MS_METRIC = UPLOADER_METRIC_PREFIX + "." + "upload.time.ms";
    public static final String UPLOAD_COUNT_METRIC = UPLOADER_METRIC_PREFIX + "." + "upload.count";
    public static final String UPLOAD_INITIATED_METRIC = UPLOADER_METRIC_PREFIX + "." + "initiated";
    public static final String UPLOAD_COMPLETED_METRIC = UPLOADER_METRIC_PREFIX + "." + "completed";
    public static final String COMMITTED_TIME_LAG_MS_METRIC = UPLOADER_METRIC_PREFIX + "." + "committed.time.lag.ms";
    public static final String PROCESSING_TIME_LAG_MS_METRIC = UPLOADER_METRIC_PREFIX + "." + "processing.time.lag.ms";
    public static final String OFFSET_LAG_METRIC = UPLOADER_METRIC_PREFIX + "." + "offset.lag";
    public static final String SEGMENT_ACTIVE_SET_METRIC = UPLOADER_METRIC_PREFIX + "." + "segment.active.set";
    public static final String SEGMENT_ACTIVE_COUNT_METRIC = UPLOADER_METRIC_PREFIX + "." + "segment.active.count";
    public static final String SEGMENT_ACTIVE_REMOVED_METRIC = UPLOADER_METRIC_PREFIX + "." + "segment.active.removed";
    public static final String SEGMENT_QUEUED_COUNT_METRIC = UPLOADER_METRIC_PREFIX + "." + "segment.queued.count";
    public static final String WATCHER_ADDED_METRIC = UPLOADER_METRIC_PREFIX + "." + "watcher.added";
    public static final String WATCHER_REMOVED_METRIC = UPLOADER_METRIC_PREFIX + "." + "watcher.removed";
    public static final String WATCHER_COUNT_METRIC = UPLOADER_METRIC_PREFIX + "." + "watcher.count";
    public static final String HEARTBEAT_METRIC = UPLOADER_METRIC_PREFIX + "." + "heartbeat";
    public static final String KAFKA_LEADER_SET_METRIC = UPLOADER_METRIC_PREFIX + "." + "kafka.leader.set";
    public static final String KAFKA_LEADER_UNSET_METRIC = UPLOADER_METRIC_PREFIX + "." + "kafka.leader.unset";
    public static final String KAFKA_LEADER_COUNT_METRIC = UPLOADER_METRIC_PREFIX + "." + "kafka.leader.count";
    public static final String WATCHER_ZK_RESET_METRIC = UPLOADER_METRIC_PREFIX + "." + "watcher.zk.reset";
    public static final String WATCHER_NOT_ADDED_METRIC = UPLOADER_METRIC_PREFIX + "." + "watcher.not.added";
    public static final String SKIPPED_ENTRY_MODIFY_EVENT_METRIC = UPLOADER_METRIC_PREFIX + "." + "skipped.entry.modify";
    public static final String ADD_WATCHER_FAILED_METRIC = UPLOADER_METRIC_PREFIX + "." + "add.watcher.failed";
    public static final String ENQUEUE_TO_UPLOAD_LATENCY_MS_METRIC = UPLOADER_METRIC_PREFIX + "." + "enqueue.to.upload.latency.ms";
    public static final String RETRY_MARKED_FOR_DELETION_COUNT_METRIC = UPLOADER_METRIC_PREFIX + "." + "retry.marked.for.deletion.count";
    public static final String WATCHER_ZK_EXCEPTION_METRIC = UPLOADER_METRIC_PREFIX + "." + "watcher.zk.exception";
}

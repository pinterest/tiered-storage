package com.pinterest.kafka.tieredstorage.uploader;

import com.google.common.annotations.VisibleForTesting;
import com.pinterest.kafka.tieredstorage.common.metrics.MetricRegistryManager;
import com.pinterest.kafka.tieredstorage.uploader.dlq.DeadLetterQueueHandler;
import com.pinterest.kafka.tieredstorage.uploader.leadership.LeadershipWatcher;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Watches and processes filesystem events on the log.dir directory. It watches for file creation, modification, and deletion.
 *
 * Upon file creation, it moves the existing active segment to the segment queue and makes the newly created file the active segment.
 * Upon file modification, it moves the active segment to the segment queue and makes the modified file the active segment.
 * Upon file deletion, it removes the file from the active segment and segment queue and alerts for possible data loss.
 *
 * Upon moving the active segment to the segment queue, it also enqueues the segment file for upload. Each uploaded offset will upload
 * 3 files to S3: the .log file, the .index file, and the .timeindex file. After all 3 of these files are uploaded, the watermark file
 * offset.wm is uploaded to S3. The watermark file contains the offset of the last uploaded file, and is used to track the progress of
 * the uploader, similar to "committing" offsets.
 */
public class DirectoryTreeWatcher implements Runnable {
    private static final Logger LOG = LogManager.getLogger(DirectoryTreeWatcher.class);
    private static final String[] MONITORED_EXTENSIONS = {".timeindex", ".index", ".log"};
    private static final Pattern MONITORED_FILE_PATTERN = Pattern.compile("^\\d+(" + String.join("|", MONITORED_EXTENSIONS) + ")$");
    private static Map<TopicPartition, String> activeSegment;
    private static Map<TopicPartition, Set<String>> segmentsQueue;
    private static LeadershipWatcher leadershipWatcher;
    private final Path topLevelPath;
    private final WatchService watchService;
    private final ConcurrentLinkedQueue<UploadTask> uploadTasks = new ConcurrentLinkedQueue<>();
    private final S3FileUploader s3FileUploader;
    private final ThreadLocal<WatermarkFileHandler> tempFileGenerator = ThreadLocal.withInitial(WatermarkFileHandler::new);
    private final ConcurrentHashMap<TopicPartition, String> latestUploadedOffset = new ConcurrentHashMap<>();
    private final S3FileDownloader s3FileDownloader;
    private final Pattern SKIP_TOPICS_PATTERN = Pattern.compile(
            "^__consumer_offsets$|^__transaction_state$|.+\\.changlog$|.+\\.repartition$"
    );
    private final Pattern TOPIC_PARTITION_FILEPATH_PATTERN = Pattern.compile("(?<topic>[a-zA-Z0-9-_.]+)-(?<partition>[0-9]+)");
    private final ExecutorService s3UploadHandler;
    private final Map<Path, WatchKey> watchKeyMap = new HashMap<>();
    private final Heartbeat heartbeat;
    private final SegmentUploaderConfiguration config;
    private final KafkaEnvironmentProvider environmentProvider;
    private DeadLetterQueueHandler deadLetterQueueHandler;
    private final Object watchKeyMapLock = new Object();
    private Thread thread;
    private boolean cancelled = false;

    public static void setLeadershipWatcher(LeadershipWatcher suppliedLeadershipWatcher) {
        if (leadershipWatcher == null)
            leadershipWatcher = suppliedLeadershipWatcher;
    }

    @VisibleForTesting
    protected static void unsetLeadershipWatcher() {
        leadershipWatcher = null;
    }

    public DirectoryTreeWatcher(S3FileUploader s3FileUploader, SegmentUploaderConfiguration config, KafkaEnvironmentProvider environmentProvider) throws Exception {
        this.environmentProvider = environmentProvider;
        this.topLevelPath = Paths.get(environmentProvider.logDir());
        this.watchService = FileSystems.getDefault().newWatchService();
        activeSegment = new HashMap<>();
        segmentsQueue = new HashMap<>();
        this.s3FileUploader = s3FileUploader;
        this.s3UploadHandler = Executors.newSingleThreadExecutor();
        this.s3FileDownloader = new S3FileDownloader(s3FileUploader.getStorageServiceEndpointProvider(), config);
        heartbeat = new Heartbeat("watcher.logs", config, environmentProvider);
        this.config = config;
        this.deadLetterQueueHandler = DeadLetterQueueHandler.createHandler(config);
    }

    /**
     * Start the s3 upload handler thread and register the top level path for watching.
     * @throws IOException
     * @throws InterruptedException
     * @throws KeeperException
     */
    public void initialize() throws Exception {
        if (leadershipWatcher == null) {
            throw new IllegalStateException("LeadershipWatcher must be set before initializing DirectoryTreeWatcher");
        }
        s3UploadHandler.submit(() -> {
            while (!cancelled) {
                if (uploadTasks.isEmpty()) {
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        LOG.warn(e.getMessage());
                    }
                    continue;
                }
                UploadTask task = uploadTasks.remove();
                if (!task.isReadyForUpload()) {
                    enqueueUploadTask(task);
                    continue;
                } else if (task.getTries() > 0) {
                    LOG.info(String.format("UploadTask %s is ready for retry #%s", task.getAbsolutePath(), task.getTries()));
                }
                MetricRegistryManager.getInstance(config.getMetricsConfiguration()).incrementCounter(
                        task.getTopicPartition().topic(),
                        task.getTopicPartition().partition(),
                        UploaderMetrics.ENQUEUE_TO_UPLOAD_LATENCY_MS_METRIC,
                        System.currentTimeMillis() - task.getEnqueueTimestamp(),
                        "cluster=" + environmentProvider.clusterId(),
                        "broker=" + environmentProvider.brokerId()
                );
                s3FileUploader.uploadFile(task, this::handleUploadCallback);
            }
        });
        LOG.info("Starting LeadershipWatcher: " + leadershipWatcher.getClass().getName());
        leadershipWatcher.start();
        LOG.info("Submitting s3UploadHandler loop");
    }

    @VisibleForTesting
    protected void handleUploadCallback(UploadTask uploadTask, long totalTimeMs, Throwable throwable, int statusCode) {
        TopicPartition topicPartition = uploadTask.getTopicPartition();
        MetricRegistryManager.getInstance(config.getMetricsConfiguration()).incrementCounter(
                topicPartition.topic(),
                topicPartition.partition(),
                UploaderMetrics.UPLOAD_STATUS_CODE_METRIC,
                "cluster=" + environmentProvider.clusterId(),
                "broker=" + environmentProvider.brokerId(),
                "status=" + statusCode
        );
        if (throwable != null) {
            handleUploadException(uploadTask, throwable, topicPartition);
        } else {
            if (uploadTask.getTries() > 0) {
                // send a successful retry metric (to close the loop)
                handleSuccessfulRetries(uploadTask, topicPartition);
            }
            if (uploadTask.getSubPath().endsWith(".log")) {
                if (uploadTask.getFullFilename().endsWith(".log")) {
                    MetricRegistryManager.getInstance(config.getMetricsConfiguration()).updateHistogram(
                            topicPartition.topic(),
                            topicPartition.partition(),
                            UploaderMetrics.UPLOAD_SIZE_BYTES_METRIC,
                            uploadTask.getSizeBytes(),
                            "cluster=" + environmentProvider.clusterId(),
                            "broker=" + environmentProvider.brokerId()
                    );
                    MetricRegistryManager.getInstance(config.getMetricsConfiguration()).updateHistogram(
                            topicPartition.topic(),
                            topicPartition.partition(),
                            UploaderMetrics.UPLOAD_TIME_MS_METRIC,
                            totalTimeMs,
                            "cluster=" + environmentProvider.clusterId(),
                            "broker=" + environmentProvider.brokerId()
                    );
                    MetricRegistryManager.getInstance(config.getMetricsConfiguration()).incrementCounter(
                            topicPartition.topic(),
                            topicPartition.partition(),
                            UploaderMetrics.UPLOAD_COUNT_METRIC,
                            "cluster=" + environmentProvider.clusterId(),
                            "broker=" + environmentProvider.brokerId()
                    );
                    MetricRegistryManager.getInstance(config.getMetricsConfiguration()).updateCounter(
                            topicPartition.topic(),
                            topicPartition.partition(),
                            UploaderMetrics.PROCESSING_TIME_LAG_MS_METRIC,
                            System.currentTimeMillis() - uploadTask.getSegmentLastModifiedTimestamp(),
                            "cluster=" + environmentProvider.clusterId(),
                            "broker=" + environmentProvider.brokerId()
                    );
                }
                // this should be the last file in the allowed extensions
                String filename = uploadTask.getFullFilename().substring(0, uploadTask.getFullFilename().lastIndexOf(".log"));
                dequeueSegment(topicPartition, filename);
                uploadWatermarkFile(uploadTask, topicPartition, filename);
            } else if (uploadTask.getSubPath().endsWith(".wm")) {
                finalizeUpload(uploadTask, topicPartition);
            }
        }
    }

    private void finalizeUpload(UploadTask uploadTask, TopicPartition topicPartition) {
        latestUploadedOffset.compute(
                topicPartition,
                (k, v) -> (v == null || uploadTask.getOffset().compareTo(v) > 0) ? uploadTask.getOffset() : v
        );

        MetricRegistryManager.getInstance(config.getMetricsConfiguration()).updateHistogram(
                topicPartition.topic(),
                topicPartition.partition(),
                UploaderMetrics.UPLOAD_COMPLETED_METRIC,
                Long.parseLong(uploadTask.getOffset()),
                "cluster=" + environmentProvider.clusterId(),
                "broker=" + environmentProvider.brokerId()
        );
        MetricRegistryManager.getInstance(config.getMetricsConfiguration()).updateCounter(
                topicPartition.topic(),
                topicPartition.partition(),
                UploaderMetrics.COMMITTED_TIME_LAG_MS_METRIC,
                System.currentTimeMillis() - uploadTask.getSegmentLastModifiedTimestamp(), // this refers to the segment timestamp, not the watermark file timestamp
                "cluster=" + environmentProvider.clusterId(),
                "broker=" + environmentProvider.brokerId()
        );

        if (!activeSegment.containsKey(topicPartition)) {
            LOG.warn(String.format("Expected %s to have an active segment but it doesn't!", topicPartition));
        } else {
            long activeSegmentOffset = Long.parseLong(activeSegment.get(topicPartition));
            MetricRegistryManager.getInstance(config.getMetricsConfiguration()).updateCounter(
                    topicPartition.topic(),
                    topicPartition.partition(),
                    UploaderMetrics.OFFSET_LAG_METRIC,
                    activeSegmentOffset - Long.parseLong(uploadTask.getOffset()),
                    "cluster=" + environmentProvider.clusterId(),
                    "broker=" + environmentProvider.brokerId()
            );
        }

        tempFileGenerator.get().deleteWatermarkFile(WatermarkFileHandler.WATERMARK_DIRECTORY.resolve(uploadTask.getSubPath()));
    }

    private void uploadWatermarkFile(UploadTask uploadTask, TopicPartition topicPartition, String filename) {
        if (!latestUploadedOffset.containsKey(topicPartition) || filename.compareTo(latestUploadedOffset.get(topicPartition)) > 0) {
            Path wmPath = tempFileGenerator.get().getWatermarkFile(topicPartition.toString(), filename);
            if (wmPath != null) {
                enqueueWatermarkUploadTask(topicPartition, filename, wmPath, uploadTask);
                MetricRegistryManager.getInstance(config.getMetricsConfiguration()).updateHistogram(
                        topicPartition.topic(),
                        topicPartition.partition(),
                        UploaderMetrics.UPLOAD_INITIATED_METRIC,
                        Long.parseLong(filename),
                        "cluster=" + environmentProvider.clusterId(),
                        "broker=" + environmentProvider.brokerId()
                );
            }
        }
    }

    private void handleSuccessfulRetries(UploadTask uploadTask, TopicPartition topicPartition) {
        MetricRegistryManager.getInstance(config.getMetricsConfiguration()).incrementCounter(
                topicPartition.topic(),
                topicPartition.partition(),
                UploaderMetrics.UPLOAD_RETRIES_METRIC,
                uploadTask.getTries(),
                "cluster=" + environmentProvider.clusterId(),
                "broker=" + environmentProvider.brokerId(),
                "offset=" + uploadTask.getOffset(),
                "success=true"
        );
        if (uploadTask.getAbsolutePath().endsWith(".deleted")) {
            // retryMarkedForDeletion succeeded
            MetricRegistryManager.getInstance(config.getMetricsConfiguration()).incrementCounter(
                    topicPartition.topic(),
                    topicPartition.partition(),
                    UploaderMetrics.RETRY_MARKED_FOR_DELETION_COUNT_METRIC,
                    "cluster=" + environmentProvider.clusterId(),
                    "broker=" + environmentProvider.brokerId(),
                    "success=true"
            );
        }
        LOG.info(String.format("Previously failed upload of %s finally succeeded at retry %s.",
                uploadTask.getAbsolutePath(),
                uploadTask.getTries()));
    }

    private void handleUploadException(UploadTask uploadTask, Throwable throwable, TopicPartition topicPartition) {
        if (Utils.isAssignableFromRecursive(throwable, NoSuchFileException.class)) {
            if (uploadTask.getTries() <= config.getUploadMaxRetries()) {
                // retry with .deleted suffix
                LOG.info("Retrying upload with .deleted suffix for " + uploadTask.absolutePath);
                MetricRegistryManager.getInstance(config.getMetricsConfiguration()).incrementCounter(
                        topicPartition.topic(),
                        topicPartition.partition(),
                        UploaderMetrics.RETRY_MARKED_FOR_DELETION_COUNT_METRIC,
                        "cluster=" + environmentProvider.clusterId(),
                        "broker=" + environmentProvider.brokerId(),
                        "success=false"
                );
                retryUpload(uploadTask.retryMarkedForDeletion(), throwable, topicPartition);
            } else {
                // dequeue the segment since we lost it
                if (uploadTask.getFullFilename().endsWith(".log"))
                    dequeueSegment(uploadTask.getTopicPartition(), uploadTask.getOffset());
                LOG.error(String.format("Failed to upload file %s before it was deleted.", uploadTask.getAbsolutePath()));
                // send a file not found metric highlighting a missed upload
                MetricRegistryManager.getInstance(config.getMetricsConfiguration()).incrementCounter(
                        topicPartition.topic(),
                        topicPartition.partition(),
                        UploaderMetrics.FILE_NOT_FOUND_METRIC,
                        "cluster=" + environmentProvider.clusterId(),
                        "broker=" + environmentProvider.brokerId(),
                        "file=" + uploadTask.getFullFilename()
                );
                handleFailedUploadAfterAllRetries(uploadTask, throwable, topicPartition);
            }
        } else if (uploadTask.getTries() < config.getUploadMaxRetries()){
            // retry all other errors
            retryUpload(uploadTask.retry(), throwable, topicPartition);
        } else {
            // retry limit reached, upload still errors
            handleFailedUploadAfterAllRetries(uploadTask, throwable, topicPartition);
        }
    }

    /**
     * Handle a failed upload after all retries have been exhausted, including sending
     * the failed upload to the dead-letter queue if configured.
     *
     * @param uploadTask the upload task that failed
     * @param throwable the exception that caused the failure
     * @param topicPartition the topic partition of the upload task
     */
    private void handleFailedUploadAfterAllRetries(UploadTask uploadTask, Throwable throwable, TopicPartition topicPartition) {
        LOG.error(String.format("Max retries exhausted (%s) for upload: %s --> %s",
                uploadTask.getTries(), uploadTask.getAbsolutePath(), uploadTask.getUploadDestinationPathString()));
        MetricRegistryManager.getInstance(config.getMetricsConfiguration()).incrementCounter(
                topicPartition.topic(),
                topicPartition.partition(),
                UploaderMetrics.UPLOAD_ERROR_METRIC,
                "exception=" + throwable.getClass().getName(),
                "cluster=" + environmentProvider.clusterId(),
                "broker=" + environmentProvider.brokerId(),
                "offset=" + uploadTask.getOffset()
        );
        if (deadLetterQueueHandler != null) {
            Future<Boolean> result = deadLetterQueueHandler.send(uploadTask, throwable);
            boolean success;
            try {
                success = result.get(deadLetterQueueHandler.getSendTimeoutMs(), TimeUnit.MILLISECONDS);
            } catch (Exception e) {
                throw new RuntimeException(String.format("Failed to persist failed upload %s to %s to dead-letter queue." +
                        " This was a best-effort attempt.", uploadTask.getAbsolutePath(), uploadTask.getUploadDestinationPathString()), e);
            }
            if (success)
                LOG.info(String.format("Sent failed upload %s to %s to dead letter queue", uploadTask.getAbsolutePath(), uploadTask.getUploadDestinationPathString()));
            else
                LOG.error(String.format("Failed to send failed upload %s to %s to dead letter queue", uploadTask.getAbsolutePath(), uploadTask.getUploadDestinationPathString()));
        }
    }

    /**
     * Retry the upload of a file that failed to upload.
     * @param uploadTask
     * @param throwable
     * @param topicPartition
     */
    private void retryUpload(UploadTask uploadTask, Throwable throwable, TopicPartition topicPartition) {
        // send a retry metric
        MetricRegistryManager.getInstance(config.getMetricsConfiguration()).incrementCounter(
                topicPartition.topic(),
                topicPartition.partition(),
                UploaderMetrics.UPLOAD_RETRIES_METRIC,
                uploadTask.getTries(),
                "cluster=" + environmentProvider.clusterId(),
                "broker=" + environmentProvider.brokerId(),
                "offset=" + uploadTask.getOffset(),
                "success=false"
        );
        // retry the upload
        enqueueUploadTask(uploadTask);
        LOG.warn(String.format("Re-queued file %s for upload retry %s due to %s. Next retry will occur in %s ms",
                uploadTask.getAbsolutePath(),
                uploadTask.getTries(),
                throwable.getClass().getName(),
                uploadTask.getNextRetryNotBeforeTimestamp() - System.currentTimeMillis())
        );
    }

    private void printQueues() {
        LOG.info(String.format("active segments: %s", activeSegment));
        LOG.info(String.format("segments queue: %s", segmentsQueue));
    }

    @Override
    public void run() {
        try {
            while (!cancelled) {
                try {
                    WatchKey key = watchService.take();
                    for (WatchEvent<?> event : key.pollEvents()) {
                        WatchEvent.Kind<?> kind = event.kind();
                        Path subpath = (Path) event.context();
                        Path path = ((Path) key.watchable()).resolve(subpath);
                        processEvent(path, kind, key);
                    }

                    if (!key.reset()) {
                        break;
                    }
                } catch (InterruptedException e) {
                    LOG.warn("Program execution was interrupted.");
                    if (cancelled)
                        break;
                }
            }
        } catch (Exception e) {
            LOG.error("An exception occurred, exiting the program.", e);
            System.exit(2);
        } finally {
            try {
                watchService.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Process a file system event on the watched log.dir directory and its subdirectories for each {@link TopicPartition}.
     *
     * Upon file creation, it moves the existing active segment to the segment queue and makes the newly created file the active segment.
     * Upon file modification, it moves the active segment to the segment queue and makes the modified file the active segment.
     * Upon file deletion, it removes the file from the active segment and segment queue and alerts for possible data loss.
     *
     * @param path
     * @param kind
     * @param key
     * @throws IOException
     */
    private void processEvent(Path path, WatchEvent.Kind<?> kind, WatchKey key) throws IOException {
        if (!shouldWatch(path))
            return;

        boolean isFile = !Files.isDirectory(path);
        Path subpath = topLevelPath.relativize(path);
        String[] splits = subpath.toString().split("/");
        assert
                (isFile && splits.length == 2) || // topic-partition/filename.ext
                        (Files.isDirectory(path) && splits.length == 1); // topic-partition

        int tpSeparatorIndex = splits[0].lastIndexOf("-");
        TopicPartition topicPartition;
        String topic = splits[0].substring(0, tpSeparatorIndex);
        int partition = Integer.parseInt(splits[0].substring(tpSeparatorIndex + 1));
        topicPartition = new TopicPartition(topic, partition);

        if (!isWatched(topicPartition)) {
            LOG.info(String.format("Logs for %s are no longer being watched by this broker. Will skip processing.", topicPartition));
            unwatch(topicPartition);
            return;
        }

        String fullFilename = isFile ? splits[1] : null;
        if (isFile && !MONITORED_FILE_PATTERN.matcher(fullFilename).matches()) {
            // skip files that don't follow the format ###########.[log|index|timeindex]
            LOG.debug(String.format("%s - %s - Skipped (non-monitored file)", kind, path));
            return;
        } else if (!isFile) {
            // for a topic-partition directory, find the latest uploaded offset
            String offset = s3FileDownloader.getOffsetFromWatermarkFile(topicPartition);
            if (offset != null) {
                latestUploadedOffset.put(topicPartition, offset);
                LOG.info(String.format("Found committed offset for %s: %s", topicPartition, offset));
            } else {
                LOG.info(String.format("No committed offset found for %s", topicPartition));
            }
        }

        String filename = isFile ? fullFilename.split("\\.")[0] : null;
        if (filename != null && latestUploadedOffset.containsKey(topicPartition) &&
                filename.compareTo(latestUploadedOffset.get(topicPartition)) <= 0) {
            LOG.debug(String.format("Skipping filename %s as it was already uploaded before.", fullFilename));
            return;
        }

        if (isFile && isActiveSegment(topicPartition, filename)) {
            // This is already an active segment (used to bypass additional work for .index, .timeindex, .log all having the same name)
            return;
        }

        if (kind.equals(StandardWatchEventKinds.ENTRY_CREATE)) {
            if (isFile) {
                // for a newly created monitored file of a topic partition, move the
                // existing active segment to segment queue and make this filename (no extension)
                // the active segment of this partition
                if (moveActiveSegmentToQueue(topicPartition, filename)) {
                    // do this only for one extension variation (.log, .index, .timeindex) of the same filename
                    makeActiveSegment(topicPartition, filename);
                }
                LOG.debug(String.format("%s - %s - Processed (file)", kind, path));
            } else {
                // for a newly created directory, the directory may be needed to be added to watch list
                LOG.debug(String.format("%s - %s - Processed - no action (dir)", kind, path));
            }
        } else if (kind.equals(StandardWatchEventKinds.ENTRY_MODIFY)) {
            if (isFile) {
                // this file should already be an active segment (set when created) or just rotated (moved to upload queue).
                if (isInSegmentQueue(topicPartition, filename)) {
                    LOG.error(String.format("Skipped %s event for %s as it is already in upload queue. This is likely data loss!", kind, path));
                    MetricRegistryManager.getInstance(config.getMetricsConfiguration()).incrementCounter(
                            topicPartition.topic(),
                            topicPartition.partition(),
                            UploaderMetrics.SKIPPED_ENTRY_MODIFY_EVENT_METRIC,
                            "cluster=" + environmentProvider.clusterId(),
                            "broker=" + environmentProvider.brokerId(),
                            "file=" + filename
                    );
                    return;
                } else if (!isActiveSegment(topicPartition, filename))
                    LOG.warn(String.format("Processed %s and expected %s to be the active segment for %s, but it was not. " +
                                    "Will process as if it was just created.",
                            kind, path, topicPartition));
                if (moveActiveSegmentToQueue(topicPartition, filename)) {
                    makeActiveSegment(topicPartition, filename);
                } else {
                    LOG.debug(String.format("%s - %s - No-action (file)", kind, path));
                }
            } else {
                // this event occurs whenever there is a change on a file inside the directory
                LOG.debug(String.format("%s - %s - Skipped (dir)", kind, path));
            }
        } else if (kind.equals(StandardWatchEventKinds.ENTRY_DELETE)) {
            if (isFile) {
                // this indicates a log segment deletion (likely due to topic retention).
                // TODO: this log segment can only be deleted after the uploader has uploaded it.
                // the case of file deletion event due to deletion of topic is captured in
                // directory deletion event.
                // for now, if the filename is still in the segment queue we alert
                if (isActiveSegment(topicPartition, filename)) {
                    LOG.error(String.format("The active log segment %s was deleted and not processed.", path));
                } else if (isInSegmentQueue(topicPartition, filename)) {
                    LOG.error(String.format("The queued log segment %s was deleted before processing", path));
                } else {
                    LOG.debug(String.format("%s - %s - No-action (file - already processed)", kind, path));
                }
            } else {
                // a directory deletion means deletion of topic. In this case, we can just stop
                // additional processing of the topic by removing it from the active and queue
                // data structures, and cancel watcher on the path.
                deletePartition(topicPartition);
                config.deleteTopic(topic);
                if (key != null)
                    key.cancel();
                LOG.debug(String.format("%s - %s - Deleted tracking (file - watcher cancelled too)", kind, path));
            }
        } else {
            LOG.warn(String.format("%s - %s - Unexpected event type", kind, path));
        }
    }

    private void deletePartition(TopicPartition topicPartition) {
        clearActiveSegment(topicPartition);
        dequeueSegments(topicPartition);
    }

    private boolean isInSegmentQueue(TopicPartition topicPartition, String filename) {
        return segmentsQueue.containsKey(topicPartition) && segmentsQueue.get(topicPartition).contains(filename);
    }

    private boolean isActiveSegment(TopicPartition topicPartition, String filename) {
        return activeSegment.containsKey(topicPartition) && activeSegment.get(topicPartition).equals(filename);
    }

    private void makeActiveSegment(TopicPartition topicPartition, String filename) {
        addActiveSegment(topicPartition, filename);
        printQueues();
    }

    private void addActiveSegment(TopicPartition topicPartition, String filename) {
        if (!isWatched(topicPartition)) {
            LOG.info(String.format("Logs for %s are no longer being watched by this broker. Will skip adding the log segment.", topicPartition));
            unwatch(topicPartition);
            return;
        }
        activeSegment.put(topicPartition, filename);
        MetricRegistryManager.getInstance(config.getMetricsConfiguration()).incrementCounter(
                topicPartition.topic(),
                topicPartition.partition(),
                UploaderMetrics.SEGMENT_ACTIVE_SET_METRIC,
                "cluster=" + environmentProvider.clusterId(),
                "broker=" + environmentProvider.brokerId()
        );
        MetricRegistryManager.getInstance(config.getMetricsConfiguration()).updateCounter(
                topicPartition.topic(),
                topicPartition.partition(),
                UploaderMetrics.SEGMENT_ACTIVE_COUNT_METRIC,
                1,
                "cluster=" + environmentProvider.clusterId(),
                "broker=" + environmentProvider.brokerId()
        );
    }

    private void clearActiveSegment(TopicPartition topicPartition) {
        activeSegment.remove(topicPartition);
        MetricRegistryManager.getInstance(config.getMetricsConfiguration()).incrementCounter(
                topicPartition.topic(),
                topicPartition.partition(),
                UploaderMetrics.SEGMENT_ACTIVE_REMOVED_METRIC,
                "cluster=" + environmentProvider.clusterId(),
                "broker=" + environmentProvider.brokerId()
        );
        MetricRegistryManager.getInstance(config.getMetricsConfiguration()).updateCounter(
                topicPartition.topic(),
                topicPartition.partition(),
                UploaderMetrics.SEGMENT_ACTIVE_COUNT_METRIC,
                0,
                "cluster=" + environmentProvider.clusterId(),
                "broker=" + environmentProvider.brokerId()
        );
    }

    private void dequeueSegments(TopicPartition topicPartition) {
        segmentsQueue.remove(topicPartition);
        MetricRegistryManager.getInstance(config.getMetricsConfiguration()).updateCounter(
                topicPartition.topic(),
                topicPartition.partition(),
                UploaderMetrics.SEGMENT_QUEUED_COUNT_METRIC,
                0,
                "cluster=" + environmentProvider.clusterId(),
                "broker=" + environmentProvider.brokerId()
        );
    }

    private void dequeueSegment(TopicPartition topicPartition, String filename) {
        if (segmentsQueue.containsKey(topicPartition)) {
            segmentsQueue.get(topicPartition).remove(filename);
            MetricRegistryManager.getInstance(config.getMetricsConfiguration()).updateCounter(
                    topicPartition.topic(),
                    topicPartition.partition(),
                    UploaderMetrics.SEGMENT_QUEUED_COUNT_METRIC,
                    segmentsQueue.get(topicPartition).size(),
                    "cluster=" + environmentProvider.clusterId(),
                    "broker=" + environmentProvider.brokerId()
            );
        }
    }

    private void queueSegment(TopicPartition topicPartition, String filename) {
        if (!isWatched(topicPartition)) {
            LOG.info(String.format("Logs for %s are no longer being watched by this broker. Will skip queuing the log segment.", topicPartition));
            return;
        }

        if (!segmentsQueue.containsKey(topicPartition))
            segmentsQueue.put(topicPartition, new HashSet<>());
        segmentsQueue.get(topicPartition).add(filename);
        MetricRegistryManager.getInstance(config.getMetricsConfiguration()).updateCounter(
                topicPartition.topic(),
                topicPartition.partition(),
                UploaderMetrics.SEGMENT_QUEUED_COUNT_METRIC,
                segmentsQueue.get(topicPartition).size(),
                "cluster=" + environmentProvider.clusterId(),
                "broker=" + environmentProvider.brokerId()
        );
    }

    private boolean moveActiveSegmentToQueue(TopicPartition topicPartition, String filename) throws IOException {
        if (!activeSegment.containsKey(topicPartition))
            return true;

        String activeFilename = activeSegment.get(topicPartition);

        if (activeFilename.compareTo(filename) > 0 && !isInSegmentQueue(topicPartition, filename)) {
            // this is when multiple log segments are created and the order of their creation events is not the same as their naming order.
            // the file with smaller offset needs to directly move to upload queue.
            queueSegment(topicPartition, filename);
            for (String extension: MONITORED_EXTENSIONS)
                enqueueUploadTask(topicPartition, filename, extension);
            return false;
        }

        if (!segmentsQueue.containsKey(topicPartition))
            segmentsQueue.put(topicPartition, new TreeSet<>());
        else if (segmentsQueue.get(topicPartition).contains(activeFilename))
            return false;

        if (activeFilename.compareTo(filename) < 0) {
            queueSegment(topicPartition, activeFilename);
            for (String extension: MONITORED_EXTENSIONS)
                enqueueUploadTask(topicPartition, activeFilename, extension);
            return true;
        }

        return false;
    }

    private void enqueueUploadTask(TopicPartition topicPartition, String filename, String extension) {
        UploadTask uploadTask = new UploadTask(topicPartition, filename, filename + extension, topLevelPath.resolve(topicPartition + "/" + filename + extension));
        enqueueUploadTask(uploadTask);
    }

    private void enqueueUploadTask(UploadTask uploadTask) {
        uploadTask.setEnqueueTimestamp(System.currentTimeMillis());
        uploadTasks.add(uploadTask);
    }

    private void enqueueWatermarkUploadTask(TopicPartition topicPartition, String filename, Path wmPath, UploadTask uploadTask) {
        UploadTask watermarkUploadTask = new UploadTask(topicPartition, filename, WatermarkFileHandler.WATERMARK_DIRECTORY.resolve(topicPartition.toString()).relativize(wmPath).toString(), wmPath, uploadTask.getSegmentLastModifiedTimestamp());
        enqueueUploadTask(watermarkUploadTask);
    }

    public void start() {
        thread = new Thread(this);
        thread.start();
    }

    public void stop() throws InterruptedException {
        cancelled = true;
        if (thread != null && thread.isAlive()) {
            thread.interrupt();
        }
        leadershipWatcher.stop();
        heartbeat.stop();
    }

    /**
     * Upon startup or a newly watched topic partition, queue up all log segments in the log.dir directory and its subdirectories.
     * @param parentDir
     * @throws IOException
     */
    private void queueUpLogSegments(Path parentDir) throws IOException {
        if (parentDir == topLevelPath) {
            File[] list = parentDir.toFile().listFiles();
            for (File file : list) {
                if (file.isFile() || !shouldWatch(file.toPath()))
                    continue;
                queueUpLogSegments(file.toPath());
            }
        } else {
            processEvent(parentDir, StandardWatchEventKinds.ENTRY_CREATE, null);
            String[] list = parentDir.toFile().list();
            if (list == null || list.length == 0)
                return;
            Arrays.sort(list);
            TopicPartition topicPartition = getTopicPartitionFromPath(parentDir);
            if (latestUploadedOffset.get(topicPartition) == null) {
                // no committed offsets found
                LOG.info(String.format("No committed offset found for %s, will start processing according to offset.reset.strategy=%s", topicPartition, config.getOffsetResetStrategy()));
                if (config.getOffsetResetStrategy().equals(SegmentUploaderConfiguration.OffsetResetStrategy.EARLIEST)) {
                    LOG.info(String.format("offset.reset.strategy=%s; Will process all log segments in %s", config.getOffsetResetStrategy(), parentDir));
                } else if (config.getOffsetResetStrategy().equals(SegmentUploaderConfiguration.OffsetResetStrategy.LATEST)){
                    LOG.info(String.format("offset.reset.strategy=%s; Will process only the latest log segment in %s", config.getOffsetResetStrategy(), parentDir));
                    list = new String[]{list[list.length - 1]};
                }
            } else {
                LOG.info(String.format("Start processing %s from committed offset %s", topicPartition, latestUploadedOffset.get(topicPartition)));
            }
            for (String file : list)
                processEvent(parentDir.resolve(file), StandardWatchEventKinds.ENTRY_CREATE, null);
        }
    }

    /**
     * Returns true if the path should be watched, false otherwise.
     * @param absolutePath
     * @return
     */
    private boolean shouldWatch(Path absolutePath) {
        Path relativizedPath = topLevelPath.relativize(absolutePath);
        String[] splits = relativizedPath.toString().split("/");
        if (!Files.isDirectory(absolutePath) && splits.length != 2) {
            LOG.debug(String.format("Skipping processing of %s because this is not a topic log file.", relativizedPath));
            return false;
        }

        int tpSeparatorIndex = splits[0].lastIndexOf("-");
        String topic = splits[0].substring(0, tpSeparatorIndex);
        if (SKIP_TOPICS_PATTERN.matcher(topic).matches()) {
            LOG.debug(String.format("Skipping processing of %s because the topic %s is in skip list.", relativizedPath, topic));
            return false;
        }

        if (splits.length > 1 && !MONITORED_FILE_PATTERN.matcher(splits[1]).matches()) {
            // is a file
            // skip files that don't follow the format ###########.[log|index|timeindex]
            LOG.debug(String.format("Skipping processing of %s because it is non-monitored file.", relativizedPath));
            return false;
        }
        return config.shouldWatchTopic(topic);
    }

    private TopicPartition getTopicPartitionFromPath(Path dir) {
        Matcher matcher = TOPIC_PARTITION_FILEPATH_PATTERN.matcher(dir.getFileName().toString());
        if (matcher.matches()) {
            return new TopicPartition(matcher.group("topic"), Integer.parseInt(matcher.group("partition")));
        } else {
            LOG.warn(String.format("Failed to convert Pathname to TopicPartition object due to %s not matching expected pattern", dir.getFileName()));
            return null;
        }
    }

    private void updateTopicPartitionWatcherCountMetric() {
        for (Path path : watchKeyMap.keySet()) {
            // update per-partition watcher count metric
            TopicPartition tp = getTopicPartitionFromPath(path);
            if (tp != null) {
                MetricRegistryManager.getInstance(config.getMetricsConfiguration()).updateCounter(
                        tp.topic(),
                        tp.partition(),
                        UploaderMetrics.WATCHER_COUNT_METRIC,
                        1,
                        "cluster=" + environmentProvider.clusterId(),
                        "broker=" + environmentProvider.brokerId()
                );
            }
        }
    }

    /**
     * Watch the given topic partition.
     * @param topicPartition
     */
    public void watch(TopicPartition topicPartition) {
        Path dir = topLevelPath.resolve(topicPartition.toString());
        if (!watchKeyMap.containsKey(dir) && watchPath(dir)) {
            MetricRegistryManager.getInstance(config.getMetricsConfiguration()).incrementCounter(
                    topicPartition.topic(),
                    topicPartition.partition(),
                    UploaderMetrics.WATCHER_ADDED_METRIC,
                    "cluster=" + environmentProvider.clusterId(),
                    "broker=" + environmentProvider.brokerId()
            );
            MetricRegistryManager.getInstance(config.getMetricsConfiguration()).incrementCounter(
                    topicPartition.topic(),
                    topicPartition.partition(),
                    UploaderMetrics.KAFKA_LEADER_SET_METRIC,
                    "cluster=" + environmentProvider.clusterId(),
                    "broker=" + environmentProvider.brokerId()
            );
        }
        updateTopicPartitionWatcherCountMetric();
    }

    /**
     * @param dir
     * @return true if dir is put into watchKeyMap or if it already exists in watchKeyMap, false otherwise
     */
    private boolean watchPath(Path dir) {
        long now = System.currentTimeMillis();
        while (!Files.exists(dir) && System.currentTimeMillis() - now < 10000L) {   // TODO: make configurable
            // wait for a configurable amount of time for Kafka to create directory for new partitions
            LOG.debug(String.format("Waiting for %s to be created", dir));
        }

        if (!Files.exists(dir) || !Files.isDirectory(dir)) {
            LOG.warn(String.format("Skip watching %s as it doesn't exist on this broker.", dir));
            return false;
        }

        if (!shouldWatch(dir))
            return false;

        if (watchKeyMap.containsKey(dir)) {
            LOG.info(String.format("Path %s is already being watched.", dir));
            return true;
        }

        try {
            LOG.info(String.format("Adding %s to watchKeyMap", dir));
            synchronized (watchKeyMapLock) {
                addToWatchKeyMapWithRetries(dir, 0);
            }
            queueUpLogSegments(dir);
            return true;
        } catch (IOException e) {
            TopicPartition topicPartition = getTopicPartitionFromPath(dir);
            MetricRegistryManager.getInstance(config.getMetricsConfiguration()).updateCounterAndReport(
                    topicPartition.topic(),
                    topicPartition.partition(),
                    UploaderMetrics.ADD_WATCHER_FAILED_METRIC,
                    1,
                    "cluster=" + environmentProvider.clusterId(),
                    "broker=" + environmentProvider.brokerId()
            );
            LOG.error(String.format("Could not watch path %s", dir), e);
            System.exit(1); // exit to restart process
        }
        return false;
    }

    /**
     * Add the given directory to the watchKeyMap with retries.
     * @param dir
     * @param retries
     * @throws IOException
     */
    private void addToWatchKeyMapWithRetries(Path dir, int retries) throws IOException {
        LOG.info(String.format("Adding %s to watchKeyMap", dir));
        int timeoutSecs = 5;
        ExecutorService executor = Executors.newSingleThreadExecutor();
        Callable<WatchKey> registerWatchKeyTask = () -> dir.register(watchService,
                StandardWatchEventKinds.ENTRY_CREATE,
                StandardWatchEventKinds.ENTRY_MODIFY,
                StandardWatchEventKinds.ENTRY_DELETE);
        Future<WatchKey> future = executor.submit(registerWatchKeyTask);
        WatchKey watchKey;
        try {
            watchKey = future.get(timeoutSecs, TimeUnit.SECONDS);
            watchKeyMap.put(dir, watchKey);
            LOG.info(String.format("Registered watcher for %s", dir));
        } catch (ExecutionException | InterruptedException | TimeoutException e) {
            LOG.warn(String.format("Adding %s to watchKeyMap timed out after %s seconds on retry #%s", dir, timeoutSecs, retries));
            if (retries < 5) {
                Path newPath = Paths.get(dir.toString());
                addToWatchKeyMapWithRetries(newPath, retries + 1);
            } else {
                throw new IOException(String.format("Reached end of retries for adding %s to watchKeyMap", dir));
            }
        }
    }

    /**
     * Unwatch the given topic partition.
     * @param topicPartition
     */
    public void unwatch(TopicPartition topicPartition) {
        unwatchPath(topLevelPath.resolve(topicPartition.toString()));
        deletePartition(topicPartition);
        MetricRegistryManager.getInstance(config.getMetricsConfiguration()).incrementCounter(
                topicPartition.topic(),
                topicPartition.partition(),
                UploaderMetrics.WATCHER_REMOVED_METRIC,
                "cluster=" + environmentProvider.clusterId(),
                "broker=" + environmentProvider.brokerId()
        );
        MetricRegistryManager.getInstance(config.getMetricsConfiguration()).updateCounter(
                topicPartition.topic(),
                topicPartition.partition(),
                UploaderMetrics.WATCHER_COUNT_METRIC,
                0,
                "cluster=" + environmentProvider.clusterId(),
                "broker=" + environmentProvider.brokerId()
        );
        updateTopicPartitionWatcherCountMetric();
    }

    private void unwatchPath(Path dir) {
        if (!watchKeyMap.containsKey(dir)) {
            LOG.debug(String.format("Cannot unwatch %s as there is no watcher on it.", dir));
            return;
        }

        synchronized (watchKeyMapLock) {
            watchKeyMap.get(dir).cancel();
            watchKeyMap.remove(dir);
            LOG.warn(String.format("Path %s was just unwatched.", dir));
        }
    }

    private boolean isWatched(TopicPartition topicPartition) {
        return watchKeyMap.containsKey(topLevelPath.resolve(topicPartition.toString()));
    }

    @VisibleForTesting
    protected Map<Path, WatchKey> getWatchKeyMap() {
        return this.watchKeyMap;
    }

    @VisibleForTesting
    protected void setDeadLetterQueueHandler(DeadLetterQueueHandler deadLetterQueueHandler) {
        this.deadLetterQueueHandler = deadLetterQueueHandler;
    }

    public static class UploadTask {
        public static final int DEFAULT_BACKOFF_FACTOR = 150;   // 2 ^ max_tries * 150 ms is the max backoff time
        private final TopicPartition topicPartition;
        private final String offset;
        private final String fullFilename;
        private Path absolutePath;
        private long segmentLastModifiedTimestamp;   // this refers to the segment's last modified timestamp
        private long enqueueTimestamp;
        private final long sizeBytes;
        private int tries = 0;
        private long nextRetryNotBeforeTimestamp = -1;
        private String uploadDestinationPathString;

        public UploadTask(TopicPartition topicPartition, String offset, String fullFilename, Path absolutePath) {
            this.topicPartition = topicPartition;
            this.offset = offset;
            this.fullFilename = fullFilename;
            this.absolutePath = absolutePath;
            this.segmentLastModifiedTimestamp = absolutePath.toFile().lastModified();
            this.sizeBytes = absolutePath.toFile().length();
        }

        // for watermark files only
        public UploadTask(TopicPartition topicPartition, String offset, String fullFilename, Path absolutePath, long segmentTimestamp) {
            this.topicPartition = topicPartition;
            this.offset = offset;
            this.fullFilename = fullFilename;
            this.absolutePath = absolutePath;
            this.segmentLastModifiedTimestamp = segmentTimestamp;
            this.sizeBytes = absolutePath.toFile().length();
        }

        public void setEnqueueTimestamp(long timestamp) {
            this.enqueueTimestamp = timestamp;
        }

        public long getEnqueueTimestamp() {
            return enqueueTimestamp;
        }

        public TopicPartition getTopicPartition() {
            return topicPartition;
        }

        public String getOffset() {
            return offset;
        }

        public String getFullFilename() {
            return fullFilename;
        }

        public Path getAbsolutePath() {
            return absolutePath;
        }

        public String getSubPath() {
            return topicPartition.toString() + "/" + fullFilename;
        }

        public long getSegmentLastModifiedTimestamp() {
            return segmentLastModifiedTimestamp;
        }

        public long getSizeBytes() {
            return sizeBytes;
        }

        public long getNextRetryNotBeforeTimestamp() {
            return nextRetryNotBeforeTimestamp;
        }

        public String getUploadDestinationPathString() {
            return uploadDestinationPathString;
        }

        public void setUploadDestinationPathString(String uploadDestinationPathString) {
            this.uploadDestinationPathString = uploadDestinationPathString;
        }

        public boolean isReadyForUpload() {
            return tries == 0 || System.currentTimeMillis() > nextRetryNotBeforeTimestamp;
        }

        public UploadTask retry() {
            ++tries;
            nextRetryNotBeforeTimestamp = System.currentTimeMillis() + (long) Math.pow(2, tries) * DEFAULT_BACKOFF_FACTOR; // exponential backoff
            return this;
        }

        public UploadTask retryMarkedForDeletion() {
            absolutePath = absolutePath.getParent().resolve(fullFilename + ".deleted");
            segmentLastModifiedTimestamp = absolutePath.toFile().lastModified();   // timestamp of the .deleted file
            return retry();
        }

        public int getTries() {
            return tries;
        }
    }
}

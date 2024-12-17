package com.pinterest.kafka.tieredstorage.uploader.dlq;

import com.pinterest.kafka.tieredstorage.uploader.DirectoryTreeWatcher;
import com.pinterest.kafka.tieredstorage.uploader.SegmentUploaderConfiguration;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

/**
 * A {@link DeadLetterQueueHandler} that writes failed uploads to a local file with human-readable formatting.
 */
public class LocalFileDeadLetterQueueHandler extends DeadLetterQueueHandler {

    private static final Logger LOG = LogManager.getLogger(LocalFileDeadLetterQueueHandler.class);
    private static final String CONFIG_PREFIX = "localfile";
    public static final String PATH_CONFIG_KEY = DEAD_LETTER_QUEUE_CONFIG_PREFIX + "." + CONFIG_PREFIX + "." + "path";
    private final Object failedUploadFileLock = new Object();
    private final String filePath;

    public LocalFileDeadLetterQueueHandler(SegmentUploaderConfiguration config) {
        super(config);
        this.filePath = config.getProperty(PATH_CONFIG_KEY);
    }

    @Override
    protected void validateConfig() {
        if (config.getProperty(PATH_CONFIG_KEY) == null) {
            throw new IllegalArgumentException("Missing required property: " + PATH_CONFIG_KEY);
        }
    }

    @Override
    public Future<Boolean> send(DirectoryTreeWatcher.UploadTask uploadTask, Throwable throwable) {
        synchronized (failedUploadFileLock) {
            LOG.info(String.format("Writing failed upload %s --> %s to failure file: %s",
                    uploadTask.getAbsolutePath(), uploadTask.getUploadDestinationPathString(), filePath));
            try {
                long timestamp = System.currentTimeMillis();
                LocalDateTime dt = LocalDateTime.ofInstant(Instant.ofEpochMilli(timestamp), ZoneId.systemDefault());
                Files.write(
                        Paths.get(filePath),
                        Arrays.asList(
                                "timestamp: " + timestamp,
                                "human_timestamp: " + dt.format(DateTimeFormatter.ISO_DATE_TIME),
                                "task_num_retries: " + uploadTask.getTries(),
                                "local_path: " + uploadTask.getAbsolutePath(),
                                "destination_path: " + uploadTask.getUploadDestinationPathString(),
                                "exception: " + throwable.getClass().getName(),
                                "message: " + throwable.getMessage(),
                                "-------------------"
                        ),
                        StandardOpenOption.CREATE,
                        StandardOpenOption.APPEND
                );
                return CompletableFuture.completedFuture(true);
            } catch (IOException e) {
                LOG.error("Failed to write failed upload to failure file", e);
                return CompletableFuture.completedFuture(false);
            }
        }
    }

    @Override
    public Collection<DirectoryTreeWatcher.UploadTask> poll() {
        throw new UnsupportedOperationException("pollFromQueue is not supported for LocalFileDeadLetterQueueHandler yet");
    }
}

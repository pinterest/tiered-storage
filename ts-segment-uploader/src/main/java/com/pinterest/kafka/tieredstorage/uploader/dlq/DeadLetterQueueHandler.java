package com.pinterest.kafka.tieredstorage.uploader.dlq;

import com.pinterest.kafka.tieredstorage.uploader.DirectoryTreeWatcher;
import com.pinterest.kafka.tieredstorage.uploader.SegmentUploaderConfiguration;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.Collection;
import java.util.concurrent.Future;

import static com.pinterest.kafka.tieredstorage.uploader.SegmentUploaderConfiguration.TS_SEGMENT_UPLOADER_PREFIX;

public abstract class DeadLetterQueueHandler {

    private static final Logger LOG = LogManager.getLogger(DeadLetterQueueHandler.class);
    protected static final String DEAD_LETTER_QUEUE_CONFIG_PREFIX = TS_SEGMENT_UPLOADER_PREFIX + "." + "dlq";
    public static final String HANDLER_CLASS_CONFIG_KEY = DEAD_LETTER_QUEUE_CONFIG_PREFIX + "." + "handler.class";
    private static final String SEND_TIMEOUT_MS_CONFIG_KEY = DEAD_LETTER_QUEUE_CONFIG_PREFIX + "." + "send.timeout.ms";
    protected final SegmentUploaderConfiguration config;

    protected DeadLetterQueueHandler(SegmentUploaderConfiguration config) {
        this.config = config;
        validateConfig();
    }

    protected abstract void validateConfig();

    public abstract Future<Boolean> send(DirectoryTreeWatcher.UploadTask uploadTask, Throwable throwable);

    public abstract Collection<DirectoryTreeWatcher.UploadTask> poll();

    public static DeadLetterQueueHandler createHandler(SegmentUploaderConfiguration config) {
        String className = config.getProperty(HANDLER_CLASS_CONFIG_KEY);
        if (className == null) {
            LOG.info("No DeadLetterQueueHandler specified, will skip handling failed uploads after retry exhaustion.");
            return null;
        }
        LOG.info("Creating DeadLetterQueueHandler: " + className);
        try {
            Class<?> clazz = Class.forName(className);
            return (DeadLetterQueueHandler) clazz.getConstructor(SegmentUploaderConfiguration.class).newInstance(config);
        } catch (Exception e) {
            throw new RuntimeException("Failed to create DeadLetterQueueHandler", e);
        }
    }

    public long getSendTimeoutMs() {
        return Long.parseLong(config.getProperty(SEND_TIMEOUT_MS_CONFIG_KEY, String.valueOf(Defaults.DEFAULT_SEND_TIMEOUT_MS)));
    }

    private static class Defaults {
        static final long DEFAULT_SEND_TIMEOUT_MS = 10000L;
    }
}

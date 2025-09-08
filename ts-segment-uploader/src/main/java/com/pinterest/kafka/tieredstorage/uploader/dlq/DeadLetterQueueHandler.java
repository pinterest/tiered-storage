package com.pinterest.kafka.tieredstorage.uploader.dlq;

import com.pinterest.kafka.tieredstorage.uploader.DirectoryTreeWatcher;
import com.pinterest.kafka.tieredstorage.uploader.SegmentUploaderConfiguration;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.Collection;
import java.util.concurrent.Future;

import static com.pinterest.kafka.tieredstorage.uploader.SegmentUploaderConfiguration.TS_SEGMENT_UPLOADER_PREFIX;

/**
 * Abstract class for handling failed uploads after retry exhaustion by sending them to a dead letter queue.
 *
 * The handler is responsible for sending failed uploads to the dead letter queue and for polling the dead letter queue
 * for failed uploads to retry. The handler can be specified in the uploader configuration
 * (see {@link #HANDLER_CLASS_CONFIG_KEY}). The configuration expects the FQDN of the handler class.
 *
 * Note that the handler must have a public constructor so that it can be instantiated by reflection.
 */
public abstract class DeadLetterQueueHandler {

    private static final Logger LOG = LogManager.getLogger(DeadLetterQueueHandler.class);
    protected static final String DEAD_LETTER_QUEUE_CONFIG_PREFIX = TS_SEGMENT_UPLOADER_PREFIX + "." + "dlq";
    public static final String HANDLER_CLASS_CONFIG_KEY = DEAD_LETTER_QUEUE_CONFIG_PREFIX + "." + "handler.class";
    private static final String SEND_TIMEOUT_MS_CONFIG_KEY = DEAD_LETTER_QUEUE_CONFIG_PREFIX + "." + "send.timeout.ms";
    protected final SegmentUploaderConfiguration config;

    public DeadLetterQueueHandler(SegmentUploaderConfiguration config) {
        this.config = config;
        validateConfig();
    }

    /**
     * Validate the configuration.
     */
    protected abstract void validateConfig();

    /**
     * Send the failed upload to the dead letter queue. The handler should return a Future that completes successfully
     * if the upload was successfully sent to the dead letter queue, and completes exceptionally / fails if the upload
     * could not be sent to the dead letter queue.
     *
     * @param uploadTask the failed upload task
     * @param throwable the throwable that caused the upload to fail
     * @return a Future that completes successfully if the upload was successfully sent to the dead letter queue, and
     * completes exceptionally / fails if the upload could not be sent to the dead letter queue
     */
    public abstract Future<Boolean> send(DirectoryTreeWatcher.UploadTask uploadTask, Throwable throwable);

    /**
     * Poll the dead letter queue for failed uploads to retry.
     *
     * @return a collection of failed uploads to retry
     */
    public abstract Collection<DirectoryTreeWatcher.UploadTask> poll();

    /**
     * Create a DeadLetterQueueHandler from the configuration.
     *
     * @param config the configuration
     * @return the DeadLetterQueueHandler
     */
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

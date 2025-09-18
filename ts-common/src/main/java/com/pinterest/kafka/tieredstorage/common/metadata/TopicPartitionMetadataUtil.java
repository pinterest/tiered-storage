package com.pinterest.kafka.tieredstorage.common.metadata;

import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Utility for coarse-grained per-topic-partition locking of metadata updates.
 *
 * <p>Provides a static map of {@link ReentrantLock}s keyed by {@link TopicPartition} so
 * callers can coordinate read/modify/write updates to {@link TopicPartitionMetadata}
 * across multiple threads.</p>
 */
public class TopicPartitionMetadataUtil {

    private final static Logger LOG = LogManager.getLogger(TopicPartitionMetadataUtil.class.getName());

    private static final Map<TopicPartition, ReentrantLock> LOCKS = new ConcurrentHashMap<>();

    /**
     * Acquire the lock for the given topic-partition, blocking until available.
     *
     * @param topicPartition lock key
     */
    public static void acquireLock(TopicPartition topicPartition) {
        ReentrantLock lock = getReentrantLock(topicPartition);
        lock.lock();
        LOG.info("Acquired lock for TopicPartition metadata: " + topicPartition + " in thread " + Thread.currentThread().getName());
    }

    /**
     * Attempt to acquire the lock for the given topic-partition within a timeout.
     *
     * @param topicPartition lock key
     * @param timeoutMs timeout in milliseconds
     * @return true if acquired, false if timed out or interrupted
     */
    public static boolean tryAcquireLock(TopicPartition topicPartition, long timeoutMs) {
        try {
            if (getReentrantLock(topicPartition).tryLock(timeoutMs, TimeUnit.MILLISECONDS)) {
                LOG.info("Acquired lock for TopicPartition metadata: " + topicPartition + " in thread " + Thread.currentThread().getName());
                return true;
            }
        } catch (InterruptedException e) {
            LOG.warn("InterruptedException while trying to acquire TopicPartition metadata lock: " + topicPartition + " in thread " + Thread.currentThread().getName());
            return false;
        }
        LOG.info("Unable to acquire lock for TopicPartition metadata: " + topicPartition + " in thread " + Thread.currentThread().getName());
        return false;
    }

    /**
     * Release the lock for the given topic-partition.
     *
     * @param topicPartition lock key
     */
    public static void releaseLock(TopicPartition topicPartition) {
        getReentrantLock(topicPartition).unlock();
        LOG.info("Released TopicPartition metadata lock for " + topicPartition + " in thread " + Thread.currentThread().getName());
    }

    /**
     * Whether the lock is currently held for the given topic-partition.
     *
     * @param topicPartition lock key
     * @return true if locked, false otherwise
     */
    public static boolean isLocked(TopicPartition topicPartition) {
        return getReentrantLock(topicPartition).isLocked();
    }

    /**
     * Get or create the {@link ReentrantLock} for a topic-partition.
     *
     * @param topicPartition lock key
     * @return the lock instance
     */
    private static ReentrantLock getReentrantLock(TopicPartition topicPartition) {
        if (!LOCKS.containsKey(topicPartition)) {
            LOCKS.computeIfAbsent(topicPartition, k -> new ReentrantLock());
        }
        return LOCKS.get(topicPartition);
    }
}

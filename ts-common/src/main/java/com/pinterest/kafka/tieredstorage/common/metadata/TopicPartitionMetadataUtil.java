package com.pinterest.kafka.tieredstorage.common.metadata;

import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

public class TopicPartitionMetadataUtil {

    private final static Logger LOG = LogManager.getLogger(TopicPartitionMetadataUtil.class.getName());

    private static final Map<TopicPartition, ReentrantLock> LOCKS = new ConcurrentHashMap<>();

    public static void acquireLock(TopicPartition topicPartition) {
        ReentrantLock lock = getReentrantLock(topicPartition);
        lock.lock();
        LOG.info("Acquired lock for TopicPartition metadata: " + topicPartition + " in thread " + Thread.currentThread().getName());
    }

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

    public static void releaseLock(TopicPartition topicPartition) {
        getReentrantLock(topicPartition).unlock();
        LOG.info("Released TopicPartition metadata lock for " + topicPartition + " in thread " + Thread.currentThread().getName());
    }

    public static boolean isLocked(TopicPartition topicPartition) {
        return getReentrantLock(topicPartition).isLocked();
    }

    private static ReentrantLock getReentrantLock(TopicPartition topicPartition) {
        if (!LOCKS.containsKey(topicPartition)) {
            LOCKS.computeIfAbsent(topicPartition, k -> new ReentrantLock());
        }
        return LOCKS.get(topicPartition);
    }
}

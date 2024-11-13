package com.pinterest.kafka.tieredstorage.uploader.leadership;

import com.google.common.collect.Sets;
import com.pinterest.kafka.tieredstorage.common.metrics.MetricRegistryManager;
import com.pinterest.kafka.tieredstorage.uploader.DirectoryTreeWatcher;
import com.pinterest.kafka.tieredstorage.uploader.KafkaEnvironmentProvider;
import com.pinterest.kafka.tieredstorage.uploader.SegmentUploaderConfiguration;
import com.pinterest.kafka.tieredstorage.uploader.UploaderMetrics;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public abstract class LeadershipWatcher {
    private final static Logger LOG = LogManager.getLogger(LeadershipWatcher.class);
    protected final ScheduledExecutorService executorService;
    protected final DirectoryTreeWatcher directoryTreeWatcher;
    protected final SegmentUploaderConfiguration config;
    protected final KafkaEnvironmentProvider environmentProvider;
    protected final Set<TopicPartition> leadingPartitions = new HashSet<>();
    private long lastPollTime = -1;

    public LeadershipWatcher(DirectoryTreeWatcher directoryTreeWatcher, SegmentUploaderConfiguration config, KafkaEnvironmentProvider environmentProvider) throws IOException, InterruptedException {
        this.directoryTreeWatcher = directoryTreeWatcher;
        this.config = config;
        this.environmentProvider = environmentProvider;
        executorService = Executors.newSingleThreadScheduledExecutor();
        initialize();
    }

    protected abstract void initialize() throws IOException, InterruptedException;

    protected abstract Set<TopicPartition> queryCurrentLeadingPartitions() throws Exception;

    protected void applyCurrentState() throws Exception {
        long start = System.currentTimeMillis();
        LOG.info("Applying current leadership state. Last successful run was " + (System.currentTimeMillis() - lastPollTime) + "ms ago");
        Set<TopicPartition> currentLeadingPartitions = queryCurrentLeadingPartitions();
        LOG.info(String.format("Current leading partitions (%s): %s", currentLeadingPartitions.size(), currentLeadingPartitions));
        Set<TopicPartition> newPartitions = Sets.difference(currentLeadingPartitions, leadingPartitions).immutableCopy();
        LOG.info(String.format("Newly detected leading partitions (%s): %s", newPartitions.size(), newPartitions));
        Set<TopicPartition> removedPartitions = Sets.difference(leadingPartitions, currentLeadingPartitions).immutableCopy();
        LOG.info(String.format("No longer leading partitions (%s): %s", removedPartitions.size(), removedPartitions));
        for (TopicPartition topicPartition : newPartitions) {
            watchPartition(topicPartition);
        }
        for (TopicPartition topicPartition : removedPartitions) {
            unwatchPartition(topicPartition);
        }
        LOG.info("Finished applying current leadership state in " + (System.currentTimeMillis() - start) + "ms");
        lastPollTime = System.currentTimeMillis();
    }

    private void watchPartition(TopicPartition topicPartition) {
        directoryTreeWatcher.watch(topicPartition);
        leadingPartitions.add(topicPartition);
        MetricRegistryManager.getInstance(config.getMetricsConfiguration()).incrementCounter(
                topicPartition.topic(),
                topicPartition.partition(),
                UploaderMetrics.KAFKA_LEADER_SET_METRIC,
                "cluster=" + environmentProvider.clusterId(),
                "broker=" + environmentProvider.brokerId()
        );
        MetricRegistryManager.getInstance(config.getMetricsConfiguration()).updateCounter(
                topicPartition.topic(),
                topicPartition.partition(),
                UploaderMetrics.KAFKA_LEADER_COUNT_METRIC,
                1,
                "cluster=" + environmentProvider.clusterId(),
                "broker=" + environmentProvider.brokerId()
        );
    }

    private void unwatchPartition(TopicPartition topicPartition) {
        leadingPartitions.remove(topicPartition);
        directoryTreeWatcher.unwatch(topicPartition);
        MetricRegistryManager.getInstance(config.getMetricsConfiguration()).incrementCounter(
                topicPartition.topic(),
                topicPartition.partition(),
                UploaderMetrics.KAFKA_LEADER_UNSET_METRIC,
                "cluster=" + environmentProvider.clusterId(),
                "broker=" + environmentProvider.brokerId()
        );
        MetricRegistryManager.getInstance(config.getMetricsConfiguration()).updateCounter(
                topicPartition.topic(),
                topicPartition.partition(),
                UploaderMetrics.KAFKA_LEADER_COUNT_METRIC,
                0,
                "cluster=" + environmentProvider.clusterId(),
                "broker=" + environmentProvider.brokerId()
        );
    }

    public void start() throws Exception {
        applyCurrentState();
        executorService.scheduleAtFixedRate(() -> {
            try {
                applyCurrentState();
            } catch (Exception e) {
                LOG.error("Caught exception while applying current state", e);
                MetricRegistryManager.getInstance(config.getMetricsConfiguration()).incrementCounter(
                        null,
                        null,
                        UploaderMetrics.WATCHER_LEADERSHIP_EXCEPTION_METRIC,
                        "cluster=" + environmentProvider.clusterId(),
                        "broker=" + environmentProvider.brokerId()
                );
            }
        }, config.getLeadershipWatcherPollIntervalSeconds(), config.getLeadershipWatcherPollIntervalSeconds(), java.util.concurrent.TimeUnit.SECONDS);
        LOG.info("Started LeadershipWatcher with poll interval: " + config.getLeadershipWatcherPollIntervalSeconds());
    }

    public void stop() throws InterruptedException {
        executorService.shutdown();
        LOG.info("Stopped LeadershipWatcher");
    }

}

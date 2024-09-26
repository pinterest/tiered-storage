package com.pinterest.kafka.tieredstorage.uploader.leadership;

import com.google.common.collect.Sets;
import com.google.gson.Gson;
import com.pinterest.kafka.tieredstorage.common.metrics.MetricRegistryManager;
import com.pinterest.kafka.tieredstorage.uploader.DirectoryTreeWatcher;
import com.pinterest.kafka.tieredstorage.uploader.Heartbeat;
import com.pinterest.kafka.tieredstorage.uploader.KafkaEnvironmentProvider;
import com.pinterest.kafka.tieredstorage.uploader.SegmentUploaderConfiguration;
import com.pinterest.kafka.tieredstorage.uploader.TopicPartitionState;
import com.pinterest.kafka.tieredstorage.uploader.UploaderMetrics;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * KafkaLeadershipWatcher watches the leadership of Kafka partitions by pulling the state from ZooKeeper every X seconds.
 * The poll interval is configurable via the zk.watcher.poll.interval.seconds configuration.
 *
 * Note that we are not using ZkWatcher push-based implementation because it was not reliably pushing all updates whenever
 * a large amount of metadata updates were happening at the same time (e.g. a topic with thousands of partitions
 * is being rebalanced). Pull-based approach is more reliable until we can integrate a more reliable push-based library
 * that provides ZK updates.
 */
public class KafkaLeadershipWatcher {
    private final static Logger LOG = LogManager.getLogger(KafkaLeadershipWatcher.class);
    private ZooKeeper zooKeeper;
    private final static int SESSION_TIMEOUT_MS = 60000;
    private final static String TOPICS_ZK_PATH = "/brokers/topics";
    private final static String PARTITIONS_ZK_SUBPATH = "partitions";
    private final Set<TopicPartition> leadingPartitions = new HashSet<>();
    private final Stat stat = new Stat();
    private DirectoryTreeWatcher directoryTreeWatcher;
    private final Gson gson = new Gson();
    private Heartbeat heartbeat;
    private final ScheduledExecutorService executorService;
    private final SegmentUploaderConfiguration config;
    private final KafkaEnvironmentProvider environmentProvider;

    public KafkaLeadershipWatcher(DirectoryTreeWatcher directoryTreeWatcher, SegmentUploaderConfiguration config, KafkaEnvironmentProvider environmentProvider) throws IOException, InterruptedException {
        this.config = config;
        this.environmentProvider = environmentProvider;
        executorService = Executors.newSingleThreadScheduledExecutor();
        initialize(directoryTreeWatcher);
    }

    private void initialize(DirectoryTreeWatcher directoryTreeWatcher) throws IOException, InterruptedException {
        initialize(environmentProvider.zookeeperConnect(), directoryTreeWatcher);
    }

    private void initialize(String zkEndpoints, DirectoryTreeWatcher directoryTreeWatcher) throws IOException, InterruptedException {
        if (zkEndpoints == null)
            throw new IllegalArgumentException("Unexpect ZK endpoint: null");

        this.directoryTreeWatcher = directoryTreeWatcher;
        this.zooKeeper = new ZooKeeper(zkEndpoints, SESSION_TIMEOUT_MS, null);
        while (!zooKeeper.getState().isConnected()) {
            Thread.sleep(500);
        }
        heartbeat = new Heartbeat("watcher.zk", config, environmentProvider);
        LOG.info(String.format("ZK watcher state: %s", zooKeeper.getState()));
    }

    private void applyCurrentState() throws InterruptedException, KeeperException {
        LOG.info("Starting to apply current ZK state");
        long startTime = System.currentTimeMillis();
        Set<TopicPartition> currentLeadingPartitions = new HashSet<>();
        int numPartitionsProcessed = 0;
        for (String topic: zooKeeper.getChildren(TOPICS_ZK_PATH, true)) {
            String partitionsPath = String.format("%s/%s/%s", TOPICS_ZK_PATH, topic, PARTITIONS_ZK_SUBPATH);
            for (String partition: zooKeeper.getChildren(partitionsPath, true)) {
                String partitionStatePath = String.format("%s/%s/state", partitionsPath, partition);
                processNodeDataChangedForPartitionState(partitionStatePath, currentLeadingPartitions);
                numPartitionsProcessed++;
            }
        }
        unwatchDeletedPartitions(currentLeadingPartitions);
        LOG.info(String.format("Finished applying current ZK state in %dms. " +
                "Number of partitions processed=%d, number of leading partitions=%d",
                System.currentTimeMillis() - startTime, numPartitionsProcessed, currentLeadingPartitions.size()));
    }

    public void start() throws InterruptedException, KeeperException {
        applyCurrentState();
        executorService.scheduleAtFixedRate(() -> {
            try {
                applyCurrentState();
            } catch (Exception e) {
                LOG.error("Caught exception while applying current state", e);
                MetricRegistryManager.getInstance(config.getMetricsConfiguration()).incrementCounter(
                        null,
                        null,
                        UploaderMetrics.WATCHER_ZK_EXCEPTION_METRIC,
                        "cluster=" + environmentProvider.clusterId(),
                        "broker=" + environmentProvider.brokerId()
                );
            }
        }, config.getZkWatcherPollIntervalSeconds(), config.getZkWatcherPollIntervalSeconds(), java.util.concurrent.TimeUnit.SECONDS);
    }

    public void stop() throws InterruptedException {
        zooKeeper.close();
        heartbeat.stop();
    }

    private void processNodeDataChangedForPartitionState(String path, Set<TopicPartition> currentLeadingPartitions) {
        LOG.debug("Processing NodeDataChangedForPartitionState " + path);
        try {
            String data = null;
            try {
                data = new String(zooKeeper.getData(path, true, stat));
            } catch (KeeperException.NoNodeException e) {
                LOG.warn("Caught exception trying to get zk data from path. Don't panic if zNode was deleted," +
                        " we will unwatch it." + path, e);
            }
            TopicPartition topicPartition = fromPath(path);
            if (data != null) {
                TopicPartitionState topicPartitionState = gson.fromJson(data, TopicPartitionState.class);
                if (topicPartitionState.getLeader() == environmentProvider.brokerId()) {
                    LOG.info(String.format("Current leader of %s matches this broker ID: %s", path, topicPartitionState.getLeader()));
                    currentLeadingPartitions.add(topicPartition);
                    leadingPartitions.add(topicPartition);
                    directoryTreeWatcher.watch(topicPartition);
                    MetricRegistryManager.getInstance(config.getMetricsConfiguration()).updateCounter(
                            topicPartition.topic(),
                            topicPartition.partition(),
                            UploaderMetrics.KAFKA_LEADER_COUNT_METRIC,
                            1,
                            "cluster=" + environmentProvider.clusterId(),
                            "broker=" + environmentProvider.brokerId()
                    );
                } else if (leadingPartitions.contains(topicPartition)) {
                    // leadership change event
                    unwatchPartition(topicPartition);
                }
            } else if (leadingPartitions.contains(topicPartition)) {
                // node deletion event
                unwatchPartition(topicPartition);
            }
        } catch (InterruptedException | KeeperException e) {
            LOG.error(String.format("Hit a ZK exception while extracting from %s. Will bounce the ZK client.", path), e);
            try {
                MetricRegistryManager.getInstance(config.getMetricsConfiguration()).incrementCounter(
                        null,
                        null,
                        UploaderMetrics.WATCHER_ZK_RESET_METRIC,
                        "cluster=" + environmentProvider.clusterId(),
                        "broker=" + environmentProvider.brokerId()
                );
                stop();
                initialize(directoryTreeWatcher);
            } catch (IOException | InterruptedException ex) {
                LOG.error("Could not restore the ZK client.", ex);
                throw new RuntimeException(ex);
            }
        }
    }

    private void unwatchDeletedPartitions(Set<TopicPartition> currentLeadingPartitions) {
        Set<TopicPartition> deletedPartitions = Sets.difference(leadingPartitions, currentLeadingPartitions).immutableCopy();
        for (TopicPartition leadingPartition: deletedPartitions) {
            unwatchPartition(leadingPartition);
        }
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

    private TopicPartition fromPath(String path) {
        String[] parts = path.split("/");
        return new TopicPartition(parts[3], Integer.parseInt(parts[5]));
    }
}

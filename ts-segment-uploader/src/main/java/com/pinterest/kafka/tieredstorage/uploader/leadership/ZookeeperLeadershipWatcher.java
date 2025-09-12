package com.pinterest.kafka.tieredstorage.uploader.leadership;

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

/**
 * Watches the leadership of Kafka partitions by pulling the state from ZooKeeper every X seconds and returning the
 * current active set of partitions that are being led by this broker in the {@link #queryCurrentLeadingPartitions()}
 *
 * The poll interval is configurable via the leadership.watcher.poll.interval.seconds configuration, and relies on the
 * {@link java.util.concurrent.ExecutorService} in parent {@link LeadershipWatcher} to schedule the polling.
 */
public class ZookeeperLeadershipWatcher extends LeadershipWatcher {
    private final static Logger LOG = LogManager.getLogger(ZookeeperLeadershipWatcher.class);
    private ZooKeeper zooKeeper;
    private final static int SESSION_TIMEOUT_MS = 60000;
    private final static String TOPICS_ZK_PATH = "/brokers/topics";
    private final static String PARTITIONS_ZK_SUBPATH = "partitions";
    private final Stat stat = new Stat();
    private final Gson gson = new Gson();
    private Heartbeat heartbeat;
    private Set<TopicPartition> currentLeadingPartitions = new HashSet<>();

    public ZookeeperLeadershipWatcher(DirectoryTreeWatcher directoryTreeWatcher, SegmentUploaderConfiguration config, KafkaEnvironmentProvider environmentProvider) throws IOException, InterruptedException {
        super(directoryTreeWatcher, config, environmentProvider);
    }

    @Override
    protected void initialize() throws IOException, InterruptedException {
        initializeZookeeperWatcher(environmentProvider.zookeeperConnect());
    }

    private void initializeZookeeperWatcher(String zkEndpoints) throws IOException, InterruptedException {
        if (zkEndpoints == null)
            throw new IllegalArgumentException("Unexpect ZK endpoint: null");

        this.zooKeeper = new ZooKeeper(zkEndpoints, SESSION_TIMEOUT_MS, null);
        while (!zooKeeper.getState().isConnected()) {
            Thread.sleep(500);
        }
        heartbeat = new Heartbeat("watcher.zk", config, environmentProvider);
        LOG.info(String.format("ZK watcher state: %s", zooKeeper.getState()));
    }

    @Override
    protected Set<TopicPartition> queryCurrentLeadingPartitions() throws Exception {
        LOG.info("Starting to apply current ZK state");
        long startTime = System.currentTimeMillis();
        currentLeadingPartitions.clear();
        int numPartitionsProcessed = 0;
        try {
            for (String topic: zooKeeper.getChildren(TOPICS_ZK_PATH, true)) {
                String partitionsPath = String.format("%s/%s/%s", TOPICS_ZK_PATH, topic, PARTITIONS_ZK_SUBPATH);
                for (String partition : zooKeeper.getChildren(partitionsPath, true)) {
                    String path = String.format("%s/%s/state", partitionsPath, partition);
                    TopicPartition topicPartition = fromPath(path);
                    String data = null;
                    try {
                        data = new String(zooKeeper.getData(path, true, stat));
                    } catch (KeeperException.NoNodeException e) {
                        LOG.warn("Caught exception trying to get zk data from path. Don't panic if zNode was deleted," +
                                " we will unwatch it." + path, e);
                    }
                    if (data != null) {
                        TopicPartitionState topicPartitionState = gson.fromJson(data, TopicPartitionState.class);
                        if (topicPartitionState.getLeader() == environmentProvider.brokerId()) {
                            LOG.info(String.format("Current leader of %s matches this broker ID: %s", path, topicPartitionState.getLeader()));
                            currentLeadingPartitions.add(topicPartition);
                        }
                    }
                    numPartitionsProcessed++;
                }
            }
            LOG.info(String.format("Finished querying ZK for current leading partitions in %dms. " +
                            "Number of partitions processed=%d, number of leading partitions=%d",
                    System.currentTimeMillis() - startTime, numPartitionsProcessed, currentLeadingPartitions.size()));
            return currentLeadingPartitions;
        } catch (KeeperException | InterruptedException e) {
            LOG.info("Caught exception trying to get zk data from path. Will reset the ZK client.", e);
            tryResetZkClient(e);
            throw e;
        }
    }

    @Override
    public void stop() throws InterruptedException {
        super.stop();
        zooKeeper.close();
        heartbeat.stop();
        LOG.info("Stopped ZookeeperLeadershipWatcher");
    }

    private void tryResetZkClient(Exception e) {
        LOG.error("Hit a ZK exception. Will bounce the ZK client.", e);
        try {
            MetricRegistryManager.getInstance(config.getMetricsConfiguration()).incrementCounter(
                    null,
                    null,
                    UploaderMetrics.WATCHER_ZK_RESET_METRIC,
                    "cluster=" + environmentProvider.clusterId(),
                    "broker=" + environmentProvider.brokerId()
            );
            this.zooKeeper.close();
            this.heartbeat.stop();
            initialize();
        } catch (Exception ex) {
            LOG.error("Could not restore the ZK client.", ex);
            throw new RuntimeException(ex);
        }
    }

    private TopicPartition fromPath(String path) {
        String[] parts = path.split("/");
        return new TopicPartition(parts[3], Integer.parseInt(parts[5]));
    }
}

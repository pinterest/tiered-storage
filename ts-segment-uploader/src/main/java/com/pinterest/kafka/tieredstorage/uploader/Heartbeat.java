package com.pinterest.kafka.tieredstorage.uploader;

import com.pinterest.kafka.tieredstorage.common.metrics.MetricRegistryManager;

import java.util.Timer;
import java.util.TimerTask;

public class Heartbeat {
    private final Timer timer;
    private final String name;
    private final SegmentUploaderConfiguration config;
    private final KafkaEnvironmentProvider environmentProvider;

    public Heartbeat(String name, SegmentUploaderConfiguration config, KafkaEnvironmentProvider environmentProvider) {
        this.timer = new Timer();
        this.name = name;
        this.config = config;
        this.environmentProvider = environmentProvider;
        startHeartbeat();
    }

    public synchronized void stop() {
        timer.cancel();
    }

    private void startHeartbeat() {
        timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                sendHeartbeat();
            }
        }, 0, 30000);
    }

    private synchronized void sendHeartbeat() {
        MetricRegistryManager.getInstance(config.getMetricsConfiguration()).incrementCounter(
                null,
                null,
                UploaderMetrics.HEARTBEAT_METRIC + "." + name,
                "cluster=" + environmentProvider.clusterId(),
                "broker=" + environmentProvider.brokerId()
        );
    }
}

package com.pinterest.kafka.tieredstorage.common.metrics;

import org.junit.jupiter.api.Test;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestMetrics {

    @Test
    void testMetricsConcurrency() {
        MetricsConfiguration config = new MetricsConfiguration(OpenTSDBReporter.class.getName(), "localhost", 8080);
        assertEquals(0, MetricRegistryManager.getRefCount());
        MetricRegistryManager.getInstance(config).incrementCounter("test", 0, "test", "tag=value");
        assertEquals(1, MetricRegistryManager.getRefCount());
        MetricRegistryManager.getInstance(config).incrementCounter("test", 0, "test", "tag2=value");
        assertEquals(1, MetricRegistryManager.getRefCount());
        ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
        executorService.schedule(() -> {
            try {
                MetricRegistryManager instance = MetricRegistryManager.getInstance(config);
                assertEquals(2, MetricRegistryManager.getRefCount());   // new thread has incremented ref count
                instance.shutdown();
                assertEquals(1, MetricRegistryManager.getRefCount());
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }, 0, java.util.concurrent.TimeUnit.SECONDS);
        MetricRegistryManager.getInstance(config).incrementCounter("test", 0, "test", "tag3=value");
        assertEquals(1, MetricRegistryManager.getRefCount());
        MetricRegistryManager.getInstance(config).incrementCounter("test", 0, "test", "tag4=value");
        assertEquals(1, MetricRegistryManager.getRefCount());
        MetricRegistryManager.getInstance(config).incrementCounter("test", 0, "test", "tag5=value");
        assertEquals(1, MetricRegistryManager.getRefCount());
        MetricRegistryManager.getInstance(config).incrementCounter("test", 0, "test", "tag6=value");
        assertEquals(1, MetricRegistryManager.getRefCount());
        MetricRegistryManager.getInstance(config).incrementCounter("test", 0, "test", "tag7=value");
        assertEquals(1, MetricRegistryManager.getRefCount());
        MetricRegistryManager.getInstance(config).incrementCounter("test", 0, "test", "tag8=value");
        assertEquals(1, MetricRegistryManager.getRefCount());
    }
}

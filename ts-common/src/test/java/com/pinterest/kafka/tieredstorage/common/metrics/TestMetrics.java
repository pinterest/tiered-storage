package com.pinterest.kafka.tieredstorage.common.metrics;

import org.junit.jupiter.api.Test;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestMetrics {

    @Test
    void testMetricsConcurrencySingleton() throws InterruptedException {
        MetricsConfiguration config = new MetricsConfiguration(false, OpenTSDBReporter.class.getName(), "localhost", 8080);
        assertEquals(0, MetricRegistryManager.getRefCount());
        MetricRegistryManager.getInstance(config).incrementCounter("test", 0, "test", "tag=value");
        assertEquals(1, MetricRegistryManager.getRefCount());
        MetricRegistryManager.getInstance(config).incrementCounter("test", 0, "test", "tag2=value");
        assertEquals(1, MetricRegistryManager.getRefCount());
        ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
        AtomicBoolean isNewThreadShutdown = new AtomicBoolean(false);
        executorService.schedule(() -> {
            try {
                MetricRegistryManager.getInstance(config).incrementCounter("test", 0, "test", "tag111=value");
                assertEquals(1, MetricRegistryManager.getRefCount());   // new thread has incremented ref count
                MetricRegistryManager.getInstance(config).shutdown();
                assertEquals(1, MetricRegistryManager.getRefCount());
                MetricRegistryManager.getInstance(config).incrementCounter("test", 0, "test", "tag222=value");
                assertEquals(1, MetricRegistryManager.getRefCount());   // new thread has incremented ref count
                MetricRegistryManager.getInstance(config).incrementCounter("test", 0, "test", "tag333=value");
                assertEquals(1, MetricRegistryManager.getRefCount());   // new thread has incremented ref count
                MetricRegistryManager.getInstance(config).shutdown();
                assertEquals(1, MetricRegistryManager.getRefCount());
                isNewThreadShutdown.set(true);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }, 0, java.util.concurrent.TimeUnit.SECONDS);
        while (!isNewThreadShutdown.get()) {
            MetricRegistryManager.getInstance(config).incrementCounter("test", 0, "test", "tag3=value");
            MetricRegistryManager.getInstance(config).incrementCounter("test", 0, "test", "tag4=value");
            MetricRegistryManager.getInstance(config).incrementCounter("test", 0, "test", "tag5=value");
            MetricRegistryManager.getInstance(config).incrementCounter("test", 0, "test", "tag6=value");
            MetricRegistryManager.getInstance(config).incrementCounter("test", 0, "test", "tag7=value");
            MetricRegistryManager.getInstance(config).incrementCounter("test", 0, "test", "tag8=value");
        }
        assertEquals(1, MetricRegistryManager.getRefCount());
        MetricRegistryManager.getInstance(config).incrementCounter("test", 0, "test", "tag9=value");

        // call shutdown
        MetricRegistryManager.getInstance(config).shutdown();

        // ensure that the rest of the ops can still run
        MetricRegistryManager.getInstance(config).incrementCounter("test", 0, "test", "tag10=value");
        MetricRegistryManager.getInstance(config).incrementCounter("test", 0, "test", "tag11=value");
    }

    @Test
    void testMetricsConcurrencyThreadLocal() throws InterruptedException {
        MetricsConfiguration config = new MetricsConfiguration(true, OpenTSDBReporter.class.getName(), "localhost", 8080);
        assertEquals(0, MetricRegistryManager.getRefCount());
        MetricRegistryManager.getInstance(config).incrementCounter("test", 0, "test", "tag=value");
        assertEquals(1, MetricRegistryManager.getRefCount());
        MetricRegistryManager.getInstance(config).incrementCounter("test", 0, "test", "tag2=value");
        assertEquals(1, MetricRegistryManager.getRefCount());
        ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
        AtomicBoolean isNewThreadShutdown = new AtomicBoolean(false);
        executorService.schedule(() -> {
            try {
                MetricRegistryManager.getInstance(config).incrementCounter("test", 0, "test", "tag111=value");
                assertEquals(2, MetricRegistryManager.getRefCount());   // new thread has incremented ref count
                MetricRegistryManager.getInstance(config).shutdown();
                assertEquals(1, MetricRegistryManager.getRefCount());
                MetricRegistryManager.getInstance(config).incrementCounter("test", 0, "test", "tag222=value");
                assertEquals(2, MetricRegistryManager.getRefCount());   // new thread has incremented ref count
                MetricRegistryManager.getInstance(config).incrementCounter("test", 0, "test", "tag333=value");
                assertEquals(2, MetricRegistryManager.getRefCount());   // new thread has incremented ref count
                MetricRegistryManager.getInstance(config).shutdown();
                assertEquals(1, MetricRegistryManager.getRefCount());
                isNewThreadShutdown.set(true);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }, 0, java.util.concurrent.TimeUnit.SECONDS);
        while (!isNewThreadShutdown.get()) {
            MetricRegistryManager.getInstance(config).incrementCounter("test", 0, "test", "tag3=value");
            MetricRegistryManager.getInstance(config).incrementCounter("test", 0, "test", "tag4=value");
            MetricRegistryManager.getInstance(config).incrementCounter("test", 0, "test", "tag5=value");
            MetricRegistryManager.getInstance(config).incrementCounter("test", 0, "test", "tag6=value");
            MetricRegistryManager.getInstance(config).incrementCounter("test", 0, "test", "tag7=value");
            MetricRegistryManager.getInstance(config).incrementCounter("test", 0, "test", "tag8=value");
        }
        assertEquals(1, MetricRegistryManager.getRefCount());
        MetricRegistryManager.getInstance(config).incrementCounter("test", 0, "test", "tag9=value");
        MetricRegistryManager.getInstance(config).shutdown();
        assertEquals(0, MetricRegistryManager.getRefCount());
    }
}

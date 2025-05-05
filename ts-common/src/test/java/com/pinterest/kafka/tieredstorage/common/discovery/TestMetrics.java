package com.pinterest.kafka.tieredstorage.common.discovery;

import com.pinterest.kafka.tieredstorage.common.metrics.MetricRegistryManager;
import com.pinterest.kafka.tieredstorage.common.metrics.MetricsConfiguration;
import com.pinterest.kafka.tieredstorage.common.metrics.OpenTSDBReporter;
import org.junit.jupiter.api.Test;

public class TestMetrics {

    @Test
    void testMetricRegistryManagerShutdown() {
        MetricsConfiguration metricsConfiguration = new MetricsConfiguration(OpenTSDBReporter.class.getName(), "localhost", 8080);
        MetricRegistryManager.getInstance(metricsConfiguration).incrementCounter("test", 0, "test.counter", "tag=value");
        MetricRegistryManager.getInstance(metricsConfiguration).shutdown();
        MetricRegistryManager.getInstance(metricsConfiguration).incrementCounter("test", 0, "test.counter.2", "tag=value", "tag=value2");
    }
}
